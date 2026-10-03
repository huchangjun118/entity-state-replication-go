package esr

import "testing"

// authoritativeView returns what the server's authoritative view for a
// client's AOI currently is.
func authoritativeView(h *harness, end *clientEnd) *Snapshot {
	cfg := defaultServerConfig()
	var center Vec2
	for _, e := range h.srv.World().Entities() {
		if e.Owner == end.cli.ID() {
			center = e.Pos
		}
	}
	return h.srv.World().Snapshot().Filter(AOI{Center: center, Radius: cfg.ViewRadius})
}

func convergeAndCompare(t *testing.T, h *harness, end *clientEnd) {
	t.Helper()
	// Heal the link and let the client catch up.
	end.c2s.cfg.Loss, end.c2s.cfg.Duplicate = 0, 0
	end.s2c.cfg.Loss, end.s2c.cfg.Duplicate = 0, 0
	end.c2s.cfg.MaxDelay = end.c2s.cfg.MinDelay
	end.s2c.cfg.MaxDelay = end.s2c.cfg.MinDelay
	h.steps(30)
	want := authoritativeView(h, end)
	got := end.cli.Authoritative()
	if got == nil {
		t.Fatal("client has no state")
	}
	if len(got.Entities) != len(want.Entities) {
		t.Fatalf("entity count mismatch: got %d want %d", len(got.Entities), len(want.Entities))
	}
	for id, we := range want.Entities {
		ge, ok := got.Entities[id]
		if !ok {
			t.Fatalf("entity %d missing on client", id)
		}
		if !ge.Equal(we) {
			t.Fatalf("entity %d mismatch: got %+v want %+v", id, ge, we)
		}
	}
}

func TestConvergesUnderLossReorderDuplicate(t *testing.T) {
	h := newHarness(defaultServerConfig())
	bad := LinkConfig{Loss: 0.3, Duplicate: 0.2, MinDelay: 10, MaxDelay: 200, Seed: 11}
	end := h.addClient(defaultClientConfig(), bad)
	h.steps(10)
	if !end.cli.Ready() {
		t.Fatal("client never became ready")
	}
	// Drive movement while the link misbehaves.
	for i := 0; i < 40; i++ {
		end.cli.SendInput(Vec2{X: 1, Y: 0.5})
		h.step()
	}
	convergeAndCompare(t, h, end)
}

func TestNoPermanentMisalignmentAfterBurstLoss(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)
	// Total downstream blackout for a while.
	end.s2c.cfg.Loss = 1
	for i := 0; i < 20; i++ {
		end.cli.SendInput(Vec2{X: 1})
		h.step()
	}
	end.s2c.cfg.Loss = 0
	convergeAndCompare(t, h, end)
}

func TestStaleSnapshotsDiscarded(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)
	before := end.cli.Authoritative().Tick
	// Inject an ancient full snapshot directly: must be discarded.
	stale := &Delta{Tick: before - 1, Full: true, Updated: map[EntityID]EntityState{
		999: {ID: 999, Pos: Vec2{X: -50}},
	}}
	end.cli.Handle(Envelope{Kind: MsgBaseline, Delta: stale}, h.now)
	if _, ok := end.cli.Authoritative().Entities[999]; ok {
		t.Fatal("stale snapshot applied")
	}
	if end.cli.Authoritative().Tick != before {
		t.Fatal("stale snapshot moved the clock backwards")
	}
}

func TestGoodbyeRemovesEntityForOthers(t *testing.T) {
	h := newHarness(defaultServerConfig())
	first := h.addClient(defaultClientConfig(), cleanLink(1))
	second := h.addClient(defaultClientConfig(), cleanLink(2))
	h.steps(10)
	if len(first.cli.Authoritative().Entities) != 2 {
		t.Fatal("setup: first client should see 2 entities")
	}
	// Second client leaves.
	h.srv.Handle(Envelope{From: second.cli.ID(), Kind: MsgGoodbye}, h.now)
	h.steps(10)
	if len(first.cli.Authoritative().Entities) != 1 {
		t.Fatalf("leaver's entity not removed: %d entities", len(first.cli.Authoritative().Entities))
	}
}
