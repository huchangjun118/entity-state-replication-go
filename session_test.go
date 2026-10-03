package esr

import "testing"

func TestJoinAndBasicSync(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)

	if !end.cli.Ready() {
		t.Fatal("client not ready after join")
	}
	if end.cli.ID() == 0 {
		t.Fatal("client ID not assigned")
	}
	auth := end.cli.Authoritative()
	if len(auth.Entities) != 1 {
		t.Fatalf("expected 1 entity in view, got %d", len(auth.Entities))
	}
}

func TestClientDoesNotRenderBeforeBaseline(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	// Before any message is delivered, nothing may be rendered.
	if end.cli.Ready() {
		t.Fatal("ready before any message")
	}
	if got := end.cli.RenderState(h.now); got != nil {
		t.Fatalf("rendered before baseline: %+v", got)
	}
}

func TestBaselineRetryAfterLoss(t *testing.T) {
	h := newHarness(defaultServerConfig())
	// 100% loss downstream: welcome and baseline never arrive.
	lossy := LinkConfig{Loss: 1, MinDelay: 10, MaxDelay: 10, Seed: 7}
	end := h.addClient(defaultClientConfig(), lossy)
	h.steps(10)
	if end.cli.Ready() {
		t.Fatal("ready despite total downstream loss")
	}
	// No dirty state may linger: no entities, no prediction.
	if end.cli.Authoritative() != nil {
		t.Fatal("dirty authoritative state after failed handshake")
	}
	// Heal the link; the client must retry and converge.
	end.s2c.cfg.Loss = 0
	end.c2s.cfg.Loss = 0
	h.steps(20)
	if !end.cli.Ready() {
		t.Fatal("client never became ready after link healed")
	}
	if len(end.cli.Authoritative().Entities) != 1 {
		t.Fatal("baseline missing controlled entity after retry")
	}
}

func TestDuplicateHelloIsIdempotent(t *testing.T) {
	h := newHarness(defaultServerConfig())
	// 100% duplication: every message (incl. Hello) arrives twice.
	dup := LinkConfig{Duplicate: 1, MinDelay: 10, MaxDelay: 10, Seed: 3}
	end := h.addClient(defaultClientConfig(), dup)
	h.steps(10)
	if !end.cli.Ready() {
		t.Fatal("client not ready")
	}
	if got := h.srv.ClientCount(); got != 1 {
		t.Fatalf("duplicate hello created %d sessions", got)
	}
	if got := len(h.srv.World().Entities()); got != 1 {
		t.Fatalf("duplicate hello spawned %d entities", got)
	}
}

func TestLateJoinSeesExistingWorld(t *testing.T) {
	h := newHarness(defaultServerConfig())
	first := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)
	// A second client joins mid-session.
	second := h.addClient(defaultClientConfig(), cleanLink(2))
	if second.cli.Ready() {
		t.Fatal("late joiner ready before baseline")
	}
	h.steps(10)
	if !second.cli.Ready() {
		t.Fatal("late joiner never became ready")
	}
	// Both clients must see both entities (spawn points are 5 units
	// apart, well inside the view radius).
	if got := len(second.cli.Authoritative().Entities); got != 2 {
		t.Fatalf("late joiner sees %d entities, want 2", got)
	}
	if got := len(first.cli.Authoritative().Entities); got != 2 {
		t.Fatalf("first client sees %d entities, want 2", got)
	}
}
