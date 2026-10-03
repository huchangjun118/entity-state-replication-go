package esr

import "testing"

func TestBandwidthBudgetPrioritizesControlledEntity(t *testing.T) {
	cfg := defaultServerConfig()
	cfg.BytesPerTick = 32*2 + 4 // room for ~2 entities per tick
	h := newHarness(cfg)
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)
	if !end.cli.Ready() {
		t.Fatal("not ready")
	}

	// Spawn a crowd of moving entities inside the view.
	movers := make([]EntityID, 0, 10)
	for i := 0; i < 10; i++ {
		id := h.srv.World().Spawn(0, Vec2{X: float32(8 + i), Y: 1})
		h.srv.World().Mutate(id, func(e *EntityState) { e.Vel = Vec2{X: 0.1} })
		movers = append(movers, id)
	}

	// The client keeps moving; its own entity must stay fresh despite
	// the budget being too small for the whole crowd.
	var minTick Tick
	for i := 0; i < 30; i++ {
		end.cli.SendInput(Vec2{X: 1})
		h.step()
		tick := end.cli.Authoritative().Tick
		if tick < minTick {
			t.Fatalf("visible state rolled back: tick %d < %d", tick, minTick)
		}
		minTick = tick
	}
	// Stop moving and let the last updates land; the controlled entity
	// must then match the server exactly (i.e. it was never starved).
	h.steps(5)
	auth := end.cli.Authoritative()
	serverOwn, _ := h.srv.World().Entity(1) // first client's entity
	clientOwn := auth.Entities[serverOwn.ID]
	if clientOwn.Pos != serverOwn.Pos {
		t.Fatalf("controlled entity starved by budget: client %+v server %+v", clientOwn.Pos, serverOwn.Pos)
	}

	// Raise the budget: everything converges, still without rollback.
	// Freeze the movers first so the comparison is not confused by the
	// client's inherent pipeline lag behind a moving authoritative state.
	h.srv.cfg.BytesPerTick = 0
	for _, id := range movers {
		h.srv.World().Mutate(id, func(e *EntityState) { e.Vel = Vec2{} })
	}
	convergeAndCompare(t, h, end)
}

func TestUnackedFallbackSendsFullSnapshot(t *testing.T) {
	cfg := defaultServerConfig()
	cfg.MaxUnacked = 3
	h := newHarness(cfg)
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)

	// Black out downstream long enough to exceed MaxUnacked; the server
	// must fall back to full snapshots. When the link heals, a single
	// full snapshot resyncs the client regardless of how many deltas
	// were lost.
	end.s2c.cfg.Loss = 1
	for i := 0; i < 10; i++ {
		end.cli.SendInput(Vec2{Y: 1})
		h.step()
	}
	end.s2c.cfg.Loss = 0
	convergeAndCompare(t, h, end)
}
