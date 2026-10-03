package esr

import (
	"math"
	"testing"
)

func ownRendered(end *clientEnd, now int64) Vec2 {
	for _, e := range end.cli.RenderState(now) {
		if e.Owner == end.cli.ID() {
			return e.Pos
		}
	}
	return Vec2{}
}

func TestPredictionAppliesInputImmediately(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)
	before := ownRendered(end, h.now)
	end.cli.SendInput(Vec2{X: 1})
	after := ownRendered(end, h.now)
	if after.X-before.X != 1 {
		t.Fatalf("predicted move not applied immediately: %v -> %v", before, after)
	}
}

func TestReconciliationConvergesWithoutSnap(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)

	// Move for a while; prediction and authority should agree closely
	// once the pipeline drains.
	for i := 0; i < 10; i++ {
		end.cli.SendInput(Vec2{X: 1})
		h.step()
	}
	h.steps(5)
	rendered := ownRendered(end, h.now)
	server, _ := h.srv.World().Entity(1)
	if d := rendered.Dist(server.Pos); d > 0.5 {
		t.Fatalf("prediction diverged from authority: rendered %v server %v (dist %v)", rendered, server.Pos, d)
	}
}

func TestServerCorrectionIsSmoothed(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)

	// Walk right so prediction is active and stable.
	for i := 0; i < 5; i++ {
		end.cli.SendInput(Vec2{X: 1})
		h.step()
	}
	h.steps(3)

	// Server-side teleport (e.g. anti-cheat correction): the client must
	// not show a visible jump; the error is absorbed over several frames.
	h.srv.World().Mutate(1, func(e *EntityState) { e.Pos = e.Pos.Add(Vec2{X: 20}) })
	prev := ownRendered(end, h.now)
	maxJump := float32(0)
	for i := 0; i < 40; i++ {
		h.step()
		cur := ownRendered(end, h.now)
		jump := cur.Dist(prev)
		if jump > maxJump {
			maxJump = jump
		}
		prev = cur
	}
	if maxJump > 5 {
		t.Fatalf("visible snap during correction: max frame jump %v", maxJump)
	}
	// Eventually the rendered position converges to the authority.
	server, _ := h.srv.World().Entity(1)
	if d := prev.Dist(server.Pos); d > 1 {
		t.Fatalf("correction never converged: rendered %v server %v", prev, server.Pos)
	}
}

func TestCorrectionDoesNotDisturbRemoteInterpolation(t *testing.T) {
	h := newHarness(defaultServerConfig())
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)

	// A remote entity gliding through the view at constant velocity.
	remote := h.srv.World().Spawn(0, Vec2{X: 8, Y: 2})
	h.srv.World().Mutate(remote, func(e *EntityState) { e.Vel = Vec2{X: 0.5} })
	h.steps(5)

	remotePos := func() Vec2 {
		for _, e := range end.cli.RenderState(h.now) {
			if e.ID == remote {
				return e.Pos
			}
		}
		return Vec2{}
	}
	// Trigger a big correction on the controlled entity mid-stream.
	h.srv.World().Mutate(1, func(e *EntityState) { e.Pos = e.Pos.Add(Vec2{X: 30}) })

	prev := remotePos()
	for i := 0; i < 20; i++ {
		h.step()
		cur := remotePos()
		// Remote entity moves 0.5/tick; interpolation must keep its
		// per-frame motion smooth and monotonic despite the correction.
		if math.Abs(float64(cur.X-prev.X)) > 2 {
			t.Fatalf("remote interpolation disturbed: jump %v -> %v", prev, cur)
		}
		prev = cur
	}
}
