package esr

import "testing"

func TestAOIEnterLeaveView(t *testing.T) {
	cfg := defaultServerConfig()
	cfg.ViewRadius = 10
	h := newHarness(cfg)
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)

	// A far-away entity is invisible.
	far := h.srv.World().Spawn(0, Vec2{X: 500, Y: 500})
	// A nearby entity is visible.
	near := h.srv.World().Spawn(0, Vec2{X: 3, Y: 0})
	h.steps(5)
	auth := end.cli.Authoritative()
	if _, ok := auth.Entities[far]; ok {
		t.Fatal("far entity leaked into view")
	}
	if _, ok := auth.Entities[near]; !ok {
		t.Fatal("near entity missing from view")
	}

	// Teleport the far entity into view: it must appear.
	h.srv.World().Despawn(far)
	far = h.srv.World().Spawn(0, Vec2{X: 5, Y: 0})
	h.steps(5)
	if _, ok := end.cli.Authoritative().Entities[far]; !ok {
		t.Fatal("entity entering view not replicated")
	}

	// Move it out of view: it must be cleaned up.
	h.srv.World().Despawn(far)
	h.steps(5)
	if _, ok := end.cli.Authoritative().Entities[far]; ok {
		t.Fatal("entity leaving view not cleaned up")
	}
	// The nearby entity must be unaffected by the churn.
	if _, ok := end.cli.Authoritative().Entities[near]; !ok {
		t.Fatal("unrelated entity dropped during view churn")
	}
}

func TestAOIFollowsControlledEntity(t *testing.T) {
	cfg := defaultServerConfig()
	cfg.ViewRadius = 10
	h := newHarness(cfg)
	end := h.addClient(defaultClientConfig(), cleanLink(1))
	h.steps(5)

	// Static entity 15 units to the right of the client's spawn (5,0):
	// out of view initially.
	static := h.srv.World().Spawn(0, Vec2{X: 20, Y: 0})
	h.steps(3)
	if _, ok := end.cli.Authoritative().Entities[static]; ok {
		t.Fatal("static entity visible too early")
	}
	// Walk right; the view center follows and the entity appears.
	// (10 steps move the client from x=5 to x=15, within radius of 20.)
	for i := 0; i < 10; i++ {
		end.cli.SendInput(Vec2{X: 1})
		h.step()
	}
	if _, ok := end.cli.Authoritative().Entities[static]; !ok {
		t.Fatal("view did not follow controlled entity")
	}
}
