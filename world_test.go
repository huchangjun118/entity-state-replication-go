package esr

import "testing"

func testWorldConfig() WorldConfig {
	return WorldConfig{MoveSpeed: 1, MaxHP: 10, Bounds: 100}
}

func TestWorldAppliesInputsDeterministically(t *testing.T) {
	w := NewWorld(testWorldConfig())
	id := w.Spawn(1, Vec2{})
	// Submit out of order; Step must apply in (Client, Seq) order.
	if !w.SubmitInput(Input{Client: 1, Seq: 2, Move: Vec2{X: 0, Y: 1}}) {
		t.Fatal("seq 2 rejected")
	}
	if !w.SubmitInput(Input{Client: 1, Seq: 1, Move: Vec2{X: 1, Y: 0}}) {
		t.Fatal("seq 1 rejected")
	}
	w.Step()
	e, _ := w.Entity(id)
	if e.Pos != (Vec2{X: 1, Y: 1}) {
		t.Fatalf("got %+v, want {1 1}", e.Pos)
	}
}

func TestWorldRejectsDuplicateAndStaleInputs(t *testing.T) {
	w := NewWorld(testWorldConfig())
	id := w.Spawn(1, Vec2{})
	w.SubmitInput(Input{Client: 1, Seq: 1, Move: Vec2{X: 1}})
	w.Step()
	// Duplicate of an applied input.
	if w.SubmitInput(Input{Client: 1, Seq: 1, Move: Vec2{X: 1}}) {
		t.Fatal("duplicate input accepted")
	}
	// Stale (lower than last applied).
	if w.SubmitInput(Input{Client: 1, Seq: 0, Move: Vec2{X: 1}}) {
		t.Fatal("stale input accepted")
	}
	w.Step()
	e, _ := w.Entity(id)
	if e.Pos != (Vec2{X: 1}) {
		t.Fatalf("duplicate/stale input had an effect: %+v", e.Pos)
	}
	// Duplicate queued twice before a step applies once.
	if !w.SubmitInput(Input{Client: 1, Seq: 2, Move: Vec2{X: 1}}) {
		t.Fatal("seq 2 rejected")
	}
	if w.SubmitInput(Input{Client: 1, Seq: 2, Move: Vec2{X: 1}}) {
		t.Fatal("re-queued duplicate accepted")
	}
	w.Step()
	e, _ = w.Entity(id)
	if e.Pos != (Vec2{X: 2}) {
		t.Fatalf("got %+v, want {2 0}", e.Pos)
	}
}

func TestWorldClampsMoveAndBounds(t *testing.T) {
	w := NewWorld(testWorldConfig())
	id := w.Spawn(1, Vec2{})
	// Oversized move vector is clamped to unit length.
	w.SubmitInput(Input{Client: 1, Seq: 1, Move: Vec2{X: 100}})
	w.Step()
	e, _ := w.Entity(id)
	if e.Pos.X != 1 {
		t.Fatalf("move not clamped: %+v", e.Pos)
	}
	// Position is clamped to bounds.
	for seq := uint32(2); seq <= 200; seq++ {
		w.SubmitInput(Input{Client: 1, Seq: seq, Move: Vec2{X: 1}})
		w.Step()
	}
	e, _ = w.Entity(id)
	if e.Pos.X != 100 {
		t.Fatalf("position not clamped to bounds: %+v", e.Pos)
	}
}

func TestWorldSpawnDespawn(t *testing.T) {
	w := NewWorld(testWorldConfig())
	a := w.Spawn(1, Vec2{})
	b := w.Spawn(2, Vec2{X: 5})
	if len(w.Entities()) != 2 {
		t.Fatal("expected 2 entities")
	}
	w.Despawn(a)
	if _, ok := w.Entity(a); ok {
		t.Fatal("despawned entity still present")
	}
	if _, ok := w.Entity(b); !ok {
		t.Fatal("unrelated entity removed")
	}
}
