package world

import (
	"testing"
)

func TestMovementIsDeterministic(t *testing.T) {
	w := New(DefaultConfig())
	id := w.Spawn(Vec2{}, 100)
	w.ApplyInput(id, Input{Seq: 1, Move: Vec2{X: 1}})
	w.Step()
	st, _ := w.State(id)
	want := 5.0 / 20.0 // speed * dt
	if st.Pos.X != want {
		t.Fatalf("pos.x = %v, want %v", st.Pos.X, want)
	}
	// A second step with no input keeps velocity (no friction in this model).
	w.Step()
	st, _ = w.State(id)
	if st.Pos.X != want*2 {
		t.Fatalf("pos.x = %v, want %v", st.Pos.X, want*2)
	}
}

func TestInputLengthIsClamped(t *testing.T) {
	w := New(DefaultConfig())
	id := w.Spawn(Vec2{}, 100)
	w.ApplyInput(id, Input{Seq: 1, Move: Vec2{X: 2, Y: 0}}) // length 2 > 1
	w.Step()
	st, _ := w.State(id)
	// Normalized to unit vector, so speed is unchanged.
	if st.Pos.X != 5.0/20.0 {
		t.Fatalf("pos.x = %v, want %v", st.Pos.X, 5.0/20.0)
	}
}

func TestSpawnDespawnAndState(t *testing.T) {
	w := New(DefaultConfig())
	id := w.Spawn(Vec2{X: 3}, 50)
	if !w.Exists(id) {
		t.Fatal("entity should exist")
	}
	st, ok := w.State(id)
	if !ok || st.Health != 50 || st.Pos.X != 3 {
		t.Fatalf("bad initial state: %+v", st)
	}
	w.Despawn(id)
	if w.Exists(id) {
		t.Fatal("entity should be gone")
	}
	if _, ok := w.State(id); ok {
		t.Fatal("state of despawned entity should not exist")
	}
}

func TestHistoryStateAt(t *testing.T) {
	w := New(DefaultConfig())
	id := w.Spawn(Vec2{}, 100)
	w.Step() // tick 1
	w.ApplyInput(id, Input{Seq: 1, Move: Vec2{X: 1}})
	w.Step() // tick 2

	s1, ok := w.StateAt(1)
	if !ok {
		t.Fatal("state at tick 1 should be retained")
	}
	if s1[id].Pos.X != 0 {
		t.Fatalf("state at tick 1 pos = %v, want 0", s1[id].Pos.X)
	}
	s2, ok := w.StateAt(2)
	if !ok || s2[id].Pos.X != 5.0/20.0 {
		t.Fatalf("state at tick 2 pos = %v", s2[id].Pos.X)
	}
	if _, ok := w.StateAt(0); ok {
		t.Fatal("state at tick 0 should not exist")
	}
}

func TestDiffDeltaMatchesFull(t *testing.T) {
	w := New(DefaultConfig())
	a := w.Spawn(Vec2{X: 0}, 100)
	b := w.Spawn(Vec2{X: 10}, 100)
	w.Step() // tick 1

	base, _ := w.StateAt(1)
	// Change a, despawn b, spawn c.
	w.ApplyInput(a, Input{Seq: 1, Move: Vec2{X: 1}})
	w.Despawn(b)
	c := w.Spawn(Vec2{X: 20}, 100)
	w.Step() // tick 2

	current := w.Visible(Vec2{}, 1000)
	entities, leaves := Diff(base, current)

	// a changed (position), b left, c entered.
	wantLeaves := map[EntityID]bool{b: true}
	gotLeaves := map[EntityID]bool{}
	for _, id := range leaves {
		gotLeaves[id] = true
	}
	if len(leaves) != 1 || !wantLeaves[b] {
		t.Fatalf("leaves = %v, want %v", leaves, wantLeaves)
	}
	got := map[EntityID]EntitySnapshot{}
	for _, e := range entities {
		got[e.ID] = e
	}
	if len(got) != 2 {
		t.Fatalf("entities = %d, want 2 (a update, c enter)", len(got))
	}
	if got[a].Mask&FieldPosition == 0 {
		t.Fatal("a should carry position delta")
	}
	if got[c].Mask != AllFields {
		t.Fatalf("c should enter with full fields, got mask %v", got[c].Mask)
	}
	// Applying the delta to the base must reproduce the current state.
	rebuilt := map[EntityID]EntityState{}
	for id, st := range base {
		rebuilt[id] = st
	}
	for _, e := range entities {
		st := rebuilt[e.ID]
		st.ID = e.ID
		if e.Mask&FieldPosition != 0 {
			st.Pos = e.Pos
		}
		if e.Mask&FieldVelocity != 0 {
			st.Vel = e.Vel
		}
		if e.Mask&FieldHealth != 0 {
			st.Health = e.Health
		}
		rebuilt[e.ID] = st
	}
	for _, id := range leaves {
		delete(rebuilt, id)
	}
	for _, cur := range current {
		got := rebuilt[cur.ID]
		if got != cur {
			t.Fatalf("rebuilt state for %d = %+v, want %+v", cur.ID, got, cur)
		}
	}
}

func TestVisibleFiltersByRadius(t *testing.T) {
	w := New(DefaultConfig())
	w.Spawn(Vec2{X: 5}, 100)
	w.Spawn(Vec2{X: 50}, 100)
	vis := w.Visible(Vec2{}, 10)
	if len(vis) != 1 {
		t.Fatalf("visible = %d, want 1", len(vis))
	}
}
