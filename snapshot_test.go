package esr

import (
	"math/rand"
	"testing"
)

func snap(tick Tick, ents ...EntityState) *Snapshot {
	s := &Snapshot{Tick: tick, Entities: make(map[EntityID]EntityState)}
	for _, e := range ents {
		s.Entities[e.ID] = e
	}
	return s
}

func TestDiffApplyRoundTrip(t *testing.T) {
	base := snap(1,
		EntityState{ID: 1, Pos: Vec2{X: 1}},
		EntityState{ID: 2, Pos: Vec2{X: 2}},
	)
	cur := snap(2,
		EntityState{ID: 2, Pos: Vec2{X: 3}}, // changed
		EntityState{ID: 3, Pos: Vec2{X: 4}}, // added
	)
	d := Diff(base, cur)
	if d.Full {
		t.Fatal("expected incremental delta")
	}
	if len(d.Updated) != 2 || len(d.Removed) != 1 || d.Removed[0] != 1 {
		t.Fatalf("bad delta: %+v", d)
	}
	got, ok := base.Apply(d)
	if !ok {
		t.Fatal("apply failed")
	}
	if got.Tick != 2 || !got.Entities[2].Equal(cur.Entities[2]) || !got.Entities[3].Equal(cur.Entities[3]) {
		t.Fatalf("rebuilt snapshot mismatch: %+v", got)
	}
	if _, ok := got.Entities[1]; ok {
		t.Fatal("removed entity still present")
	}
}

func TestDeltaChurnConvergesToFull(t *testing.T) {
	// Frequent add/remove: incremental rebuild must equal the full
	// snapshot at every step.
	rng := rand.New(rand.NewSource(42))
	cur := snap(0)
	view, ok := (&Snapshot{}).Apply(Diff(nil, cur))
	if !ok {
		t.Fatal("initial full apply failed")
	}
	for tick := Tick(1); tick <= 200; tick++ {
		next := snap(tick)
		for id, e := range cur.Entities {
			if rng.Intn(4) != 0 { // 25% despawn
				next.Entities[id] = e
			}
		}
		for i := 0; i < rng.Intn(3); i++ { // spawn 0-2
			id := EntityID(rng.Intn(1000) + 1000*int(tick))
			next.Entities[id] = EntityState{ID: id, Pos: Vec2{X: rng.Float32() * 10}}
		}
		for id, e := range next.Entities { // jitter positions
			e.Pos.X += rng.Float32()
			next.Entities[id] = e
		}
		d := Diff(cur, next)
		view, ok = view.Apply(d)
		if !ok {
			t.Fatalf("tick %d: apply failed", tick)
		}
		if view.Tick != next.Tick || len(view.Entities) != len(next.Entities) {
			t.Fatalf("tick %d: size mismatch view=%d full=%d", tick, len(view.Entities), len(next.Entities))
		}
		for id, e := range next.Entities {
			if !view.Entities[id].Equal(e) {
				t.Fatalf("tick %d: entity %d mismatch", tick, id)
			}
		}
		cur = next
	}
}

func TestApplyRejectsBaseMismatch(t *testing.T) {
	base := snap(1, EntityState{ID: 1})
	other := snap(5, EntityState{ID: 1, Pos: Vec2{X: 9}})
	d := Diff(other, snap(6, EntityState{ID: 1, Pos: Vec2{X: 10}}))
	if _, ok := base.Apply(d); ok {
		t.Fatal("apply succeeded despite base mismatch")
	}
}

func TestApplyFullIgnoresBase(t *testing.T) {
	full := Diff(nil, snap(7, EntityState{ID: 3, Pos: Vec2{X: 1}}))
	got, ok := snap(2, EntityState{ID: 1}).Apply(full)
	if !ok {
		t.Fatal("full delta rejected")
	}
	if got.Tick != 7 || len(got.Entities) != 1 {
		t.Fatalf("bad full apply: %+v", got)
	}
}

func TestApplyRejectsDuplicateDelta(t *testing.T) {
	// A duplicated delta must not apply twice: after the first apply the
	// base no longer matches, so the copy is rejected and state is
	// unchanged.
	base := snap(1, EntityState{ID: 1, Pos: Vec2{X: 1}})
	cur := snap(2, EntityState{ID: 1, Pos: Vec2{X: 2}})
	d := Diff(base, cur)
	once, ok := base.Apply(d)
	if !ok {
		t.Fatal("first apply failed")
	}
	if _, ok := once.Apply(d); ok {
		t.Fatal("duplicate delta applied twice")
	}
	if once.Tick != 2 || once.Entities[1].Pos.X != 2 {
		t.Fatalf("state changed by duplicate: %+v", once)
	}
}
