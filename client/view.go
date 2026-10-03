package client

import "github.com/huchangjun118/entity-state-replication-go/world"

// viewEntity is one entity as presented to the player.
// ViewEntity is one entity as presented to the player.
type ViewEntity struct {
	ID     world.EntityID
	Pos    world.Vec2
	Vel    world.Vec2
	Health int
	Alpha  float64 // 0..1 for enter/exit fade
}

// viewState maintains interpolated state for remote entities.
//
// Entities are presented from a delayed render tick so that snapshots can
// be interpolated rather than stepped. Entities entering the view fade in
// and entities leaving fade out before being removed, so a single
// entity's transition never affects other entities' interpolation.
type viewState struct {
	// per-entity absolute state reconstructed from applied snapshots
	current map[world.EntityID]world.EntityState
	// position history keyed by tick (ring) for interpolation
	history []map[world.EntityID]world.Vec2
	avail   map[uint64]bool // ticks present in history
	ringCap int

	enterTick map[world.EntityID]uint64
	leaveTick map[world.EntityID]uint64
}

func newViewState() *viewState {
	const ringCap = 128
	return &viewState{
		current:   make(map[world.EntityID]world.EntityState),
		history:   make([]map[world.EntityID]world.Vec2, ringCap),
		avail:     make(map[uint64]bool),
		ringCap:   ringCap,
		enterTick: make(map[world.EntityID]uint64),
		leaveTick: make(map[world.EntityID]uint64),
	}
}

func (v *viewState) histAt(tick uint64) map[world.EntityID]world.Vec2 {
	if tick == 0 {
		return nil
	}
	return v.history[tick%uint64(v.ringCap)]
}

// apply integrates a snapshot into the view. A full baseline replaces
// the entire view (clearing stale entities that left the view while no
// delta was received); a delta updates it in place.
func (v *viewState) apply(snap world.Snapshot) {
	if snap.Full {
		v.current = make(map[world.EntityID]world.EntityState)
		v.enterTick = make(map[world.EntityID]uint64)
		v.leaveTick = make(map[world.EntityID]uint64)
		v.avail = make(map[uint64]bool)
		v.history = make([]map[world.EntityID]world.Vec2, v.ringCap)
	}
	for _, e := range snap.Entities {
		st, ok := v.current[e.ID]
		if !ok {
			// Entering the view: start a fade-in.
			v.enterTick[e.ID] = snap.Tick
			st = world.EntityState{ID: e.ID}
		}
		delete(v.leaveTick, e.ID) // re-entering cancels a pending leave
		if e.Mask&world.FieldPosition != 0 {
			st.Pos = e.Pos
		}
		if e.Mask&world.FieldVelocity != 0 {
			st.Vel = e.Vel
		}
		if e.Mask&world.FieldHealth != 0 {
			st.Health = e.Health
		}
		v.current[e.ID] = st
	}
	for _, id := range snap.Leaves {
		if _, ok := v.current[id]; ok {
			v.leaveTick[id] = snap.Tick
		}
	}

	// Record absolute positions at this tick for interpolation.
	rec := make(map[world.EntityID]world.Vec2, len(v.current))
	for id, st := range v.current {
		rec[id] = st.Pos
	}
	v.history[snap.Tick%uint64(v.ringCap)] = rec
	v.avail[snap.Tick] = true
}

// findBrackets returns the history tick at-or-before renderTick and the
// next history tick after it (0 if none). When renderTick precedes the
// first snapshot, the earliest available tick is used so a static scene
// still renders.
func (v *viewState) findBrackets(renderTick uint64) (t1, t2 uint64) {
	for t := range v.avail {
		if t <= renderTick && t > t1 {
			t1 = t
		}
	}
	if t1 == 0 {
		for t := range v.avail {
			if t1 == 0 || t < t1 {
				t1 = t
			}
		}
	}
	for t := range v.avail {
		if t > t1 && (t2 == 0 || t < t2) {
			t2 = t
		}
	}
	return t1, t2
}

// render computes presented entities at renderTick+frac (frac in [0,1)).
// The client's own entity is excluded (it is predicted separately).
func (v *viewState) render(renderTick uint64, frac float64, fadeTicks uint64, ownID world.EntityID) []ViewEntity {
	t1, t2 := v.findBrackets(renderTick)
	if t1 == 0 {
		return nil
	}
	h1 := v.histAt(t1)
	h2 := v.histAt(t2)
	if h1 == nil {
		return nil
	}

	out := make([]ViewEntity, 0, len(h1))
	for id, p1 := range h1 {
		if id == ownID {
			continue
		}
		// Enter fade.
		if et, ok := v.enterTick[id]; ok {
			if t1 < et {
				continue // not yet visible
			}
			if fadeTicks > 0 {
				a := (float64(t1-et) + frac) / float64(fadeTicks)
				if a < 1 {
					out = append(out, ViewEntity{
						ID: id, Pos: p1,
						Vel: v.current[id].Vel, Health: v.current[id].Health,
						Alpha: a,
					})
					continue
				}
			}
		}
		// Leave fade.
		if lt, ok := v.leaveTick[id]; ok {
			a := 1.0
			if fadeTicks > 0 {
				a = 1 - (float64(t1-lt)+frac)/float64(fadeTicks)
			}
			if a <= 0 {
				// Fully faded: clean up without touching other entities.
				delete(v.current, id)
				delete(v.enterTick, id)
				delete(v.leaveTick, id)
				continue
			}
			pos := p1
			if p2, ok := h2[id]; ok {
				pos = world.Lerp(p1, p2, frac)
			}
			out = append(out, ViewEntity{
				ID: id, Pos: pos,
				Vel: v.current[id].Vel, Health: v.current[id].Health,
				Alpha: a,
			})
			continue
		}
		// Normal interpolation between the two bracketing ticks.
		pos := p1
		if p2, ok := h2[id]; ok {
			pos = world.Lerp(p1, p2, frac)
		}
		st := v.current[id]
		out = append(out, ViewEntity{ID: id, Pos: pos, Vel: st.Vel, Health: st.Health, Alpha: 1})
	}
	return out
}
