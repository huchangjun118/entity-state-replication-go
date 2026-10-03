package esr

import "sort"

// Snapshot is a full copy of (a view of) the world at a tick.
type Snapshot struct {
	Tick     Tick
	Entities map[EntityID]EntityState
}

// Delta carries the difference between BaseTick and Tick. When Full is
// true, Updated is a complete snapshot and BaseTick is ignored.
type Delta struct {
	Tick     Tick
	BaseTick Tick
	Full     bool
	Updated  map[EntityID]EntityState
	Removed  []EntityID
	// InputSeq is the last input sequence the server applied for the
	// recipient client as of Tick; used for prediction reconciliation.
	InputSeq uint32
}

// Clone deep-copies the snapshot.
func (s *Snapshot) Clone() *Snapshot {
	out := &Snapshot{Tick: s.Tick, Entities: make(map[EntityID]EntityState, len(s.Entities))}
	for id, e := range s.Entities {
		out.Entities[id] = e.Clone()
	}
	return out
}

// Diff computes the delta from base to cur. A nil base yields a full delta.
// Only entities that changed (or entered/left the view) are included.
func Diff(base, cur *Snapshot) *Delta {
	d := &Delta{
		Tick:    cur.Tick,
		Updated: make(map[EntityID]EntityState),
	}
	if base == nil {
		d.Full = true
		for id, e := range cur.Entities {
			d.Updated[id] = e.Clone()
		}
		return d
	}
	d.BaseTick = base.Tick
	for id, e := range cur.Entities {
		if old, ok := base.Entities[id]; !ok || !old.Equal(e) {
			d.Updated[id] = e.Clone()
		}
	}
	for id := range base.Entities {
		if _, ok := cur.Entities[id]; !ok {
			d.Removed = append(d.Removed, id)
		}
	}
	sort.Slice(d.Removed, func(i, j int) bool { return d.Removed[i] < d.Removed[j] })
	return d
}

// Empty reports whether the delta carries no changes.
func (d *Delta) Empty() bool {
	return len(d.Updated) == 0 && len(d.Removed) == 0
}

// Apply applies d to s and returns the resulting snapshot. It fails if
// d.BaseTick does not match s.Tick (for non-full deltas), which also
// makes applying the same delta twice a safe no-op.
func (s *Snapshot) Apply(d *Delta) (*Snapshot, bool) {
	if d.Full {
		out := &Snapshot{Tick: d.Tick, Entities: make(map[EntityID]EntityState, len(d.Updated))}
		for id, e := range d.Updated {
			out.Entities[id] = e.Clone()
		}
		return out, true
	}
	if s == nil || s.Tick != d.BaseTick {
		return nil, false
	}
	out := s.Clone()
	out.Tick = d.Tick
	for _, id := range d.Removed {
		delete(out.Entities, id)
	}
	for id, e := range d.Updated {
		out.Entities[id] = e.Clone()
	}
	return out, true
}
