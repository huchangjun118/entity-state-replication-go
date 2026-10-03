package esr

// AOI is a circular area of interest.
type AOI struct {
	Center Vec2
	Radius float32
}

// Visible reports whether the entity is inside the area.
func (a AOI) Visible(s EntityState) bool {
	return a.Center.Dist(s.Pos) <= a.Radius
}

// Filter returns a snapshot containing only visible entities.
// The result shares no memory with the original.
func (s *Snapshot) Filter(a AOI) *Snapshot {
	out := &Snapshot{Tick: s.Tick, Entities: make(map[EntityID]EntityState)}
	for id, e := range s.Entities {
		if a.Visible(e) {
			out.Entities[id] = e.Clone()
		}
	}
	return out
}
