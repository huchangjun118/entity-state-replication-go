package esr

import "sort"

// WorldConfig tunes the authoritative simulation.
type WorldConfig struct {
	MoveSpeed float32 // units per tick applied to a unit Move vector
	MaxHP     float32
	Bounds    float32 // world is clamped to [-Bounds, Bounds] on both axes
}

// World holds the authoritative shared state. Only the server owns one.
type World struct {
	cfg      WorldConfig
	tick     Tick
	entities map[EntityID]*EntityState
	nextID   EntityID

	// lastSeq[c] is the highest input sequence applied for client c.
	lastSeq map[ClientID]uint32
	seenSeq map[ClientID]map[uint32]bool
	pending []Input
}

func NewWorld(cfg WorldConfig) *World {
	return &World{
		cfg:      cfg,
		entities: make(map[EntityID]*EntityState),
		nextID:   1,
		lastSeq:  make(map[ClientID]uint32),
		seenSeq:  make(map[ClientID]map[uint32]bool),
	}
}

// Tick returns the current authoritative tick.
func (w *World) Tick() Tick { return w.tick }

// Spawn adds an entity owned by the given client and returns its ID.
func (w *World) Spawn(owner ClientID, pos Vec2) EntityID {
	id := w.nextID
	w.nextID++
	w.entities[id] = &EntityState{
		ID:    id,
		Owner: owner,
		Pos:   pos,
		HP:    w.cfg.MaxHP,
		Props: make(map[string]float32),
	}
	return id
}

// Despawn removes an entity.
func (w *World) Despawn(id EntityID) { delete(w.entities, id) }

// Mutate applies fn to the authoritative state of one entity.
// Server-side only; clients never call this.
func (w *World) Mutate(id EntityID, fn func(*EntityState)) bool {
	e, ok := w.entities[id]
	if !ok {
		return false
	}
	fn(e)
	return true
}

// Entity returns a copy of one entity's state.
func (w *World) Entity(id EntityID) (EntityState, bool) {
	e, ok := w.entities[id]
	if !ok {
		return EntityState{}, false
	}
	return e.Clone(), true
}

// Entities returns copies of all entity states.
func (w *World) Entities() []EntityState {
	out := make([]EntityState, 0, len(w.entities))
	for _, e := range w.entities {
		out = append(out, e.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SubmitInput queues a client input. Duplicate or stale inputs are
// rejected and produce no effect. Inputs from clients that have never
// sent anything must start at Seq == 1.
func (w *World) SubmitInput(in Input) bool {
	seen := w.seenSeq[in.Client]
	if seen == nil {
		seen = make(map[uint32]bool)
		w.seenSeq[in.Client] = seen
	}
	if seen[in.Seq] || in.Seq <= w.lastSeq[in.Client] {
		return false // duplicate or stale
	}
	seen[in.Seq] = true
	w.pending = append(w.pending, in)
	return true
}

// Step applies queued inputs in deterministic order, integrates the
// simulation and advances the tick.
func (w *World) Step() {
	// Deterministic order: sort by (Client, Seq).
	sort.Slice(w.pending, func(i, j int) bool {
		a, b := w.pending[i], w.pending[j]
		if a.Client != b.Client {
			return a.Client < b.Client
		}
		return a.Seq < b.Seq
	})
	for _, in := range w.pending {
		if in.Seq <= w.lastSeq[in.Client] {
			continue // already applied (defensive; SubmitInput filters)
		}
		w.applyInput(in)
		w.lastSeq[in.Client] = in.Seq
	}
	w.pending = w.pending[:0]

	// Integrate velocities and clamp to bounds.
	for _, e := range w.entities {
		e.Pos = e.Pos.Add(e.Vel)
		e.Pos.X = clamp(e.Pos.X, -w.cfg.Bounds, w.cfg.Bounds)
		e.Pos.Y = clamp(e.Pos.Y, -w.cfg.Bounds, w.cfg.Bounds)
	}
	w.tick++
}

// applyInput mutates the entity controlled by the input's client.
// Action bit 0 is a "hit" that damages every other entity in range 2.
func (w *World) applyInput(in Input) {
	for _, e := range w.entities {
		if e.Owner != in.Client {
			continue
		}
		move := in.Move
		if l := move.Len(); l > 1 {
			move = move.Mul(1 / l) // clamp to unit vector
		}
		e.Pos = e.Pos.Add(move.Mul(w.cfg.MoveSpeed))
		e.Pos.X = clamp(e.Pos.X, -w.cfg.Bounds, w.cfg.Bounds)
		e.Pos.Y = clamp(e.Pos.Y, -w.cfg.Bounds, w.cfg.Bounds)
		if in.Action&1 != 0 {
			for _, other := range w.entities {
				if other.ID == e.ID {
					continue
				}
				if other.Pos.Dist(e.Pos) <= 2 {
					other.HP -= 1
				}
			}
		}
		return
	}
	// Input for a client with no entity: valid but no-op.
}

// LastSeq returns the highest input sequence applied for a client.
func (w *World) LastSeq(c ClientID) uint32 { return w.lastSeq[c] }

// Snapshot captures the full authoritative state at the current tick.
func (w *World) Snapshot() *Snapshot {
	s := &Snapshot{Tick: w.tick, Entities: make(map[EntityID]EntityState, len(w.entities))}
	for id, e := range w.entities {
		s.Entities[id] = e.Clone()
	}
	return s
}

func clamp(v, lo, hi float32) float32 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
