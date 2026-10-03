package world

import "sync"

// Config tunes the deterministic world simulation.
type Config struct {
	TickRate   int     // server ticks per second
	MoveSpeed  float64 // movement units per second at full input
	HistoryLen int     // number of past ticks retained for delta baselines
}

// DefaultConfig returns a usable configuration.
func DefaultConfig() Config {
	return Config{
		TickRate:   20,
		MoveSpeed:  5,
		HistoryLen: 256,
	}
}

// World is the authoritative entity container. All mutation happens through
// its methods so that server-side application stays deterministic.
type World struct {
	mu     sync.Mutex
	cfg    Config
	tick   uint64
	nextID EntityID

	entities map[EntityID]*EntityState
	// history ring: history[tick%len] = full entity states at that tick.
	history []map[EntityID]EntityState
}

// New creates an empty world.
func New(cfg Config) *World {
	if cfg.TickRate <= 0 {
		cfg.TickRate = 20
	}
	if cfg.MoveSpeed <= 0 {
		cfg.MoveSpeed = 5
	}
	if cfg.HistoryLen <= 0 {
		cfg.HistoryLen = 256
	}
	return &World{
		cfg:      cfg,
		nextID:   1,
		entities: make(map[EntityID]*EntityState),
		history:  make([]map[EntityID]EntityState, cfg.HistoryLen),
	}
}

// Tick returns the current server tick.
func (w *World) Tick() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tick
}

// Config returns the world configuration.
func (w *World) Config() Config { return w.cfg }

// Spawn adds a new entity and returns its id.
func (w *World) Spawn(pos Vec2, health int) EntityID {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := w.nextID
	w.nextID++
	w.entities[id] = &EntityState{ID: id, Pos: pos, Health: health}
	return id
}

// Despawn removes an entity.
func (w *World) Despawn(id EntityID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.entities, id)
}

// Exists reports whether an entity is currently in the world.
func (w *World) Exists(id EntityID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.entities[id]
	return ok
}

// State returns a copy of the current entity state.
func (w *World) State(id EntityID) (EntityState, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.entities[id]
	if !ok {
		return EntityState{}, false
	}
	return *e, true
}

// Entities returns the ids of all entities currently in the world.
func (w *World) Entities() []EntityID {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]EntityID, 0, len(w.entities))
	for id := range w.entities {
		out = append(out, id)
	}
	return out
}

// SetPosition teleports an entity to a position (server-side gameplay use).
func (w *World) SetPosition(id EntityID, pos Vec2) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.entities[id]; ok {
		e.Pos = pos
	}
}

// SetHealth changes an entity's health (server-side gameplay use).
func (w *World) SetHealth(id EntityID, health int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.entities[id]; ok {
		e.Health = health
	}
}

// ApplyInput applies a validated movement input to an entity. The input is
// expected to have been validated by the caller (server).
func (w *World) ApplyInput(id EntityID, in Input) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.entities[id]
	if !ok {
		return
	}
	move := in.Move
	if move.Length() > 1 {
		move = move.Normalize()
	}
	e.Vel = move.Scale(w.cfg.MoveSpeed)
}

// Step advances the simulation by one tick: integrate movement and record
// history for delta baselines.
func (w *World) Step() {
	w.mu.Lock()
	defer w.mu.Unlock()

	dt := 1.0 / float64(w.cfg.TickRate)
	for _, e := range w.entities {
		e.Pos = e.Pos.Add(e.Vel.Scale(dt))
	}

	w.tick++
	snap := make(map[EntityID]EntityState, len(w.entities))
	for id, e := range w.entities {
		snap[id] = *e
	}
	w.history[w.tick%uint64(len(w.history))] = snap
}

// Visible returns the states of entities strictly within radius of center.
func (w *World) Visible(center Vec2, radius float64) []EntityState {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []EntityState
	for _, e := range w.entities {
		if e.Pos.Distance(center) <= radius {
			out = append(out, *e)
		}
	}
	return out
}

// StateAt returns the full entity states at a past tick if still retained.
func (w *World) StateAt(tick uint64) (map[EntityID]EntityState, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tick == 0 || tick > w.tick {
		return nil, false
	}
	// History ring only covers the last HistoryLen ticks.
	if w.tick-tick >= uint64(len(w.history)) {
		return nil, false
	}
	s := w.history[tick%uint64(len(w.history))]
	if s == nil {
		return nil, false
	}
	out := make(map[EntityID]EntityState, len(s))
	for id, e := range s {
		out[id] = e
	}
	return out, true
}

// Diff computes a snapshot delta from base (client-known states) to the
// current visible set. Entities in current but not in base are returned as
// full snapshots; entities in base but not current are returned as leaves.
// Field values are absolute, so applying a delta is idempotent and safe
// against skipped or reordered snapshots.
func Diff(base map[EntityID]EntityState, current []EntityState) (entities []EntitySnapshot, leaves []EntityID) {
	seen := make(map[EntityID]bool, len(current))
	for _, cur := range current {
		seen[cur.ID] = true
		old, ok := base[cur.ID]
		if !ok {
			entities = append(entities, EntitySnapshot{
				ID: cur.ID, Mask: AllFields,
				Pos: cur.Pos, Vel: cur.Vel, Health: cur.Health,
			})
			continue
		}
		var mask FieldMask
		if old.Pos != cur.Pos {
			mask |= FieldPosition
		}
		if old.Vel != cur.Vel {
			mask |= FieldVelocity
		}
		if old.Health != cur.Health {
			mask |= FieldHealth
		}
		if mask != 0 {
			entities = append(entities, EntitySnapshot{
				ID: cur.ID, Mask: mask,
				Pos: cur.Pos, Vel: cur.Vel, Health: cur.Health,
			})
		}
	}
	for id := range base {
		if !seen[id] {
			leaves = append(leaves, id)
		}
	}
	return entities, leaves
}
