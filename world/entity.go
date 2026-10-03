package world

// EntityID uniquely identifies an entity within a world.
type EntityID uint64

// FieldMask marks which fields of an EntitySnapshot are present/changed.
type FieldMask uint16

const (
	FieldPosition FieldMask = 1 << iota
	FieldVelocity
	FieldHealth

	// AllFields is used for full snapshots (baselines and enters).
	AllFields = FieldPosition | FieldVelocity | FieldHealth
)

// EntityState is the authoritative replicated state of one entity.
type EntityState struct {
	ID     EntityID
	Pos    Vec2
	Vel    Vec2
	Health int
}

// EntitySnapshot is either a full state (Mask == AllFields) or a delta
// containing only the changed fields (absolute values, never deltas of
// deltas).
type EntitySnapshot struct {
	ID     EntityID
	Mask   FieldMask
	Pos    Vec2
	Vel    Vec2
	Health int
}

// IsFull reports whether the snapshot carries every field.
func (e EntitySnapshot) IsFull() bool { return false }
