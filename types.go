package esr

import "math"

// Tick is the authoritative simulation step counter.
type Tick uint32

// ClientID identifies a connected client. 0 is reserved for the server.
type ClientID uint32

// EntityID identifies an entity in the shared world.
type EntityID uint32

// Vec2 is a 2D vector used for positions, velocities and movement input.
type Vec2 struct {
	X, Y float32
}

func (v Vec2) Add(o Vec2) Vec2    { return Vec2{v.X + o.X, v.Y + o.Y} }
func (v Vec2) Sub(o Vec2) Vec2    { return Vec2{v.X - o.X, v.Y - o.Y} }
func (v Vec2) Mul(s float32) Vec2 { return Vec2{v.X * s, v.Y * s} }

func (v Vec2) Len() float32 { return float32(math.Sqrt(float64(v.X*v.X + v.Y*v.Y))) }

func (v Vec2) Dist(o Vec2) float32 { return v.Sub(o).Len() }

// EntityState is the replicated state of a single entity.
type EntityState struct {
	ID    EntityID
	Owner ClientID
	Pos   Vec2
	Vel   Vec2
	HP    float32
	Props map[string]float32
}

// Clone deep-copies the state.
func (e EntityState) Clone() EntityState {
	out := e
	if e.Props != nil {
		out.Props = make(map[string]float32, len(e.Props))
		for k, v := range e.Props {
			out.Props[k] = v
		}
	}
	return out
}

// Equal reports whether two states carry identical replicated data.
func (e EntityState) Equal(o EntityState) bool {
	if e.ID != o.ID || e.Owner != o.Owner || e.Pos != o.Pos || e.Vel != o.Vel || e.HP != o.HP {
		return false
	}
	if len(e.Props) != len(o.Props) {
		return false
	}
	for k, v := range e.Props {
		if ov, ok := o.Props[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

// Input is a client-issued command. It never mutates shared state
// directly; the server validates, orders and applies it.
type Input struct {
	Client ClientID
	Seq    uint32
	Move   Vec2
	Action uint32
}
