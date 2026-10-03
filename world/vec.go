package world

import "math"

// Vec2 is a 2D vector used for positions and velocities.
type Vec2 struct {
	X float64
	Y float64
}

func (v Vec2) Add(o Vec2) Vec2      { return Vec2{X: v.X + o.X, Y: v.Y + o.Y} }
func (v Vec2) Sub(o Vec2) Vec2      { return Vec2{X: v.X - o.X, Y: v.Y - o.Y} }
func (v Vec2) Scale(s float64) Vec2 { return Vec2{X: v.X * s, Y: v.Y * s} }
func (v Vec2) Length() float64      { return math.Hypot(v.X, v.Y) }
func (v Vec2) Distance(o Vec2) float64 {
	dx := v.X - o.X
	dy := v.Y - o.Y
	return math.Hypot(dx, dy)
}

// Normalize returns a unit vector. Zero vectors stay zero.
func (v Vec2) Normalize() Vec2 {
	l := v.Length()
	if l == 0 {
		return v
	}
	return v.Scale(1 / l)
}

// Lerp linearly interpolates between a and b by t in [0,1].
func Lerp(a, b Vec2, t float64) Vec2 {
	return Vec2{X: a.X + (b.X-a.X)*t, Y: a.Y + (b.Y-a.Y)*t}
}
