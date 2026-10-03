package client

import "github.com/huchangjun118/entity-state-replication-go/world"

// predictor keeps client-side prediction state for the local entity.
//
// The client simulates its own entity ahead of the server. When an
// authoritative snapshot arrives, it rewinds to the acknowledged input
// sequence and replays all later inputs, so the simulation position stays
// in sync without ever waiting for a round trip. The render position
// chases the simulation position with a smoothing factor to avoid visible
// snaps.
type predictor struct {
	entityID world.EntityID
	speed    float64
	seq      uint64

	// ring of predicted steps (each step's movement delta is replayed)
	history []predictedStep
	ringCap int

	// corrected simulation position (authoritative + replayed inputs)
	sim world.Vec2
	// smoothed render position (chases sim)
	render world.Vec2
	// render is snapped to sim until the first authoritative state arrives
	hasAuth bool
}

type predictedStep struct {
	seq  uint64
	move world.Vec2
	dt   float64
}

func newPredictor(entityID world.EntityID, speed float64) *predictor {
	return &predictor{
		entityID: entityID,
		speed:    speed,
		ringCap:  256,
	}
}

// predict applies one input step locally.
func (p *predictor) predict(move world.Vec2, dt float64) {
	if move.Length() > 1 {
		move = move.Normalize()
	}
	p.seq++
	p.history = append(p.history, predictedStep{seq: p.seq, move: move, dt: dt})
	if len(p.history) > p.ringCap {
		p.history = p.history[len(p.history)-p.ringCap:]
	}
	p.sim = p.sim.Add(move.Scale(p.speed * dt))
	if !p.hasAuth {
		p.render = p.sim
	}
}

// reconcile rewinds to the authoritative state at ackSeq and replays
// inputs with seq > ackSeq.
func (p *predictor) reconcile(ackSeq uint64, authoritative world.Vec2) {
	// Find the predicted step at or before ackSeq.
	idx := -1
	for i := len(p.history) - 1; i >= 0; i-- {
		if p.history[i].seq <= ackSeq {
			idx = i
			break
		}
	}
	// Replay steps after ackSeq from the authoritative position.
	pos := authoritative
	for i := idx + 1; i < len(p.history); i++ {
		st := p.history[i]
		pos = pos.Add(st.move.Scale(p.speed * st.dt))
	}
	p.sim = pos
	if !p.hasAuth {
		p.render = pos
		p.hasAuth = true
	}
	// Drop steps at or before the acknowledged sequence.
	if idx >= 0 {
		p.history = p.history[idx+1:]
	}
}

// smooth moves the render position towards the simulation position.
func (p *predictor) smooth(rate float64) {
	if rate <= 1 {
		p.render = world.Lerp(p.render, p.sim, rate)
	} else {
		p.render = p.sim
	}
}
