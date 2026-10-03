package esr

import "math/rand"

// ClientConfig tunes client-side replication behaviour.
type ClientConfig struct {
	InterpDelayMs   int64   // render delay for remote entity interpolation
	CorrectionRate  float32 // fraction of prediction error corrected per Update [0,1]
	BaselineRetryMs int64   // re-request join/baseline if none arrives within this window
}

type snapshotAt struct {
	snap *Snapshot
	at   int64
}

// Client maintains a locally reconstructed view of the world.
type Client struct {
	cfg  ClientConfig
	link *Link

	id         ClientID
	nonce      uint64
	controlled EntityID
	worldCfg   WorldConfig
	welcomed   bool

	// Authoritative reconstruction. latest is nil until a full
	// baseline arrives; nothing is rendered before that.
	latest *Snapshot
	hist   []snapshotAt // recent applied snapshots for interpolation

	// Prediction of the controlled entity.
	nextSeq      uint32
	pending      []Input // inputs not yet confirmed by the server
	predicted    EntityState
	hasPredicted bool
	renderErr    Vec2 // smoothing offset, decays to zero

	outbox          []Envelope
	lastHelloAt     int64
	lastBaselineReq int64
	helloSent       bool
}

func NewClient(cfg ClientConfig) *Client {
	return &Client{cfg: cfg, nonce: rand.Uint64()}
}

// ID returns the assigned client ID (0 before the welcome arrives).
func (c *Client) ID() ClientID { return c.id }

// Ready reports whether a full baseline has been received; the client
// must not render world state before this is true.
func (c *Client) Ready() bool { return c.latest != nil }

// ConnectLink registers the link used to reach the server.
func (c *Client) ConnectLink(link *Link) { c.link = link }

// Join queues a join request.
func (c *Client) Join() {
	c.outbox = append(c.outbox, Envelope{Kind: MsgHello, Nonce: c.nonce})
	c.helloSent = true
	c.lastHelloAt = 0
}

// Handle processes one inbound message from the server.
func (c *Client) Handle(env Envelope, now int64) {
	switch env.Kind {
	case MsgWelcome:
		if c.welcomed {
			return // duplicate welcome
		}
		c.id = env.ClientID
		c.controlled = env.EntityID
		c.worldCfg = env.Cfg
		c.welcomed = true
	case MsgBaseline, MsgDelta:
		c.applyDelta(env.Delta, now)
	}
}

// applyDelta folds an incoming snapshot update into the local view.
// Stale, duplicated or out-of-window updates are discarded; the stream
// self-heals because the server always bases deltas on the last ack.
func (c *Client) applyDelta(d *Delta, now int64) {
	if d == nil {
		return
	}
	if c.latest != nil && d.Tick <= c.latest.Tick {
		return // stale or duplicate
	}
	if !d.Full && c.latest == nil {
		return // delta before baseline: wait for the full snapshot
	}
	next, ok := c.latest.Apply(d)
	if !ok {
		// Base mismatch (a delta was lost or reordered). Discard; the
		// server will re-send from the last acked base, and eventually
		// fall back to a full snapshot.
		return
	}
	c.latest = next
	c.hist = append(c.hist, snapshotAt{snap: next, at: now})
	if len(c.hist) > 8 {
		c.hist = c.hist[len(c.hist)-8:]
	}
	c.ack(d.Tick)
	c.reconcile(d.InputSeq)
}

// reconcile folds the authoritative state of the controlled entity into
// the local prediction: rewind to the authoritative state, then re-apply
// all inputs the server has not confirmed yet. The render offset absorbs
// the discontinuity so the correction is smoothed, not snapped.
func (c *Client) reconcile(authInputSeq uint32) {
	auth, ok := c.latest.Entities[c.controlled]
	if !ok {
		c.hasPredicted = false
		c.pending = c.pending[:0]
		return
	}
	kept := c.pending[:0]
	for _, in := range c.pending {
		if in.Seq > authInputSeq {
			kept = append(kept, in)
		}
	}
	c.pending = kept

	newPred := auth.Clone()
	for _, in := range c.pending {
		newPred = applyMove(newPred, in.Move, c.worldCfg)
	}
	if c.hasPredicted {
		oldRender := c.predicted.Pos.Add(c.renderErr)
		c.renderErr = oldRender.Sub(newPred.Pos)
	}
	c.predicted = newPred
	c.hasPredicted = true
}

// applyMove mirrors the server's movement rule for prediction.
func applyMove(e EntityState, move Vec2, cfg WorldConfig) EntityState {
	if l := move.Len(); l > 1 {
		move = move.Mul(1 / l)
	}
	e.Pos = e.Pos.Add(move.Mul(cfg.MoveSpeed))
	e.Pos.X = clamp(e.Pos.X, -cfg.Bounds, cfg.Bounds)
	e.Pos.Y = clamp(e.Pos.Y, -cfg.Bounds, cfg.Bounds)
	return e
}

// SendInput queues a movement input and applies it to the local
// prediction of the controlled entity.
func (c *Client) SendInput(move Vec2) {
	if !c.welcomed {
		return
	}
	c.nextSeq++
	in := Input{Client: c.id, Seq: c.nextSeq, Move: move}
	c.pending = append(c.pending, in)
	c.outbox = append(c.outbox, Envelope{Kind: MsgInput, Input: in})
	if c.hasPredicted {
		c.predicted = applyMove(c.predicted, move, c.worldCfg)
	}
}

// Update performs periodic work: join/baseline retries and decay of the
// prediction-correction offset.
func (c *Client) Update(now int64) {
	retry := c.cfg.BaselineRetryMs
	if retry <= 0 {
		retry = 1000
	}
	if !c.welcomed {
		if c.helloSent && now-c.lastHelloAt >= retry {
			c.outbox = append(c.outbox, Envelope{Kind: MsgHello, Nonce: c.nonce})
			c.lastHelloAt = now
		}
		return
	}
	if c.latest == nil && now-c.lastBaselineReq >= retry {
		c.outbox = append(c.outbox, Envelope{From: c.id, Kind: MsgBaselineRequest})
		c.lastBaselineReq = now
	}
	// Smoothly absorb remaining correction error.
	rate := c.cfg.CorrectionRate
	if rate <= 0 {
		rate = 0.2
	}
	c.renderErr = c.renderErr.Mul(1 - rate)
	if c.renderErr.Len() < 0.01 {
		c.renderErr = Vec2{}
	}
}

// RenderState returns the states to render: interpolated remote entities
// plus the smoothly corrected prediction of the controlled entity.
// Returns nil until Ready.
func (c *Client) RenderState(now int64) []EntityState {
	if c.latest == nil {
		return nil
	}
	renderAt := now - c.cfg.InterpDelayMs
	from, to := c.bracket(renderAt)
	out := make([]EntityState, 0, len(c.latest.Entities))
	for id, e := range c.latest.Entities {
		if id == c.controlled && c.hasPredicted {
			p := c.predicted.Clone()
			p.Pos = p.Pos.Add(c.renderErr)
			out = append(out, p)
			continue
		}
		out = append(out, interpolate(e, id, from, to, renderAt))
	}
	return out
}

// bracket finds the two history snapshots surrounding time t.
func (c *Client) bracket(t int64) (from, to snapshotAt) {
	for _, h := range c.hist {
		if h.at <= t {
			from = h
		} else {
			to = h
			return from, to
		}
	}
	// t is at/after the newest snapshot: interpolate between the last two.
	n := len(c.hist)
	if n >= 2 {
		return c.hist[n-2], c.hist[n-1]
	}
	if n == 1 {
		return c.hist[0], c.hist[0]
	}
	return snapshotAt{}, snapshotAt{}
}

// interpolate computes the rendered state of a remote entity at time t,
// blending between the two bracketing snapshots. The current state e is
// the fallback when history is missing.
func interpolate(e EntityState, id EntityID, from, to snapshotAt, t int64) EntityState {
	if from.snap == nil || to.snap == nil || from.snap == to.snap {
		return e
	}
	a, okA := from.snap.Entities[id]
	b, okB := to.snap.Entities[id]
	if !okA || !okB {
		return e // entered or left the view between snapshots
	}
	span := to.at - from.at
	if span <= 0 {
		return b
	}
	alpha := float32(t-from.at) / float32(span)
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}
	out := b.Clone()
	out.Pos = a.Pos.Add(b.Pos.Sub(a.Pos).Mul(alpha))
	return out
}

// Authoritative returns the latest applied authoritative snapshot.
func (c *Client) Authoritative() *Snapshot { return c.latest }

// Outbox returns queued outbound messages and clears the queue.
func (c *Client) Outbox() []Envelope {
	out := c.outbox
	c.outbox = nil
	return out
}

func (c *Client) ack(t Tick) {
	c.outbox = append(c.outbox, Envelope{From: c.id, Kind: MsgAck, AckTick: t})
}
