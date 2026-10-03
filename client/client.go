package client

import (
	"sort"
	"sync"

	"github.com/huchangjun118/entity-state-replication-go/transport"
	"github.com/huchangjun118/entity-state-replication-go/world"
)

// ConnState is the client's session state.
type ConnState uint8

const (
	StateDisconnected    ConnState = iota
	StateJoining                   // handshake sent, waiting for ack
	StateWaitingBaseline           // ack received, waiting for full snapshot
	StateJoined
)

// Config tunes client behavior.
type Config struct {
	TickRate       int     // client simulation steps per second
	MoveSpeed      float64 // local prediction speed (must match server)
	InterpDelay    uint64  // render delay in ticks for interpolation
	JoinTimeout    int     // steps before retrying handshake/baseline
	CorrectionRate float64 // smoothing factor [0,1] for prediction correction
	FadeTicks      uint64  // ticks for enter/exit fade
}

// DefaultConfig returns a usable client configuration.
func DefaultConfig() Config {
	return Config{
		TickRate:       20,
		MoveSpeed:      5,
		InterpDelay:    2,
		JoinTimeout:    100,
		CorrectionRate: 0.25,
		FadeTicks:      4,
	}
}

// Client is a player's replicated view of the world.
type Client struct {
	mu  sync.Mutex
	cfg Config
	ch  *transport.Channel

	clientID uint64
	name     string

	state    ConnState
	entityID world.EntityID

	// snapshot buffer: received snapshots keyed by tick
	snaps       map[uint64]world.Snapshot
	lastApplied uint64 // last in-order applied snapshot tick
	latestTick  uint64 // newest snapshot tick received

	pred *predictor
	view *viewState

	now          int
	joinStep     int
	lastRecvStep int

	move world.Vec2
}

// New creates a client.
func New(cfg Config, ch *transport.Channel, clientID uint64, name string) *Client {
	if cfg.TickRate <= 0 {
		cfg.TickRate = 20
	}
	if cfg.MoveSpeed <= 0 {
		cfg.MoveSpeed = 5
	}
	if cfg.JoinTimeout <= 0 {
		cfg.JoinTimeout = 100
	}
	if cfg.CorrectionRate <= 0 {
		cfg.CorrectionRate = 0.25
	}
	if cfg.FadeTicks == 0 {
		cfg.FadeTicks = 4
	}
	return &Client{
		cfg:      cfg,
		ch:       ch,
		clientID: clientID,
		name:     name,
		snaps:    make(map[uint64]world.Snapshot),
		view:     newViewState(),
	}
}

// State reports the current connection state.
func (c *Client) State() ConnState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// EntityID returns the client's own entity id (valid after join).
func (c *Client) EntityID() world.EntityID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entityID
}

// LastAppliedTick returns the newest snapshot tick applied in order.
func (c *Client) LastAppliedTick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastApplied
}

// SetInput sets the current desired movement direction for the local entity.
func (c *Client) SetInput(move world.Vec2) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if move.Length() > 1 {
		move = move.Normalize()
	}
	c.move = move
}

// Step advances the client by one simulation step.
func (c *Client) Step() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now++

	c.drain()

	switch c.state {
	case StateDisconnected:
		c.startJoin()
	case StateJoining:
		if c.entityID != 0 {
			c.state = StateWaitingBaseline
			c.joinStep = c.now
			c.applyPending()
		} else if c.now-c.joinStep > c.cfg.JoinTimeout {
			c.startJoin() // handshake lost: retry
		}
	case StateWaitingBaseline:
		c.applyPending()
		if c.state == StateWaitingBaseline && c.now-c.joinStep > c.cfg.JoinTimeout {
			// Baseline lost: reset any partial state and re-handshake.
			c.resetSession()
			c.startJoin()
		}
	case StateJoined:
		// Once joined, the client stays joined: snapshots only arrive when
		// the world changes, so a static world produces no traffic. The
		// client keeps predicting locally and rendering the latest state;
		// it only re-handshakes if the baseline itself was never received
		// (handled in StateWaitingBaseline).
		dt := 1.0 / float64(c.cfg.TickRate)
		c.pred.predict(c.move, dt)
		c.ch.Send(c.clientID, 0, &transport.Input{
			ClientID: c.clientID, Seq: c.pred.seq,
			MoveX: c.move.X, MoveY: c.move.Y,
		})
		c.applyPending()
		c.pred.smooth(c.cfg.CorrectionRate)
	}
}

func (c *Client) startJoin() {
	c.state = StateJoining
	c.entityID = 0
	c.joinStep = c.now
	c.ch.Send(c.clientID, 0, &transport.JoinRequest{
		ClientID: c.clientID, Name: c.name,
	})
}

func (c *Client) resetSession() {
	c.state = StateDisconnected
	c.entityID = 0
	c.snaps = make(map[uint64]world.Snapshot)
	c.lastApplied = 0
	c.latestTick = 0
	c.pred = nil
	c.view = newViewState()
	c.move = world.Vec2{}
}

func (c *Client) drain() {
	for {
		_, msg, ok := c.ch.Receive(c.clientID)
		if !ok {
			break
		}
		switch m := msg.(type) {
		case *transport.JoinAck:
			if c.state == StateJoining {
				c.entityID = m.EntityID
				c.pred = newPredictor(m.EntityID, c.cfg.MoveSpeed)
			}
		case *transport.Snapshot:
			c.lastRecvStep = c.now
			if _, exists := c.snaps[m.Snap.Tick]; !exists {
				c.snaps[m.Snap.Tick] = m.Snap
				if m.Snap.Tick > c.latestTick {
					c.latestTick = m.Snap.Tick
				}
			}
		}
	}
}

// applyPending applies received snapshots in ascending tick order. Gaps
// (lost ticks) are skipped: snapshots carry absolute field values, so
// skipping a lost tick is safe and can never roll the view backwards.
// Stale snapshots (older than the last applied tick) are discarded.
func (c *Client) applyPending() {
	ticks := make([]uint64, 0, len(c.snaps))
	for t := range c.snaps {
		if t > c.lastApplied {
			ticks = append(ticks, t)
		}
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i] < ticks[j] })
	for _, t := range ticks {
		snap := c.snaps[t]
		c.applySnapshot(snap)
		c.lastApplied = t
		delete(c.snaps, t)
		c.ch.Send(c.clientID, 0, &transport.Ack{ClientID: c.clientID, Tick: t})
	}
	// Discard stale snapshots that can never be applied.
	for t := range c.snaps {
		if t <= c.lastApplied {
			delete(c.snaps, t)
		}
	}
}

func (c *Client) applySnapshot(snap world.Snapshot) {
	c.view.apply(snap)
	if c.pred != nil {
		if own, ok := findEntity(snap.Entities, c.pred.entityID); ok {
			if own.Mask&world.FieldPosition != 0 {
				c.pred.reconcile(snap.AckInputSeq, own.Pos)
			}
		}
	}
	if c.state == StateWaitingBaseline {
		c.state = StateJoined
	}
}

func findEntity(ents []world.EntitySnapshot, id world.EntityID) (world.EntitySnapshot, bool) {
	for _, e := range ents {
		if e.ID == id {
			return e, true
		}
	}
	return world.EntitySnapshot{}, false
}

// Render returns the entities to present at render time (renderTick+frac).
// It returns nil until a complete baseline has been applied, so a
// half-initialized world is never shown.
func (c *Client) Render(frac float64) []ViewEntity {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != StateJoined || c.pred == nil {
		return nil
	}
	renderTick := c.lastApplied
	if renderTick > c.cfg.InterpDelay {
		renderTick -= c.cfg.InterpDelay
	} else {
		renderTick = 0
	}
	out := c.view.render(renderTick, frac, c.cfg.FadeTicks, c.pred.entityID)
	out = append(out, ViewEntity{
		ID: c.pred.entityID, Pos: c.pred.render, Alpha: 1,
	})
	return out
}

// SimPosition returns the client's own predicted simulation position.
func (c *Client) SimPosition() world.Vec2 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pred == nil {
		return world.Vec2{}
	}
	return c.pred.sim
}

// receiveSnapshotForTest injects a snapshot directly into the client's
// buffer. For testing only.
func (c *Client) receiveSnapshotForTest(snap world.Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if snap.Tick > c.lastApplied {
		c.snaps[snap.Tick] = snap
		if snap.Tick > c.latestTick {
			c.latestTick = snap.Tick
		}
	}
}
