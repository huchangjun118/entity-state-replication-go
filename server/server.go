package server

import (
	"sort"
	"sync"

	"github.com/huchangjun118/entity-state-replication-go/transport"
	"github.com/huchangjun118/entity-state-replication-go/world"
)

// Config tunes server behavior.
type Config struct {
	ViewRadius     float64 // default vision radius for clients
	SnapshotBudget int     // max snapshot bytes sent to one client per step; 0 = unlimited
	SendEvery      uint64  // send a snapshot to each client every N ticks
}

// DefaultConfig returns a usable server configuration.
func DefaultConfig() Config {
	return Config{
		ViewRadius: 30,
		SendEvery:  1,
	}
}

// ClientSession is the server-side state of a connected client.
type ClientSession struct {
	ClientID   uint64
	EntityID   world.EntityID
	ViewRadius float64

	lastInputSeq uint64 // highest input seq processed
	ackedTick    uint64 // highest snapshot tick acked AND fully delivered
	lastSentTick uint64 // newest snapshot tick sent
	lastComplete uint64 // newest tick whose snapshot was complete (within budget)
	joined       bool   // at least one snapshot acked
	rrOffset     int    // round-robin offset for budget truncation
}

// Server is the authoritative game server.
type Server struct {
	mu  sync.Mutex
	cfg Config

	world    *world.World
	channel  *transport.Channel
	sessions map[uint64]*ClientSession

	// pending inputs collected during the current tick, sorted before apply
	inputs []pendingInput
}

type pendingInput struct {
	clientID uint64
	seq      uint64
	move     world.Vec2
}

// New creates a server bound to a channel.
func New(cfg Config, w *world.World, ch *transport.Channel) *Server {
	if cfg.ViewRadius <= 0 {
		cfg.ViewRadius = 30
	}
	if cfg.SendEvery <= 0 {
		cfg.SendEvery = 1
	}
	return &Server{
		cfg:      cfg,
		world:    w,
		channel:  ch,
		sessions: make(map[uint64]*ClientSession),
	}
}

// World exposes the authoritative world.
func (s *Server) World() *world.World { return s.world }

// Session returns the session for a client, if any.
func (s *Server) Session(clientID uint64) *ClientSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[clientID]
}

// Step runs one server tick: process acks and inputs, simulate, build and
// send snapshots.
func (s *Server) Step() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.drainMessages()
	s.applyInputs()
	s.world.Step()
	s.sendSnapshots()
}

func (s *Server) drainMessages() {
	for {
		src, msg, ok := s.channel.Receive(0)
		if !ok {
			break
		}
		switch m := msg.(type) {
		case *transport.JoinRequest:
			s.handleJoin(m)
		case *transport.Input:
			s.handleInput(src, m)
		case *transport.Ack:
			s.handleAck(m)
		}
	}
}

func (s *Server) handleJoin(m *transport.JoinRequest) {
	sess, ok := s.sessions[m.ClientID]
	if !ok {
		// New session: possess an existing entity for this client id or
		// spawn a fresh one.
		entID := world.EntityID(m.ClientID)
		if !s.world.Exists(entID) {
			entID = s.world.Spawn(world.Vec2{}, 100)
		}
		sess = &ClientSession{
			ClientID:   m.ClientID,
			EntityID:   entID,
			ViewRadius: s.cfg.ViewRadius,
		}
		s.sessions[m.ClientID] = sess
	}
	// (Re)send join ack.
	s.channel.Send(0, m.ClientID, &transport.JoinAck{
		ClientID:   m.ClientID,
		EntityID:   sess.EntityID,
		Tick:       s.world.Tick(),
		ViewRadius: sess.ViewRadius,
	})
}

func (s *Server) handleInput(src uint64, m *transport.Input) {
	sess, ok := s.sessions[m.ClientID]
	if !ok || src != m.ClientID {
		return
	}
	// Duplicate or expired input: no effect.
	if m.Seq <= sess.lastInputSeq {
		return
	}
	if !s.world.Exists(sess.EntityID) {
		return
	}
	move := world.Vec2{X: m.MoveX, Y: m.MoveY}
	if move.Length() > 1 {
		move = move.Normalize()
	}
	s.inputs = append(s.inputs, pendingInput{
		clientID: m.ClientID,
		seq:      m.Seq,
		move:     move,
	})
}

func (s *Server) applyInputs() {
	// Deterministic order: by client id, then input sequence.
	sort.Slice(s.inputs, func(i, j int) bool {
		if s.inputs[i].clientID != s.inputs[j].clientID {
			return s.inputs[i].clientID < s.inputs[j].clientID
		}
		return s.inputs[i].seq < s.inputs[j].seq
	})
	for _, in := range s.inputs {
		sess := s.sessions[in.clientID]
		if sess == nil {
			continue
		}
		// Skip inputs that became stale while queued.
		if in.seq <= sess.lastInputSeq {
			continue
		}
		s.world.ApplyInput(sess.EntityID, world.Input{Seq: in.seq, Move: in.move})
		sess.lastInputSeq = in.seq
	}
	s.inputs = s.inputs[:0]
}

func (s *Server) handleAck(m *transport.Ack) {
	sess, ok := s.sessions[m.ClientID]
	if !ok {
		return
	}
	if m.Tick > s.world.Tick() {
		return
	}
	sess.joined = true
	if m.Tick > sess.ackedTick {
		// Only advance the baseline when the client acked a complete
		// snapshot; partial snapshots keep the old baseline so deferred
		// entities are not lost.
		if m.Tick <= sess.lastComplete {
			sess.ackedTick = m.Tick
		}
	}
}

func (s *Server) sendSnapshots() {
	tick := s.world.Tick()
	for _, sess := range s.sessions {
		if tick%s.cfg.SendEvery != 0 {
			continue
		}
		snap := s.buildSnapshot(sess, tick)
		if snap == nil {
			continue
		}
		s.channel.Send(0, sess.ClientID, &transport.Snapshot{
			ClientID: sess.ClientID,
			Snap:     *snap,
		})
		sess.lastSentTick = tick
	}
}

func (s *Server) buildSnapshot(sess *ClientSession, tick uint64) *world.Snapshot {
	center, ok := s.world.State(sess.EntityID)
	if !ok {
		return nil
	}
	current := s.world.Visible(center.Pos, sess.ViewRadius)

	// Not yet joined (or baseline aged out): send a full snapshot.
	if !sess.joined || sess.ackedTick == 0 {
		snap := &world.Snapshot{
			Tick: tick, BaseTick: 0,
			AckInputSeq: sess.lastInputSeq,
			Full:        true,
			Entities:    make([]world.EntitySnapshot, 0, len(current)),
		}
		for _, e := range current {
			snap.Entities = append(snap.Entities, world.EntitySnapshot{
				ID: e.ID, Mask: world.AllFields,
				Pos: e.Pos, Vel: e.Vel, Health: e.Health,
			})
		}
		sess.lastComplete = tick
		return snap
	}

	baseStates, ok := s.world.StateAt(sess.ackedTick)
	if !ok {
		// Baseline evicted from history: re-baseline.
		sess.ackedTick = 0
		return s.buildSnapshot(sess, tick)
	}
	// Reconstruct the client's visible set at the baseline tick.
	baseCenter := baseStates[sess.EntityID]
	baseVisible := make(map[world.EntityID]world.EntityState)
	for id, st := range baseStates {
		if st.Pos.Distance(baseCenter.Pos) <= sess.ViewRadius {
			baseVisible[id] = st
		}
	}

	entities, leaves := world.Diff(baseVisible, current)
	if len(entities) == 0 && len(leaves) == 0 {
		return nil
	}

	snap := &world.Snapshot{
		Tick: tick, BaseTick: sess.ackedTick,
		AckInputSeq: sess.lastInputSeq,
		Leaves:      leaves,
	}
	snap.Entities, sess.rrOffset = s.applyBudget(sess, center.Pos, entities)
	if len(snap.Entities) == len(entities) {
		sess.lastComplete = tick
	}
	return snap
}

// applyBudget truncates an entity update list to fit the snapshot byte
// budget. The client's own entity always wins; remaining entities are
// ordered by distance to the view center with a rotating offset so that
// under sustained congestion every entity eventually gets through.
func (s *Server) applyBudget(sess *ClientSession, center world.Vec2, entities []world.EntitySnapshot) ([]world.EntitySnapshot, int) {
	if s.cfg.SnapshotBudget <= 0 {
		return entities, 0
	}
	full := &transport.Snapshot{ClientID: sess.ClientID, Snap: world.Snapshot{Entities: entities}}
	if full.Size() <= s.cfg.SnapshotBudget {
		return entities, 0
	}

	// Split own entity from the rest.
	var own []world.EntitySnapshot
	rest := make([]world.EntitySnapshot, 0, len(entities))
	for _, e := range entities {
		if e.ID == sess.EntityID {
			own = append(own, e)
		} else {
			rest = append(rest, e)
		}
	}
	sort.Slice(rest, func(i, j int) bool {
		di := rest[i].Pos.Distance(center)
		dj := rest[j].Pos.Distance(center)
		if di != dj {
			return di < dj
		}
		return rest[i].ID < rest[j].ID
	})

	budget := s.cfg.SnapshotBudget - (&transport.Snapshot{ClientID: sess.ClientID}).Size()
	out := make([]world.EntitySnapshot, 0, len(entities))
	for _, e := range own {
		if budget-entitySize(e) >= 0 {
			out = append(out, e)
			budget -= entitySize(e)
		}
	}
	rr := sess.rrOffset
	n := len(rest)
	for i := 0; i < n; i++ {
		e := rest[(rr+i)%n]
		if budget-entitySize(e) < 0 {
			rr = (rr + i) % max1(n, 1)
			break
		}
		out = append(out, e)
		budget -= entitySize(e)
	}
	if n > 0 {
		rr = (rr + 1) % n
	}
	return out, rr
}

func entitySize(e world.EntitySnapshot) int {
	s := world.Snapshot{Entities: []world.EntitySnapshot{e}}
	return (&transport.Snapshot{Snap: s}).Size() - (&transport.Snapshot{}).Size()
}

func max1(a, b int) int {
	if a > b {
		return a
	}
	return b
}
