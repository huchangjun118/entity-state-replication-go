package esr

import "sort"

// ServerConfig tunes replication behaviour.
type ServerConfig struct {
	World          WorldConfig
	ViewRadius     float32 // AOI radius around each client's controlled entity
	MaxUnacked     int     // force a full snapshot after this many unacked deltas
	BytesPerTick   int     // per-client downstream budget per tick; 0 = unlimited
	PriorityRadius float32 // entities within this range are sent first under budget pressure
}

// serverClient tracks the replication state of one connected client.
type serverClient struct {
	id         ClientID
	link       *Link
	controlled EntityID
	viewCenter Vec2

	// sent[t] is the exact client-visible state after applying the
	// update sent at tick t. Deltas are always based on the snapshot
	// the client has acked, so lost packets never corrupt the stream.
	sent      map[Tick]*Snapshot
	lastAck   Tick
	unacked   int
	baselined bool
}

// Server is the authoritative node. It owns the World and one outbound
// link per client.
type Server struct {
	cfg        ServerConfig
	world      *World
	clients    map[ClientID]*serverClient
	links      map[ClientID]*Link // pre-registered outbound links
	byNonce    map[uint64]ClientID
	nextClient ClientID
}

func NewServer(cfg ServerConfig) *Server {
	return &Server{
		cfg:        cfg,
		world:      NewWorld(cfg.World),
		clients:    make(map[ClientID]*serverClient),
		links:      make(map[ClientID]*Link),
		byNonce:    make(map[uint64]ClientID),
		nextClient: 1,
	}
}

// World exposes the authoritative world (server-side use only).
func (s *Server) World() *World { return s.world }

// ConnectLink registers the link used to reach a client. It may be
// called before the client joins; the link is attached on join.
func (s *Server) ConnectLink(client ClientID, link *Link) {
	s.links[client] = link
	if c, ok := s.clients[client]; ok {
		c.link = link
	}
}

// ClientIDs returns the IDs of all joined clients.
func (s *Server) ClientIDs() []ClientID {
	out := make([]ClientID, 0, len(s.clients))
	for id := range s.clients {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ClientCount returns the number of joined clients.
func (s *Server) ClientCount() int { return len(s.clients) }

// Handle processes one inbound message from a client.
func (s *Server) Handle(env Envelope, now int64) {
	switch env.Kind {
	case MsgHello:
		s.join(env.Nonce, now)
	case MsgInput:
		s.world.SubmitInput(env.Input)
	case MsgAck:
		s.ack(env.From, env.AckTick)
	case MsgBaselineRequest:
		if c, ok := s.clients[env.From]; ok {
			s.sendFull(c, now)
		}
	case MsgGoodbye:
		s.leave(env.From)
	}
}

// join registers a new client, spawns its controlled entity and sends
// the welcome plus an initial full baseline. The client supplies a
// random nonce so that retried or duplicated Hellos are idempotent:
// they never create a second session or entity.
func (s *Server) join(nonce uint64, now int64) {
	if id, ok := s.byNonce[nonce]; ok {
		c := s.clients[id]
		s.sendWelcome(c, now)
		s.sendFull(c, now)
		return
	}
	id := s.nextClient
	s.nextClient++
	pos := Vec2{X: float32(id) * 5, Y: 0}
	ent := s.world.Spawn(id, pos)
	c := &serverClient{
		id:         id,
		link:       s.links[id],
		controlled: ent,
		viewCenter: pos,
		sent:       make(map[Tick]*Snapshot),
	}
	s.clients[id] = c
	s.byNonce[nonce] = id
	s.sendWelcome(c, now)
	s.sendFull(c, now)
}

func (s *Server) sendWelcome(c *serverClient, now int64) {
	s.sendTo(c, Envelope{
		To:       c.id,
		Kind:     MsgWelcome,
		ClientID: c.id,
		EntityID: c.controlled,
		Cfg:      s.cfg.World,
	}, now)
}

// leave removes a client and its controlled entity.
func (s *Server) leave(id ClientID) {
	c, ok := s.clients[id]
	if !ok {
		return
	}
	s.world.Despawn(c.controlled)
	delete(s.clients, id)
	for n, cid := range s.byNonce {
		if cid == id {
			delete(s.byNonce, n)
		}
	}
}

// ack records the client's acknowledgement of a snapshot tick.
func (s *Server) ack(client ClientID, tick Tick) {
	c, ok := s.clients[client]
	if !ok {
		return
	}
	if tick < c.lastAck {
		return // stale ack (reordered/duplicate)
	}
	if _, ok := c.sent[tick]; !ok {
		return // unknown tick
	}
	c.lastAck = tick
	c.unacked = 0
	c.baselined = true
	for t := range c.sent {
		if t < tick {
			delete(c.sent, t)
		}
	}
}

// Tick advances the simulation and emits per-client updates.
func (s *Server) Tick(now int64) {
	s.world.Step()
	full := s.world.Snapshot()
	for _, c := range s.clients {
		if e, ok := s.world.Entity(c.controlled); ok {
			c.viewCenter = e.Pos
		}
		view := full.Filter(AOI{Center: c.viewCenter, Radius: s.cfg.ViewRadius})
		s.sendUpdate(c, view, now)
	}
}

// sendUpdate builds and transmits the delta for one client, based on
// the last acked state, trimmed to the bandwidth budget if needed.
func (s *Server) sendUpdate(c *serverClient, view *Snapshot, now int64) {
	if c.link == nil {
		return
	}
	if !c.baselined || c.unacked >= s.cfg.MaxUnacked {
		s.sendFull(c, now)
		return
	}
	base := c.sent[c.lastAck]
	d := Diff(base, view)
	if d.Empty() {
		return
	}
	d = s.trim(c, d)
	// Record the exact state the client will hold after applying d,
	// so future diffs stay consistent even if d was trimmed.
	next, ok := base.Apply(d)
	if !ok {
		return // cannot happen: base ticks match
	}
	d.InputSeq = s.world.LastSeq(c.id)
	c.sent[d.Tick] = next
	c.unacked++
	s.sendTo(c, Envelope{To: c.id, Kind: MsgDelta, Delta: d}, now)
}

// sendFull transmits a complete baseline of the client's current view.
func (s *Server) sendFull(c *serverClient, now int64) {
	if c.link == nil {
		return
	}
	full := s.world.Snapshot()
	view := full.Filter(AOI{Center: c.viewCenter, Radius: s.cfg.ViewRadius})
	d := Diff(nil, view)
	d = s.trim(c, d)
	// A trimmed full snapshot is not a valid baseline; budget trimming
	// only drops updates, never removals, so the client still converges
	// once follow-up deltas arrive. Record what was actually sent.
	sent := &Snapshot{Tick: d.Tick, Entities: make(map[EntityID]EntityState, len(d.Updated))}
	for id, e := range d.Updated {
		sent.Entities[id] = e.Clone()
	}
	d.InputSeq = s.world.LastSeq(c.id)
	c.sent[d.Tick] = sent
	c.unacked++
	s.sendTo(c, Envelope{To: c.id, Kind: MsgBaseline, Delta: d}, now)
}

// trim enforces the per-tick byte budget, keeping the controlled entity
// and nearby (priority) entities first. Dropped entities simply stay
// stale on the client until a later update; nothing rolls back.
func (s *Server) trim(c *serverClient, d *Delta) *Delta {
	budget := s.cfg.BytesPerTick
	if budget <= 0 {
		return d
	}
	const entityBytes = 32
	const removeBytes = 4
	cost := len(d.Removed) * removeBytes
	if cost >= budget {
		return &Delta{Tick: d.Tick, BaseTick: d.BaseTick, Full: d.Full,
			Updated: map[EntityID]EntityState{}, Removed: d.Removed}
	}
	ids := make([]EntityID, 0, len(d.Updated))
	for id := range d.Updated {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		// Lower value = higher priority: controlled entity (-1) first,
		// then by distance, far entities last.
		return s.priority(c, d.Updated[ids[i]]) < s.priority(c, d.Updated[ids[j]])
	})
	kept := make(map[EntityID]EntityState, len(ids))
	for _, id := range ids {
		if cost+entityBytes > budget {
			break
		}
		kept[id] = d.Updated[id]
		cost += entityBytes
	}
	return &Delta{Tick: d.Tick, BaseTick: d.BaseTick, Full: d.Full, Updated: kept, Removed: d.Removed}
}

// priority ranks entities for bandwidth allocation: the controlled
// entity first, then entities inside the priority radius, then the rest
// ordered by distance to the view center.
func (s *Server) priority(c *serverClient, e EntityState) float64 {
	if e.ID == c.controlled {
		return -1
	}
	d := float64(c.viewCenter.Dist(e.Pos))
	if d <= float64(s.cfg.PriorityRadius) {
		return d
	}
	return d + 1e6
}

func (s *Server) sendTo(c *serverClient, env Envelope, now int64) {
	if c.link == nil {
		return
	}
	env.From = 0 // server
	c.link.Send(env, now)
}
