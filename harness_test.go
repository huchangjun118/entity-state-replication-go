package esr

// harness wires one server and one or more clients through simulated
// unreliable links, driven by manual time steps.
type harness struct {
	srv *Server
	now int64
	dt  int64

	clients map[ClientID]*clientEnd
	nextID  ClientID
}

type clientEnd struct {
	cli *Client
	c2s *Link
	s2c *Link
	id  ClientID // 0 until known
}

func newHarness(srvCfg ServerConfig) *harness {
	return &harness{
		srv:     NewServer(srvCfg),
		dt:      50,
		clients: make(map[ClientID]*clientEnd),
		nextID:  1,
	}
}

// addClient creates a client with its own pair of links and queues a
// join. linkCfg may be adjusted per client to model loss etc.
func (h *harness) addClient(cliCfg ClientConfig, linkCfg LinkConfig) *clientEnd {
	end := &clientEnd{
		cli: NewClient(cliCfg),
		c2s: NewLink(linkCfg),
		s2c: NewLink(linkCfg),
		id:  h.nextID,
	}
	h.nextID++
	h.srv.ConnectLink(end.id, end.s2c)
	end.cli.ConnectLink(end.c2s) // client->server direction
	h.clients[end.id] = end
	end.cli.Join()
	return end
}

// pump delivers all due messages in both directions and flushes client
// outboxes into the network.
func (h *harness) pump() {
	for _, end := range h.clients {
		for _, env := range end.cli.Outbox() {
			end.c2s.Send(env, h.now)
		}
		for _, env := range end.c2s.Recv(h.now) {
			h.srv.Handle(env, h.now)
		}
		for _, env := range end.s2c.Recv(h.now) {
			end.cli.Handle(env, h.now)
		}
	}
}

// step advances time by one frame: clients update, the server ticks,
// and messages are pumped (twice, so responses to freshly delivered
// messages also move).
func (h *harness) step() {
	h.now += h.dt
	for _, end := range h.clients {
		end.cli.Update(h.now)
	}
	h.pump()
	h.srv.Tick(h.now)
	h.pump()
}

func (h *harness) steps(n int) {
	for i := 0; i < n; i++ {
		h.step()
	}
}

func defaultServerConfig() ServerConfig {
	return ServerConfig{
		World:          WorldConfig{MoveSpeed: 1, MaxHP: 10, Bounds: 100},
		ViewRadius:     50,
		MaxUnacked:     5,
		BytesPerTick:   0,
		PriorityRadius: 20,
	}
}

func defaultClientConfig() ClientConfig {
	return ClientConfig{InterpDelayMs: 100, CorrectionRate: 0.2, BaselineRetryMs: 200}
}

func cleanLink(seed int64) LinkConfig {
	return LinkConfig{MinDelay: 10, MaxDelay: 10, Seed: seed}
}
