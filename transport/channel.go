package transport

import (
	"math/rand"
	"sync"
)

// MsgType identifies the kind of a channel message.
type MsgType uint8

const (
	MsgJoinRequest MsgType = iota + 1
	MsgJoinAck
	MsgInput
	MsgAck
	MsgSnapshot
)

// Message is the interface implemented by every transmissible message.
type Message interface {
	Type() MsgType
	Size() int // serialized size in bytes, used for bandwidth accounting
}

// Config configures the lossy simulation channel.
type Config struct {
	Loss        float64 // probability [0,1] that a delivered message is dropped
	Duplicate   float64 // probability [0,1] that a message is duplicated
	Reorder     float64 // probability [0,1] of extra delivery delay (reordering)
	Latency     int     // base delivery delay in simulation steps
	Jitter      int     // random extra delay in steps
	Bandwidth   int     // bytes delivered per step; 0 = unlimited
	MaxInFlight int     // max queued messages before tail drop; 0 = unlimited
	Seed        int64   // RNG seed for reproducible chaos
}

// DefaultConfig returns a channel with mild, reproducible loss.
func DefaultConfig() Config {
	return Config{
		Loss:        0.05,
		Duplicate:   0.05,
		Reorder:     0.1,
		Latency:     2,
		Jitter:      2,
		Bandwidth:   0,
		MaxInFlight: 256,
		Seed:        1,
	}
}

// Channel is a tick-driven unreliable transport between a server and its
// clients. It intentionally models loss, duplication, reordering, latency
// and finite bandwidth. It is safe for concurrent use.
//
// Messages are delivered into per-destination queues: the server receives
// with dst=0, each client with its own client id.
type Channel struct {
	mu  sync.Mutex
	cfg Config
	rng *rand.Rand

	now      int
	inFlight []*envelope
	out      map[uint64][]*envelope
}

type envelope struct {
	src     uint64
	dst     uint64
	msg     Message
	release int // step at which it becomes deliverable
}

// New creates a channel.
func New(cfg Config) *Channel {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 256
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = 1
	}
	return &Channel{
		cfg: cfg,
		rng: rand.New(rand.NewSource(seed)),
		out: make(map[uint64][]*envelope),
	}
}

// Send enqueues a message from src to dst.
func (c *Channel) Send(src, dst uint64, msg Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Congestion: tail-drop the oldest in-flight message when saturated.
	if c.cfg.MaxInFlight > 0 && len(c.inFlight) >= c.cfg.MaxInFlight {
		c.inFlight = c.inFlight[1:]
	}

	delay := c.cfg.Latency
	if c.cfg.Jitter > 0 {
		delay += c.rng.Intn(c.cfg.Jitter + 1)
	}
	if c.rng.Float64() < c.cfg.Reorder {
		delay += 2 + c.rng.Intn(3)
	}
	c.inFlight = append(c.inFlight, &envelope{
		src: src, dst: dst, msg: msg,
		release: c.now + delay,
	})
}

// Step advances the channel by one simulation step, applying latency,
// loss, duplication, reordering and bandwidth limits.
func (c *Channel) Step() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now++

	var due, kept []*envelope
	for _, e := range c.inFlight {
		if e.release <= c.now {
			due = append(due, e)
		} else {
			kept = append(kept, e)
		}
	}
	c.inFlight = kept

	// Shuffle due messages so bandwidth tail-drop is not order-biased.
	c.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })

	used := 0
	var deferred []*envelope
	for _, e := range due {
		// Loss: the network drops the message.
		if c.rng.Float64() < c.cfg.Loss {
			continue
		}
		// Duplication: a copy wanders off with extra delay.
		if c.rng.Float64() < c.cfg.Duplicate {
			dup := *e
			dup.release = c.now + 2 + c.rng.Intn(2)
			c.inFlight = append(c.inFlight, &dup)
		}
		// Bandwidth: the first due message always gets through (a large
		// packet is not dropped merely because it exceeds the per-step
		// budget); subsequent messages that overflow the budget are
		// deferred to the next step (backpressure) rather than dropped.
		size := e.msg.Size()
		if c.cfg.Bandwidth > 0 && used+size > c.cfg.Bandwidth && used > 0 {
			deferred = append(deferred, e)
			continue
		}
		used += size
		c.out[e.dst] = append(c.out[e.dst], e)
	}
	// Re-enqueue deferred messages for the next step.
	for _, e := range deferred {
		e.release = c.now + 1
		c.inFlight = append(c.inFlight, e)
	}
	// Congestion: if the in-flight queue saturated, tail-drop the oldest.
	if c.cfg.MaxInFlight > 0 && len(c.inFlight) > c.cfg.MaxInFlight {
		drop := len(c.inFlight) - c.cfg.MaxInFlight
		c.inFlight = c.inFlight[drop:]
	}
}

// Receive pops the next delivered message for destination dst.
func (c *Channel) Receive(dst uint64) (src uint64, msg Message, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.out[dst]
	if len(q) == 0 {
		return 0, nil, false
	}
	e := q[0]
	c.out[dst] = q[1:]
	return e.src, e.msg, true
}

// SetLoss updates the loss probability at runtime (used by tests).
func (c *Channel) SetLoss(loss float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.Loss = loss
}

// Pending reports how many messages are still in flight.
func (c *Channel) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.inFlight)
	for _, q := range c.out {
		n += len(q)
	}
	return n
}
