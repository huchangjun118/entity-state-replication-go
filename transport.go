package esr

import "math/rand"

// MessageKind identifies the payload carried by an Envelope.
type MessageKind uint8

const (
	MsgHello           MessageKind = iota + 1 // client -> server: request join
	MsgWelcome                                // server -> client: assigned ClientID + controlled entity
	MsgBaseline                               // server -> client: full snapshot
	MsgBaselineRequest                        // client -> server: (re)request full snapshot
	MsgDelta                                  // server -> client: incremental update
	MsgInput                                  // client -> server: player input
	MsgAck                                    // client -> server: acknowledge snapshot tick
	MsgGoodbye                                // client -> server: leave
)

// Envelope is a single network message. Only the fields matching Kind
// carry meaningful data.
type Envelope struct {
	From, To ClientID
	Kind     MessageKind

	ClientID ClientID    // MsgWelcome
	EntityID EntityID    // MsgWelcome
	Nonce    uint64      // MsgHello: makes join idempotent
	Cfg      WorldConfig // MsgWelcome: simulation parameters for prediction
	Delta    *Delta      // MsgBaseline / MsgDelta
	Input    Input       // MsgInput
	AckTick  Tick        // MsgAck
}

// LinkConfig describes an unreliable link.
type LinkConfig struct {
	Loss      float64 // probability [0,1] of dropping a packet
	Duplicate float64 // probability [0,1] of sending an extra copy
	MinDelay  int64   // minimum delivery delay (ms)
	MaxDelay  int64   // maximum delivery delay (ms); >= MinDelay
	Seed      int64
}

type inFlight struct {
	env       Envelope
	deliverAt int64
}

// Link is a one-directional unreliable channel with loss, duplication
// and variable delay (hence reordering). Delivery is driven manually by
// calling Recv with the current time, which keeps tests deterministic.
type Link struct {
	cfg   LinkConfig
	rng   *rand.Rand
	queue []inFlight
}

func NewLink(cfg LinkConfig) *Link {
	if cfg.MaxDelay < cfg.MinDelay {
		cfg.MaxDelay = cfg.MinDelay
	}
	return &Link{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed))}
}

// Send queues a message for delivery. The message may be dropped,
// duplicated and/or delayed according to the link config.
func (l *Link) Send(env Envelope, now int64) {
	copies := 1
	if l.rng.Float64() < l.cfg.Duplicate {
		copies++
	}
	for i := 0; i < copies; i++ {
		if l.rng.Float64() < l.cfg.Loss {
			continue
		}
		delay := l.cfg.MinDelay
		if span := l.cfg.MaxDelay - l.cfg.MinDelay; span > 0 {
			delay += l.rng.Int63n(span + 1)
		}
		l.queue = append(l.queue, inFlight{env: env, deliverAt: now + delay})
	}
}

// Recv returns all messages due at or before now (ms) and removes them.
// Messages due at the same time are returned in random order to model
// reordering on the wire.
func (l *Link) Recv(now int64) []Envelope {
	var due []Envelope
	rest := l.queue[:0]
	for _, f := range l.queue {
		if f.deliverAt <= now {
			due = append(due, f.env)
		} else {
			rest = append(rest, f)
		}
	}
	l.queue = rest
	l.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	return due
}

// Pending returns the number of in-flight messages.
func (l *Link) Pending() int { return len(l.queue) }

// Flush drops all in-flight messages.
func (l *Link) Flush() { l.queue = l.queue[:0] }
