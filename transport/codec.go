package transport

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/huchangjun118/entity-state-replication-go/world"
)

// Binary layout: a leading MsgType byte followed by type-specific fields.
// Integers are unsigned varints; floats are 8-byte little-endian; strings
// are length-prefixed with a 2-byte little-endian length.

var errBadMessage = errors.New("transport: malformed message")

type writer struct {
	b []byte
}

func (w *writer) uvarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	w.b = append(w.b, buf[:n]...)
}

func (w *writer) u8(v uint8) { w.b = append(w.b, v) }

func (w *writer) f64(v float64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	w.b = append(w.b, buf[:]...)
}

func (w *writer) str(s string) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(len(s)))
	w.b = append(w.b, buf[:]...)
	w.b = append(w.b, s...)
}

type reader struct {
	b   []byte
	pos int
}

func (r *reader) remain() int { return len(r.b) - r.pos }

func (r *reader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		return 0, errBadMessage
	}
	r.pos += n
	return v, nil
}

func (r *reader) u8() (uint8, error) {
	if r.pos >= len(r.b) {
		return 0, errBadMessage
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}

func (r *reader) f64() (float64, error) {
	if r.pos+8 > len(r.b) {
		return 0, errBadMessage
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.b[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *reader) str() (string, error) {
	if r.pos+2 > len(r.b) {
		return "", errBadMessage
	}
	n := int(binary.LittleEndian.Uint16(r.b[r.pos:]))
	r.pos += 2
	if r.pos+n > len(r.b) {
		return "", errBadMessage
	}
	s := string(r.b[r.pos : r.pos+n])
	r.pos += n
	return s, nil
}

func writeSnapshot(w *writer, s world.Snapshot) {
	w.uvarint(s.Tick)
	w.uvarint(s.BaseTick)
	w.uvarint(s.AckInputSeq)
	full := uint8(0)
	if s.Full {
		full = 1
	}
	w.u8(full)
	w.uvarint(uint64(len(s.Entities)))
	w.uvarint(uint64(len(s.Leaves)))
	for _, e := range s.Entities {
		w.uvarint(uint64(e.ID))
		w.u8(uint8(e.Mask))
		if e.Mask&world.FieldPosition != 0 {
			w.f64(e.Pos.X)
			w.f64(e.Pos.Y)
		}
		if e.Mask&world.FieldVelocity != 0 {
			w.f64(e.Vel.X)
			w.f64(e.Vel.Y)
		}
		if e.Mask&world.FieldHealth != 0 {
			w.uvarint(uint64(e.Health))
		}
	}
	for _, id := range s.Leaves {
		w.uvarint(uint64(id))
	}
}

func readSnapshot(r *reader) (world.Snapshot, error) {
	var s world.Snapshot
	var err error
	if s.Tick, err = r.uvarint(); err != nil {
		return s, err
	}
	if s.BaseTick, err = r.uvarint(); err != nil {
		return s, err
	}
	if s.AckInputSeq, err = r.uvarint(); err != nil {
		return s, err
	}
	full, err := r.u8()
	if err != nil {
		return s, err
	}
	s.Full = full != 0
	nEnt, err := r.uvarint()
	if err != nil {
		return s, err
	}
	nLeave, err := r.uvarint()
	if err != nil {
		return s, err
	}
	if nEnt > 1<<20 || nLeave > 1<<20 {
		return s, errBadMessage
	}
	s.Entities = make([]world.EntitySnapshot, 0, nEnt)
	for i := uint64(0); i < nEnt; i++ {
		id, err := r.uvarint()
		if err != nil {
			return s, err
		}
		mask, err := r.u8()
		if err != nil {
			return s, err
		}
		e := world.EntitySnapshot{ID: world.EntityID(id), Mask: world.FieldMask(mask)}
		if e.Mask&world.FieldPosition != 0 {
			if e.Pos.X, err = r.f64(); err != nil {
				return s, err
			}
			if e.Pos.Y, err = r.f64(); err != nil {
				return s, err
			}
		}
		if e.Mask&world.FieldVelocity != 0 {
			if e.Vel.X, err = r.f64(); err != nil {
				return s, err
			}
			if e.Vel.Y, err = r.f64(); err != nil {
				return s, err
			}
		}
		if e.Mask&world.FieldHealth != 0 {
			h, err := r.uvarint()
			if err != nil {
				return s, err
			}
			e.Health = int(h)
		}
		s.Entities = append(s.Entities, e)
	}
	s.Leaves = make([]world.EntityID, 0, nLeave)
	for i := uint64(0); i < nLeave; i++ {
		id, err := r.uvarint()
		if err != nil {
			return s, err
		}
		s.Leaves = append(s.Leaves, world.EntityID(id))
	}
	return s, nil
}

// Encode serializes a message into bytes.
func Encode(msg Message) ([]byte, error) {
	w := &writer{}
	w.u8(uint8(msg.Type()))
	switch m := msg.(type) {
	case *JoinRequest:
		w.uvarint(m.ClientID)
		w.str(m.Name)
	case *JoinAck:
		w.uvarint(m.ClientID)
		w.uvarint(uint64(m.EntityID))
		w.uvarint(m.Tick)
		w.f64(m.ViewRadius)
	case *Input:
		w.uvarint(m.ClientID)
		w.uvarint(m.Seq)
		w.f64(m.MoveX)
		w.f64(m.MoveY)
	case *Ack:
		w.uvarint(m.ClientID)
		w.uvarint(m.Tick)
	case *Snapshot:
		w.uvarint(m.ClientID)
		writeSnapshot(w, m.Snap)
	default:
		return nil, errBadMessage
	}
	return w.b, nil
}

// Decode deserializes a message produced by Encode.
func Decode(data []byte) (Message, error) {
	if len(data) == 0 {
		return nil, errBadMessage
	}
	r := &reader{b: data, pos: 1}
	switch MsgType(data[0]) {
	case MsgJoinRequest:
		id, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		name, err := r.str()
		if err != nil {
			return nil, err
		}
		return &JoinRequest{ClientID: id, Name: name}, nil
	case MsgJoinAck:
		id, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		eid, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		tick, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		vr, err := r.f64()
		if err != nil {
			return nil, err
		}
		return &JoinAck{ClientID: id, EntityID: world.EntityID(eid), Tick: tick, ViewRadius: vr}, nil
	case MsgInput:
		id, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		seq, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		mx, err := r.f64()
		if err != nil {
			return nil, err
		}
		my, err := r.f64()
		if err != nil {
			return nil, err
		}
		return &Input{ClientID: id, Seq: seq, MoveX: mx, MoveY: my}, nil
	case MsgAck:
		id, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		tick, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		return &Ack{ClientID: id, Tick: tick}, nil
	case MsgSnapshot:
		id, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		snap, err := readSnapshot(r)
		if err != nil {
			return nil, err
		}
		return &Snapshot{ClientID: id, Snap: snap}, nil
	default:
		return nil, errBadMessage
	}
}
