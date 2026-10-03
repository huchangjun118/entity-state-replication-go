package transport

import (
	"encoding/binary"

	"github.com/huchangjun118/entity-state-replication-go/world"
)

// JoinRequest is sent by a client to enter the session.
type JoinRequest struct {
	ClientID uint64
	Name     string
}

// JoinAck is the server's response to a join request.
type JoinAck struct {
	ClientID   uint64
	EntityID   world.EntityID
	Tick       uint64
	ViewRadius float64
}

// Input carries a client's movement input for a given sequence.
type Input struct {
	ClientID uint64
	Seq      uint64
	MoveX    float64
	MoveY    float64
}

// Ack acknowledges that a client has applied a snapshot tick.
type Ack struct {
	ClientID uint64
	Tick     uint64
}

// Snapshot wraps a world snapshot for a specific client.
type Snapshot struct {
	ClientID uint64
	Snap     world.Snapshot
}

func (m *JoinRequest) Type() MsgType { return MsgJoinRequest }
func (m *JoinAck) Type() MsgType     { return MsgJoinAck }
func (m *Input) Type() MsgType       { return MsgInput }
func (m *Ack) Type() MsgType         { return MsgAck }
func (m *Snapshot) Type() MsgType    { return MsgSnapshot }

func uvarintSize(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

func snapshotSize(s world.Snapshot) int {
	n := uvarintSize(s.Tick) + uvarintSize(s.BaseTick) + uvarintSize(s.AckInputSeq)
	n += 1 // full flag
	n += uvarintSize(uint64(len(s.Entities))) + uvarintSize(uint64(len(s.Leaves)))
	for _, e := range s.Entities {
		n += uvarintSize(uint64(e.ID)) + 1 // id + mask
		if e.Mask&world.FieldPosition != 0 {
			n += 16
		}
		if e.Mask&world.FieldVelocity != 0 {
			n += 16
		}
		if e.Mask&world.FieldHealth != 0 {
			n += uvarintSize(uint64(e.Health))
		}
	}
	for _, id := range s.Leaves {
		n += uvarintSize(uint64(id))
	}
	return n
}

// Size reports the serialized size in bytes.
func (m *JoinRequest) Size() int {
	return 1 + uvarintSize(m.ClientID) + 2 + len(m.Name)
}

func (m *JoinAck) Size() int {
	return 1 + uvarintSize(m.ClientID) + uvarintSize(uint64(m.EntityID)) +
		uvarintSize(m.Tick) + 8
}

func (m *Input) Size() int {
	return 1 + uvarintSize(m.ClientID) + uvarintSize(m.Seq) + 16
}

func (m *Ack) Size() int {
	return 1 + uvarintSize(m.ClientID) + uvarintSize(m.Tick)
}

func (m *Snapshot) Size() int {
	return 1 + uvarintSize(m.ClientID) + snapshotSize(m.Snap)
}
