package transport

import (
	"testing"

	"github.com/huchangjun118/entity-state-replication-go/world"
)

func drain(ch *Channel, dst uint64) []Message {
	var out []Message
	for {
		_, msg, ok := ch.Receive(dst)
		if !ok {
			break
		}
		out = append(out, msg)
	}
	return out
}

func TestLossDropsMessages(t *testing.T) {
	ch := New(Config{Loss: 1, Latency: 0, Seed: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 1})
	ch.Step()
	if got := drain(ch, 0); len(got) != 0 {
		t.Fatalf("got %d messages, want 0 (all lost)", len(got))
	}
}

func TestDuplicateCopiesMessages(t *testing.T) {
	ch := New(Config{Duplicate: 1, Latency: 0, Seed: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 1})
	ch.Step()
	got := drain(ch, 0)
	if len(got) != 1 {
		t.Fatalf("got %d messages, want 1 (duplicate arrives later)", len(got))
	}
	// The duplicate copy arrives on a later step.
	for i := 0; i < 5; i++ {
		ch.Step()
	}
	got = drain(ch, 0)
	if len(got) != 1 {
		t.Fatalf("got %d duplicate messages, want 1", len(got))
	}
}

func TestReorderDelaysMessages(t *testing.T) {
	// With reorder=1 every message gets extra delay; latency=0 means base
	// delivery at the next step.
	ch := New(Config{Reorder: 1, Latency: 0, Jitter: 0, Seed: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 1})
	ch.Step()
	if got := drain(ch, 0); len(got) != 0 {
		t.Fatal("message delivered too early (reorder should delay)")
	}
	ch.Step()
	if got := drain(ch, 0); len(got) != 1 {
		t.Fatalf("got %d messages, want 1 after delay", len(got))
	}
}

func TestBandwidthTailDrop(t *testing.T) {
	// Each Ack is 1+uvarint(1)+uvarint(1)=3 bytes. Budget 5 allows one.
	ch := New(Config{Latency: 0, Bandwidth: 5, Seed: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 2})
	ch.Step()
	if got := drain(ch, 0); len(got) != 1 {
		t.Fatalf("got %d messages, want 1 (bandwidth tail-drop)", len(got))
	}
}

func TestPerDestinationDelivery(t *testing.T) {
	ch := New(Config{Latency: 0, Seed: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 1})
	ch.Send(0, 1, &JoinAck{ClientID: 1, Tick: 1})
	ch.Step()
	if got := drain(ch, 0); len(got) != 1 {
		t.Fatalf("server got %d messages, want 1", len(got))
	}
	if got := drain(ch, 1); len(got) != 1 {
		t.Fatalf("client got %d messages, want 1", len(got))
	}
}

func TestMaxInFlightTailDrop(t *testing.T) {
	ch := New(Config{Latency: 0, MaxInFlight: 2, Seed: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 1})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 2})
	ch.Send(1, 0, &Ack{ClientID: 1, Tick: 3}) // drops oldest
	if ch.Pending() > 2 {
		t.Fatalf("pending = %d, want <= 2", ch.Pending())
	}
}

func TestCodecRoundTrip(t *testing.T) {
	msgs := []Message{
		&JoinRequest{ClientID: 42, Name: "abc"},
		&JoinAck{ClientID: 42, EntityID: 7, Tick: 99, ViewRadius: 30},
		&Input{ClientID: 42, Seq: 5, MoveX: 0.5, MoveY: -0.5},
		&Ack{ClientID: 42, Tick: 99},
		&Snapshot{ClientID: 42, Snap: world.Snapshot{
			Tick: 100, BaseTick: 50, AckInputSeq: 8, Full: true,
			Entities: []world.EntitySnapshot{
				{ID: 1, Mask: world.AllFields, Pos: world.Vec2{X: 1, Y: 2}, Vel: world.Vec2{X: 3}, Health: 100},
				{ID: 2, Mask: world.FieldPosition, Pos: world.Vec2{X: 4, Y: 5}},
			},
			Leaves: []world.EntityID{3, 4},
		}},
	}
	for _, m := range msgs {
		data, err := Encode(m)
		if err != nil {
			t.Fatalf("encode %T: %v", m, err)
		}
		dec, err := Decode(data)
		if err != nil {
			t.Fatalf("decode %T: %v", m, err)
		}
		if dec.Type() != m.Type() {
			t.Fatalf("type mismatch: got %v, want %v", dec.Type(), m.Type())
		}
		if dec.Size() != m.Size() {
			t.Fatalf("size mismatch for %T: got %d, want %d", m, dec.Size(), m.Size())
		}
	}
}

func TestSnapshotSizeMatchesEncoding(t *testing.T) {
	s := world.Snapshot{
		Tick: 100, BaseTick: 50, AckInputSeq: 8, Full: false,
		Entities: []world.EntitySnapshot{
			{ID: 1, Mask: world.AllFields, Pos: world.Vec2{X: 1, Y: 2}, Vel: world.Vec2{X: 3}, Health: 100},
		},
		Leaves: []world.EntityID{3},
	}
	m := &Snapshot{ClientID: 42, Snap: s}
	data, _ := Encode(m)
	if len(data) != m.Size() {
		t.Fatalf("encoded len = %d, Size() = %d", len(data), m.Size())
	}
}
