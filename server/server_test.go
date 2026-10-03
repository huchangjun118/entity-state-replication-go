package server

import (
	"testing"

	"github.com/huchangjun118/entity-state-replication-go/transport"
	"github.com/huchangjun118/entity-state-replication-go/world"
)

func drain(ch *transport.Channel, dst uint64) []transport.Message {
	var out []transport.Message
	for {
		_, msg, ok := ch.Receive(dst)
		if !ok {
			break
		}
		out = append(out, msg)
	}
	return out
}

func join(t *testing.T, ch *transport.Channel, srv *Server, clientID uint64) uint64 {
	t.Helper()
	ch.Send(clientID, 0, &transport.JoinRequest{ClientID: clientID, Name: "p"})
	ch.Step()
	srv.Step()
	ch.Step()
	for _, m := range drain(ch, clientID) {
		if a, ok := m.(*transport.JoinAck); ok {
			return uint64(a.EntityID)
		}
	}
	t.Fatal("no join ack")
	return 0
}

func TestDuplicateInputHasNoEffect(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)
	ent := join(t, ch, srv, 1)

	// Send the same input seq twice.
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 1, MoveX: 1})
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 1, MoveX: 1})
	ch.Step()
	srv.Step()
	st1, _ := w.State(world.EntityID(ent))

	// Send a newer input.
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 2, MoveX: 1})
	ch.Step()
	srv.Step()
	st2, _ := w.State(world.EntityID(ent))

	if st2.Pos.X <= st1.Pos.X {
		t.Fatalf("new input should move entity: %v -> %v", st1.Pos.X, st2.Pos.X)
	}
}

func TestExpiredInputHasNoEffect(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)
	ent := join(t, ch, srv, 1)

	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 5, MoveX: 1})
	ch.Step()
	srv.Step()
	st1, _ := w.State(world.EntityID(ent))

	// An expired (lower seq) input with a different direction must be ignored.
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 2, MoveX: 0})
	ch.Step()
	srv.Step()
	st2, _ := w.State(world.EntityID(ent))

	if st2.Vel.X != st1.Vel.X {
		t.Fatal("expired input changed velocity")
	}
}

func TestInputForUnknownSessionIgnored(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)
	// No join; input from unknown client must not panic or create state.
	ch.Send(99, 0, &transport.Input{ClientID: 99, Seq: 1, MoveX: 1})
	ch.Step()
	srv.Step()
}

func TestDeterministicInputOrder(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)
	join(t, ch, srv, 1)
	join(t, ch, srv, 2)

	// Both clients move; order within a tick must be (clientID, seq).
	ch.Send(2, 0, &transport.Input{ClientID: 2, Seq: 1, MoveX: 1})
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 1, MoveX: 1})
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 2, MoveX: 1})
	ch.Step()
	srv.Step()

	s1 := srv.Session(1)
	s2 := srv.Session(2)
	if s1.lastInputSeq != 2 || s2.lastInputSeq != 1 {
		t.Fatalf("input seqs: c1=%d c2=%d, want 2/1", s1.lastInputSeq, s2.lastInputSeq)
	}
}

func TestBaselineRetransmittedUntilAcked(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)
	join(t, ch, srv, 1)

	// No ack is ever sent; the server must keep retransmitting full snapshots.
	for i := 0; i < 4; i++ {
		ch.Step()
		srv.Step()
	}
	ch.Step()
	sess := srv.Session(1)
	if sess.joined {
		t.Fatal("session should not be joined without ack")
	}
	if sess.ackedTick != 0 {
		t.Fatal("ackedTick should stay 0")
	}
	var full int
	for _, m := range drain(ch, 1) {
		if snap, ok := m.(*transport.Snapshot); ok && snap.Snap.Full {
			full++
		}
	}
	if full < 4 {
		t.Fatalf("got %d full snapshots, want >= 4 (retransmit)", full)
	}
}

func TestDeltaSnapshotContainsOnlyChanges(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)
	ent := join(t, ch, srv, 1)

	// Ack the baseline.
	ch.Send(1, 0, &transport.Ack{ClientID: 1, Tick: w.Tick()})
	ch.Step()
	srv.Step()

	// Move the entity.
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 1, MoveX: 1})
	ch.Step()
	srv.Step()
	ch.Step()

	var snap *transport.Snapshot
	for _, m := range drain(ch, 1) {
		if s, ok := m.(*transport.Snapshot); ok {
			snap = s
		}
	}
	if snap == nil {
		t.Fatal("no snapshot sent")
	}
	if snap.Snap.Full {
		t.Fatal("expected delta snapshot after baseline ack")
	}
	if len(snap.Snap.Entities) != 1 {
		t.Fatalf("delta entities = %d, want 1 (only own entity changed)", len(snap.Snap.Entities))
	}
	if snap.Snap.Entities[0].ID != world.EntityID(ent) {
		t.Fatalf("delta entity = %d, want %d", snap.Snap.Entities[0].ID, ent)
	}
}

func TestDeltaRebuildsFullState(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)

	// Join and capture the baseline snapshot.
	ch.Send(1, 0, &transport.JoinRequest{ClientID: 1, Name: "p"})
	ch.Step()
	srv.Step()
	ch.Step()
	var baseline *transport.Snapshot
	for _, m := range drain(ch, 1) {
		if s, ok := m.(*transport.Snapshot); ok {
			baseline = s
		}
	}
	if baseline == nil || !baseline.Snap.Full {
		t.Fatal("expected full baseline snapshot")
	}

	// Ack baseline at tick 1.
	ch.Send(1, 0, &transport.Ack{ClientID: 1, Tick: 1})
	ch.Step()
	srv.Step()
	ch.Step()
	// Move own entity and spawn another.
	ch.Send(1, 0, &transport.Input{ClientID: 1, Seq: 1, MoveX: 1})
	ch.Step()
	srv.Step()
	ch.Step()
	w.Spawn(world.Vec2{X: 5}, 100)
	ch.Step()
	srv.Step()
	ch.Step()

	// Reconstruct the client's visible state from baseline + deltas.
	view := map[world.EntityID]world.EntityState{}
	apply := func(snap world.Snapshot) {
		for _, e := range snap.Entities {
			st := view[e.ID]
			st.ID = e.ID
			if e.Mask&world.FieldPosition != 0 {
				st.Pos = e.Pos
			}
			if e.Mask&world.FieldVelocity != 0 {
				st.Vel = e.Vel
			}
			if e.Mask&world.FieldHealth != 0 {
				st.Health = e.Health
			}
			view[e.ID] = st
		}
		for _, id := range snap.Leaves {
			delete(view, id)
		}
	}
	apply(baseline.Snap)
	for _, m := range drain(ch, 1) {
		if s, ok := m.(*transport.Snapshot); ok {
			apply(s.Snap)
		}
	}
	// The reconstructed view must match the server's current visible set.
	center, _ := w.State(world.EntityID(1))
	current := w.Visible(center.Pos, srv.cfg.ViewRadius)
	if len(view) != len(current) {
		t.Fatalf("rebuilt %d entities, want %d", len(view), len(current))
	}
	for _, cur := range current {
		got := view[cur.ID]
		if got != cur {
			t.Fatalf("entity %d: rebuilt %+v, want %+v", cur.ID, got, cur)
		}
	}
}

func TestReBaselineAfterHistoryEviction(t *testing.T) {
	cfg := world.DefaultConfig()
	cfg.HistoryLen = 4 // evict quickly
	w := world.New(cfg)
	ch := transport.New(transport.Config{Latency: 0, Seed: 1})
	srv := New(DefaultConfig(), w, ch)
	join(t, ch, srv, 1)

	// Ack baseline at tick 1.
	ch.Send(1, 0, &transport.Ack{ClientID: 1, Tick: 1})
	ch.Step()
	srv.Step()

	// Advance well beyond the history window without acking.
	for i := 0; i < 10; i++ {
		ch.Step()
		srv.Step()
	}
	// The next snapshot should be a full re-baseline.
	var gotFull bool
	for _, m := range drain(ch, 1) {
		if s, ok := m.(*transport.Snapshot); ok && s.Snap.Full {
			gotFull = true
		}
	}
	if !gotFull {
		t.Fatal("expected full re-baseline after history eviction")
	}
}
