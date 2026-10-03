package client

import (
	"math"
	"testing"

	"github.com/huchangjun118/entity-state-replication-go/server"
	"github.com/huchangjun118/entity-state-replication-go/transport"
	"github.com/huchangjun118/entity-state-replication-go/world"
)

func setup(t *testing.T, ch *transport.Channel) (*world.World, *server.Server, *Client) {
	t.Helper()
	w := world.New(world.DefaultConfig())
	srv := server.New(server.DefaultConfig(), w, ch)
	cl := New(DefaultConfig(), ch, 100, "player")
	return w, srv, cl
}

func step(ch *transport.Channel, srv *server.Server, cl *Client) {
	ch.Step()
	srv.Step()
	cl.Step()
}

func TestJoinFlow(t *testing.T) {
	ch := transport.New(transport.Config{Latency: 1, Seed: 1})
	_, srv, cl := setup(t, ch)
	for i := 0; i < 30; i++ {
		step(ch, srv, cl)
	}
	if cl.State() != StateJoined {
		t.Fatalf("state = %v, want joined", cl.State())
	}
	if cl.EntityID() == 0 {
		t.Fatal("entity id not assigned")
	}
}

func TestNoRenderBeforeBaseline(t *testing.T) {
	ch := transport.New(transport.Config{Latency: 1, Seed: 1})
	_, srv, cl := setup(t, ch)
	// Before joining, Render must return nil (no half-initialized state).
	step(ch, srv, cl)
	if cl.State() == StateJoined {
		t.Fatal("should not be joined yet")
	}
	if ents := cl.Render(0); ents != nil {
		t.Fatal("render before baseline must be nil")
	}
}

func TestBaselineLossRetriesAndJoins(t *testing.T) {
	// All snapshots lost initially; client must retry and eventually join
	// when the channel recovers.
	ch := transport.New(transport.Config{Latency: 1, Loss: 1, Seed: 1})
	w := world.New(world.DefaultConfig())
	srv := server.New(server.DefaultConfig(), w, ch)
	cfg := DefaultConfig()
	cfg.JoinTimeout = 5
	cl := New(cfg, ch, 100, "player")
	for i := 0; i < 20; i++ {
		step(ch, srv, cl)
	}
	if cl.State() == StateJoined {
		t.Fatal("should not join while all packets are lost")
	}
	// Recover the channel.
	ch.SetLoss(0)
	for i := 0; i < 30; i++ {
		step(ch, srv, cl)
	}
	if cl.State() != StateJoined {
		t.Fatalf("state = %v, want joined after recovery", cl.State())
	}
}

func TestPredictionConvergesToServer(t *testing.T) {
	ch := transport.New(transport.Config{Latency: 1, Seed: 1})
	w, srv, cl := setup(t, ch)
	for i := 0; i < 30; i++ {
		step(ch, srv, cl)
	}
	// Move the client entity to the right.
	cl.SetInput(world.Vec2{X: 1})
	for i := 0; i < 40; i++ {
		step(ch, srv, cl)
	}
	// Client's predicted position should match the server's authoritative
	// position (within a small tolerance for prediction lead).
	srvPos, _ := w.State(cl.EntityID())
	clPos := cl.SimPosition()
	if math.Abs(srvPos.Pos.X-clPos.X) > 1.0 {
		t.Fatalf("client pos = %v, server pos = %v, want close", clPos.X, srvPos.Pos.X)
	}
}

func TestPredictionDoesNotBreakInterpolation(t *testing.T) {
	ch := transport.New(transport.Config{Latency: 1, Seed: 1})
	w, srv, cl := setup(t, ch)
	// A remote entity moving back and forth.
	remote := w.Spawn(world.Vec2{X: 15}, 100)
	for i := 0; i < 30; i++ {
		step(ch, srv, cl)
	}
	// Move the remote entity on the server.
	for i := 0; i < 20; i++ {
		st, _ := w.State(remote)
		st.Pos.X -= 2
		w.SetPosition(remote, st.Pos)
		step(ch, srv, cl)
	}
	ents := cl.Render(0)
	var found bool
	for _, e := range ents {
		if e.ID == remote {
			found = true
			if e.Alpha < 1 {
				t.Fatalf("remote entity alpha = %v, want 1 (fully visible)", e.Alpha)
			}
		}
	}
	if !found {
		t.Fatal("remote entity not rendered")
	}
}

func TestEnterExitFade(t *testing.T) {
	ch := transport.New(transport.Config{Latency: 1, Seed: 1})
	w, srv, cl := setup(t, ch)
	// Entity starts far away (outside view).
	remote := w.Spawn(world.Vec2{X: 100}, 100)
	for i := 0; i < 30; i++ {
		step(ch, srv, cl)
	}
	// Move the remote entity into view.
	for i := 0; i < 30; i++ {
		st, _ := w.State(remote)
		st.Pos.X -= 3
		w.SetPosition(remote, st.Pos)
		step(ch, srv, cl)
	}
	// Eventually the entity should be rendered (fading in or visible).
	var found bool
	for i := 0; i < 20; i++ {
		step(ch, srv, cl)
		for _, e := range cl.Render(0) {
			if e.ID == remote {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("entity entering view was never rendered")
	}
}

func TestStaleSnapshotDiscarded(t *testing.T) {
	ch := transport.New(transport.Config{Latency: 1, Seed: 1})
	_, srv, cl := setup(t, ch)
	for i := 0; i < 30; i++ {
		step(ch, srv, cl)
	}
	// Inject a stale (old) snapshot directly; it must not roll back state.
	cl.mu.Lock()
	old := cl.lastApplied
	cl.mu.Unlock()
	cl.receiveSnapshotForTest(world.Snapshot{Tick: old - 1, Full: true})
	cl.mu.Lock()
	if cl.lastApplied != old {
		t.Fatal("stale snapshot changed lastApplied")
	}
	cl.mu.Unlock()
}

func TestFullSnapshotRebuildsView(t *testing.T) {
	v := newViewState()
	// First baseline with two entities.
	v.apply(world.Snapshot{
		Tick: 1, Full: true,
		Entities: []world.EntitySnapshot{
			{ID: 1, Mask: world.AllFields, Pos: world.Vec2{X: 1}},
			{ID: 2, Mask: world.AllFields, Pos: world.Vec2{X: 2}},
		},
	})
	// Second full baseline (re-baseline) with only entity 1.
	v.apply(world.Snapshot{
		Tick: 5, Full: true,
		Entities: []world.EntitySnapshot{
			{ID: 1, Mask: world.AllFields, Pos: world.Vec2{X: 10}},
		},
	})
	ents := v.render(5, 0, 0, 99)
	if len(ents) != 1 {
		t.Fatalf("rendered %d entities, want 1 (stale entity removed)", len(ents))
	}
	if ents[0].ID != 1 || ents[0].Pos.X != 10 {
		t.Fatalf("entity = %+v, want id=1 pos.x=10", ents[0])
	}
}
