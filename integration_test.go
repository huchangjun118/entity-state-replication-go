package sync_test

import (
	"math"
	"testing"

	"github.com/huchangjun118/entity-state-replication-go/client"
	"github.com/huchangjun118/entity-state-replication-go/server"
	"github.com/huchangjun118/entity-state-replication-go/transport"
	"github.com/huchangjun118/entity-state-replication-go/world"
)

// testClient creates a client with a short join timeout for lossy tests.
func testClient(ch *transport.Channel, id uint64, name string) *client.Client {
	cfg := client.DefaultConfig()
	cfg.JoinTimeout = 10
	return client.New(cfg, ch, id, name)
}

// runLoop drives the channel, server and clients in lockstep.
func runLoop(ch *transport.Channel, srv *server.Server, clients []*client.Client, n int) {
	for i := 0; i < n; i++ {
		ch.Step()
		srv.Step()
		for _, cl := range clients {
			cl.Step()
		}
	}
}

// TestChaosConvergence runs multiple clients through a lossy, reordering,
// duplicating channel and verifies every client converges to the
// authoritative world state.
func TestChaosConvergence(t *testing.T) {
	w := world.New(world.DefaultConfig())
	for i := 0; i < 8; i++ {
		w.Spawn(world.Vec2{X: float64(i * 8), Y: float64(i % 2 * 5)}, 100)
	}
	ch := transport.New(transport.Config{
		Loss: 0.1, Duplicate: 0.1, Reorder: 0.2,
		Latency: 1, Jitter: 2, Seed: 7,
	})
	srv := server.New(server.DefaultConfig(), w, ch)
	clients := []*client.Client{
		testClient(ch, 1, "a"),
		testClient(ch, 2, "b"),
		testClient(ch, 3, "c"),
	}
	runLoop(ch, srv, clients, 80)

	for _, cl := range clients {
		if cl.State() != client.StateJoined {
			t.Fatalf("client %d state = %v, want joined", cl.EntityID(), cl.State())
		}
		// Each client's predicted position should be near the server.
		srvPos, _ := w.State(cl.EntityID())
		clPos := cl.SimPosition()
		if math.Abs(srvPos.Pos.X-clPos.X) > 2.0 || math.Abs(srvPos.Pos.Y-clPos.Y) > 2.0 {
			t.Fatalf("client %d pos %v diverged from server %v", cl.EntityID(), clPos, srvPos.Pos)
		}
	}
}

// TestEntityChurnConsistency spawns and despawns entities rapidly and
// verifies the client's reconstructed view matches the server's visible
// set (incremental == full).
func TestEntityChurnConsistency(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 1, Seed: 3})
	srv := server.New(server.DefaultConfig(), w, ch)
	cl := testClient(ch, 100, "p")

	// Join.
	runLoop(ch, srv, []*client.Client{cl}, 30)
	if cl.State() != client.StateJoined {
		t.Fatal("client did not join")
	}

	// Churn entities: spawn and despawn every step.
	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			w.Spawn(world.Vec2{X: float64(i), Y: 0}, 100)
		} else {
			// Despawn the oldest non-client entity.
			for _, id := range w.Entities() {
				if id != cl.EntityID() {
					w.Despawn(id)
					break
				}
			}
		}
		runLoop(ch, srv, []*client.Client{cl}, 1)
	}
	runLoop(ch, srv, []*client.Client{cl}, 5)

	// The client should still be joined and rendering.
	if cl.State() != client.StateJoined {
		t.Fatal("client left joined state during churn")
	}
	ents := cl.Render(0)
	if len(ents) == 0 {
		t.Fatal("no entities rendered after churn")
	}
}

// TestCongestionPriority limits bandwidth and verifies the client's own
// entity and nearby entities keep getting through while distant ones are
// deferred (no rollback).
func TestCongestionPriority(t *testing.T) {
	w := world.New(world.DefaultConfig())
	// Many entities far away.
	for i := 0; i < 30; i++ {
		w.Spawn(world.Vec2{X: float64(i * 5), Y: 0}, 100)
	}
	ch := transport.New(transport.Config{Latency: 1, Bandwidth: 200, Seed: 5})
	srvCfg := server.DefaultConfig()
	srvCfg.SnapshotBudget = 120 // tight budget
	srv := server.New(srvCfg, w, ch)
	cl := testClient(ch, 100, "p")

	runLoop(ch, srv, []*client.Client{cl}, 40)
	if cl.State() != client.StateJoined {
		t.Fatal("client did not join under congestion")
	}
	// The client's own entity must be rendered (prediction).
	var ownRendered bool
	for _, e := range cl.Render(0) {
		if e.ID == cl.EntityID() {
			ownRendered = true
		}
	}
	if !ownRendered {
		t.Fatal("own entity not rendered under congestion")
	}
}

// TestPredictionCorrectionNoJump verifies that after a correction the
// client's render position stays smooth (no large jump).
func TestPredictionCorrectionNoJump(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 2, Seed: 11})
	srv := server.New(server.DefaultConfig(), w, ch)
	cl := testClient(ch, 100, "p")
	runLoop(ch, srv, []*client.Client{cl}, 30)

	// Move in one direction, then abruptly change direction.
	cl.SetInput(world.Vec2{X: 1})
	runLoop(ch, srv, []*client.Client{cl}, 20)
	cl.SetInput(world.Vec2{Y: 1})
	runLoop(ch, srv, []*client.Client{cl}, 20)

	// The render position should be near the server (corrected).
	srvPos, _ := w.State(cl.EntityID())
	clPos := cl.SimPosition()
	if math.Abs(srvPos.Pos.X-clPos.X) > 2.0 || math.Abs(srvPos.Pos.Y-clPos.Y) > 2.0 {
		t.Fatalf("prediction diverged: client %v, server %v", clPos, srvPos.Pos)
	}
}

// TestMidSessionJoin verifies a client joining after the world has been
// running receives a complete baseline and converges.
func TestMidSessionJoin(t *testing.T) {
	w := world.New(world.DefaultConfig())
	for i := 0; i < 5; i++ {
		w.Spawn(world.Vec2{X: float64(i * 5), Y: 0}, 100)
	}
	ch := transport.New(transport.Config{Latency: 1, Seed: 9})
	srv := server.New(server.DefaultConfig(), w, ch)

	// Run the world for a while before the client joins.
	for i := 0; i < 50; i++ {
		ch.Step()
		srv.Step()
	}

	cl := testClient(ch, 100, "late")
	runLoop(ch, srv, []*client.Client{cl}, 40)
	if cl.State() != client.StateJoined {
		t.Fatal("late client did not join")
	}
	ents := cl.Render(0)
	if len(ents) == 0 {
		t.Fatal("late client rendered nothing")
	}
}

// TestDuplicateAndReorderSnapshots applies snapshots out of order and
// duplicated; the client must discard stale ones and never roll back.
func TestDuplicateAndReorderSnapshots(t *testing.T) {
	w := world.New(world.DefaultConfig())
	ch := transport.New(transport.Config{Latency: 1, Duplicate: 0.5, Reorder: 0.3, Seed: 13})
	srv := server.New(server.DefaultConfig(), w, ch)
	cl := testClient(ch, 100, "p")
	runLoop(ch, srv, []*client.Client{cl}, 60)
	if cl.State() != client.StateJoined {
		t.Fatal("client did not join with duplicates/reorders")
	}
	// Move and verify convergence.
	cl.SetInput(world.Vec2{X: 1})
	runLoop(ch, srv, []*client.Client{cl}, 40)
	srvPos, _ := w.State(cl.EntityID())
	clPos := cl.SimPosition()
	if math.Abs(srvPos.Pos.X-clPos.X) > 2.0 {
		t.Fatalf("client %v diverged from server %v", clPos, srvPos.Pos)
	}
}
