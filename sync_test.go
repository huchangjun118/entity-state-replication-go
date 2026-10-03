package sync_test

import (
	"testing"

	"github.com/huchangjun118/entity-state-replication-go/client"
	"github.com/huchangjun118/entity-state-replication-go/server"
	"github.com/huchangjun118/entity-state-replication-go/transport"
	"github.com/huchangjun118/entity-state-replication-go/world"
)

func run(t *testing.T, ch *transport.Channel, srv *server.Server, cl *client.Client, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ch.Step()
		srv.Step()
		cl.Step()
	}
}

func TestSmokeJoinAndSync(t *testing.T) {
	w := world.New(world.DefaultConfig())
	for i := 0; i < 5; i++ {
		w.Spawn(world.Vec2{X: float64(i * 10), Y: 0}, 100)
	}
	ch := transport.New(transport.Config{Latency: 1, Jitter: 0, Seed: 42})
	srv := server.New(server.DefaultConfig(), w, ch)
	cl := client.New(client.DefaultConfig(), ch, 100, "player")

	run(t, ch, srv, cl, 60)

	if cl.State() != client.StateJoined {
		t.Fatalf("client state = %v, want joined", cl.State())
	}
	ents := cl.Render(0)
	if len(ents) == 0 {
		t.Fatal("no entities rendered")
	}
	for _, e := range ents {
		t.Logf("  entity %d pos=%v alpha=%.2f", e.ID, e.Pos, e.Alpha)
	}
	t.Logf("joined: entity=%d lastApplied=%d rendered=%d", cl.EntityID(), cl.LastAppliedTick(), len(ents))
}
