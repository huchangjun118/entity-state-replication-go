package world

// Snapshot is a server -> client replicated world snapshot.
//
// Snapshots carry absolute field values rather than relative deltas: a
// client may safely skip snapshots (loss) or apply them in tick order and
// always converge to the authoritative state at the newest applied tick.
type Snapshot struct {
	Tick        uint64 // server tick this snapshot represents
	BaseTick    uint64 // tick the delta is computed against (0 for baselines)
	AckInputSeq uint64 // highest input seq the server has processed for this client
	Full        bool   // true for join baselines (all visible entities, full fields)

	Entities []EntitySnapshot // entered or updated entities
	Leaves   []EntityID       // entities that left the client's view
}
