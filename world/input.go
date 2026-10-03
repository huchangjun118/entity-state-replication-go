package world

// Input is a client movement input sampled at the client.
// Seq is a per-client monotonically increasing sequence number used for
// server-side validation and client-side prediction reconciliation.
type Input struct {
	Seq  uint64
	Move Vec2 // desired movement direction, length <= 1
}
