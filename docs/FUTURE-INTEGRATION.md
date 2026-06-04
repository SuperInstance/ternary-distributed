# Future Integration: ternary-distributed

## Current State
Distributed systems primitives with `TernaryNode` (holds ternary state + peers + vector clock), `VectorClock` for causal ordering, gossip protocol for epidemic propagation, partition detection, consensus, and anti-entropy synchronization. All built around {-1, 0, +1} state space.

## Integration Opportunities

### With Room Coordination Across Fleet
Each room is a `TernaryNode`. `VectorClock` tracks room state versions for causal consistency. Gossip protocol propagates room state changes (door openings, agent arrivals) across the fleet without a central coordinator. When rooms go offline, partition detection identifies the split and `SyncStatus` tracks divergence. Anti-entropy sync reconciles when connectivity returns.

### With ternary-cell (Distributed Tissue)
A cell tissue spanning multiple nodes uses `VectorClock` to synchronize tick boundaries. `TernaryNode::peers` are neighboring cells in the tissue, potentially on different physical machines. Gossip propagation carries `TernaryMessenger` signals (Signal/Silence/Suppress) across node boundaries at tissue speed.

### With ternary-protocol
`SyncProtocol::Diff` from ternary-protocol rides on top of the gossip layer. Gossip carries diffs; anti-entropy carries full state reconciliation. `Handshake` occurs when nodes first discover each other via gossip. The combination: gossip for discovery and light sync, ternary-protocol for structured state exchange.

## Potential in Mature Systems
This is the fleet's nervous system. Every room runs a gossip node. The fleet is an epidemic propagation network where room states, agent movements, and skill registrations spread organically. No single point of failure. `VectorClock` ensures causality is preserved even during partitions. The fleet self-heals: anti-entropy catches up any node that was partitioned.

## Cross-Pollination Ideas
- `VectorClock` could be visualized in `ternary-spreadsheet` as a timeline column per room
- Gossip propagation metrics feed into `ternary-entropy` for measuring information spread entropy
- Partition detection connects to `avoidance-cascade` — a partition is a social cascade in the network topology
- `TernaryNode` state could use `ternary-tensor` for multi-dimensional node state beyond single Trit

## Dependencies for Next Steps
- Transport layer (TCP/QUIC/serial) abstraction beneath gossip
- Integration with ternary-room for room-as-node mapping
- Cryptographic authentication for node discovery (prevent rogue nodes)
- Benchmarks for gossip convergence time at fleet scale
