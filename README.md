# ternary-distributed: Distributed systems primitives for {-1, 0, +1} state spaces

A Rust library providing node management, gossip propagation, vector clocks, partition detection, Paxos-like consensus, and anti-entropy synchronization — all designed around ternary values.

## Why This Exists

Standard distributed systems tools treat state as binary (up/down) or arbitrary bytes. Some protocols genuinely need three-valued logic: accept/reject/abstain in voting, leader/follower/candidate in consensus, healthy/degraded/failed in health checks. This library builds those primitives natively on ternary values, eliminating the impedance mismatch of encoding three states into boolean pairs or enums.

## Core Concepts

**Ternary value** — One of three states: Negative (-1), Zero (0), or Positive (+1). Called a *trit* (ternary digit), analogous to a bit in binary.

**Gossip protocol** — An epidemic-style dissemination algorithm. Each node shares its state with random peers every round. Over time, the entire cluster converges to a consistent state.

**Vector clock** — A map from node IDs to monotonically increasing counters. Used to determine causal ordering: whether event A happened before event B, or they are concurrent.

**Partition detector** — Tracks node liveness via heartbeats and detects when a network split leaves less than a quorum of nodes reachable.

**Consensus protocol** — A simplified Paxos variant where acceptors vote with ternary values (Negative/Abstain/Positive). The final decision is the sum of accepted votes.

**Anti-entropy sync** — A background repair mechanism. Compares vector clocks between node pairs and reconciles divergent state using a merge strategy.

## Quick Start

```toml
# Cargo.toml
[dependencies]
ternary-distributed = "0.1"
```

```rust
use ternary_distributed::*;

// Create a 3-node cluster
let mut gossip = GossipProtocol::new();
let mut n1 = TernaryNode::with_state(1, Trit::Pos);
n1.add_peer(2);
n1.add_peer(3);
let mut n2 = TernaryNode::new(2);
n2.add_peer(1);
n2.add_peer(3);
let mut n3 = TernaryNode::new(3);
n3.add_peer(1);
n3.add_peer(2);

gossip.add_node(n1);
gossip.add_node(n2);
gossip.add_node(n3);

let rounds = gossip.run_until_converged(10);
println!("Converged in {} rounds", rounds);
assert!(gossip.is_converged());
```

## API Overview

| Type | Description |
|------|-------------|
| `Trit` | A ternary value: `Neg`, `Zero`, or `Pos` |
| `TernaryNode` | A node holding ternary state, a peer set, and a vector clock |
| `GossipProtocol` | Epidemic-style state propagation across a cluster |
| `VectorClock` | Causal ordering tracker mapping node IDs to counters |
| `PartitionDetector` | Detects network splits by tracking heartbeat recency |
| `ConsensusProtocol` | Paxos-like consensus for ternary votes |
| `Vote` | A ternary vote: `Negative`, `Abstain`, or `Positive` |
| `AntiEntropySync` | Background state repair between node pairs |

## How It Works

**Gossip** uses a dominant-trit rule: each round, nodes examine their peers' states and adopt the most common non-zero value (ties broken toward Positive). Convergence depends on network topology; a fully connected cluster of N nodes typically converges in O(log N) rounds.

**Vector clocks** track causal history. Each node increments its own counter on every state change. Two clocks are compared entry-by-entry: if all entries in A are ≤ B and at least one is strictly <, then A happened-before B. If neither is ≤ the other, they are concurrent.

**Consensus** follows a simplified Paxos flow: a proposer gets a proposal number, acceptors promise not to accept lower-numbered proposals, then the proposer asks acceptors to accept a value. A quorum of acceptances yields a decision. The final value is the ternary sum of all accepted votes.

**Anti-entropy** compares vector clocks between node pairs. If one node's clock is strictly behind, it adopts the leader's state. If clocks are concurrent, the dominant trit wins.

## Known Limitations

- Gossip convergence is not guaranteed for disconnected clusters or clusters with odd numbers of conflicting states (ties may oscillate).
- The consensus protocol does not implement leader election; proposers must be coordinated externally.
- Anti-entropy merge is last-write-wins with dominant-trit tie-breaking — this can lose writes in highly concurrent scenarios.
- No persistence layer: all state is in-memory and lost on crash.

## Use Cases

- **Cluster health monitoring** — Nodes report healthy (+1), degraded (0), or failed (-1). Gossip propagates cluster-wide health view.
- **Distributed voting** — Accept/reject/abstain ballots across a cluster of decision nodes with quorum-based finalization.
- **Configuration reconciliation** — Nodes converge on a ternary configuration state (enable/disable/neutral) through anti-entropy.
- **Partition tolerance** — Detect network splits in a ternary-aware system where quorum must be maintained.

## Ecosystem Context

Part of the SuperInstance ternary computing ecosystem. Related crates:

- `ternary-consensus` — Lower-level consensus building blocks
- `ternary-protocol` — Wire protocol for ternary messages
- `ternary-clock` — Logical clock implementations for ternary systems
- `ternary-voting` — Voting algorithm variants

This crate provides higher-level distributed systems abstractions built on those foundations.

## License

MIT

## See Also
- **ternary-consensus** — related
- **ternary-mesh** — related
- **ternary-network** — related
- **ternary-protocol** — related
- **ternary-beacon** — related

