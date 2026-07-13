# Ternary Distributed — Consensus, Gossip, and Synchronization over {−1, 0, +1}

**Ternary Distributed** provides distributed systems primitives — node management, gossip propagation, vector clocks, partition detection, Paxos-like consensus, and anti-entropy synchronization — built natively on the ternary value space **T = {−1, 0, +1}**. Every node holds a ternary state, every protocol operates on trits, and every consensus decision is a ternary vote.

## Why It Matters

Standard distributed systems encode three-valued states (accept/reject/abstain, leader/follower/candidate, healthy/degraded/failed) into binary pairs or enums — an impedance mismatch that wastes bits and complicates wire protocols. Ternary distributed primitives eliminate this mismatch: three states are native, not emulated.

Beyond convenience, ternary consensus offers **mathematical advantages**:

- **Byzantine bounds:** In GF(3), the Byzantine agreement lower bound is tighter. With ternary votes, the number of loyal nodes needed is 3f + 1 (same as binary), but the information density per round is log₂(3) ≈ 1.585× higher.
- **Quorum arithmetic:** Ternary quorum uses Σ(votes) ∈ {−q, ..., +q} rather than counting booleans. The sign and magnitude of the sum carry information about agreement strength, not just its direction.
- **Convergence:** Gossip with the dominant-trit rule converges in O(log N) rounds for N nodes, matching the binary epidemic bound, but with three-way agreement semantics.

## How It Works

### Gossip Protocol (Epidemic Dissemination)

Each round, every node examines its peers' states and adopts the **dominant non-zero trit**:

```
dominant(peer_states) = Pos    if count(Pos) ≥ count(Neg) and count(Pos) > 0
                      = Neg    if count(Neg) > count(Pos)
                      = None   if all peers are Zero
```

Ties are broken toward **Pos** (optimistic convergence). A node only adopts a peer's state if the peer's vector clock is ≥ its own, ensuring **causal consistency** — nodes never regress to stale information.

**Convergence bound:** For a fully connected cluster of N nodes where initially one node holds state s ∈ {Pos, Neg}, the expected number of rounds to convergence is:

```
E[rounds] = O(log N) + O(log(1/ε))
```

where ε is the desired probability of full convergence. This matches the classical epidemic spreading bound.

**Complexity:** O(N × d) per round, where d is the average node degree. Memory: O(N) for state storage.

### Vector Clocks (Causal Ordering)

Each node maintains a counter map. On every state change, the node increments its own counter:

```
VC[i] ← VC[i] + 1
```

Two vector clocks are compared element-wise:

```
A → B  (A happened-before B)  iff  ∀i: A[i] ≤ B[i]  and  ∃j: A[j] < B[j]
A ‖ B  (concurrent)            iff  ¬(A → B) ∧ ¬(B → A) ∧ A ≠ B
```

The vector clock defines a **partial order** on events. The dimensionality is O(N) — one counter per node. Comparison is O(N) in the worst case.

**Merge:** `merge(A, B)[i] = max(A[i], B[i])` — takes the causal history of both clocks.

**Complexity:** Increment O(1). Comparison O(N). Merge O(N).

### Partition Detection (Heartbeat Liveness)

Each node sends a heartbeat every round. The detector marks a node as partitioned if:

```
current_round − last_seen[node] > timeout_rounds
```

A **quorum** exists when more than half the nodes are alive (`alive > N/2`, the crash-fault majority quorum implemented by `has_quorum()`). The Byzantine bound `⌈(2N + 1)/3⌉` is **not** implemented — this crate targets crash-fault tolerance only.

**Complexity:** O(1) per heartbeat update. O(N) to check all nodes.

### Consensus Protocol (Simplified Paxos)

The consensus follows a three-phase Paxos flow adapted for ternary votes:

**Phase 1 — Prepare/Promise:**
```
Proposer → Acceptors:  PREPOSE(proposal_number n)
Acceptors → Proposer:   PROMISE(n) [if n > highest seen]
```

**Phase 2 — Accept:**
```
Proposer → Acceptors:  ACCEPT(n, value)
Acceptors → Proposer:   ACCEPTED(n, value) [if n ≥ highest promised]
```

**Phase 3 — Decide:**
```
decision = sign(Σ accepted_votes)
         = Pos     if Σ > 0
         = Neg     if Σ < 0
         = None    if |Σ| = 0 or no quorum
```

The ternary sum replaces the majority-counting of binary Paxos. The magnitude |Σ| indicates **agreement strength** — unanimous votes have |Σ| = quorum_size, split votes have |Σ| near 0.

**Complexity:** O(N) messages per phase. O(N) to tally votes. Total: O(N) per consensus instance.

**Fault tolerance:** Tolerates `f < N/2` crash failures (majority quorum, `⌊N/2⌋ + 1` accepts required to choose a value). This is the crash-fault bound; it does not tolerate Byzantine (arbitrary) faults, which would require the `f < N/3` bound and signed/validated votes that this crate does not implement.

### Anti-Entropy Synchronization

Background repair compares vector clocks between node pairs:

```
sync(A, B):
  if VC_A → VC_B:  B adopts A's state  (B is behind)
  if VC_B → VC_A:  A adopts B's state  (A is behind)
  if VC_A ‖ VC_B:  adopt dominant_trit(A.state, B.state)  (concurrent)
```

**Complexity:** O(N) per sync pair (vector clock comparison + state update).

## Quick Start

```rust
use ternary_distributed::*;

// Build a 5-node cluster
let mut gossip = GossipProtocol::new();
for i in 1..=5 {
    let mut node = TernaryNode::new(i);
    for j in 1..=5 {
        if i != j { node.add_peer(j); }
    }
    gossip.add_node(node);
}

// Seed node 1 with positive state
gossip.nodes.get_mut(&1).unwrap().set_state(Trit::Pos);

// Run gossip until convergence
let rounds = gossip.run_until_converged(20);
assert!(gossip.is_converged());
println!("Converged in {} rounds", rounds);

// Consensus
let mut cp = ConsensusProtocol::new(&[1, 2, 3, 4, 5]);
let proposal = cp.prepare(1);
for i in 1..=5 { cp.promise(i, proposal); }
cp.accept(1, proposal, Vote::Positive);
cp.accept(2, proposal, Vote::Positive);
cp.accept(3, proposal, Vote::Positive);
assert_eq!(cp.decide(), Some(Vote::Positive));

// Partition detection
let mut pd = PartitionDetector::new(5, 3);
for i in 1..=5 { pd.heartbeat(i); }
pd.advance_round();
assert!(pd.has_quorum());
```

```bash
cargo add ternary-distributed
```

## API

| Type | Complexity | Description |
|---|---|---|
| `Trit` | — | Enum: `Neg(−1)`, `Zero(0)`, `Pos(+1)` |
| `TernaryNode` | — | Node with state, peer set, vector clock |
| `GossipProtocol::run_round()` | O(N·d) | One epidemic dissemination round |
| `GossipProtocol::run_until_converged()` | O(R·N·d) | R rounds until convergence |
| `VectorClock::increment()` | O(1) | Local counter increment |
| `VectorClock::happened_before()` | O(N) | Causal ordering comparison |
| `VectorClock::merge()` | O(N) | Pointwise max merge |
| `PartitionDetector::is_alive()` | O(1) | Liveness check |
| `PartitionDetector::has_quorum()` | O(N) | Quorum verification |
| `ConsensusProtocol::prepare/promise/accept/decide` | O(N) per phase | Paxos-like ternary consensus |
| `AntiEntropySync::sync_pair()` | O(N) | Pairwise state repair |

## Architecture Notes

In the **SuperInstance** ecosystem, `ternary-distributed` manages fleet-wide state convergence. Drone agents report ternary health signals (Pos = optimal, Zero = degraded, Neg = failing) that propagate through the gossip mesh. The consensus protocol resolves conflicting fleet-wide decisions — deployment approvals, routing changes, emergency aborts — through ternary voting where the sign of the vote sum determines the outcome.

The **γ + η = C** conservation law governs cluster state: γ (growth/convergence) increases as nodes reach agreement; η (entropy/divergence) increases when partitions or conflicts arise. C remains constant — total cluster state is conserved. A partitioned cluster has high η (divergent states) and low γ (no convergence). When the partition heals, anti-entropy sync converts η back to γ without losing C.

## References

1. Lamport, L. (1998). "The Part-Time Parliament." *ACM Transactions on Computer Systems*, 16(2), 133–169. — Paxos consensus algorithm.
2. Demers, A. et al. (1987). "Epidemic Algorithms for Replicated Database Maintenance." *PODC '87*, 1–12. — Gossip protocols.
3. Mattern, F. (1989). "Virtual Time and Global States of Distributed Systems." *Proceedings of the International Workshop on Parallel and Distributed Algorithms*. — Vector clocks.
4. Lynch, N. A. (1996). *Distributed Algorithms*. Morgan Kaufmann. — Comprehensive distributed systems theory.
5. Castro, M. & Liskov, B. (2002). "Practical Byzantine Fault Tolerance and Proactive Recovery." *ACM Transactions on Computer Systems*, 20(4), 398–461. — Byzantine fault tolerance.
6. Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapter 5: Replication. — Practical distributed systems patterns.

## License

MIT
