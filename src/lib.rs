#![forbid(unsafe_code)]

//! Distributed systems primitives for ternary protocols.
//!
//! Provides node management, gossip propagation, vector clocks, partition detection,
//! consensus, and anti-entropy synchronization — all built around the ternary value
//! space {-1, 0, +1}.

use std::collections::{HashMap, HashSet};

/// A ternary value: Negative (-1), Zero (0), or Positive (+1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Trit {
    Neg = -1,
    Zero = 0,
    Pos = 1,
}

impl Trit {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            -1 => Some(Trit::Neg),
            0 => Some(Trit::Zero),
            1 => Some(Trit::Pos),
            _ => None,
        }
    }

    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// Unique identifier for a node in the distributed system.
pub type NodeId = u64;

/// A node in a ternary distributed system.
///
/// Each node holds a ternary state and participates in gossip, consensus,
/// and anti-entropy protocols.
#[derive(Debug, Clone)]
pub struct TernaryNode {
    pub id: NodeId,
    pub state: Trit,
    pub peers: HashSet<NodeId>,
    pub vector_clock: VectorClock,
    pub is_alive: bool,
}

impl TernaryNode {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            state: Trit::Zero,
            peers: HashSet::new(),
            vector_clock: VectorClock::new(),
            is_alive: true,
        }
    }

    pub fn with_state(id: NodeId, state: Trit) -> Self {
        Self {
            id,
            state,
            peers: HashSet::new(),
            vector_clock: VectorClock::new(),
            is_alive: true,
        }
    }

    pub fn add_peer(&mut self, peer_id: NodeId) {
        if peer_id != self.id {
            self.peers.insert(peer_id);
        }
    }

    pub fn remove_peer(&mut self, peer_id: NodeId) {
        self.peers.remove(&peer_id);
    }

    pub fn set_state(&mut self, state: Trit) {
        self.state = state;
        self.vector_clock.increment(self.id);
    }
}

/// Gossip protocol for propagating ternary state across a cluster.
///
/// Each round, nodes share their state with random peers. State converges
/// using a "dominant trit" rule: if any peer has a non-zero state, the
/// receiving node adopts the most common non-zero value (with ties broken
/// toward Pos).
#[derive(Debug, Clone)]
pub struct GossipProtocol {
    pub nodes: HashMap<NodeId, TernaryNode>,
    pub round: u64,
}

impl GossipProtocol {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            round: 0,
        }
    }

    pub fn add_node(&mut self, node: TernaryNode) {
        self.nodes.insert(node.id, node);
    }

    pub fn run_round(&mut self) -> u32 {
        let states: HashMap<NodeId, (Trit, VectorClock)> = self
            .nodes
            .iter()
            .map(|(id, n)| (*id, (n.state, n.vector_clock.clone())))
            .collect();

        let mut updates = 0u32;
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();

        for node_id in &node_ids {
            let node = self.nodes.get(node_id).unwrap();
            let mut peer_states: Vec<Trit> = Vec::new();

            for peer_id in &node.peers {
                if let Some((state, vc)) = states.get(peer_id) {
                    // Only adopt from peers with equal or higher vector clock
                    if vc >= &node.vector_clock {
                        peer_states.push(*state);
                    }
                }
            }

            if let Some(new_state) = dominant_trit(&peer_states) {
                if new_state != node.state {
                    if let Some(n) = self.nodes.get_mut(node_id) {
                        n.state = new_state;
                        n.vector_clock.increment(*node_id);
                        updates += 1;
                    }
                }
            }
        }

        self.round += 1;
        updates
    }

    pub fn run_until_converged(&mut self, max_rounds: u64) -> u64 {
        for i in 0..max_rounds {
            if self.run_round() == 0 {
                return i + 1;
            }
        }
        max_rounds
    }

    pub fn is_converged(&self) -> bool {
        let states: HashSet<Trit> = self.nodes.values().map(|n| n.state).collect();
        states.len() <= 1
    }
}

/// Returns the dominant non-zero trit from a list. Ties broken toward Pos.
fn dominant_trit(trits: &[Trit]) -> Option<Trit> {
    let mut neg = 0u32;
    let mut pos = 0u32;
    for t in trits {
        match t {
            Trit::Neg => neg += 1,
            Trit::Pos => pos += 1,
            Trit::Zero => {}
        }
    }
    if pos == 0 && neg == 0 {
        return None;
    }
    if pos >= neg {
        Some(Trit::Pos)
    } else {
        Some(Trit::Neg)
    }
}

/// Vector clock for tracking causal ordering in ternary distributed systems.
///
/// Maps node IDs to monotonically increasing counters. Two vector clocks can
/// be compared to determine happened-before, concurrent, or equal relationships.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorClock {
    pub counters: HashMap<NodeId, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    pub fn increment(&mut self, node_id: NodeId) -> u64 {
        let counter = self.counters.entry(node_id).or_insert(0);
        *counter += 1;
        *counter
    }

    pub fn get(&self, node_id: NodeId) -> u64 {
        *self.counters.get(&node_id).unwrap_or(&0)
    }

    pub fn merge(&self, other: &VectorClock) -> VectorClock {
        let mut merged = self.counters.clone();
        for (node_id, counter) in &other.counters {
            let entry = merged.entry(*node_id).or_insert(0);
            *entry = (*entry).max(*counter);
        }
        VectorClock { counters: merged }
    }

    /// Returns true if self happened-before other.
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let all_keys: HashSet<NodeId> = self
            .counters
            .keys()
            .chain(other.counters.keys())
            .copied()
            .collect();

        let mut at_least_one_less = false;
        for key in &all_keys {
            let s = self.get(*key);
            let o = other.get(*key);
            if s > o {
                return false;
            }
            if s < o {
                at_least_one_less = true;
            }
        }
        at_least_one_less
    }

    /// Returns true if self and other are concurrent (neither happened-before the other).
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }
}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            Some(std::cmp::Ordering::Equal)
        } else if self.happened_before(other) {
            Some(std::cmp::Ordering::Less)
        } else if other.happened_before(self) {
            Some(std::cmp::Ordering::Greater)
        } else {
            None // concurrent
        }
    }
}

/// Partition detector identifies network splits by tracking node reachability.
///
/// Maintains a heartbeat map. Nodes that haven't been heard from within
/// `timeout_rounds` are considered partitioned. The detector then checks
/// if the remaining reachable nodes form a quorum.
#[derive(Debug, Clone)]
pub struct PartitionDetector {
    pub last_seen: HashMap<NodeId, u64>,
    pub timeout_rounds: u64,
    pub current_round: u64,
    pub total_nodes: usize,
}

impl PartitionDetector {
    pub fn new(total_nodes: usize, timeout_rounds: u64) -> Self {
        Self {
            last_seen: HashMap::new(),
            timeout_rounds,
            current_round: 0,
            total_nodes,
        }
    }

    pub fn heartbeat(&mut self, node_id: NodeId) {
        self.last_seen.insert(node_id, self.current_round);
    }

    pub fn advance_round(&mut self) {
        self.current_round += 1;
    }

    pub fn is_alive(&self, node_id: NodeId) -> bool {
        self.last_seen
            .get(&node_id)
            .map(|&r| self.current_round.saturating_sub(r) <= self.timeout_rounds)
            .unwrap_or(false)
    }

    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.last_seen
            .keys()
            .filter(|&&id| self.is_alive(id))
            .copied()
            .collect()
    }

    pub fn partitioned_nodes(&self) -> Vec<NodeId> {
        self.last_seen
            .keys()
            .filter(|&&id| !self.is_alive(id))
            .copied()
            .collect()
    }

    pub fn has_quorum(&self) -> bool {
        let alive = self.alive_nodes().len();
        alive * 2 > self.total_nodes
    }

    pub fn is_partitioned(&self) -> bool {
        !self.has_quorum()
    }
}

/// A ternary vote: Negative (-1), Abstain (0), or Positive (+1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vote {
    Negative,
    Abstain,
    Positive,
}

impl Vote {
    pub fn to_trit(self) -> Trit {
        match self {
            Vote::Negative => Trit::Neg,
            Vote::Abstain => Trit::Zero,
            Vote::Positive => Trit::Pos,
        }
    }

    pub fn from_trit(t: Trit) -> Self {
        match t {
            Trit::Neg => Vote::Negative,
            Trit::Zero => Vote::Abstain,
            Trit::Pos => Vote::Positive,
        }
    }
}

/// A Paxos-like consensus protocol for ternary votes.
///
/// Supports prepare/promise and accept/ack phases. A proposal is accepted
/// when a quorum of acceptors responds. The final value is determined by
/// summing the ternary votes: negative sum → Neg, positive sum → Pos, zero → Zero.
#[derive(Debug, Clone)]
pub struct ConsensusProtocol {
    pub proposers: HashSet<NodeId>,
    pub acceptors: HashSet<NodeId>,
    pub learners: HashSet<NodeId>,
    pub promised_proposal: HashMap<NodeId, u64>,
    pub accepted_value: HashMap<NodeId, (u64, Vote)>,
    pub proposal_counter: u64,
    pub quorum_size: usize,
}

impl ConsensusProtocol {
    pub fn new(nodes: &[NodeId]) -> Self {
        let node_set: HashSet<NodeId> = nodes.iter().copied().collect();
        let quorum_size = nodes.len() / 2 + 1;
        Self {
            proposers: node_set.clone(),
            acceptors: node_set.clone(),
            learners: node_set,
            promised_proposal: HashMap::new(),
            accepted_value: HashMap::new(),
            proposal_counter: 0,
            quorum_size,
        }
    }

    pub fn prepare(&mut self, proposer: NodeId) -> u64 {
        self.proposal_counter += 1;
        let proposal_num = self.proposal_counter;
        // Check if proposer can promise
        if let Some(&promised) = self.promised_proposal.get(&proposer) {
            if promised >= proposal_num {
                return 0; // rejected
            }
        }
        proposal_num
    }

    pub fn promise(&mut self, acceptor: NodeId, proposal_num: u64) -> bool {
        if let Some(&promised) = self.promised_proposal.get(&acceptor) {
            if promised > proposal_num {
                return false;
            }
        }
        self.promised_proposal.insert(acceptor, proposal_num);
        true
    }

    pub fn accept(&mut self, acceptor: NodeId, proposal_num: u64, value: Vote) -> bool {
        if let Some(&promised) = self.promised_proposal.get(&acceptor) {
            if promised > proposal_num {
                return false;
            }
        }
        self.accepted_value
            .insert(acceptor, (proposal_num, value));
        true
    }

    pub fn decide(&self) -> Option<Vote> {
        let values: Vec<&(u64, Vote)> = self.accepted_value.values().collect();
        if values.len() < self.quorum_size {
            return None;
        }

        // Find the highest proposal number with quorum
        let mut proposal_counts: HashMap<u64, Vec<Vote>> = HashMap::new();
        for (num, vote) in &values {
            proposal_counts.entry(*num).or_default().push(*vote);
        }

        let max_proposal = proposal_counts.keys().max()?;
        let votes = proposal_counts.get(max_proposal)?;
        if votes.len() < self.quorum_size {
            return None;
        }

        let sum: i32 = votes.iter().map(|v| v.to_trit().to_i8() as i32).sum();
        if sum < 0 {
            Some(Vote::Negative)
        } else if sum > 0 {
            Some(Vote::Positive)
        } else {
            Some(Vote::Abstain)
        }
    }
}

/// Anti-entropy synchronization for repairing state divergence between nodes.
///
/// Compares vector clocks to detect conflicts. If clocks are concurrent,
/// applies a merge strategy. Otherwise, the lagging node adopts the leader's state.
#[derive(Debug, Clone)]
pub struct AntiEntropySync {
    pub nodes: HashMap<NodeId, TernaryNode>,
}

impl AntiEntropySync {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: TernaryNode) {
        self.nodes.insert(node.id, node);
    }

    /// Synchronize two nodes. Returns true if any state was changed.
    pub fn sync_pair(&mut self, node_a: NodeId, node_b: NodeId) -> bool {
        let (state_a, vc_a, peers_a) = {
            let a = self.nodes.get(&node_a).unwrap();
            (a.state, a.vector_clock.clone(), a.peers.clone())
        };
        let (state_b, vc_b, peers_b) = {
            let b = self.nodes.get(&node_b).unwrap();
            (b.state, b.vector_clock.clone(), b.peers.clone())
        };

        let mut changed = false;

        if vc_a.happened_before(&vc_b) {
            // A is behind, adopt B's state
            if let Some(a) = self.nodes.get_mut(&node_a) {
                if a.state != state_b {
                    a.state = state_b;
                    a.vector_clock = vc_a.merge(&vc_b);
                    a.vector_clock.increment(node_a);
                    changed = true;
                }
            }
        } else if vc_b.happened_before(&vc_a) {
            // B is behind, adopt A's state
            if let Some(b) = self.nodes.get_mut(&node_b) {
                if b.state != state_a {
                    b.state = state_a;
                    b.vector_clock = vc_b.merge(&vc_a);
                    b.vector_clock.increment(node_b);
                    changed = true;
                }
            }
        } else if vc_a.is_concurrent(&vc_b) {
            // Concurrent — merge by dominant trit
            let merged = dominant_trit(&[state_a, state_b]).unwrap_or(Trit::Zero);
            let merged_vc = vc_a.merge(&vc_b);
            if let Some(a) = self.nodes.get_mut(&node_a) {
                if a.state != merged {
                    a.state = merged;
                    a.vector_clock = merged_vc.clone();
                    a.vector_clock.increment(node_a);
                    changed = true;
                }
            }
            if let Some(b) = self.nodes.get_mut(&node_b) {
                if b.state != merged {
                    b.state = merged;
                    b.vector_clock = merged_vc;
                    b.vector_clock.increment(node_b);
                    changed = true;
                }
            }
        }

        // Also sync peer lists
        if let Some(a) = self.nodes.get_mut(&node_a) {
            for p in &peers_b {
                a.peers.insert(*p);
            }
        }
        if let Some(b) = self.nodes.get_mut(&node_b) {
            for p in &peers_a {
                b.peers.insert(*p);
            }
        }

        changed
    }

    /// Run full anti-entropy: every pair of connected nodes synchronizes.
    pub fn sync_all(&mut self) -> u32 {
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        let mut changes = 0u32;

        for i in 0..node_ids.len() {
            for j in (i + 1)..node_ids.len() {
                let a_id = node_ids[i];
                let b_id = node_ids[j];
                let a = self.nodes.get(&a_id).unwrap();
                let b = self.nodes.get(&b_id).unwrap();
                if a.peers.contains(&b_id) || b.peers.contains(&a_id) {
                    if self.sync_pair(a_id, b_id) {
                        changes += 1;
                    }
                }
            }
        }

        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trit_from_i8() {
        assert_eq!(Trit::from_i8(-1), Some(Trit::Neg));
        assert_eq!(Trit::from_i8(0), Some(Trit::Zero));
        assert_eq!(Trit::from_i8(1), Some(Trit::Pos));
        assert_eq!(Trit::from_i8(2), None);
    }

    #[test]
    fn test_trit_to_i8() {
        assert_eq!(Trit::Neg.to_i8(), -1);
        assert_eq!(Trit::Zero.to_i8(), 0);
        assert_eq!(Trit::Pos.to_i8(), 1);
    }

    #[test]
    fn test_ternary_node_new() {
        let node = TernaryNode::new(1);
        assert_eq!(node.id, 1);
        assert_eq!(node.state, Trit::Zero);
        assert!(node.peers.is_empty());
        assert!(node.is_alive);
    }

    #[test]
    fn test_ternary_node_add_peer() {
        let mut node = TernaryNode::new(1);
        node.add_peer(2);
        node.add_peer(3);
        assert!(node.peers.contains(&2));
        assert!(node.peers.contains(&3));
        assert_eq!(node.peers.len(), 2);
    }

    #[test]
    fn test_ternary_node_no_self_peer() {
        let mut node = TernaryNode::new(1);
        node.add_peer(1);
        assert!(node.peers.is_empty());
    }

    #[test]
    fn test_ternary_node_set_state() {
        let mut node = TernaryNode::new(1);
        node.set_state(Trit::Pos);
        assert_eq!(node.state, Trit::Pos);
        assert_eq!(node.vector_clock.get(1), 1);
        node.set_state(Trit::Neg);
        assert_eq!(node.state, Trit::Neg);
        assert_eq!(node.vector_clock.get(1), 2);
    }

    #[test]
    fn test_vector_clock_increment() {
        let mut vc = VectorClock::new();
        assert_eq!(vc.increment(1), 1);
        assert_eq!(vc.increment(1), 2);
        assert_eq!(vc.increment(2), 1);
    }

    #[test]
    fn test_vector_clock_happened_before() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(1);
        vc2.increment(1);
        assert!(vc1.happened_before(&vc2));
        assert!(!vc2.happened_before(&vc1));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);
        assert!(vc1.is_concurrent(&vc2));
        assert!(vc2.is_concurrent(&vc1));
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);
        let merged = vc1.merge(&vc2);
        assert_eq!(merged.get(1), 1);
        assert_eq!(merged.get(2), 1);
    }

    #[test]
    fn test_vector_clock_partial_ord() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(1);
        vc2.increment(2);
        assert!(vc1 < vc2);
        assert!(vc2 > vc1);
    }

    #[test]
    fn test_gossip_single_round() {
        let mut gossip = GossipProtocol::new();
        let mut n1 = TernaryNode::with_state(1, Trit::Pos);
        n1.add_peer(2);
        let mut n2 = TernaryNode::new(2);
        n2.add_peer(1);
        gossip.add_node(n1);
        gossip.add_node(n2);
        let updates = gossip.run_round();
        assert!(updates > 0);
        assert!(gossip.is_converged());
    }

    #[test]
    fn test_gossip_converged() {
        let mut gossip = GossipProtocol::new();
        let mut n1 = TernaryNode::with_state(1, Trit::Pos);
        n1.add_peer(2);
        let mut n2 = TernaryNode::with_state(2, Trit::Pos);
        n2.add_peer(1);
        gossip.add_node(n1);
        gossip.add_node(n2);
        let rounds = gossip.run_until_converged(10);
        assert_eq!(rounds, 1);
        assert!(gossip.is_converged());
    }

    #[test]
    fn test_partition_detector_alive() {
        let mut pd = PartitionDetector::new(3, 2);
        pd.heartbeat(1);
        pd.heartbeat(2);
        pd.heartbeat(3);
        assert!(pd.is_alive(1));
        assert!(pd.is_alive(2));
        assert!(pd.is_alive(3));
    }

    #[test]
    fn test_partition_detector_timeout() {
        let mut pd = PartitionDetector::new(3, 1);
        pd.heartbeat(1);
        pd.heartbeat(2);
        pd.heartbeat(3);
        pd.advance_round();
        pd.advance_round();
        assert!(!pd.is_alive(1));
        assert_eq!(pd.partitioned_nodes().len(), 3);
    }

    #[test]
    fn test_partition_detector_quorum() {
        let mut pd = PartitionDetector::new(3, 2);
        pd.heartbeat(1);
        pd.heartbeat(2);
        assert!(pd.has_quorum());
        assert!(!pd.is_partitioned());
    }

    #[test]
    fn test_partition_detector_no_quorum() {
        let mut pd = PartitionDetector::new(5, 2);
        pd.heartbeat(1);
        pd.heartbeat(2);
        assert!(!pd.has_quorum());
        assert!(pd.is_partitioned());
    }

    #[test]
    fn test_consensus_prepare_promise() {
        let mut cp = ConsensusProtocol::new(&[1, 2, 3]);
        let proposal = cp.prepare(1);
        assert!(proposal > 0);
        assert!(cp.promise(1, proposal));
        assert!(cp.promise(2, proposal));
        assert!(cp.promise(3, proposal));
    }

    #[test]
    fn test_consensus_accept_decide() {
        let mut cp = ConsensusProtocol::new(&[1, 2, 3]);
        let proposal = cp.prepare(1);
        cp.promise(1, proposal);
        cp.promise(2, proposal);
        cp.promise(3, proposal);
        cp.accept(1, proposal, Vote::Positive);
        cp.accept(2, proposal, Vote::Positive);
        cp.accept(3, proposal, Vote::Positive);
        assert_eq!(cp.decide(), Some(Vote::Positive));
    }

    #[test]
    fn test_consensus_negative_decision() {
        let mut cp = ConsensusProtocol::new(&[1, 2, 3]);
        let proposal = cp.prepare(1);
        cp.promise(1, proposal);
        cp.promise(2, proposal);
        cp.promise(3, proposal);
        cp.accept(1, proposal, Vote::Negative);
        cp.accept(2, proposal, Vote::Negative);
        cp.accept(3, proposal, Vote::Abstain);
        assert_eq!(cp.decide(), Some(Vote::Negative));
    }

    #[test]
    fn test_consensus_no_quorum() {
        let mut cp = ConsensusProtocol::new(&[1, 2, 3]);
        assert_eq!(cp.decide(), None);
    }

    #[test]
    fn test_anti_entropy_sync_pair() {
        let mut sync = AntiEntropySync::new();
        let mut n1 = TernaryNode::with_state(1, Trit::Pos);
        n1.add_peer(2);
        let mut n2 = TernaryNode::new(2);
        n2.add_peer(1);
        n2.vector_clock.increment(2); // Give n2 a clock
        sync.add_node(n1);
        sync.add_node(n2);
        sync.sync_pair(1, 2);
        // n2 should adopt n1's state since n1 has state set via with_state
        // but n1's vc is empty while n2 has a tick
        let state2 = sync.nodes.get(&2).unwrap().state;
        assert_eq!(state2, Trit::Zero); // n2 already zero, n1 has no vc ticks
    }

    #[test]
    fn test_vote_trit_conversion() {
        assert_eq!(Vote::Negative.to_trit(), Trit::Neg);
        assert_eq!(Vote::Abstain.to_trit(), Trit::Zero);
        assert_eq!(Vote::Positive.to_trit(), Trit::Pos);
        assert_eq!(Vote::from_trit(Trit::Neg), Vote::Negative);
        assert_eq!(Vote::from_trit(Trit::Zero), Vote::Abstain);
        assert_eq!(Vote::from_trit(Trit::Pos), Vote::Positive);
    }

    #[test]
    fn test_dominant_trit() {
        assert_eq!(dominant_trit(&[Trit::Pos, Trit::Pos, Trit::Neg]), Some(Trit::Pos));
        assert_eq!(dominant_trit(&[Trit::Neg, Trit::Neg, Trit::Pos]), Some(Trit::Neg));
        assert_eq!(dominant_trit(&[Trit::Zero, Trit::Zero]), None);
        assert_eq!(dominant_trit(&[]), None);
    }
}
