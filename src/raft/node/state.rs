use crate::raft::raft_types::LogEntry;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum State {
    Candidate {
        votes: usize,
    },
    Leader,
    #[default]
    Follower,
}

/// Plain data held by a [`Node`](super::Node): the Raft state the algorithm
/// reads and mutates. It carries no behaviour and owns no collaborators —
/// channels, the state machine, the persister and the random generator stay on
/// the `Node` itself.
#[derive(Debug, Default)]
pub(super) struct NodeState {
    pub(super) id: String,
    pub(super) peers: Vec<String>,

    pub(super) current_term: u64,
    pub(super) state: State,
    pub(super) voted_for: Option<String>,
    pub(super) leader: String,

    pub(super) entries: Vec<LogEntry>,
    pub(super) commit_index: u64,
    pub(super) last_applied: u64,

    pub(super) next_index: HashMap<String, u64>,
    pub(super) match_index: HashMap<String, u64>,

    pub(super) snapshot_last_index: u64,
    pub(super) snapshot_last_term: u64,
}

impl NodeState {
    pub(super) fn new(id: String, peers: Vec<String>) -> Self {
        let next_index = peers.iter().map(|p| (p.clone(), 0)).collect();
        let match_index = peers.iter().map(|p| (p.clone(), 0)).collect();
        NodeState {
            id,
            peers,
            next_index,
            match_index,
            ..Default::default()
        }
    }
}
