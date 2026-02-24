use crate::raft::network_types::OutMsg;
use crate::raft::raft_types::{ChangeStateReply, LogEntry, RaftMsg};
use crate::raft::state_persister::{PersistentState, Persister};
use crate::raft::state_machine::StorageEngine;
use crate::raft::state_machine::StateMachine;
use rand::Rng;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum State {
    Candidate {
        votes: usize,
    },
    Leader,
    #[default]
    Follower,
}

pub struct Node<T, SM: StorageEngine> {
    pub(super) current_term: u64,
    pub(super) state: State,
    pub(super) peers: Vec<String>,
    pub(super) voted_for: Option<String>,
    pub(super) entries: Vec<LogEntry>,
    pub(super) network_inbox: Sender<OutMsg>,
    pub(super) id: String,
    pub(super) commit_index: u64,

    pub(super) leader: String,
    pub(super) last_applied: u64,
    pub(super) next_index: HashMap<String, u64>,
    pub(super) match_index: HashMap<String, u64>,
    pub(super) snapshot_last_index: u64,
    pub(super) snapshot_last_term: u64,

    pub(super) state_machine: StateMachine<SM>,
    pub(super) pending_clients: HashMap<u64, tokio::sync::oneshot::Sender<ChangeStateReply>>,

    pub(super) state_persister: T,
}

impl<T: Persister + Send + Sync, SM: StorageEngine> Node<T, SM> {
    pub async fn new(
        peers: Vec<String>,
        network_inbox: Sender<OutMsg>,
        id: String,
        state_persister: T,
        storage_engine: SM,
    ) -> anyhow::Result<Self> {
        let mut next_index_map = HashMap::new();
        let mut match_index_map = HashMap::new();
        for peer in peers.clone() {
            next_index_map.insert(peer.clone(), 0);
            match_index_map.insert(peer, 0);
        }
        let mut node = Node {
            leader: String::new(),
            current_term: 0,
            state: State::default(),
            peers,
            voted_for: None,
            network_inbox,
            entries: vec![],
            id,
            commit_index: 0,
            last_applied: 0,
            next_index: next_index_map,
            match_index: match_index_map,
            snapshot_last_index: 0,
            snapshot_last_term: 0,
            state_machine: StateMachine::new(storage_engine),
            pending_clients: HashMap::new(),
            state_persister,
        };
        let init_state = node.state_persister.load_state().await?;
        node.current_term = init_state.current_term;
        node.voted_for = init_state.voted_for;
        node.entries = init_state.entries;
        node.commit_index = init_state.commit_index;
        node.apply_commands().await?;
        Ok(node)
    }

    pub async fn run(mut self, mut inbox: Receiver<RaftMsg>) -> anyhow::Result<()> {
        loop {
            let mut duration: u64 = rand::rng().random_range(150..300);
            if self.state == State::Leader {
                duration = 50;
            }

            tokio::select! {
                option = inbox.recv() => {
                    let Some(msg) = option else { return Ok(()); };
                    self.handle_message(msg).await?;
                },
                _ = tokio::time::sleep(Duration::from_millis(duration)) => {
                    if self.state == State::Leader {
                        self.send_heartbeat().await?;
                        continue;
                    }
                    self.start_election().await?;
                }
            }
        }
    }

    pub(super) async fn persist_state(&self) -> anyhow::Result<()> {
        let persistent_state = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            entries: self.entries.clone(),
            commit_index: self.commit_index,
        };
        self.state_persister.save_state(&persistent_state).await?;
        Ok(())
    }

    pub(super) async fn handle_message(&mut self, msg: RaftMsg) -> anyhow::Result<()> {
        match msg {
            RaftMsg::VoteRequest {
                vote_request,
                reply_channel,
            } => {
                eprintln!("Got requestVote in node");
                let vote_reply = self.handle_vote_request(vote_request).await?;
                self.send_to_reply_channel(reply_channel, vote_reply)?;
            }
            RaftMsg::AppendEntries {
                append_request,
                reply_channel,
            } => {
                let reply = self.handle_append_entries(append_request).await?;
                self.send_to_reply_channel(reply_channel, reply)?;
            }
            RaftMsg::AppendEntriesReply {
                append_reply,
                reply_channel,
            } => {
                self.handle_append_entries_reply(append_reply).await?;
                self.send_to_reply_channel(reply_channel, ())?;
            }
            RaftMsg::RequestVoteReply {
                vote_reply,
                reply_channel,
            } => {
                eprintln!("Received vote reply: {:?}", vote_reply);
                self.handle_request_vote_reply(vote_reply).await?;
                self.send_to_reply_channel(reply_channel, ())?;
            }
            RaftMsg::ChangeState {
                command,
                reply_channel,
            } => {
                if !matches!(self.state, State::Leader) {
                    let reply = ChangeStateReply {
                        success: false,
                        leader: self.leader.clone(),
                    };
                    self.send_to_reply_channel(reply_channel, reply)?;
                } else {
                    let prev_log_index = self.last_log_index();
                    let prev_log_term = self
                        .entries
                        .last()
                        .map_or(self.snapshot_last_term, |e| e.term);
                    for peer in &self.peers {
                        let append_entries = OutMsg::AppendEntries {
                            term: self.current_term,
                            peer: peer.clone(),
                            prev_log_index,
                            prev_log_term,
                            leader_commit: self.commit_index,
                            leader_id: self.id.clone(),
                            entries: vec![LogEntry {
                                term: self.current_term,
                                command: command.clone(),
                            }],
                        };
                        eprintln!("replicating to {}", peer.clone());
                        self.network_inbox.send(append_entries).await?;
                    }

                    self.entries.push(LogEntry {
                        term: self.current_term,
                        command,
                    });
                    self.persist_state()
                        .await
                        .expect("Failed to persist state after adding new command");
                    let log_index = self.last_log_index();
                    if let Some(reply_channel) = reply_channel {
                        self.pending_clients.insert(log_index, reply_channel);
                    }
                }
            }
            RaftMsg::GetState { key, reply_channel } => {
                let val = self.state_machine.get(&key).unwrap_or_default().to_owned();
                self.send_to_reply_channel(Some(reply_channel), val)?;
            }
        }

        self.apply_commands().await?;
        Ok(())
    }

    pub(super) async fn apply_commands(&mut self) -> anyhow::Result<()> {
        if self.commit_index <= self.last_applied {
            return Ok(());
        }

        for i in self.last_applied + 1..=self.commit_index {
            let command = self
                .get_log_entry(i)
                .expect("committed entry missing from log")
                .command
                .clone();

            self.state_machine.apply(command)?;
            self.last_applied = i;

            let reply_channel = self.pending_clients.remove(&i);
            let reply = ChangeStateReply {
                success: true,
                leader: self.id.clone(),
            };
            self.send_to_reply_channel(reply_channel, reply)?;
        }

        Ok(())
    }

    pub(super) fn is_majority(&self, count: usize) -> bool {
        count > self.peers.len().div_ceil(2)
    }

    pub(super) fn send_to_reply_channel<R>(
        &self,
        reply_channel: Option<tokio::sync::oneshot::Sender<R>>,
        reply: R,
    ) -> anyhow::Result<()> {
        let Some(reply_channel) = reply_channel else {
            return Ok(());
        };
        reply_channel
            .send(reply)
            .map_err(|_| anyhow::anyhow!("reply receiver dropped"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_types::{
        AppendEntriesData, AppendEntriesReplyData, RequestVoteData, RequestVoteReplyData,
    };
    use crate::raft::state_persister::PersistentState;
    use crate::storage::lsm_tree::LSMTree;
    use std::sync::{Arc, Mutex};

    struct TestPersister;
    struct LoadedStatePersister {
        state: PersistentState,
    }
    struct FailingLoadPersister;
    struct RecordingPersister {
        saved_state: Arc<Mutex<Option<PersistentState>>>,
    }

    #[async_trait::async_trait]
    impl Persister for TestPersister {
        async fn save_state(&self, _state: &PersistentState) -> anyhow::Result<()> {
            Ok(())
        }
        async fn create_snapshot(&self, _: u64, _: u64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Ok(PersistentState { current_term: 0, voted_for: None, entries: vec![], commit_index: 0 })
        }
    }

    #[async_trait::async_trait]
    impl Persister for LoadedStatePersister {
        async fn save_state(&self, _state: &PersistentState) -> anyhow::Result<()> {
            Ok(())
        }
        async fn create_snapshot(&self, _: u64, _: u64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Ok(PersistentState {
                current_term: self.state.current_term,
                voted_for: self.state.voted_for.clone(),
                entries: self.state.entries.clone(),
                commit_index: self.state.commit_index,
            })
        }
    }

    #[async_trait::async_trait]
    impl Persister for FailingLoadPersister {
        async fn save_state(&self, _state: &PersistentState) -> anyhow::Result<()> {
            Ok(())
        }
        async fn create_snapshot(&self, _: u64, _: u64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Err(anyhow::anyhow!("failed to load state"))
        }
    }

    #[async_trait::async_trait]
    impl Persister for RecordingPersister {
        async fn save_state(&self, state: &PersistentState) -> anyhow::Result<()> {
            let mut saved = self.saved_state.lock().expect("lock");
            *saved = Some(PersistentState {
                current_term: state.current_term,
                voted_for: state.voted_for.clone(),
                entries: state.entries.clone(),
                commit_index: state.commit_index,
            });
            Ok(())
        }
        async fn create_snapshot(&self, _: u64, _: u64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Ok(PersistentState { current_term: 0, voted_for: None, entries: vec![], commit_index: 0 })
        }
    }

    #[tokio::test]
    async fn test_new_loads_persistent_state() -> anyhow::Result<()> {
        let peers = vec!["node2".to_string()];
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 7,
                voted_for: Some("node3".to_string()),
                entries: vec![
                    LogEntry { term: 5, command: "set key1 val1".to_string() },
                    LogEntry { term: 7, command: "set key2 val2".to_string() },
                ],
                commit_index: 2,
            },
        };
        let node = Node::new(peers, network_inbox, "node1".to_string(), persister, LSMTree::init()).await?;
        assert_eq!(node.current_term, 7);
        assert_eq!(node.voted_for, Some("node3".to_string()));
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[0].term, 5);
        assert_eq!(node.entries[0].command, "set key1 val1");
        assert_eq!(node.entries[1].term, 7);
        assert_eq!(node.entries[1].command, "set key2 val2");
        assert_eq!(node.commit_index, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_new_returns_error_when_state_loading_fails() {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let result = Node::new(vec![], network_inbox, "node1".to_string(), FailingLoadPersister, LSMTree::init()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_new_replays_committed_entries_into_state_machine() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 4,
                voted_for: Some("node2".to_string()),
                entries: vec![
                    LogEntry { term: 4, command: "set key1 val1".to_string() },
                    LogEntry { term: 4, command: "set key2 val2".to_string() },
                    LogEntry { term: 4, command: "set key1 val3".to_string() },
                ],
                commit_index: 2,
            },
        };
        let node = Node::new(vec![], network_inbox, "node1".to_string(), persister, LSMTree::init()).await?;
        assert_eq!(node.state_machine.get("key1").unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").unwrap(), "val2");
        assert_eq!(node.last_applied, 2);
        assert_eq!(node.commit_index, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_new_replays_all_committed_entries_updates_existing_key() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 4,
                voted_for: Some("node2".to_string()),
                entries: vec![
                    LogEntry { term: 4, command: "set key1 val1".to_string() },
                    LogEntry { term: 4, command: "set key2 val2".to_string() },
                    LogEntry { term: 4, command: "set key1 val3".to_string() },
                ],
                commit_index: 3,
            },
        };
        let node = Node::new(vec![], network_inbox, "node1".to_string(), persister, LSMTree::init()).await?;
        assert_eq!(node.state_machine.get("key1").unwrap(), "val3");
        assert_eq!(node.state_machine.get("key2").unwrap(), "val2");
        assert_eq!(node.last_applied, 3);
        assert_eq!(node.commit_index, 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_leader_change_state_persists_new_entry() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let saved_state = Arc::new(Mutex::new(None));
        let persister = RecordingPersister { saved_state: saved_state.clone() };
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), persister, LSMTree::init()).await?;
        node.state = State::Leader;
        node.current_term = 3;
        node.handle_message(RaftMsg::ChangeState { command: "set key1 value1".to_string(), reply_channel: None }).await?;
        let persisted = saved_state.lock().unwrap();
        let persisted = persisted.as_ref().expect("should persist");
        assert_eq!(persisted.entries.len(), 1);
        assert_eq!(persisted.entries[0].term, 3);
        assert_eq!(persisted.entries[0].command, "set key1 value1");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_uses_snapshot_index_and_term_for_prev_log_match() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 4;
        node.snapshot_last_index = 5;
        node.snapshot_last_term = 3;
        let reply = node.handle_append_entries(AppendEntriesData {
            term: 4, prev_log_index: 5, prev_log_term: 3, leader_commit: 10,
            leader_id: "node2".to_string(),
            entries: vec![LogEntry { term: 4, command: "set key val".to_string() }],
        }).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 1);
        assert_eq!(node.last_log_index(), 6);
        assert_eq!(node.commit_index, 6);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_rejects_when_snapshot_prev_log_term_mismatches() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 4;
        node.snapshot_last_index = 5;
        node.snapshot_last_term = 3;
        let reply = node.handle_append_entries(AppendEntriesData {
            term: 4, prev_log_index: 5, prev_log_term: 9, leader_commit: 10,
            leader_id: "node2".to_string(),
            entries: vec![LogEntry { term: 4, command: "set key val".to_string() }],
        }).await?;
        assert!(!reply.success);
        assert!(node.entries.is_empty());
        assert_eq!(node.last_log_index(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        let reply = node.handle_vote_request(RequestVoteData { term: 1, last_log_index: 0, last_log_term: 0, candidate: "node1".to_string() }).await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_already_voted() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.voted_for = Some("node2".to_string());
        let reply = node.handle_vote_request(RequestVoteData { term: 0, last_log_index: 0, last_log_term: 0, candidate: "node3".to_string() }).await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_not_up_to_date() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        let reply = node.handle_vote_request(RequestVoteData { term: 2, last_log_index: 0, last_log_term: 0, candidate: "node1".to_string() }).await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_index_up_to_date() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        let reply = node.handle_vote_request(RequestVoteData { term: 2, last_log_index: 2, last_log_term: 1, candidate: "node1".to_string() }).await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_voted_for_same_candidate() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.voted_for = Some("node2".to_string());
        let reply = node.handle_vote_request(RequestVoteData { term: 1, last_log_index: 0, last_log_term: 0, candidate: "node2".to_string() }).await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_higher_log_index_not_up_to_date() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 1, command: "cmd2".to_string() });
        let reply = node.handle_vote_request(RequestVoteData { term: 2, last_log_index: 1, last_log_term: 1, candidate: "node1".to_string() }).await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_mismatch() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        let reply = node.handle_vote_request(RequestVoteData { term: 2, last_log_index: 1, last_log_term: 0, candidate: "node1".to_string() }).await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_stale_term() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        let reply = node.handle_vote_request(RequestVoteData { term: 1, last_log_index: 0, last_log_term: 0, candidate: "node1".to_string() }).await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_request_vote_reply_become_leader() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec!["node1".to_string(), "node2".to_string()], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };
        node.handle_request_vote_reply(RequestVoteReplyData { term: 1, vote: true }).await?;
        assert_eq!(node.state, State::Leader);
        assert_eq!(node.current_term, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_candidate_has_higher_term() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 1, command: "cmd2".to_string() });
        let reply = node.handle_vote_request(RequestVoteData { term: 3, last_log_index: 1, last_log_term: 2, candidate: "node1".to_string() }).await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_reply_no_majority() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec!["node1".to_string(), "node2".to_string()], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };
        node.handle_request_vote_reply(RequestVoteReplyData { term: 1, vote: false }).await?;
        assert_eq!(node.state, State::Candidate { votes: 1 });
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 1;
        node.state = State::Leader;
        let reply = node.handle_append_entries(AppendEntriesData { term: 2, prev_log_index: 0, prev_log_term: 0, leader_commit: 0, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(reply.success);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_term() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 1;
        node.state = State::Leader;
        let reply = node.handle_append_entries(AppendEntriesData { term: 2, prev_log_index: 0, prev_log_term: 0, leader_commit: 0, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(reply.success);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_request() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Leader;
        let reply = node.handle_append_entries(AppendEntriesData { term: 1, prev_log_index: 0, prev_log_term: 0, leader_commit: 0, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(!reply.success);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Leader);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_prev_log_index_high() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Leader;
        let reply = node.handle_append_entries(AppendEntriesData { term: 3, prev_log_index: 1, prev_log_term: 0, leader_commit: 0, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(!reply.success);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_prev_log_term_mismatch() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Leader;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        let reply = node.handle_append_entries(AppendEntriesData { term: 3, prev_log_index: 1, prev_log_term: 0, leader_commit: 0, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(!reply.success);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_successful_append() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        let reply = node.handle_append_entries(AppendEntriesData {
            term: 2, prev_log_index: 1, prev_log_term: 1, leader_commit: 0,
            leader_id: "node2".to_string(),
            entries: vec![LogEntry { term: 2, command: "cmd2".to_string() }],
        }).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[1].term, 2);
        assert_eq!(node.entries[1].command, "cmd2");
        assert_eq!(node.commit_index, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_conflicting_entries() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd2".to_string() });
        let reply = node.handle_append_entries(AppendEntriesData {
            term: 3, prev_log_index: 1, prev_log_term: 1, leader_commit: 0,
            leader_id: "node2".to_string(),
            entries: vec![LogEntry { term: 3, command: "cmd3".to_string() }],
        }).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[1].term, 3);
        assert_eq!(node.entries[1].command, "cmd3");
        assert_eq!(node.commit_index, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_heartbeat_update_commit_index_with_leader() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd2".to_string() });
        let reply = node.handle_append_entries(AppendEntriesData { term: 3, prev_log_index: 2, prev_log_term: 2, leader_commit: 2, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.commit_index, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_update_commit_with_entries_length() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd2".to_string() });
        let reply = node.handle_append_entries(AppendEntriesData { term: 3, prev_log_index: 2, prev_log_term: 2, leader_commit: 4, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.commit_index, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_update_commit_with_new_entries_length() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd2".to_string() });
        let reply = node.handle_append_entries(AppendEntriesData {
            term: 3, prev_log_index: 2, prev_log_term: 2, leader_commit: 4,
            leader_id: "node2".to_string(),
            entries: vec![LogEntry { term: 3, command: "cmd3".to_string() }],
        }).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 3);
        assert_eq!(node.entries[2].term, 3);
        assert_eq!(node.entries[2].command, "cmd3");
        assert_eq!(node.commit_index, 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_step_down() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.handle_append_entries_reply(AppendEntriesReplyData { term: 2, success: false, peer: "test".to_owned(), entries_count: 0 }).await?;
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_no_step_down() -> anyhow::Result<()> {
        let (network_inbox, _rx) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec!["test".to_owned()], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.state = State::Leader;
        node.next_index.insert("test".to_owned(), 1);
        node.handle_append_entries_reply(AppendEntriesReplyData { term: 1, success: false, peer: "test".to_owned(), entries_count: 0 }).await?;
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Leader);
        assert_eq!(*node.next_index.get("test").unwrap(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_unsuccess_decrement_next_index() -> anyhow::Result<()> {
        let (network_inbox, _rx) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec!["test".to_owned()], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.next_index.insert("test".to_owned(), 3);
        node.state = State::Leader;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd2".to_string() });
        node.entries.push(LogEntry { term: 1, command: "cmd3".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd4".to_string() });
        node.handle_append_entries_reply(AppendEntriesReplyData { term: 2, success: false, peer: "test".to_owned(), entries_count: 0 }).await?;
        assert_eq!(*node.next_index.get("test").unwrap(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_unsuccess_next_index_stays_1() -> anyhow::Result<()> {
        let (network_inbox, _rx) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec!["test".to_owned()], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.next_index.insert("test".to_owned(), 1);
        node.state = State::Leader;
        node.entries.push(LogEntry { term: 1, command: "cmd1".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd2".to_string() });
        node.entries.push(LogEntry { term: 1, command: "cmd3".to_string() });
        node.entries.push(LogEntry { term: 2, command: "cmd4".to_string() });
        node.handle_append_entries_reply(AppendEntriesReplyData { term: 2, success: false, peer: "test".to_owned(), entries_count: 0 }).await?;
        assert_eq!(*node.next_index.get("test").unwrap(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_commands_applies_everything_after_commit_index() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec!["test".to_owned()], network_inbox, "self".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.commit_index = 3;
        node.entries.push(LogEntry { term: 1, command: "set key1 val1".to_string() });
        node.entries.push(LogEntry { term: 1, command: "set key2 val2".to_string() });
        node.entries.push(LogEntry { term: 1, command: "set key1 val3".to_string() });
        let (snd1, rcv1) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd2, rcv2) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd3, rcv3) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        node.pending_clients.insert(1, snd1);
        node.pending_clients.insert(2, snd2);
        node.pending_clients.insert(3, snd3);
        node.apply_commands().await?;
        let res1 = rcv1.await?;
        let res2 = rcv2.await?;
        let res3 = rcv3.await?;
        assert_eq!(node.state_machine.get("key1").unwrap(), "val3");
        assert_eq!(node.state_machine.get("key2").unwrap(), "val2");
        assert!(res1.success);
        assert!(res2.success);
        assert!(res3.success);
        assert_eq!(node.last_applied, node.commit_index);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_commands_has_state_applies_everything_after_commit_index() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec!["test".to_owned()], network_inbox, "self".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.commit_index = 3;
        node.last_applied = 1;
        node.state_machine.apply("set key1 val1".to_string()).unwrap();
        node.entries.push(LogEntry { term: 1, command: "set key1 val3".to_string() });
        node.entries.push(LogEntry { term: 1, command: "set key2 val2".to_string() });
        node.entries.push(LogEntry { term: 1, command: "set key3 val3".to_string() });
        let (snd2, rcv2) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd3, rcv3) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        node.pending_clients.insert(2, snd2);
        node.pending_clients.insert(3, snd3);
        node.apply_commands().await?;
        let res2 = rcv2.await?;
        let res3 = rcv3.await?;
        assert_eq!(node.state_machine.get("key1").unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").unwrap(), "val2");
        assert_eq!(node.state_machine.get("key3").unwrap(), "val3");
        assert!(res2.success);
        assert!(res3.success);
        assert_eq!(node.last_applied, node.commit_index);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_same_term_does_not_reset_voted_for() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(vec![], network_inbox, "node1".to_string(), TestPersister, LSMTree::init()).await?;
        node.current_term = 2;
        node.voted_for = Some("node2".to_string());
        node.state = State::Follower;
        let reply = node.handle_append_entries(AppendEntriesData { term: 2, prev_log_index: 0, prev_log_term: 0, leader_commit: 0, leader_id: "node2".to_string(), entries: vec![] }).await?;
        assert!(reply.success);
        assert_eq!(node.voted_for, Some("node2".to_string()));
        assert_eq!(node.current_term, 2);
        Ok(())
    }
}
