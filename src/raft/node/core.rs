use crate::raft::network_types::OutMsg;
use crate::raft::raft_types::{ChangeStateReply, LogEntry, RaftMsg};
use crate::raft::state_machine::StateMachine;
use crate::raft::state_machine::StorageEngine;
use crate::raft::state_persister::{PersistentState, Persister};
use rand::Rng;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;

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
        let init_node_state = node.state_persister.load_state().await?;
        node.current_term = init_node_state.current_term;
        node.voted_for = init_node_state.voted_for;
        node.entries = init_node_state.entries;
        node.commit_index = init_node_state.commit_index;
        node.snapshot_last_index = init_node_state.snapshot_last_index;
        node.snapshot_last_term = init_node_state.snapshot_last_term;
        node.last_applied = node.snapshot_last_index;

        // Recover state machine
        // before applying any committed entries to ensure the state machine is up to date
        // with the latest persisted state
        node.state_machine.recover().await?;
        Ok(node)
    }

    fn reset_election_timer(&self) -> Instant {
        let duration = if self.state == State::Leader {
            Duration::from_millis(50)
        } else {
            let mut rng = rand::rng();
            Duration::from_millis(rng.random_range(150..300))
        };
        Instant::now() + duration
    }
    pub async fn run(mut self, mut inbox: Receiver<RaftMsg>) -> anyhow::Result<()> {
        let sleep = tokio::time::sleep(Duration::from_millis(0));
        tokio::pin!(sleep);
        sleep.as_mut().reset(self.reset_election_timer());
        loop {
            tokio::select! {
                option = inbox.recv() => {
                    let Some(msg) = option else { return Ok(()); };
                    let reset_timer = self.handle_message(msg).await?;
                    if reset_timer {
                        sleep.as_mut().reset(self.reset_election_timer());
                    }
                },
                () = sleep.as_mut() => {
                    if self.state == State::Leader {
                        self.send_heartbeat().await?;
                    }
                    else{
                        self.start_election().await?;
                    }
                    sleep.as_mut().reset(self.reset_election_timer());
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
            snapshot_last_index: self.snapshot_last_index,
            snapshot_last_term: self.snapshot_last_term,
        };
        self.state_persister.save_state(&persistent_state).await?;
        Ok(())
    }

    pub(super) async fn handle_message(&mut self, msg: RaftMsg) -> anyhow::Result<bool> {
        let mut reset_timer = false;
        match msg {
            RaftMsg::VoteRequest {
                vote_request,
                reply_channel,
            } => {
                eprintln!("Got requestVote in node");
                let vote_reply = self.handle_vote_request(vote_request).await?;
                reset_timer = vote_reply.vote;
                self.send_to_reply_channel(reply_channel, vote_reply)?;
            }
            RaftMsg::AppendEntries {
                append_request,
                reply_channel,
            } => {
                let reply = self.handle_append_entries(append_request).await?;
                reset_timer = true;
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

                    self.entries.push(LogEntry {
                        term: self.current_term,
                        command: command.clone(),
                    });
                    self.persist_state()
                        .await
                        .expect("Failed to persist state after adding new command");
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

                    // this is required for sinle node clusters
                    // as it will have mojority of 1 and can commit immediately
                    self.move_commit_index().await?;

                    let log_index = self.last_log_index();
                    if let Some(reply_channel) = reply_channel {
                        self.pending_clients.insert(log_index, reply_channel);
                    }
                }
            }
            RaftMsg::GetState { key, reply_channel } => {
                let val = self
                    .state_machine
                    .get(&key)
                    .await
                    .unwrap_or_default()
                    .to_owned();
                self.send_to_reply_channel(Some(reply_channel), val)?;
            }
        }

        self.apply_commands().await?;
        Ok(reset_timer)
    }

    pub(super) async fn apply_commands(&mut self) -> anyhow::Result<()> {
        if self.commit_index <= self.last_applied {
            return Ok(());
        }

        let range = self.last_applied + 1..=self.commit_index;
        let mut last_applied_term = self.snapshot_last_term;
        for i in range {
            let entry = self
                .get_log_entry(i)
                .expect("committed entry missing from log")
                .clone();
            let command = entry.command.clone();

            self.state_machine.apply(i, command).await?;
            last_applied_term = entry.term;

            let reply_channel = self.pending_clients.remove(&i);
            let reply = ChangeStateReply {
                success: true,
                leader: self.id.clone(),
            };
            self.send_to_reply_channel(reply_channel, reply)?;
        }

        let entries_applied = (self.commit_index - self.snapshot_last_index) as usize;
        self.entries.drain(..entries_applied);
        self.last_applied = self.commit_index;
        self.snapshot_last_index = self.commit_index;
        self.snapshot_last_term = last_applied_term;

        self.persist_state().await?;

        Ok(())
    }

    pub(super) async fn move_commit_index(&mut self) -> anyhow::Result<()> {
        for log_index in self.commit_index + 1..=self.last_log_index() {
            let mut count = 1; // self
            for (_, value) in self.match_index.iter() {
                if *value >= log_index {
                    count += 1;
                }
            }

            if self.is_majority(count)
                && self.get_log_entry(log_index).map_or(0, |e| e.term) == self.current_term
            {
                self.commit_index = log_index;
            }
        }

        self.persist_state().await?;
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
    use crate::raft::raft_types::{AppendEntriesData, RequestVoteData, RequestVoteReplyData};
    use crate::raft::state_persister::PersistentState;
    use crate::storage::entry::Entry as WalEntry;
    use crate::storage::lsm_tree::LSMTree as RealLSMTree;
    use crate::storage::wal::WalStorage;
    use std::io;
    use std::sync::{Arc, Mutex};

    struct TestPersister;
    struct LoadedStatePersister {
        state: PersistentState,
    }
    struct FailingLoadPersister;
    struct RecordingPersister {
        saved_state: Arc<Mutex<Option<PersistentState>>>,
    }

    struct MockWal {}

    #[async_trait::async_trait]
    impl WalStorage for MockWal {
        async fn append(&mut self, _entry: &WalEntry) -> io::Result<()> {
            Ok(())
        }

        async fn read_all(&mut self) -> io::Result<Vec<WalEntry>> {
            Ok(vec![])
        }
    }

    /// A mock WAL that returns a fixed, pre-loaded set of entries from `read_all`.
    struct PreloadedMockWal {
        entries: Vec<WalEntry>,
    }

    #[async_trait::async_trait]
    impl WalStorage for PreloadedMockWal {
        async fn append(&mut self, _entry: &WalEntry) -> io::Result<()> {
            Ok(())
        }

        async fn read_all(&mut self) -> io::Result<Vec<WalEntry>> {
            Ok(self.entries.clone())
        }
    }

    struct LSMTree;

    impl LSMTree {
        async fn new() -> RealLSMTree {
            let wal = Box::new(MockWal {});
            RealLSMTree::with_wal(wal).await
        }
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
            Ok(PersistentState {
                current_term: 0,
                voted_for: None,
                entries: vec![],
                commit_index: 0,
                snapshot_last_index: 0,
                snapshot_last_term: 0,
            })
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
                snapshot_last_index: self.state.snapshot_last_index,
                snapshot_last_term: self.state.snapshot_last_term,
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
                snapshot_last_index: state.snapshot_last_index,
                snapshot_last_term: state.snapshot_last_term,
            });
            Ok(())
        }
        async fn create_snapshot(&self, _: u64, _: u64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Ok(PersistentState {
                current_term: 0,
                voted_for: None,
                entries: vec![],
                commit_index: 0,
                snapshot_last_index: 0,
                snapshot_last_term: 0,
            })
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
                    LogEntry {
                        term: 5,
                        command: "set key1 val1".to_string(),
                    },
                    LogEntry {
                        term: 7,
                        command: "set key2 val2".to_string(),
                    },
                ],
                commit_index: 2,
                snapshot_last_index: 1,
                snapshot_last_term: 5,
            },
        };
        let node = Node::new(
            peers,
            network_inbox,
            "node1".to_string(),
            persister,
            LSMTree::new().await,
        )
        .await?;
        assert_eq!(node.current_term, 7);
        assert_eq!(node.voted_for, Some("node3".to_string()));
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[0].term, 5);
        assert_eq!(node.entries[0].command, "set key1 val1");
        assert_eq!(node.entries[1].term, 7);
        assert_eq!(node.entries[1].command, "set key2 val2");
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.snapshot_last_index, 1);
        assert_eq!(node.snapshot_last_term, 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_new_returns_error_when_state_loading_fails() {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let result = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            FailingLoadPersister,
            LSMTree::new().await,
        )
        .await;
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
                    LogEntry {
                        term: 4,
                        command: "set key1 val1".to_string(),
                    },
                    LogEntry {
                        term: 4,
                        command: "set key2 val2".to_string(),
                    },
                    LogEntry {
                        term: 4,
                        command: "set key1 val3".to_string(),
                    },
                ],
                commit_index: 2,
                snapshot_last_index: 2,
                snapshot_last_term: 4,
            },
        };
        // Pre-populate the WAL with the two committed entries so that WAL recovery
        // restores the state machine instead of relying on apply_commands.
        let wal = PreloadedMockWal {
            entries: vec![
                WalEntry::set(1, b"key1".to_vec(), b"val1".to_vec()),
                WalEntry::set(2, b"key2".to_vec(), b"val2".to_vec()),
            ],
        };
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(Box::new(wal)).await,
        )
        .await?;
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        assert_eq!(node.last_applied, 2);
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.snapshot_last_index, 2);
        assert_eq!(node.snapshot_last_term, 4);
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
                    LogEntry {
                        term: 4,
                        command: "set key1 val1".to_string(),
                    },
                    LogEntry {
                        term: 4,
                        command: "set key2 val2".to_string(),
                    },
                    LogEntry {
                        term: 4,
                        command: "set key1 val3".to_string(),
                    },
                ],
                commit_index: 3,
                snapshot_last_index: 3,
                snapshot_last_term: 4,
            },
        };
        // Pre-populate the WAL with all three committed entries.
        let wal = PreloadedMockWal {
            entries: vec![
                WalEntry::set(1, b"key1".to_vec(), b"val1".to_vec()),
                WalEntry::set(2, b"key2".to_vec(), b"val2".to_vec()),
                WalEntry::set(3, b"key1".to_vec(), b"val3".to_vec()),
            ],
        };
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(Box::new(wal)).await,
        )
        .await?;
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val3");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        assert_eq!(node.last_applied, 3);
        assert_eq!(node.commit_index, 3);
        assert_eq!(node.snapshot_last_index, 3);
        assert_eq!(node.snapshot_last_term, 4);
        Ok(())
    }

    // WAL recovery is the source of truth for last_applied:
    // after Node::new, last_applied must equal the number of entries
    // the WAL restored — independent of commit_index.
    #[tokio::test]
    async fn test_recover_sets_last_applied_from_wal() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 1,
                voted_for: None,
                entries: vec![
                    LogEntry {
                        term: 1,
                        command: "set key1 val1".to_string(),
                    },
                    LogEntry {
                        term: 1,
                        command: "set key2 val2".to_string(),
                    },
                ],
                commit_index: 2,
                snapshot_last_index: 2,
                snapshot_last_term: 1,
            },
        };
        let wal = PreloadedMockWal {
            entries: vec![
                WalEntry::set(1, b"key1".to_vec(), b"val1".to_vec()),
                WalEntry::set(2, b"key2".to_vec(), b"val2".to_vec()),
            ],
        };
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(Box::new(wal)).await,
        )
        .await?;
        // last_applied must come from the WAL (highest raft index = 2).
        assert_eq!(node.last_applied, 2);
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        Ok(())
    }

    // After WAL recovery last_applied == commit_index, so apply_commands must
    // be a no-op — the already-applied entries must not be applied again.
    #[tokio::test]
    async fn test_recover_does_not_reapply_entries_on_apply_commands() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 1,
                voted_for: None,
                entries: vec![
                    LogEntry {
                        term: 1,
                        command: "set key1 val1".to_string(),
                    },
                    LogEntry {
                        term: 1,
                        command: "set key1 val2".to_string(),
                    },
                ],
                commit_index: 2,
                snapshot_last_index: 2,
                snapshot_last_term: 1,
            },
        };
        let wal = PreloadedMockWal {
            entries: vec![
                WalEntry::set(1, b"key1".to_vec(), b"val1".to_vec()),
                WalEntry::set(2, b"key1".to_vec(), b"val2".to_vec()),
            ],
        };
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(Box::new(wal)).await,
        )
        .await?;
        assert_eq!(node.last_applied, 2);
        // Explicitly calling apply_commands must be a no-op because
        // last_applied already equals commit_index.
        node.apply_commands().await?;
        assert_eq!(node.last_applied, 2);
        // Value must still be val2 — apply_commands did not re-run the entries.
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val2");
        Ok(())
    }

    // Simulates a crash between a Raft commit and the corresponding WAL flush.
    // The WAL only has entry 1; entry 2 was committed but never flushed.
    // Node::new must NOT apply the gap — last_applied stays at 1 and the
    // uncommitted-to-WAL entry is absent from the state machine.
    #[tokio::test]
    async fn test_recover_with_partial_wal_gap_not_applied_on_init() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 1,
                voted_for: None,
                entries: vec![
                    LogEntry {
                        term: 1,
                        command: "set key1 val1".to_string(),
                    },
                    LogEntry {
                        term: 1,
                        command: "set key2 val2".to_string(),
                    },
                ],
                // Both entries are committed according to persisted Raft state …
                commit_index: 2,
                snapshot_last_index: 1,
                snapshot_last_term: 4,
            },
        };
        // … but the WAL only recorded the first one before the crash.
        let wal = PreloadedMockWal {
            entries: vec![WalEntry::set(1, b"key1".to_vec(), b"val1".to_vec())],
        };
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(Box::new(wal)).await,
        )
        .await?;
        // last_applied reflects WAL state only — the gap entry was not applied.
        assert_eq!(node.last_applied, 1);
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.snapshot_last_index, 1);
        assert_eq!(node.snapshot_last_term, 4);
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val1");
        // key2 was never flushed to WAL, so it must be absent.
        assert!(node.state_machine.get("key2").await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_single_node_leader_change_state_applies_new_entry() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let saved_state = Arc::new(Mutex::new(None));
        let persister = RecordingPersister {
            saved_state: saved_state.clone(),
        };
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            LSMTree::new().await,
        )
        .await?;
        node.state = State::Leader;
        node.current_term = 3;
        node.handle_message(RaftMsg::ChangeState {
            command: "set key1 value1".to_string(),
            reply_channel: None,
        })
        .await?;
        let persisted = saved_state.lock().unwrap();
        let persisted = persisted.as_ref().expect("should persist");
        assert_eq!(persisted.entries.len(), 0); // in a single node cluster 1 is a majority and the entry is immediately committed and applied, so it should be removed from the log
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_commands_applies_everything_after_commit_index() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["test".to_owned()],
            network_inbox,
            "self".to_string(),
            TestPersister,
            LSMTree::new().await,
        )
        .await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.commit_index = 3;
        node.entries.push(LogEntry {
            term: 1,
            command: "set key1 val1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key2 val2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key1 val3".to_string(),
        });
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
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val3");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        assert!(res1.success);
        assert!(res2.success);
        assert!(res3.success);
        assert_eq!(node.last_applied, node.commit_index);
        assert_eq!(node.snapshot_last_index, node.commit_index);
        assert_eq!(node.snapshot_last_term, 1);
        assert_eq!(node.entries.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_commands_has_state_applies_everything_after_commit_index()
    -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["test".to_owned()],
            network_inbox,
            "self".to_string(),
            TestPersister,
            LSMTree::new().await,
        )
        .await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.commit_index = 1;
        node.entries.push(LogEntry {
            term: 1,
            command: "set key1 val3".to_string(),
        });
        node.apply_commands().await?;
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val3");
        assert_eq!(node.last_applied, node.commit_index);
        assert_eq!(node.snapshot_last_index, node.commit_index);
        assert_eq!(node.snapshot_last_term, 1);
        assert_eq!(node.entries.len(), 0);

        node.commit_index = 3;
        node.entries.push(LogEntry {
            term: 1,
            command: "set key2 val2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "set key3 val3".to_string(),
        });
        let (snd2, rcv2) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd3, rcv3) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        node.pending_clients.insert(2, snd2);
        node.pending_clients.insert(3, snd3);
        node.apply_commands().await?;
        let res2 = rcv2.await?;
        let res3 = rcv3.await?;
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val3");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        assert_eq!(node.state_machine.get("key3").await.unwrap(), "val3");
        assert!(res2.success);
        assert!(res3.success);
        assert_eq!(node.last_applied, node.commit_index);
        assert_eq!(node.snapshot_last_index, node.commit_index);
        assert_eq!(node.snapshot_last_term, 2);
        assert_eq!(node.entries.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_message_append_entries_resets_timer() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new().await,
        )
        .await?;

        node.current_term = 2;
        node.state = State::Follower;

        let reset_timer = node
            .handle_message(RaftMsg::AppendEntries {
                append_request: AppendEntriesData {
                    term: 2,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    leader_commit: 0,
                    leader_id: "node2".to_string(),
                    entries: vec![],
                },
                reply_channel: None,
            })
            .await?;

        assert!(reset_timer);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_message_vote_request_granted_resets_timer() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new().await,
        )
        .await?;

        let reset_timer = node
            .handle_message(RaftMsg::VoteRequest {
                vote_request: RequestVoteData {
                    term: 1,
                    last_log_index: 0,
                    last_log_term: 0,
                    candidate: "node2".to_string(),
                },
                reply_channel: None,
            })
            .await?;

        assert!(reset_timer);
        assert_eq!(node.voted_for, Some("node2".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_message_get_state_does_not_reset_timer() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new().await,
        )
        .await?;

        let (tx, _rx) = tokio::sync::oneshot::channel();

        let reset_timer = node
            .handle_message(RaftMsg::GetState {
                key: "missing".to_string(),
                reply_channel: tx,
            })
            .await?;

        assert!(!reset_timer);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_message_request_vote_reply_does_not_reset_timer() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["node2".to_string()],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new().await,
        )
        .await?;

        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };

        let reset_timer = node
            .handle_message(RaftMsg::RequestVoteReply {
                vote_reply: RequestVoteReplyData {
                    term: 1,
                    vote: true,
                },
                reply_channel: None,
            })
            .await?;

        assert!(!reset_timer);
        Ok(())
    }
}
