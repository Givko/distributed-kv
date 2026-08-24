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

        // Recover state machine
        // before applying any committed entries to ensure the state machine is up to date
        // with the latest persisted state
        node.state_machine.recover().await;

        // last_applied is derived from the WAL: it reflects exactly how many
        // Raft entries the state machine has durably applied (WAL entry count
        // equals the 1-based Raft log index of the last applied command).
        node.last_applied = node.state_machine.last_applied_index();

        // Close the gap left by a crash between a Raft commit and the WAL flush:
        // those entries are committed and still in the log, so they must be
        // applied now rather than waiting for the first incoming message. This
        // is safe — committed entries are never truncated — and it keeps the
        // "commit_index > last_applied implies apply" invariant true from startup.
        node.apply_commands().await?;
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
        };
        self.state_persister.save_state(&persistent_state).await?;
        Ok(())
    }

    /// Step down to follower for a newer term: adopt the term, clear any vote
    /// cast in the old term, and drop to the follower role. This only mutates
    /// in-memory state — callers are responsible for persisting afterwards.
    pub(super) fn step_down(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.voted_for = None;
        self.state = State::Follower;
    }

    /// Transition to leader after winning an election: initialize the per-peer
    /// replication indices and assume the leader role.
    pub(super) fn become_leader(&mut self) {
        let next_index = self.last_log_index() + 1;
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), next_index);
            self.match_index.insert(peer.clone(), 0);
        }
        self.state = State::Leader;
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

        for i in self.last_applied + 1..=self.commit_index {
            let command = self
                .get_log_entry(i)
                .expect("committed entry missing from log")
                .command
                .clone();

            self.state_machine.apply(command).await?;
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
    use crate::raft::node::test_helpers::{
        FailingLoadPersister, LSMTree, LoadedStatePersister, PreloadedMockWal, TestPersister,
    };
    use crate::raft::raft_types::{AppendEntriesData, RequestVoteData, RequestVoteReplyData};
    use crate::raft::state_persister::PersistentState;
    use crate::storage::entry::Entry as WalEntry;
    use crate::storage::lsm_tree::LSMTree as RealLSMTree;

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
            },
        };
        let node = Node::new(
            peers,
            network_inbox,
            "node1".to_string(),
            persister,
            LSMTree::new(),
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
            LSMTree::new(),
        )
        .await;
        assert!(result.is_err());
    }

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
            },
        };
        let wal = PreloadedMockWal(vec![
            WalEntry::set(0, b"key1".to_vec(), b"val1".to_vec()),
            WalEntry::set(1, b"key2".to_vec(), b"val2".to_vec()),
        ]);
        let node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(wal),
        )
        .await?;

        assert_eq!(node.last_applied, 2);
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        Ok(())
    }

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
            },
        };
        let wal = PreloadedMockWal(vec![
            WalEntry::set(0, b"key1".to_vec(), b"val1".to_vec()),
            WalEntry::set(1, b"key1".to_vec(), b"val2".to_vec()),
        ]);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(wal),
        )
        .await?;

        assert_eq!(node.last_applied, 2);
        node.apply_commands().await?;
        assert_eq!(node.last_applied, 2);
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val2");
        Ok(())
    }

    #[tokio::test]
    async fn test_recover_applies_wal_gap_from_log_on_init() -> anyhow::Result<()> {
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
            },
        };

        let wal = PreloadedMockWal(vec![WalEntry::set(0, b"key1".to_vec(), b"val1".to_vec())]);
        let node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(wal),
        )
        .await?;

        assert_eq!(node.last_applied, 2);
        assert_eq!(node.commit_index, 2);
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_message_applies_newly_committed_entries() -> anyhow::Result<()> {
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
                commit_index: 0,
            },
        };
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            RealLSMTree::with_wal(PreloadedMockWal(vec![])),
        )
        .await?;
        assert_eq!(node.last_applied, 0);
        assert!(node.state_machine.get("key1").await.is_none());

        node.handle_message(RaftMsg::AppendEntries {
            append_request: AppendEntriesData {
                term: 1,
                prev_log_index: 2,
                prev_log_term: 1,
                leader_commit: 2,
                leader_id: "node2".to_string(),
                entries: vec![],
            },
            reply_channel: None,
        })
        .await?;

        assert_eq!(node.commit_index, 2);
        assert_eq!(node.last_applied, 2);
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
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
            LSMTree::new(),
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
            LSMTree::new(),
        )
        .await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.commit_index = 3;
        node.last_applied = 1;
        node.state_machine
            .apply("set key1 val1".to_string())
            .await
            .unwrap();
        node.entries.push(LogEntry {
            term: 1,
            command: "set key1 val3".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key2 val2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key3 val3".to_string(),
        });
        let (snd2, rcv2) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd3, rcv3) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        node.pending_clients.insert(2, snd2);
        node.pending_clients.insert(3, snd3);
        node.apply_commands().await?;
        let res2 = rcv2.await?;
        let res3 = rcv3.await?;
        assert_eq!(node.state_machine.get("key1").await.unwrap(), "val1");
        assert_eq!(node.state_machine.get("key2").await.unwrap(), "val2");
        assert_eq!(node.state_machine.get("key3").await.unwrap(), "val3");
        assert!(res2.success);
        assert!(res3.success);
        assert_eq!(node.last_applied, node.commit_index);
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
            LSMTree::new(),
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
            LSMTree::new(),
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
            LSMTree::new(),
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
            LSMTree::new(),
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
