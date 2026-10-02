use super::{Node, State};
use crate::raft::network_types::OutMsg;
use crate::raft::raft_types::{AppendEntriesData, AppendEntriesReplyData, LogEntry};
use crate::raft::state_machine::StorageEngine;
use crate::raft::state_persister::Persister;

impl<T: Persister + Send + Sync, SM: StorageEngine> Node<T, SM> {
    pub(super) async fn send_heartbeat(&self) -> anyhow::Result<()> {
        for peer in &self.node_state.peers {
            let prev_log_index = self.last_log_index();
            let prev_log_term = self
                .node_state
                .entries
                .last()
                .map_or(self.node_state.snapshot_last_term, |e| e.term);
            let out_msg = OutMsg::AppendEntries {
                term: self.node_state.current_term,
                leader_id: self.node_state.id.clone(),
                entries: vec![],
                prev_log_index,
                prev_log_term,
                leader_commit: self.node_state.commit_index,
                peer: peer.clone(),
            };
            self.network_inbox.send(out_msg).await?;
        }
        Ok(())
    }

    /// Moves `commit_index` to the highest current-term entry replicated on a
    /// majority. In a single-node cluster the leader alone is a majority, so
    /// this commits new entries without waiting for any reply.
    pub(super) fn advance_commit_index(&mut self) {
        for log_index in self.node_state.commit_index + 1..=self.last_log_index() {
            let mut count = 1; // self
            for value in self.node_state.match_index.values() {
                if *value >= log_index {
                    count += 1;
                }
            }

            if self.is_majority(count)
                && self.get_log_entry(log_index).map_or(0, |e| e.term)
                    == self.node_state.current_term
            {
                self.node_state.commit_index = log_index;
            }
        }
    }

    pub(super) async fn handle_append_entries(
        &mut self,
        append_request: AppendEntriesData,
    ) -> anyhow::Result<AppendEntriesReplyData> {
        if self.node_state.current_term > append_request.term
            || self.last_log_index() < append_request.prev_log_index
            || (append_request.prev_log_index != 0
                && self.get_log_term(append_request.prev_log_index) != append_request.prev_log_term)
        {
            if self.node_state.current_term < append_request.term {
                self.step_down(append_request.term);
            }

            self.persist_state().await?;
            return Ok(AppendEntriesReplyData {
                term: self.node_state.current_term,
                success: false,
                peer: self.node_state.id.clone(),
                entries_count: 0,
            });
        }

        if self.node_state.current_term < append_request.term {
            self.node_state.current_term = append_request.term;
            self.node_state.voted_for = None;
        }

        if self.node_state.state != State::Follower {
            self.node_state.leader = append_request.leader_id.clone();
            self.node_state.state = State::Follower;
        }

        if append_request.prev_log_index != 0
            && self.last_log_index() > append_request.prev_log_index
            && append_request.prev_log_index >= self.node_state.snapshot_last_index
        {
            let truncate_to =
                (append_request.prev_log_index - self.node_state.snapshot_last_index) as usize;

            self.truncate_log(truncate_to).await?;
        }

        let entries_count = append_request.entries.len();
        for entry in append_request.entries {
            self.append_entry(entry).await;
        }

        if append_request.leader_commit > self.node_state.commit_index {
            self.node_state.commit_index =
                std::cmp::min(self.last_log_index(), append_request.leader_commit);
        }

        self.persist_state().await?;
        Ok(AppendEntriesReplyData {
            term: self.node_state.current_term,
            success: true,
            peer: self.node_state.id.clone(),
            entries_count: entries_count as u64,
        })
    }

    pub(super) async fn handle_append_entries_reply(
        &mut self,
        append_entries_reply_data: AppendEntriesReplyData,
    ) -> anyhow::Result<()> {
        if append_entries_reply_data.success {
            let prev_log_index = *self
                .node_state
                .next_index
                .get(&append_entries_reply_data.peer)
                .expect("no peer found")
                - 1;

            let match_index = prev_log_index + append_entries_reply_data.entries_count;
            let next_index = match_index + 1;
            self.node_state
                .next_index
                .insert(append_entries_reply_data.peer.clone(), next_index);
            self.node_state
                .match_index
                .insert(append_entries_reply_data.peer.clone(), match_index);

            self.advance_commit_index();
            self.persist_state().await?;
            return Ok(());
        }

        if self.node_state.current_term < append_entries_reply_data.term {
            self.step_down(append_entries_reply_data.term);
            self.persist_state().await?;
            return Ok(());
        }

        let mut next_index = *self
            .node_state
            .next_index
            .get(&append_entries_reply_data.peer)
            .expect("no peer found in state");

        if next_index > 1 {
            next_index -= 1;
        }

        let prev_log_index = next_index - 1;
        let prev_log_term = self.get_log_term(prev_log_index);

        // TODO: if prev_log_index < snapshot_last_index, we need InstallSnapshot instead
        let start_index = (prev_log_index.saturating_sub(self.node_state.snapshot_last_index)
            as usize)
            .min(self.node_state.entries.len());

        let entries_to_send: Vec<LogEntry> = self.node_state.entries[start_index..]
            .iter()
            .map(|e| LogEntry {
                term: e.term,
                command: e.command.clone(),
            })
            .collect();

        self.node_state
            .next_index
            .insert(append_entries_reply_data.peer.clone(), next_index);

        let append_entries = OutMsg::AppendEntries {
            term: self.node_state.current_term,
            peer: append_entries_reply_data.peer,
            prev_log_index,
            prev_log_term,
            leader_commit: self.node_state.commit_index,
            leader_id: self.node_state.id.clone(),
            entries: entries_to_send,
        };

        self.network_inbox.send(append_entries).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::common::encoder::Encoder;
    use crate::common::entry::Entry;
    use crate::raft::node::test_helpers::{
        InMemoryWal, LSMTree, LoadedStatePersister, MockWal, RecordingPersister, TestPersister,
    };
    use crate::raft::node::utils::{RandGen, SystemClock};
    use crate::raft::raft_types::{Command, RaftMsg, RequestVoteData};
    use crate::raft::state_persister::PersistentState;

    // ============================================================
    // Replication: AppendEntries handling, AppendEntries replies,
    //              and leader command replication / commit advance
    // ============================================================

    #[tokio::test]
    async fn test_leader_change_state_persists_new_entry() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let saved_state = Arc::new(Mutex::new(None));
        let persister = RecordingPersister {
            saved_state: saved_state.clone(),
        };
        let (entries_wal, wal_data) = InMemoryWal::new();
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            persister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(entries_wal),
        )
        .await?;
        node.node_state.state = State::Leader;
        node.node_state.current_term = 3;
        node.handle_message(RaftMsg::ChangeState {
            command: Command::Set {
                key: "key1".to_string(),
                value: "value1".to_string(),
            },
            reply_channel: None,
        })
        .await?;

        // The entry itself is durable in the WAL now, not in the persisted state.
        let entries = Encoder::decode_all(&wal_data.lock().unwrap())?;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0],
            Entry::set(3, b"key1".to_vec(), b"value1".to_vec())
        );

        // The persister still carries the term and the advanced commit index.
        let persisted = saved_state.lock().unwrap();
        let persisted = persisted.as_ref().expect("should persist");
        assert_eq!(persisted.current_term, 3);
        assert_eq!(persisted.commit_index, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_uses_snapshot_index_and_term_for_prev_log_match()
    -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 4;
        node.node_state.snapshot_last_index = 5;
        node.node_state.snapshot_last_term = 3;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 4,
                prev_log_index: 5,
                prev_log_term: 3,
                leader_commit: 10,
                leader_id: "node2".to_string(),
                entries: vec![LogEntry {
                    term: 4,
                    command: Command::Set {
                        key: "key1".to_string(),
                        value: "val1".to_string(),
                    },
                }],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.entries.len(), 1);
        assert_eq!(node.last_log_index(), 6);
        assert_eq!(node.node_state.commit_index, 6);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_rejects_when_snapshot_prev_log_term_mismatches()
    -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 4;
        node.node_state.snapshot_last_index = 5;
        node.node_state.snapshot_last_term = 3;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 4,
                prev_log_index: 5,
                prev_log_term: 9,
                leader_commit: 10,
                leader_id: "node2".to_string(),
                entries: vec![LogEntry {
                    term: 4,
                    command: Command::Set {
                        key: "key1".to_string(),
                        value: "val1".to_string(),
                    },
                }],
            })
            .await?;
        assert!(!reply.success);
        assert!(node.node_state.entries.is_empty());
        assert_eq!(node.last_log_index(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 1;
        node.node_state.state = State::Leader;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.current_term, 2);
        assert_eq!(node.node_state.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_term() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 1;
        node.node_state.state = State::Leader;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.current_term, 2);
        assert_eq!(node.node_state.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_request() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Leader;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(!reply.success);
        assert_eq!(node.node_state.current_term, 2);
        assert_eq!(node.node_state.state, State::Leader);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_prev_log_index_high() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Leader;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 3,
                prev_log_index: 1,
                prev_log_term: 0,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(!reply.success);
        assert_eq!(node.node_state.current_term, 3);
        assert_eq!(node.node_state.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_prev_log_term_mismatch() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Leader;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 3,
                prev_log_index: 1,
                prev_log_term: 0,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(!reply.success);
        assert_eq!(node.node_state.current_term, 3);
        assert_eq!(node.node_state.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_successful_append() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Follower;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 2,
                prev_log_index: 1,
                prev_log_term: 1,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![LogEntry {
                    term: 2,
                    command: Command::Set {
                        key: "key2".to_string(),
                        value: "val2".to_string(),
                    },
                }],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.entries.len(), 2);
        assert_eq!(node.node_state.entries[1].term, 2);
        assert_eq!(
            node.node_state.entries[1].command,
            Command::Set {
                key: "key2".to_string(),
                value: "val2".to_string(),
            }
        );
        assert_eq!(node.node_state.commit_index, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_conflicting_entries() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Follower;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key2".to_string(),
                value: "val2".to_string(),
            },
        });
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 3,
                prev_log_index: 1,
                prev_log_term: 1,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![LogEntry {
                    term: 3,
                    command: Command::Set {
                        key: "key3".to_string(),
                        value: "val3".to_string(),
                    },
                }],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.entries.len(), 2);
        assert_eq!(node.node_state.entries[1].term, 3);
        assert_eq!(
            node.node_state.entries[1].command,
            Command::Set {
                key: "key3".to_string(),
                value: "val3".to_string(),
            }
        );
        assert_eq!(node.node_state.commit_index, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_heartbeat_update_commit_index_with_leader()
    -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Follower;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key2".to_string(),
                value: "val2".to_string(),
            },
        });
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 3,
                prev_log_index: 2,
                prev_log_term: 2,
                leader_commit: 2,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.entries.len(), 2);
        assert_eq!(node.node_state.commit_index, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_update_commit_with_entries_length() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Follower;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key2".to_string(),
                value: "val2".to_string(),
            },
        });
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 3,
                prev_log_index: 2,
                prev_log_term: 2,
                leader_commit: 4,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.entries.len(), 2);
        assert_eq!(node.node_state.commit_index, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_update_commit_with_new_entries_length() -> anyhow::Result<()>
    {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Follower;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key2".to_string(),
                value: "val2".to_string(),
            },
        });
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 3,
                prev_log_index: 2,
                prev_log_term: 2,
                leader_commit: 4,
                leader_id: "node2".to_string(),
                entries: vec![LogEntry {
                    term: 3,
                    command: Command::Set {
                        key: "key3".to_string(),
                        value: "val3".to_string(),
                    },
                }],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.entries.len(), 3);
        assert_eq!(node.node_state.entries[2].term, 3);
        assert_eq!(
            node.node_state.entries[2].command,
            Command::Set {
                key: "key3".to_string(),
                value: "val3".to_string(),
            }
        );
        assert_eq!(node.node_state.commit_index, 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_same_term_does_not_reset_voted_for() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.voted_for = Some("node2".to_string());
        node.node_state.state = State::Follower;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 0,
                leader_id: "node2".to_string(),
                entries: vec![],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.node_state.voted_for, Some("node2".to_string()));
        assert_eq!(node.node_state.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_step_down() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 1;
        node.node_state.state = State::Leader;
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 2,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(node.node_state.current_term, 2);
        assert_eq!(node.node_state.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_no_step_down() -> anyhow::Result<()> {
        let (network_inbox, _rx) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["test".to_owned()],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.state = State::Leader;
        node.node_state.next_index.insert("test".to_owned(), 1);
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 1,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(node.node_state.current_term, 2);
        assert_eq!(node.node_state.state, State::Leader);
        assert_eq!(*node.node_state.next_index.get("test").unwrap(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_unsuccess_decrement_next_index() -> anyhow::Result<()>
    {
        let (network_inbox, _rx) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["test".to_owned()],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.next_index.insert("test".to_owned(), 3);
        node.node_state.state = State::Leader;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key2".to_string(),
                value: "val2".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key3".to_string(),
                value: "val3".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key4".to_string(),
                value: "val4".to_string(),
            },
        });
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 2,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(*node.node_state.next_index.get("test").unwrap(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_unsuccess_next_index_stays_1() -> anyhow::Result<()> {
        let (network_inbox, _rx) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["test".to_owned()],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.current_term = 2;
        node.node_state.next_index.insert("test".to_owned(), 1);
        node.node_state.state = State::Leader;
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key2".to_string(),
                value: "val2".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 1,
            command: Command::Set {
                key: "key3".to_string(),
                value: "val3".to_string(),
            },
        });
        node.node_state.entries.push(LogEntry {
            term: 2,
            command: Command::Set {
                key: "key4".to_string(),
                value: "val4".to_string(),
            },
        });
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 2,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(*node.node_state.next_index.get("test").unwrap(), 1);
        Ok(())
    }

    // ------------------------------------------------------------
    // Term advance on a *rejected* AppendEntries must clear the vote
    // ------------------------------------------------------------

    #[tokio::test]
    async fn test_rejected_append_entries_at_higher_term_clears_vote() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 1,
                voted_for: Some("node2".to_string()),
                commit_index: 0,
            },
        };
        let mut node = Node::new(
            vec!["node2".to_string(), "node3".to_string()],
            network_inbox,
            "node1".to_string(),
            persister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;
        node.node_state.state = State::Candidate { votes: 1 };

        // prev_log_index 5 is past the end of an empty log, so this is rejected
        // -- but it still carries a newer term, so the node must step down and
        // forget the vote it cast in the old term.
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 2,
                prev_log_index: 5,
                prev_log_term: 1,
                leader_commit: 0,
                leader_id: "node3".to_string(),
                entries: vec![],
            })
            .await?;

        assert!(!reply.success);
        assert_eq!(reply.term, 2);
        assert_eq!(node.node_state.current_term, 2);
        assert_eq!(node.node_state.voted_for, None);
        assert_eq!(node.node_state.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_vote_granted_in_term_entered_by_rejected_append_entries() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 1,
                voted_for: Some("node2".to_string()),
                commit_index: 0,
            },
        };
        let mut node = Node::new(
            vec!["node2".to_string(), "node3".to_string()],
            network_inbox,
            "node1".to_string(),
            persister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(MockWal),
        )
        .await?;

        node.handle_append_entries(AppendEntriesData {
            term: 2,
            prev_log_index: 5,
            prev_log_term: 1,
            leader_commit: 0,
            leader_id: "node3".to_string(),
            entries: vec![],
        })
        .await?;

        // A candidate for term 2 now asks for the vote. The node never voted in
        // term 2 -- only in term 1 -- so it must grant it. Leaving `voted_for`
        // set across the term change would deny every candidate in term 2 and
        // stall the election until another timeout.
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 2,
                candidate: "node3".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            })
            .await?;

        assert!(reply.vote);
        assert_eq!(reply.term, 2);
        assert_eq!(node.node_state.voted_for, Some("node3".to_string()));
        Ok(())
    }
}
