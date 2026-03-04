use super::{Node, State};
use crate::raft::network_types::OutMsg;
use crate::raft::raft_types::{AppendEntriesData, AppendEntriesReplyData, LogEntry};
use crate::raft::state_persister::Persister;
use crate::raft::state_machine::StorageEngine;

impl<T: Persister + Send + Sync, SM: StorageEngine> Node<T, SM> {
    pub(super) async fn send_heartbeat(&self) -> anyhow::Result<()> {
        for peer in &self.peers {
            let prev_log_index = self.next_index.get(peer).unwrap_or(&1) - 1;
            let prev_log_term = self.get_log_term(prev_log_index);

            let out_msg = OutMsg::AppendEntries {
                term: self.current_term,
                leader_id: self.id.clone(),
                entries: vec![],
                prev_log_index,
                prev_log_term,
                leader_commit: self.commit_index,
                peer: peer.clone(),
            };
            self.network_inbox.send(out_msg).await?;
        }
        Ok(())
    }

    pub(super) async fn handle_append_entries(
        &mut self,
        append_request: AppendEntriesData,
    ) -> anyhow::Result<AppendEntriesReplyData> {
        if self.current_term > append_request.term
            || self.last_log_index() < append_request.prev_log_index
            || (append_request.prev_log_index != 0
                && self.get_log_term(append_request.prev_log_index)
                    != append_request.prev_log_term)
        {
            eprintln!("current_term {}", self.current_term);
            eprintln!("prev_log_index {}", append_request.prev_log_index);
            eprintln!("prev_log_term {}", append_request.prev_log_term);
            self.state = if self.current_term < append_request.term {
                State::Follower
            } else {
                self.state
            };
            self.current_term = std::cmp::max(self.current_term, append_request.term);

            self.persist_state().await?;
            return Ok(AppendEntriesReplyData {
                term: self.current_term,
                success: false,
                peer: self.id.clone(),
                entries_count: 0,
            });
        }

        // Reset voted_for on receiving heartbeat from a higher term
        if self.current_term < append_request.term {
            self.current_term = append_request.term;
            self.voted_for = None;
        }

        self.leader = append_request.leader_id.clone();
        if self.state != State::Follower {
            eprintln!("Received append entries, stepping down to follower");
            self.state = State::Follower;
        }

        // Remove conflicting entries
        if append_request.prev_log_index != 0
            && self.last_log_index() > append_request.prev_log_index
            && append_request.prev_log_index >= self.snapshot_last_index
        {
            let truncate_to =
                (append_request.prev_log_index - self.snapshot_last_index) as usize;
            self.entries.truncate(truncate_to);
        }

        let entries_count = append_request.entries.len();
        if entries_count > 0 {
            eprintln!("committing new entries {}", entries_count);
        }

        for entry in append_request.entries {
            self.entries.push(entry);
        }

        // Update commit index
        if append_request.leader_commit > self.commit_index {
            self.commit_index =
                std::cmp::min(self.last_log_index(), append_request.leader_commit);
        }

        self.persist_state().await?;
        Ok(AppendEntriesReplyData {
            term: self.current_term,
            success: true,
            peer: self.id.clone(),
            entries_count: entries_count as u64,
        })
    }

    pub(super) async fn handle_append_entries_reply(
        &mut self,
        append_entries_reply_data: AppendEntriesReplyData,
    ) -> anyhow::Result<()> {
        if append_entries_reply_data.success {
            let prev_log_index = *self
                .next_index
                .get(&append_entries_reply_data.peer)
                .expect("no peer found")
                - 1;

            let match_index = prev_log_index + append_entries_reply_data.entries_count;
            let next_index = match_index + 1;
            self.next_index
                .insert(append_entries_reply_data.peer.clone(), next_index);
            self.match_index
                .insert(append_entries_reply_data.peer.clone(), match_index);

            for log_index in self.commit_index + 1..=self.last_log_index() {
                let mut count = 1; // self
                for (_, value) in self.match_index.iter() {
                    if *value >= log_index {
                        count += 1;
                    }
                }

                if self.is_majority(count)
                    && self.get_log_entry(log_index).map_or(0, |e| e.term)
                        == self.current_term
                {
                    self.commit_index = log_index;
                }
            }

            self.persist_state().await?;
            return Ok(());
        }

        if self.current_term < append_entries_reply_data.term {
            self.current_term = append_entries_reply_data.term;
            self.state = State::Follower;
            eprint!(
                "Stepping down to follower due to new leader with higher term {}",
                append_entries_reply_data.term
            );
            self.voted_for = None;
            self.persist_state().await?;
            return Ok(());
        }

        let mut next_index = *self
            .next_index
            .get(&append_entries_reply_data.peer)
            .expect("no peer found in state");

        eprintln!(
            "next_index {} for peer {}",
            next_index, append_entries_reply_data.peer
        );
        if next_index > 1 {
            next_index -= 1;
        }

        let prev_log_index = next_index - 1;
        let prev_log_term = self.get_log_term(prev_log_index);

        // TODO: if prev_log_index < snapshot_last_index, we need InstallSnapshot instead
        let start_index = (prev_log_index.saturating_sub(self.snapshot_last_index) as usize)
            .min(self.entries.len());

        let entries_to_send: Vec<LogEntry> = self.entries[start_index..]
            .iter()
            .map(|e| LogEntry {
                term: e.term,
                command: e.command.clone(),
            })
            .collect();

        eprintln!(
            "setting new next_index {} for {}",
            next_index,
            append_entries_reply_data.peer.clone()
        );
        self.next_index
            .insert(append_entries_reply_data.peer.clone(), next_index);

        let append_entries = OutMsg::AppendEntries {
            term: self.current_term,
            peer: append_entries_reply_data.peer,
            prev_log_index,
            prev_log_term,
            leader_commit: self.commit_index,
            leader_id: self.id.clone(),
            entries: entries_to_send,
        };

        self.network_inbox.send(append_entries).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_types::{AppendEntriesData, AppendEntriesReplyData};
    use crate::raft::state_persister::PersistentState;
    use crate::storage::entry::Entry as WalEntry;
    use crate::storage::lsm_tree::LSMTree as RealLSMTree;
    use crate::storage::wal::WalStorage;
    use std::io;
    use std::path::PathBuf;

    struct TestPersister;

    struct MockWal;

    #[async_trait::async_trait]
    impl WalStorage for MockWal {
        async fn append(&mut self, _entry: &WalEntry) -> io::Result<()> {
            Ok(())
        }

        async fn read_all(&mut self) -> io::Result<Vec<WalEntry>> {
            Ok(vec![])
        }

        fn path(&self) -> &PathBuf {
            static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
            PATH.get_or_init(|| PathBuf::from("mock-wal.log"))
        }
    }

    struct LSMTree;

    impl LSMTree {
        fn new() -> RealLSMTree {
            RealLSMTree::with_wal(Box::new(MockWal))
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
            })
        }
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
        )
        .await?;
        node.current_term = 4;
        node.snapshot_last_index = 5;
        node.snapshot_last_term = 3;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 4,
                prev_log_index: 5,
                prev_log_term: 3,
                leader_commit: 10,
                leader_id: "node2".to_string(),
                entries: vec![LogEntry {
                    term: 4,
                    command: "set key val".to_string(),
                }],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 1);
        assert_eq!(node.last_log_index(), 6);
        assert_eq!(node.commit_index, 6);
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
        )
        .await?;
        node.current_term = 4;
        node.snapshot_last_index = 5;
        node.snapshot_last_term = 3;
        let reply = node
            .handle_append_entries(AppendEntriesData {
                term: 4,
                prev_log_index: 5,
                prev_log_term: 9,
                leader_commit: 10,
                leader_id: "node2".to_string(),
                entries: vec![LogEntry {
                    term: 4,
                    command: "set key val".to_string(),
                }],
            })
            .await?;
        assert!(!reply.success);
        assert!(node.entries.is_empty());
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
        )
        .await?;
        node.current_term = 1;
        node.state = State::Leader;
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
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
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
        )
        .await?;
        node.current_term = 1;
        node.state = State::Leader;
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
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Leader;
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
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Leader);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Leader;
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
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Leader;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
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
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
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
                    command: "cmd2".to_string(),
                }],
            })
            .await?;
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
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
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
                    command: "cmd3".to_string(),
                }],
            })
            .await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[1].term, 3);
        assert_eq!(node.entries[1].command, "cmd3");
        assert_eq!(node.commit_index, 0);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
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
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.commit_index, 2);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
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
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.commit_index, 2);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
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
                    command: "cmd3".to_string(),
                }],
            })
            .await?;
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
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 2,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
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
        )
        .await?;
        node.current_term = 2;
        node.state = State::Leader;
        node.next_index.insert("test".to_owned(), 1);
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 1,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Leader);
        assert_eq!(*node.next_index.get("test").unwrap(), 1);
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
        )
        .await?;
        node.current_term = 2;
        node.next_index.insert("test".to_owned(), 3);
        node.state = State::Leader;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd3".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd4".to_string(),
        });
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 2,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(*node.next_index.get("test").unwrap(), 2);
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
        )
        .await?;
        node.current_term = 2;
        node.next_index.insert("test".to_owned(), 1);
        node.state = State::Leader;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd3".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd4".to_string(),
        });
        node.handle_append_entries_reply(AppendEntriesReplyData {
            term: 2,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        })
        .await?;
        assert_eq!(*node.next_index.get("test").unwrap(), 1);
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
        )
        .await?;
        node.current_term = 2;
        node.voted_for = Some("node2".to_string());
        node.state = State::Follower;
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
        assert_eq!(node.voted_for, Some("node2".to_string()));
        assert_eq!(node.current_term, 2);
        Ok(())
    }
}
