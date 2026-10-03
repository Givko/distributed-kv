use super::Node;
use crate::common::encoder::Encoder;
use crate::common::entry::{Entry, OP_DELETE, OP_SET};
use crate::raft::raft_types::{Command, LogEntry};
use crate::raft::state_machine::StorageEngine;
use crate::raft::state_persister::Persister;

impl<T: Persister + Send + Sync, SM: StorageEngine> Node<T, SM> {
    pub(super) fn last_log_index(&self) -> u64 {
        self.node_state.snapshot_last_index + self.node_state.entries.len() as u64
    }

    pub(super) fn get_log_entry(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.node_state.snapshot_last_index {
            None
        } else {
            self.node_state
                .entries
                .get((index - self.node_state.snapshot_last_index - 1) as usize)
        }
    }

    pub(super) fn get_log_term(&self, index: u64) -> u64 {
        if index == self.node_state.snapshot_last_index {
            self.node_state.snapshot_last_term
        } else {
            self.get_log_entry(index).map_or(0, |e| e.term)
        }
    }
    pub(super) async fn truncate_log(&mut self, index: usize) -> anyhow::Result<()> {
        let bytes_to_keep: usize = self.node_state.entries[..index]
            .iter()
            .map(|e| Encoder::encoded_len(&e.to_entry()))
            .sum();

        self.entries_wal.truncate(bytes_to_keep).await?;
        self.node_state.entries.truncate(index);
        Ok(())
    }

    pub(super) async fn append_entry(&mut self, entry: LogEntry) {
        let wal_entry: Entry = entry.to_entry();
        let encoded_entry = Encoder::encode(&wal_entry);
        self.entries_wal
            .append(&encoded_entry)
            .await
            .expect("Failed to append entry to WAL");
        self.node_state.entries.push(entry);
    }

    pub(super) async fn recover_log(&mut self) -> anyhow::Result<()> {
        match self.entries_wal.read_all().await {
            Ok(data) => {
                let entries = Encoder::decode_all(&data)?;
                for entry in entries {
                    match entry.op {
                        OP_SET => self.node_state.entries.push(LogEntry {
                            term: entry.index,
                            command: Command::Set {
                                key: String::from_utf8_lossy(entry.key).to_string(),
                                value: String::from_utf8_lossy(entry.value).to_string(),
                            },
                        }),
                        OP_DELETE => self.node_state.entries.push(LogEntry {
                            term: entry.index,
                            command: Command::Delete {
                                key: String::from_utf8_lossy(entry.key).to_string(),
                            },
                        }),
                        _ => Err(anyhow::anyhow!(
                            "Unknown operation in WAL entry: {}",
                            entry.op
                        ))?,
                    };
                }
                Ok(())
            }
            Err(e) => {
                eprintln!("Failed to read WAL during recovery: {e}");
                Err(anyhow::anyhow!(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::node::test_helpers::{LSMTree, PreloadedMockWal, TestPersister, encoded_log};
    use crate::raft::node::utils::{RandGen, SystemClock};

    /// Builds a node whose entries WAL already holds `wal_bytes`, which is what
    /// `Node::new` replays through `recover_log`.
    async fn recover_from(
        wal_bytes: Vec<u8>,
    ) -> anyhow::Result<
        Node<TestPersister, crate::storage::lsm_tree::LSMTree<super::super::test_helpers::MockWal>>,
    > {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
            Box::new(RandGen),
            Box::new(SystemClock),
            Box::new(PreloadedMockWal(wal_bytes)),
        )
        .await
    }

    #[tokio::test]
    async fn test_recover_log_rejects_unknown_op() {
        // An op byte that is neither OP_SET nor OP_DELETE means the WAL is from
        // a different format or is corrupt. Skipping the record would shift
        // every later entry down an index and silently diverge the log from the
        // leader's, so recovery has to fail loudly instead.
        let bogus = Encoder::encode(&Entry {
            index: 3,
            op: 99,
            key: b"key1",
            value: b"val1",
        });

        let error = match recover_from(bogus).await {
            Ok(_) => panic!("unknown op must fail recovery"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("Unknown operation"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn test_recover_log_rejects_unknown_op_after_valid_entries() {
        let mut wal = encoded_log(&[(
            1,
            &Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        )]);
        wal.extend(Encoder::encode(&Entry {
            index: 2,
            op: 42,
            key: b"key2",
            value: &[],
        }));

        assert!(recover_from(wal).await.is_err());
    }

    #[tokio::test]
    async fn test_recover_log_restores_delete_command() -> anyhow::Result<()> {
        let node = recover_from(encoded_log(&[
            (
                1,
                &Command::Set {
                    key: "key1".to_string(),
                    value: "val1".to_string(),
                },
            ),
            (
                4,
                &Command::Delete {
                    key: "key1".to_string(),
                },
            ),
        ]))
        .await?;

        assert_eq!(node.node_state.entries.len(), 2);
        assert_eq!(node.node_state.entries[1].term, 4);
        assert_eq!(
            node.node_state.entries[1].command,
            Command::Delete {
                key: "key1".to_string(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_recover_log_preserves_keys_and_values_containing_spaces() -> anyhow::Result<()> {
        // The command used to be a single `"set <key> <value>"` string that was
        // re-parsed on `split_whitespace`, so anything with a space in it came
        // back mangled. The typed `Command` has to survive the WAL round trip
        // byte for byte.
        let command = Command::Set {
            key: "user name".to_string(),
            value: "John  Doe".to_string(),
        };
        let node = recover_from(encoded_log(&[(1, &command)])).await?;

        assert_eq!(node.node_state.entries.len(), 1);
        assert_eq!(node.node_state.entries[0].command, command);
        Ok(())
    }

    #[tokio::test]
    async fn test_recover_log_restores_empty_value() -> anyhow::Result<()> {
        let command = Command::Set {
            key: "key1".to_string(),
            value: String::new(),
        };
        let node = recover_from(encoded_log(&[(1, &command)])).await?;

        assert_eq!(node.node_state.entries[0].command, command);
        Ok(())
    }
}
