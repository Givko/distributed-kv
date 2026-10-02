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
            .map(|e| {
                Encoder::encoded_len(&e.to_entry().expect("Failed to convert LogEntry to Entry"))
            })
            .sum();

        self.entries_wal.truncate(bytes_to_keep).await?;
        self.node_state.entries.truncate(index);
        Ok(())
    }

    pub(super) async fn append_entry(&mut self, entry: LogEntry) {
        let wal_entry: Entry = entry
            .to_entry()
            .expect("Failed to convert LogEntry to Entry");
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
                                key: String::from_utf8_lossy(&entry.key).to_string(),
                                value: String::from_utf8_lossy(&entry.value).to_string(),
                            },
                        }),
                        OP_DELETE => self.node_state.entries.push(LogEntry {
                            term: entry.index,
                            command: Command::Delete {
                                key: String::from_utf8_lossy(&entry.key).to_string(),
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
