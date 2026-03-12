use super::Node;
use crate::raft::raft_types::LogEntry;
use crate::raft::state_machine::StorageEngine;
use crate::raft::state_persister::Persister;

impl<T: Persister + Send + Sync, SM: StorageEngine> Node<T, SM> {
    pub(super) fn last_log_index(&self) -> u64 {
        self.snapshot_last_index + self.entries.len() as u64
    }

    pub(super) fn get_log_entry(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.snapshot_last_index {
            None
        } else {
            // Convert to 0-based index for the entries vector
            self.entries
                .get((index - self.snapshot_last_index - 1) as usize)
        }
    }

    pub(super) fn get_log_term(&self, index: u64) -> u64 {
        if index == self.snapshot_last_index {
            self.snapshot_last_term
        } else {
            self.get_log_entry(index).map_or(0, |e| e.term)
        }
    }
}
