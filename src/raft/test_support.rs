use std::collections::HashMap;

use crate::raft::state_machine::{GetResult, StorageEngine};

/// Minimal in-memory `StorageEngine` for exercising `Node` and `StateMachine`
/// in tests without pulling in the real LSM-tree storage stack (memtable, WAL,
/// SSTables). Use this whenever a test needs *a* storage engine as scaffolding
/// rather than exercising real persistence.
#[derive(Default)]
pub(in crate::raft) struct MockEngine {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

#[async_trait::async_trait]
impl StorageEngine for MockEngine {
    async fn set(&mut self, _raft_index: u64, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    async fn delete(&mut self, _raft_index: u64, key: &[u8]) {
        self.data.remove(key);
    }

    async fn get(&mut self, key: &[u8]) -> GetResult {
        match self.data.get(key) {
            Some(v) => GetResult::Value(v.clone()),
            None => GetResult::NotFound,
        }
    }

    async fn recover(&mut self) -> anyhow::Result<()> {
        // No-op for the mock engine
        Ok(())
    }
}
