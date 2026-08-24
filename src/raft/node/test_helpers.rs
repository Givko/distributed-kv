//! Shared test doubles for the `Node` unit tests across the `core`,
//! `election`, and `replication` submodules.

use crate::raft::state_persister::{PersistentState, Persister};
use crate::storage::entry::Entry as WalEntry;
use crate::storage::lsm_tree::LSMTree as RealLSMTree;
use crate::storage::wal::WalStorage;
use std::io;
use std::sync::{Arc, Mutex};

pub(super) struct TestPersister;
pub(super) struct LoadedStatePersister {
    pub(super) state: PersistentState,
}
pub(super) struct FailingLoadPersister;
pub(super) struct RecordingPersister {
    pub(super) saved_state: Arc<Mutex<Option<PersistentState>>>,
}

pub(super) struct MockWal;

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
pub(super) struct PreloadedMockWal(pub(super) Vec<WalEntry>);

#[async_trait::async_trait]
impl WalStorage for PreloadedMockWal {
    async fn append(&mut self, _entry: &WalEntry) -> io::Result<()> {
        Ok(())
    }

    async fn read_all(&mut self) -> io::Result<Vec<WalEntry>> {
        Ok(self.0.clone())
    }
}

pub(super) struct LSMTree;

impl LSMTree {
    pub(super) fn new() -> RealLSMTree<MockWal> {
        RealLSMTree::with_wal(MockWal)
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
        Ok(PersistentState {
            current_term: 0,
            voted_for: None,
            entries: vec![],
            commit_index: 0,
        })
    }
}
