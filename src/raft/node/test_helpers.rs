//! Shared test doubles for the `Node` unit tests across the `core`,
//! `election`, and `replication` submodules.

use crate::common::encoder::Encoder;
use crate::common::entry::Entry;
use crate::common::wal::WalStorage;
use crate::raft::raft_types::LogEntry;
use crate::raft::state_persister::{PersistentState, Persister};
use crate::storage::lsm_tree::LSMTree as RealLSMTree;
use std::io;
use std::sync::{Arc, Mutex};

pub(super) struct TestPersister;
pub(super) struct LoadedStatePersister {
    pub(super) state: PersistentState,
}
pub(super) struct FailingLoadPersister;

#[allow(dead_code)]
pub(super) struct RecordingPersister {
    pub(super) saved_state: Arc<Mutex<Option<PersistentState>>>,
}

pub(super) struct MockWal;

#[async_trait::async_trait]
impl WalStorage for MockWal {
    async fn append(&mut self, _entry: &[u8]) -> io::Result<()> {
        Ok(())
    }

    async fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(vec![])
    }
    async fn truncate(&mut self, _len: usize) -> io::Result<()> {
        Ok(())
    }
}

/// A mock WAL that returns a fixed, pre-loaded set of entries from `read_all`.
pub(super) struct PreloadedMockWal(pub(super) Vec<u8>);

#[async_trait::async_trait]
impl WalStorage for PreloadedMockWal {
    async fn append(&mut self, _entry: &[u8]) -> io::Result<()> {
        Ok(())
    }

    async fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.0.clone())
    }

    async fn truncate(&mut self, _len: usize) -> io::Result<()> {
        Ok(())
    }
}

/// Encodes `commands` the way the node's entries WAL holds them: one record per
/// `LogEntry`, with the term in the record index. Feed the result to
/// `PreloadedMockWal` to give a recovering node a non-empty Raft log.
pub(super) fn encoded_log(commands: &[(u64, &str)]) -> Vec<u8> {
    commands
        .iter()
        .flat_map(|(term, command)| {
            let entry = LogEntry {
                term: *term,
                command: (*command).to_string(),
            };
            Encoder::encode(&entry.to_entry().expect("test command must parse"))
        })
        .collect()
}

/// Encodes `writes` the way the state machine's WAL holds them: one `set` record
/// per write, numbered from 0 as `LSMTree::set` numbers them. This is what
/// drives `last_applied_index` after recovery, so the length of `writes` is the
/// Raft index the state machine has durably applied up to.
pub(super) fn encoded_applied(writes: &[(&str, &str)]) -> Vec<u8> {
    writes
        .iter()
        .enumerate()
        .flat_map(|(index, (key, value))| {
            Encoder::encode(&Entry::set(
                index as u64,
                key.as_bytes().to_vec(),
                value.as_bytes().to_vec(),
            ))
        })
        .collect()
}

/// A mock WAL that serves `preloaded` from `read_all` and records every `append`
/// in a handle the test keeps. Lets a test assert that recovery wrote nothing
/// back, which is the only externally visible difference between "recovered from
/// the WAL" and "replayed from the log".
pub(super) struct RecordingMockWal {
    preloaded: Vec<u8>,
    appends: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl RecordingMockWal {
    /// Returns the WAL and a handle onto the appends it will record.
    pub(super) fn new(preloaded: Vec<u8>) -> (Self, Arc<Mutex<Vec<Vec<u8>>>>) {
        let appends = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                preloaded,
                appends: appends.clone(),
            },
            appends,
        )
    }
}

#[async_trait::async_trait]
impl WalStorage for RecordingMockWal {
    async fn append(&mut self, entry: &[u8]) -> io::Result<()> {
        self.appends.lock().unwrap().push(entry.to_vec());
        Ok(())
    }

    async fn read_all(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.preloaded.clone())
    }

    async fn truncate(&mut self, _len: usize) -> io::Result<()> {
        Ok(())
    }
}

pub(super) struct LSMTree;

impl LSMTree {
    #[allow(clippy::new_ret_no_self)]
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
            commit_index: 0,
        })
    }
}
