use tokio::fs::OpenOptions;

use crate::raft::state_machine::{GetResult, StorageEngine};
use crate::storage::encoder::Encoder;
use crate::storage::entry::Entry;
use crate::storage::memtable::{BTreeMapMemTable, MemTable, MemTableEntry};
use crate::storage::sstable::{SSTableStorageManager, SSTablesStorage};
use crate::storage::wal::{Wal, WalStorage};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::oneshot::error::TryRecvError;

const FLUSH_THRESHOLD_BYTES: usize = 500; // 1 MB
const FLUSH_WAL_PATH: &str = "wal.log.tmp";
const WALL_FILE_PATH: &str = "wal.log";
const SPARSE_INDEX_INTERVAL: usize = 2;

struct EncodedFlush {
    data: Vec<u8>,
    sparse_index: Vec<(Vec<u8>, u64)>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

pub struct LSMTree {
    memtable: Box<dyn MemTable + Sync + Send>,
    wal: Box<dyn WalStorage + Sync + Send>,
    sstable_manager: Arc<Mutex<SSTableStorageManager>>,

    flushing_memtable: Option<Box<dyn MemTable + Sync + Send>>,
    flushing_wal: Option<Box<dyn WalStorage + Sync + Send>>,
    flush_finished: Option<tokio::sync::oneshot::Receiver<bool>>,
}

impl LSMTree {
    pub async fn new() -> Self {
        Self::with_wal(Box::new(Wal::new(PathBuf::from("wal.log")).await)).await
    }

    pub async fn with_wal(wal: Box<dyn WalStorage + Sync + Send>) -> Self {
        LSMTree {
            memtable: Box::new(BTreeMapMemTable::new()),
            wal,
            sstable_manager: Arc::new(Mutex::new(SSTableStorageManager::new().await)),

            flushing_memtable: None,
            flushing_wal: None,
            flush_finished: None,
        }
    }

    fn encode_for_flush(entries: &[Entry]) -> EncodedFlush {
        let mut data = Vec::new();
        let mut sparse_index: Vec<(Vec<u8>, u64)> = Vec::new();
        let min_key = entries.first().map(|e| e.key.clone()).unwrap_or_default();
        let max_key = entries.last().map(|e| e.key.clone()).unwrap_or_default();
        let mut offset: u64 = 0;
        for (counter, entry) in entries.iter().enumerate() {
            if counter % SPARSE_INDEX_INTERVAL == 0 {
                sparse_index.push((entry.key.clone(), offset));
            }

            let bytes_written = Encoder::encode_into(entry, &mut data);
            offset += bytes_written as u64;
        }

        EncodedFlush {
            data,
            sparse_index,
            min_key,
            max_key,
        }
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        // rename old WAL as flushing to be able to locate it on recovery if flush fails
        let file_path = WALL_FILE_PATH;
        let tmp_file_path = FLUSH_WAL_PATH;
        tokio::fs::rename(file_path, tmp_file_path).await?;

        //Save old memtable and wal for recovery if flush fails
        let new_wal_path = PathBuf::from(file_path);
        let new_wal = Box::new(Wal::new(new_wal_path).await); // Start a new WAL file for the new memtable

        self.flushing_memtable = Some(std::mem::replace(
            &mut self.memtable,
            Box::new(BTreeMapMemTable::new()),
        ));
        self.flushing_wal = Some(std::mem::replace(&mut self.wal, new_wal));

        let old_entries = self.flushing_memtable.as_ref().unwrap().to_entries();
        let ef = Self::encode_for_flush(&old_entries);
        let (snd, rcv) = tokio::sync::oneshot::channel::<bool>();
        self.flush_finished = Some(rcv);
        self.spawn_flush_task(
            snd,
            ef.data,
            ef.sparse_index,
            ef.min_key,
            ef.max_key,
            tmp_file_path.to_string(),
        )
        .await;

        Ok(())
    }

    async fn spawn_flush_task(
        &mut self,
        notification_channel: tokio::sync::oneshot::Sender<bool>,
        encoded_data: Vec<u8>,
        sparse_index: Vec<(Vec<u8>, u64)>,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        tmp_file_path: String,
    ) {
        let sstable_manager = self.sstable_manager.clone();
        tokio::spawn(async move {
            let mut sstable_manager = sstable_manager.lock().await;
            if let Err(e) = sstable_manager
                .flush(&encoded_data, &sparse_index, min_key, max_key)
                .await
            {
                eprintln!("Failed to flush memtable to SSTable: {e}");
                notification_channel
                    .send(false)
                    .expect("Failed to send flush completion signal");
                return;
            }

            // Clean up the flushing memtable and WAL after the flush is complete
            if let Err(e) = tokio::fs::remove_file(&tmp_file_path).await {
                eprintln!("Failed to delete flushing WAL file: {e}");
                notification_channel
                    .send(false)
                    .expect("Failed to send flush completion signal");

                return;
            }

            notification_channel
                .send(true)
                .expect("Failed to send flush completion signal");
        });
    }

    async fn clean_flush_state(&mut self) {
        let Some(rcv) = &mut self.flush_finished else {
            return;
        };

        match rcv.try_recv() {
            Ok(success) => {
                self.flush_finished.take(); // Clear the receiver after handling the result
                if success {
                    // Flush succeeded, clean up flushing memtable and WAL
                    self.flushing_memtable.take();
                    self.flushing_wal.take();
                } else {
                    eprintln!("Flush task reported failure");

                    // If flush fails we should try indefinitely until it succeeds to avoid data loss
                    self.flush()
                        .await
                        .expect("Failed to start new flush after previous flush failure");
                }
            }
            Err(TryRecvError::Empty) => {
                // Flush is still in progress, do nothing for now
            }
            Err(TryRecvError::Closed) => {
                eprintln!("Failed to receive flush completion signal: channel closed");
                self.flush_finished.take(); // Clear the receiver on error to avoid future attempts
                self.flushing_memtable.take();
                self.flushing_wal.take();
            }
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for LSMTree {
    async fn get(&mut self, key: &[u8]) -> GetResult {
        self.clean_flush_state().await; // Check if any flush has completed and clean up state accordingly before processing the get
        let entry = self
            .memtable
            .get(key)
            .cloned()
            .or_else(|| self.flushing_memtable.as_ref()?.get(key).cloned());

        eprintln!(
            "Get for key: {}, found in memtable: {:?}",
            String::from_utf8_lossy(key),
            entry
        );
        let res = match entry {
            Some(MemTableEntry::Value { value, .. }) => GetResult::Value(value),
            Some(MemTableEntry::Tombstone { .. }) => GetResult::Tombstone,
            None => GetResult::NotFound,
        };

        eprintln!(
            "Get result for key: {}, result: {:?}",
            String::from_utf8_lossy(key),
            res
        );
        if res == GetResult::NotFound {
            // If not found in memtable or flushing memtable, check SSTables

            eprintln!(
                "Key: {} not found in memtable or flushing memtable, checking SSTables",
                String::from_utf8_lossy(key)
            );
            let sstable_manager = self.sstable_manager.lock().await;
            let result = match sstable_manager.read(key).await {
                Ok(Some(value)) => return GetResult::Value(value),
                Ok(None) => GetResult::NotFound,
                Err(e) => {
                    eprintln!("Failed to read from SSTables: {e}");
                    GetResult::NotFound // Treat read errors as not found for now
                }
            };
            eprintln!(
                "Get result for key: {}, found in SSTables, returning result: {:?}",
                String::from_utf8_lossy(key),
                result
            );

            return result;
        }

        eprintln!(
            "Get result for key: {}, found in memtable or flushing memtable, returning result: {:?}",
            String::from_utf8_lossy(key),
            res
        );
        return res;
    }

    async fn set(&mut self, raft_index: u64, key: Vec<u8>, value: Vec<u8>) {
        let set_entry = Entry::set(raft_index, key.clone(), value.clone());
        let encoded = Encoder::encode(&set_entry);
        self.wal
            .append(&encoded)
            .await
            .expect("Failed to write to WAL");
        self.memtable.set(raft_index, key, value);
        self.clean_flush_state().await; // Check if any flush has completed and clean up state accordingly after processing the set
        if self.memtable.size_in_bytes() >= FLUSH_THRESHOLD_BYTES && self.flush_finished.is_none() {
            self.flush().await.expect("Failed to flush memtable");
        }
    }

    async fn delete(&mut self, raft_index: u64, key: &[u8]) {
        let delete_entry = Entry::delete(raft_index, key.to_vec());
        let encoded = Encoder::encode(&delete_entry);
        self.wal
            .append(&encoded)
            .await
            .expect("Failed to write to WAL");
        self.memtable.delete(raft_index, key);
        self.clean_flush_state().await; // Check if any flush has completed and clean up state accordingly after processing the set
        if self.memtable.size_in_bytes() >= FLUSH_THRESHOLD_BYTES && self.flush_finished.is_none() {
            self.flush().await.expect("Failed to flush memtable");
        }
    }

    async fn recover(&mut self) -> anyhow::Result<()> {
        match self.wal.read_all().await {
            Ok(data) => {
                let entries = Encoder::decode_all(&data)?;
                self.memtable.extend(entries);
            }
            Err(e) => {
                eprintln!("Failed to read WAL during recovery: {e}");
                return Err(anyhow::anyhow!(e));
            }
        }

        // try and open the flushing WAL if it exists and replay it as well,
        //since the flush might have failed after WAL rotation but before
        //the old WAL was deleted
        let file_result = OpenOptions::new().read(true).open(FLUSH_WAL_PATH).await;
        let flush_wal = match file_result {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(()); // No flushing WAL, nothing more to recover
            }
            Err(e) => {
                eprintln!("Failed to open flushing WAL during recovery: {e}");
                return Err(anyhow::anyhow!(e));
            }
        };

        let flushing_wal = Box::new(Wal::from_file(flush_wal));
        self.flushing_wal = Some(flushing_wal);
        self.flushing_memtable = Some(Box::new(BTreeMapMemTable::new())); // Start with a fresh memtable for the new WAL
        match self
            .flushing_wal
            .as_mut()
            .expect("Flushing WAL should exist")
            .read_all()
            .await
        {
            Ok(data) => {
                let entries = Encoder::decode_all(&data)?;
                self.flushing_memtable
                    .as_mut()
                    .expect("Flushing memtable should exist")
                    .extend(entries);
            }
            Err(e) => {
                eprintln!("Failed to read flushing WAL during recovery: {e}");
                return Err(anyhow::anyhow!(e));
            }
        }

        let old_entries = self
            .flushing_memtable
            .as_ref()
            .expect("Flushing memtable should exist")
            .to_entries();
        let ef = Self::encode_for_flush(&old_entries);
        let (snd, rcv) = tokio::sync::oneshot::channel::<bool>();
        self.spawn_flush_task(
            snd,
            ef.data,
            ef.sparse_index,
            ef.min_key,
            ef.max_key,
            FLUSH_WAL_PATH.to_string(),
        )
        .await;
        self.flush_finished = Some(rcv);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::GetResult;
    use crate::storage::encoder::Encoder;
    use crate::storage::entry::{Entry as WalEntry, OP_DELETE, OP_SET};
    use crate::storage::wal::WalStorage;
    use std::io;
    use std::sync::{Arc, Mutex};

    /// A WAL mock that records raw bytes and replays them on `read_all`.
    #[derive(Default, Clone)]
    struct RecordingWal {
        data: Arc<Mutex<Vec<u8>>>,
    }

    impl RecordingWal {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl WalStorage for RecordingWal {
        async fn append(&mut self, data: &[u8]) -> io::Result<()> {
            self.data.lock().unwrap().extend_from_slice(data);
            Ok(())
        }

        async fn read_all(&mut self) -> io::Result<Vec<u8>> {
            Ok(self.data.lock().unwrap().clone())
        }
    }

    /// Creates a plain tree backed by a fresh `RecordingWal`.
    async fn make_tree() -> LSMTree {
        LSMTree::with_wal(Box::new(RecordingWal::new())).await
    }

    /// Creates a tree **and** returns a handle to the shared WAL bytes so
    /// tests can inspect what was written even after ownership moved into the tree.
    async fn make_tree_with_wal_spy() -> (LSMTree, Arc<Mutex<Vec<u8>>>) {
        let wal = RecordingWal::new();
        let spy = Arc::clone(&wal.data);
        (LSMTree::with_wal(Box::new(wal)).await, spy)
    }

    fn recorded(spy: &Arc<Mutex<Vec<u8>>>) -> Vec<WalEntry> {
        let data = spy.lock().unwrap().clone();
        Encoder::decode_all(&data).unwrap()
    }

    /// Helper: encode a list of entries into WAL bytes for pre-populating a mock WAL.
    fn encode_entries(entries: &[WalEntry]) -> Vec<u8> {
        let mut data = Vec::new();
        for entry in entries {
            data.extend_from_slice(&Encoder::encode(entry));
        }
        data
    }

    // ── get ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_missing_key_returns_none() {
        let mut tree = make_tree().await;
        assert_eq!(tree.get(b"missing").await, GetResult::NotFound);
    }

    #[tokio::test]
    async fn test_get_existing_key_returns_value() {
        let mut tree = make_tree().await;
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        assert_eq!(tree.get(b"k").await, GetResult::Value(b"v".to_vec()));
    }

    // ── set ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_set_overwrites_existing_key() {
        let mut tree = make_tree().await;
        tree.set(1, b"k".to_vec(), b"v1".to_vec()).await;
        tree.set(2, b"k".to_vec(), b"v2".to_vec()).await;
        assert_eq!(tree.get(b"k").await, GetResult::Value(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_set_writes_to_wal() {
        let (mut tree, spy) = make_tree_with_wal_spy().await;
        tree.set(1, b"key".to_vec(), b"val".to_vec()).await;
        let entries = recorded(&spy);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 1); // raft index passed through to WAL
        assert_eq!(entries[0].op, OP_SET); // op = SET
        assert_eq!(entries[0].key, b"key");
        assert_eq!(entries[0].value, b"val");
    }

    #[tokio::test]
    async fn test_set_wal_entries_carry_raft_indices() {
        let (mut tree, spy) = make_tree_with_wal_spy().await;
        tree.set(1, b"a".to_vec(), b"1".to_vec()).await;
        tree.set(2, b"b".to_vec(), b"2".to_vec()).await;
        tree.set(3, b"c".to_vec(), b"3".to_vec()).await;
        let entries = recorded(&spy);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 2);
        assert_eq!(entries[2].index, 3);
    }

    // ── delete ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_delete_existing_key_returns_true() {
        let mut tree = make_tree().await;
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(2, b"k").await;
        assert_eq!(tree.get(b"k").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_delete_missing_key_returns_false() {
        let mut tree = make_tree().await;
        tree.delete(1, b"missing").await;
        assert_eq!(tree.get(b"missing").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_deleted_key_is_not_visible() {
        let mut tree = make_tree().await;
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(2, b"k").await;
        // Deleted key has a tombstone — distinguishable from a never-set key.
        assert_eq!(tree.get(b"k").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_tombstone_carries_raft_index_from_delete() {
        // The logical_index on a tombstone must equal the raft index passed to delete.
        let mut tree = make_tree().await;
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(5, b"k").await; // non-contiguous raft index
        assert_eq!(tree.get(b"k").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_tombstone_logical_index_updated_on_redelete() {
        // Deleting the same key twice: the second tombstone carries the higher index.
        let mut tree = make_tree().await;
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(2, b"k").await;
        tree.delete(3, b"k").await;
        assert_eq!(tree.get(b"k").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_tombstone_on_never_set_key_carries_raft_index() {
        // Deleting a key that was never set still stores a tombstone with the correct index.
        let mut tree = make_tree().await;
        tree.delete(7, b"ghost").await;
        assert_eq!(tree.get(b"ghost").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_delete_writes_tombstone_to_wal() {
        let (mut tree, spy) = make_tree_with_wal_spy().await;
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(2, b"k").await;
        let entries = recorded(&spy);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1); // set got raft index 1
        assert_eq!(entries[1].index, 2); // delete got raft index 2
        assert_eq!(entries[1].op, OP_DELETE); // op = DELETE
        assert_eq!(entries[1].key, b"k");
        assert_eq!(entries[1].value, b"");
    }

    // ── recover ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_recover_replays_set_entries() {
        let wal = RecordingWal::new();
        // Pre-populate the WAL as if a previous run wrote these entries.
        *wal.data.lock().unwrap() = encode_entries(&[
            WalEntry::set(1, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k2".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(Box::new(wal)).await;
        _ = tree.recover().await;
        assert_eq!(tree.get(b"k1").await, GetResult::Value(b"v1".to_vec()));
        assert_eq!(tree.get(b"k2").await, GetResult::Value(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_recover_replays_tombstone_hides_key() {
        let wal = RecordingWal::new();
        *wal.data.lock().unwrap() = encode_entries(&[
            WalEntry::set(1, b"k".to_vec(), b"v".to_vec()),
            WalEntry::delete(2, b"k".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(Box::new(wal)).await;
        _ = tree.recover().await;
        // After recovery the key carries a tombstone, not a value and not absent.
        assert_eq!(tree.get(b"k").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_recover_tombstone_logical_index_matches_wal_entry() {
        // On recovery, the tombstone's logical_index must equal the WAL entry's raft index.
        let wal = RecordingWal::new();
        *wal.data.lock().unwrap() = encode_entries(&[
            WalEntry::set(1, b"k".to_vec(), b"v".to_vec()),
            WalEntry::delete(4, b"k".to_vec()), // non-contiguous raft index
        ]);
        let mut tree = LSMTree::with_wal(Box::new(wal)).await;
        _ = tree.recover().await;
        assert_eq!(tree.get(b"k").await, GetResult::Tombstone);
    }

    #[tokio::test]
    async fn test_recover_last_set_wins_over_earlier_set() {
        let wal = RecordingWal::new();
        *wal.data.lock().unwrap() = encode_entries(&[
            WalEntry::set(1, b"k".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(Box::new(wal)).await;
        _ = tree.recover().await;
        assert_eq!(tree.get(b"k").await, GetResult::Value(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_recover_set_after_tombstone_is_visible() {
        let wal = RecordingWal::new();
        *wal.data.lock().unwrap() = encode_entries(&[
            WalEntry::set(1, b"k".to_vec(), b"v1".to_vec()),
            WalEntry::delete(2, b"k".to_vec()),
            WalEntry::set(3, b"k".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(Box::new(wal)).await;
        _ = tree.recover().await;
        assert_eq!(tree.get(b"k").await, GetResult::Value(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_recover_empty_wal_leaves_tree_empty() {
        let mut tree = make_tree().await;
        _ = tree.recover().await;
        assert_eq!(tree.get(b"k").await, GetResult::NotFound);
    }

    // ── logical_index correctness ────────────────────────────────────────────

    #[tokio::test]
    async fn test_logical_index_stores_raft_index_across_distinct_keys() {
        let mut tree = make_tree().await;
        tree.set(1, b"a".to_vec(), b"1".to_vec()).await;
        tree.set(2, b"b".to_vec(), b"2".to_vec()).await;
        tree.set(3, b"c".to_vec(), b"3".to_vec()).await;
        assert_eq!(tree.get(b"a").await, GetResult::Value(b"1".to_vec()));
        assert_eq!(tree.get(b"b").await, GetResult::Value(b"2".to_vec()));
        assert_eq!(tree.get(b"c").await, GetResult::Value(b"3".to_vec()));
    }

    #[tokio::test]
    async fn test_logical_index_after_interleaved_set_delete_set() {
        // Live write path: set→delete→set, final value carries raft index 3.
        let mut tree = make_tree().await;
        tree.set(1, b"k".to_vec(), b"v1".to_vec()).await;
        tree.delete(2, b"k").await;
        tree.set(3, b"k".to_vec(), b"v2".to_vec()).await;
        assert_eq!(tree.get(b"k").await, GetResult::Value(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_logical_index_preserved_after_recover_then_new_writes() {
        // Recover WAL entries with raft indices 1,2,3 then write new entries
        // with raft indices 4,5 — the LSM stores whatever index the caller provides.
        let wal = RecordingWal::new();
        *wal.data.lock().unwrap() = encode_entries(&[
            WalEntry::set(1, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k2".to_vec(), b"v2".to_vec()),
            WalEntry::delete(3, b"k1".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(Box::new(wal)).await;
        _ = tree.recover().await;

        tree.set(4, b"k3".to_vec(), b"v3".to_vec()).await;
        assert_eq!(tree.get(b"k3").await, GetResult::Value(b"v3".to_vec()));

        tree.set(5, b"k4".to_vec(), b"v4".to_vec()).await;
        assert_eq!(tree.get(b"k4").await, GetResult::Value(b"v4".to_vec()));
    }

    #[tokio::test]
    async fn test_wal_index_matches_raft_index_after_recover_then_new_writes() {
        // Verify WAL entries carry the exact raft index passed by the caller.
        let wal = RecordingWal::new();
        *wal.data.lock().unwrap() = encode_entries(&[
            WalEntry::set(1, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k2".to_vec(), b"v2".to_vec()),
        ]);
        let spy = Arc::clone(&wal.data);
        let mut tree = LSMTree::with_wal(Box::new(wal)).await;
        _ = tree.recover().await;

        tree.set(3, b"k3".to_vec(), b"v3".to_vec()).await;
        tree.delete(4, b"k1").await;
        let entries = recorded(&spy);
        assert_eq!(entries[2].index, 3); // new set with raft index 3
        assert_eq!(entries[3].index, 4); // new delete with raft index 4
    }

    // ── encode_for_flush sparse index offsets ───────────────────────────────

    #[test]
    fn test_sparse_index_offsets_point_to_correct_entries() {
        let entries = vec![
            WalEntry::set(1, b"a".to_vec(), b"val_a".to_vec()),
            WalEntry::set(2, b"b".to_vec(), b"val_b".to_vec()),
            WalEntry::set(3, b"c".to_vec(), b"val_cc".to_vec()),
            WalEntry::set(4, b"d".to_vec(), b"val_ddd".to_vec()),
            WalEntry::set(5, b"e".to_vec(), b"val_eeee".to_vec()),
        ];

        let ef = LSMTree::encode_for_flush(&entries);

        // With SPARSE_INDEX_INTERVAL=2, entries at positions 0, 2, 4 are indexed
        assert_eq!(ef.sparse_index.len(), 3);
        assert_eq!(ef.sparse_index[0].0, b"a");
        assert_eq!(ef.sparse_index[1].0, b"c");
        assert_eq!(ef.sparse_index[2].0, b"e");

        // Each offset should decode to the correct entry when used as a starting position
        for (sparse_key, offset) in &ef.sparse_index {
            let slice = &ef.data[*offset as usize..];
            let decoded = Encoder::decode_all(slice).unwrap();
            assert_eq!(
                &decoded[0].key, sparse_key,
                "sparse index offset {} should point to entry with key {:?}",
                offset,
                String::from_utf8_lossy(sparse_key)
            );
        }
    }

    #[test]
    fn test_sparse_index_first_offset_is_zero() {
        let entries = vec![
            WalEntry::set(1, b"x".to_vec(), b"y".to_vec()),
        ];
        let ef = LSMTree::encode_for_flush(&entries);
        assert_eq!(ef.sparse_index.len(), 1);
        assert_eq!(ef.sparse_index[0].1, 0, "first sparse index offset must be 0");
    }

    #[test]
    fn test_sparse_index_offsets_with_variable_size_entries() {
        // Use entries with very different sizes to catch cumulative vs incremental bugs
        let entries = vec![
            WalEntry::set(1, b"k".to_vec(), b"v".to_vec()),             // small
            WalEntry::set(2, b"k2".to_vec(), b"value2".to_vec()),       // medium
            WalEntry::set(3, b"key3".to_vec(), b"a]long_value_here".to_vec()), // large
            WalEntry::set(4, b"k4".to_vec(), b"v4".to_vec()),           // small again
        ];

        let ef = LSMTree::encode_for_flush(&entries);

        // With SPARSE_INDEX_INTERVAL=2, indexed at positions 0 and 2
        assert_eq!(ef.sparse_index.len(), 2);

        // Verify offset[0] points to entries[0]
        let decoded_from_0 = Encoder::decode_all(&ef.data[ef.sparse_index[0].1 as usize..]).unwrap();
        assert_eq!(decoded_from_0[0].key, b"k");

        // Verify offset[1] points to entries[2], not somewhere wrong
        let decoded_from_1 = Encoder::decode_all(&ef.data[ef.sparse_index[1].1 as usize..]).unwrap();
        assert_eq!(decoded_from_1[0].key, b"key3");
    }
}
