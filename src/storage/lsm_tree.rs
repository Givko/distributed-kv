use std::collections::BTreeMap;
use std::path::PathBuf;
use crate::raft::state_machine::StorageEngine;
use crate::storage::wal::{Wal, WalEntry, WalStorage, OP_DELETE, OP_SET};

#[derive(Clone, PartialEq, Debug)]
pub enum Entry {
    Value(Vec<u8>),
    Tombstone,
}

pub struct LSMTree<W: WalStorage = Wal> {
    memtable: BTreeMap<Vec<u8>, Entry>,
    wal: W,
    next_index: u64,
}

impl LSMTree<Wal> {
    pub fn new() -> Self {
        Self::with_wal(Wal::new(PathBuf::from("wal.log")))
    }

    /// Creates an LSMTree whose WAL file is named `<sanitized_id>-wal.log`,
    /// where the node ID (e.g. `127.0.0.1:5051`) has `:` and `.` replaced by `_`.
    pub fn with_node_id(id: &str) -> Self {
        let sanitized = id.replace(['.', ':'], "_");
        Self::with_wal(Wal::new(PathBuf::from(format!("{sanitized}-wal.log"))))
    }
}

impl<W: WalStorage> LSMTree<W> {
    pub fn with_wal(wal: W) -> Self {
        LSMTree {
            memtable: BTreeMap::new(),
            wal,
            next_index: 0,
        }
    }
}

#[async_trait::async_trait]
impl<W: WalStorage> StorageEngine for LSMTree<W> {
    async fn get(&self, key: &[u8]) -> Option<Entry> {
        self.memtable.get(key).cloned()
    }

    async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let set_entry = WalEntry::set(self.next_index, key.clone(), value.clone());
        self.wal.append(&set_entry).await.expect("Failed to write to WAL");
        self.next_index += 1;
        self.memtable.insert(key, Entry::Value(value));
    }

    async fn delete(&mut self, key: &[u8]) -> bool {
        let delete_entry = WalEntry::delete(self.next_index, key.to_vec());
        self.wal.append(&delete_entry).await.expect("Failed to write to WAL");
        self.next_index += 1;
        self.memtable.insert(key.to_vec(), Entry::Tombstone).is_some()
    }

    async fn recover(&mut self) {
        if let Ok(entries) = self.wal.read_all().await {
            for entry in entries {
                self.next_index = entry.index + 1;
                match entry.op {
                    OP_SET => self.memtable.insert(entry.key, Entry::Value(entry.value)),
                    OP_DELETE => self.memtable.insert(entry.key, Entry::Tombstone),
                    _ => None,
                };
            }
        }
    }

    fn last_applied_index(&self) -> u64 {
        // next_index is the count of WAL entries written (0-based indices, so
        // entries 0..next_index-1 exist). Raft log indices are 1-based, so
        // the last applied Raft entry index equals next_index.
        self.next_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::{WalEntry, WalStorage, OP_DELETE, OP_SET};
    use std::io;
    use std::sync::Mutex;

    /// A WAL mock that records every written entry and replays them on `read_all`.
    #[derive(Default)]
    struct RecordingWal {
        entries: Mutex<Vec<WalEntry>>,
    }

    impl RecordingWal {
        fn recorded(&self) -> Vec<WalEntry> {
            self.entries.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl WalStorage for RecordingWal {
        async fn append(&self, entry: &WalEntry) -> io::Result<()> {
            self.entries.lock().unwrap().push(entry.clone());
            Ok(())
        }

        async fn read_all(&self) -> io::Result<Vec<WalEntry>> {
            Ok(self.entries.lock().unwrap().clone())
        }
    }

    fn make_tree() -> LSMTree<RecordingWal> {
        LSMTree::with_wal(RecordingWal::default())
    }

    // ── get ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_missing_key_returns_none() {
        let tree = make_tree();
        assert_eq!(tree.get(b"missing").await, None); // truly absent → None
    }

    #[tokio::test]
    async fn test_get_existing_key_returns_value() {
        let mut tree = make_tree();
        tree.set(b"k".to_vec(), b"v".to_vec()).await;
        assert_eq!(tree.get(b"k").await, Some(Entry::Value(b"v".to_vec())));
    }

    // ── set ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_set_overwrites_existing_key() {
        let mut tree = make_tree();
        tree.set(b"k".to_vec(), b"v1".to_vec()).await;
        tree.set(b"k".to_vec(), b"v2".to_vec()).await;
        assert_eq!(tree.get(b"k").await, Some(Entry::Value(b"v2".to_vec())));
    }

    #[tokio::test]
    async fn test_set_writes_to_wal() {
        let mut tree = make_tree();
        tree.set(b"key".to_vec(), b"val".to_vec()).await;
        let entries = tree.wal.recorded();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].op, OP_SET);           // op = SET
        assert_eq!(entries[0].key, b"key");
        assert_eq!(entries[0].value, b"val");
    }

    // ── delete ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_delete_existing_key_returns_true() {
        let mut tree = make_tree();
        tree.set(b"k".to_vec(), b"v".to_vec()).await;
        assert!(tree.delete(b"k").await);
    }

    #[tokio::test]
    async fn test_delete_missing_key_returns_false() {
        let mut tree = make_tree();
        assert!(!tree.delete(b"missing").await);
    }

    #[tokio::test]
    async fn test_deleted_key_is_not_visible() {
        let mut tree = make_tree();
        tree.set(b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(b"k").await;
        // Deleted key has a tombstone — distinguishable from a never-set key.
        assert_eq!(tree.get(b"k").await, Some(Entry::Tombstone));
    }

    #[tokio::test]
    async fn test_delete_writes_tombstone_to_wal() {
        let mut tree = make_tree();
        tree.set(b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(b"k").await;
        let entries = tree.wal.recorded();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].op, OP_DELETE);           // op = DELETE
        assert_eq!(entries[1].key, b"k");
        assert_eq!(entries[1].value, b"");
    }

    // ── recover ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_recover_replays_set_entries() {
        let wal = RecordingWal::default();
        // Pre-populate the WAL as if a previous run wrote these entries.
        wal.entries.lock().unwrap().extend([
            WalEntry::set(0, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(1, b"k2".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        assert_eq!(tree.get(b"k1").await, Some(Entry::Value(b"v1".to_vec())));
        assert_eq!(tree.get(b"k2").await, Some(Entry::Value(b"v2".to_vec())));
    }

    #[tokio::test]
    async fn test_recover_replays_tombstone_hides_key() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(0, b"k".to_vec(), b"v".to_vec()),
            WalEntry::delete(1, b"k".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        // After recovery the key carries a tombstone, not a value and not absent.
        assert_eq!(tree.get(b"k").await, Some(Entry::Tombstone));
    }

    #[tokio::test]
    async fn test_recover_last_set_wins_over_earlier_set() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(0, b"k".to_vec(), b"v1".to_vec()),
            WalEntry::set(1, b"k".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        assert_eq!(tree.get(b"k").await, Some(Entry::Value(b"v2".to_vec())));
    }

    #[tokio::test]
    async fn test_recover_set_after_tombstone_is_visible() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(0, b"k".to_vec(), b"v1".to_vec()),
            WalEntry::delete(1, b"k".to_vec()),
            WalEntry::set(2, b"k".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        assert_eq!(tree.get(b"k").await, Some(Entry::Value(b"v2".to_vec())));
    }

    #[tokio::test]
    async fn test_recover_empty_wal_leaves_tree_empty() {
        let mut tree = make_tree();
        tree.recover().await;
        assert_eq!(tree.get(b"k").await, None); // truly absent → None
    }

    // ── last_applied_index / next_index ──────────────────────────────────────

    #[tokio::test]
    async fn test_last_applied_index_is_zero_for_empty_tree() {
        let tree = make_tree();
        assert_eq!(tree.last_applied_index(), 0);
    }

    #[tokio::test]
    async fn test_last_applied_index_increments_after_each_set() {
        let mut tree = make_tree();
        tree.set(b"k1".to_vec(), b"v1".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 1);
        tree.set(b"k2".to_vec(), b"v2".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 2);
        tree.set(b"k3".to_vec(), b"v3".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 3);
    }

    #[tokio::test]
    async fn test_last_applied_index_increments_after_delete() {
        let mut tree = make_tree();
        tree.set(b"k".to_vec(), b"v".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 1);
        tree.delete(b"k").await;
        assert_eq!(tree.last_applied_index(), 2);
    }

    #[tokio::test]
    async fn test_last_applied_index_increments_after_delete_of_missing_key() {
        // delete always writes a tombstone to WAL regardless of key existence.
        let mut tree = make_tree();
        tree.delete(b"nonexistent").await;
        assert_eq!(tree.last_applied_index(), 1);
    }

    #[tokio::test]
    async fn test_last_applied_index_restored_after_recover() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(0, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(1, b"k2".to_vec(), b"v2".to_vec()),
            WalEntry::delete(2, b"k1".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        // 3 WAL entries (indices 0, 1, 2) → next_index = 3 → last_applied = 3.
        assert_eq!(tree.last_applied_index(), 3);
    }

    #[tokio::test]
    async fn test_last_applied_index_zero_after_recover_from_empty_wal() {
        let mut tree = make_tree();
        tree.recover().await;
        assert_eq!(tree.last_applied_index(), 0);
    }
}