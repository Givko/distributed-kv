use crate::raft::state_machine::StorageEngine;
use crate::storage::entry::{Entry, OP_DELETE, OP_SET};
use crate::storage::sstable::{SSTableStorageManager, SSTablesStorage};
use crate::storage::wal::{Wal, WalStorage};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, PartialEq, Debug)]
pub enum MemTableEntry {
    Value {
        logical_index: u64, // Logical index of the last WAL entry that set this value. Used to resolve conflicts during recovery.
        value: Vec<u8>,
    },
    Tombstone{
      logical_index: u64, // Logical index of the last WAL entry that deleted this key. Used to resolve conflicts during recovery.
    },
}

pub struct LSMTree<W: WalStorage = Wal> {
    memtable: BTreeMap<Vec<u8>, MemTableEntry>,
    wal: W,
    sstable_storage: Box<dyn SSTablesStorage + Sync + Send>,
    last_raft_index: u64,
}

impl LSMTree<Wal> {
    pub async fn new() -> Self {
        Self::with_wal(Wal::new(PathBuf::from("wal.log")).await)
    }

    /// Creates an LSMTree whose WAL file is named `<sanitized_id>-wal.log`,
    /// where the node ID (e.g. `127.0.0.1:5051`) has `:` and `.` replaced by `_`.
    pub async fn with_node_id(id: &str) -> Self {
        let sanitized = id.replace(['.', ':'], "_");
        Self::with_wal(Wal::new(PathBuf::from(format!("{sanitized}-wal.log"))).await)
    }
}

impl<W: WalStorage> LSMTree<W> {
    pub fn with_wal(wal: W) -> Self {
        LSMTree {
            memtable: BTreeMap::new(),
            wal,
            sstable_storage: Box::new(SSTableStorageManager::new()), // Replace with actual initialization
            last_raft_index: 0,
        }
    }

    fn flush_to_sstable(&mut self) {
        // Implement logic to flush the memtable to an SSTable using self.sstable_storage.flush()
        // After flushing, clear the memtable and reset last_raft_index if necessary.
        let _cur_entries = self.memtable.iter().map(|(k, v)| {
            let (index, value) = match v {
                MemTableEntry::Value { logical_index, value } => (*logical_index, value.clone()),
                MemTableEntry::Tombstone { logical_index } => (*logical_index, Vec::new()),
            };
            Entry {
                index,
                op: if matches!(v, MemTableEntry::Tombstone { .. }) { OP_DELETE } else { OP_SET },
                key: k.clone(),
                value,
            }
        }).collect::<Vec<_>>();
    
    }
}

#[async_trait::async_trait]
impl<W: WalStorage> StorageEngine for LSMTree<W> {
    async fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        self.memtable.get(key).cloned()
    }

    async fn set(&mut self, raft_index: u64, key: Vec<u8>, value: Vec<u8>) {
        let set_entry = Entry::set(raft_index, key.clone(), value.clone());
        self.wal
            .append(&set_entry)
            .await
            .expect("Failed to write to WAL");
        self.last_raft_index = raft_index;
        self.memtable.insert(key, MemTableEntry::Value { logical_index: raft_index, value });
    }

    async fn delete(&mut self, raft_index: u64, key: &[u8]) -> bool {
        let delete_entry = Entry::delete(raft_index, key.to_vec());
        self.wal
            .append(&delete_entry)
            .await
            .expect("Failed to write to WAL");
        self.last_raft_index = raft_index;
        self.memtable
            .insert(key.to_vec(), MemTableEntry::Tombstone { logical_index: raft_index })
            .is_some()
    }

    async fn recover(&mut self) {
        match self.wal.read_all().await {
            Ok(entries) => {
                for entry in entries {
                    self.last_raft_index = entry.index;
                    match entry.op {
                        OP_SET => self
                            .memtable
                            .insert(entry.key, MemTableEntry::Value { logical_index: entry.index, value: entry.value }),
                        OP_DELETE => self.memtable.insert(entry.key, MemTableEntry::Tombstone { logical_index: entry.index }),
                        _ => None,
                    };
                }
            }
            Err(e) => eprintln!("Failed to read WAL during recovery: {e}"),
        }
    }

    fn last_applied_index(&self) -> u64 {
        self.last_raft_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::{Entry as WalEntry, OP_DELETE, OP_SET};
    use crate::storage::wal::WalStorage;
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
        async fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
            self.entries.lock().unwrap().push(entry.clone());
            Ok(())
        }

        async fn read_all(&mut self) -> io::Result<Vec<WalEntry>> {
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
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Value { logical_index: 1, value: b"v".to_vec() })
        );
    }

    // ── set ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_set_overwrites_existing_key() {
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v1".to_vec()).await;
        tree.set(2, b"k".to_vec(), b"v2".to_vec()).await;
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Value { logical_index: 2, value: b"v2".to_vec() })
        );
    }

    #[tokio::test]
    async fn test_set_writes_to_wal() {
        let mut tree = make_tree();
        tree.set(1, b"key".to_vec(), b"val".to_vec()).await;
        let entries = tree.wal.recorded();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 1); // raft index passed through to WAL
        assert_eq!(entries[0].op, OP_SET); // op = SET
        assert_eq!(entries[0].key, b"key");
        assert_eq!(entries[0].value, b"val");
    }

    #[tokio::test]
    async fn test_set_wal_entries_carry_raft_indices() {
        let mut tree = make_tree();
        tree.set(1, b"a".to_vec(), b"1".to_vec()).await;
        tree.set(2, b"b".to_vec(), b"2".to_vec()).await;
        tree.set(3, b"c".to_vec(), b"3".to_vec()).await;
        let entries = tree.wal.recorded();
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 2);
        assert_eq!(entries[2].index, 3);
    }

    // ── delete ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_delete_existing_key_returns_true() {
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        assert!(tree.delete(2, b"k").await);
    }

    #[tokio::test]
    async fn test_delete_missing_key_returns_false() {
        let mut tree = make_tree();
        assert!(!tree.delete(1, b"missing").await);
    }

    #[tokio::test]
    async fn test_deleted_key_is_not_visible() {
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(2, b"k").await;
        // Deleted key has a tombstone — distinguishable from a never-set key.
        assert_eq!(tree.get(b"k").await, Some(MemTableEntry::Tombstone { logical_index: 2 }));
    }

    #[tokio::test]
    async fn test_tombstone_carries_raft_index_from_delete() {
        // The logical_index on a tombstone must equal the raft index passed to delete.
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(5, b"k").await; // non-contiguous raft index
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Tombstone { logical_index: 5 })
        );
    }

    #[tokio::test]
    async fn test_tombstone_logical_index_updated_on_redelete() {
        // Deleting the same key twice: the second tombstone carries the higher index.
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(2, b"k").await;
        tree.delete(3, b"k").await;
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Tombstone { logical_index: 3 })
        );
    }

    #[tokio::test]
    async fn test_tombstone_on_never_set_key_carries_raft_index() {
        // Deleting a key that was never set still stores a tombstone with the correct index.
        let mut tree = make_tree();
        tree.delete(7, b"ghost").await;
        assert_eq!(
            tree.get(b"ghost").await,
            Some(MemTableEntry::Tombstone { logical_index: 7 })
        );
    }

    #[tokio::test]
    async fn test_delete_writes_tombstone_to_wal() {
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        tree.delete(2, b"k").await;
        let entries = tree.wal.recorded();
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
        let wal = RecordingWal::default();
        // Pre-populate the WAL as if a previous run wrote these entries.
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k2".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        assert_eq!(
            tree.get(b"k1").await,
            Some(MemTableEntry::Value { logical_index: 1, value: b"v1".to_vec() })
        );
        assert_eq!(
            tree.get(b"k2").await,
            Some(MemTableEntry::Value { logical_index: 2, value: b"v2".to_vec() })
        );
    }

    #[tokio::test]
    async fn test_recover_replays_tombstone_hides_key() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k".to_vec(), b"v".to_vec()),
            WalEntry::delete(2, b"k".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        // After recovery the key carries a tombstone, not a value and not absent.
        assert_eq!(tree.get(b"k").await, Some(MemTableEntry::Tombstone { logical_index: 2 }));
    }

    #[tokio::test]
    async fn test_recover_tombstone_logical_index_matches_wal_entry() {
        // On recovery, the tombstone's logical_index must equal the WAL entry's raft index.
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k".to_vec(), b"v".to_vec()),
            WalEntry::delete(4, b"k".to_vec()), // non-contiguous raft index
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Tombstone { logical_index: 4 })
        );
    }

    #[tokio::test]
    async fn test_recover_last_set_wins_over_earlier_set() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Value { logical_index: 2, value: b"v2".to_vec() })
        );
    }

    #[tokio::test]
    async fn test_recover_set_after_tombstone_is_visible() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k".to_vec(), b"v1".to_vec()),
            WalEntry::delete(2, b"k".to_vec()),
            WalEntry::set(3, b"k".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Value { logical_index: 3, value: b"v2".to_vec() })
        );
    }

    #[tokio::test]
    async fn test_recover_empty_wal_leaves_tree_empty() {
        let mut tree = make_tree();
        tree.recover().await;
        assert_eq!(tree.get(b"k").await, None); // truly absent → None
    }

    // ── logical_index correctness ────────────────────────────────────────────

    #[tokio::test]
    async fn test_logical_index_stores_raft_index_across_distinct_keys() {
        let mut tree = make_tree();
        tree.set(1, b"a".to_vec(), b"1".to_vec()).await;
        tree.set(2, b"b".to_vec(), b"2".to_vec()).await;
        tree.set(3, b"c".to_vec(), b"3".to_vec()).await;
        assert_eq!(
            tree.get(b"a").await,
            Some(MemTableEntry::Value { logical_index: 1, value: b"1".to_vec() })
        );
        assert_eq!(
            tree.get(b"b").await,
            Some(MemTableEntry::Value { logical_index: 2, value: b"2".to_vec() })
        );
        assert_eq!(
            tree.get(b"c").await,
            Some(MemTableEntry::Value { logical_index: 3, value: b"3".to_vec() })
        );
    }

    #[tokio::test]
    async fn test_logical_index_after_interleaved_set_delete_set() {
        // Live write path: set→delete→set, final value carries raft index 3.
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v1".to_vec()).await;
        tree.delete(2, b"k").await;
        tree.set(3, b"k".to_vec(), b"v2".to_vec()).await;
        assert_eq!(
            tree.get(b"k").await,
            Some(MemTableEntry::Value { logical_index: 3, value: b"v2".to_vec() })
        );
    }

    #[tokio::test]
    async fn test_logical_index_preserved_after_recover_then_new_writes() {
        // Recover WAL entries with raft indices 1,2,3 then write new entries
        // with raft indices 4,5 — the LSM stores whatever index the caller provides.
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k2".to_vec(), b"v2".to_vec()),
            WalEntry::delete(3, b"k1".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;

        tree.set(4, b"k3".to_vec(), b"v3".to_vec()).await;
        assert_eq!(
            tree.get(b"k3").await,
            Some(MemTableEntry::Value { logical_index: 4, value: b"v3".to_vec() })
        );

        tree.set(5, b"k4".to_vec(), b"v4".to_vec()).await;
        assert_eq!(
            tree.get(b"k4").await,
            Some(MemTableEntry::Value { logical_index: 5, value: b"v4".to_vec() })
        );
    }

    #[tokio::test]
    async fn test_wal_index_matches_raft_index_after_recover_then_new_writes() {
        // Verify WAL entries carry the exact raft index passed by the caller.
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k2".to_vec(), b"v2".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;

        tree.set(3, b"k3".to_vec(), b"v3".to_vec()).await;
        tree.delete(4, b"k1").await;
        let entries = tree.wal.recorded();
        assert_eq!(entries[2].index, 3); // new set with raft index 3
        assert_eq!(entries[3].index, 4); // new delete with raft index 4
    }

    // ── last_applied_index ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_last_applied_index_is_zero_for_empty_tree() {
        let tree = make_tree();
        assert_eq!(tree.last_applied_index(), 0);
    }

    #[tokio::test]
    async fn test_last_applied_index_tracks_raft_index_after_set() {
        let mut tree = make_tree();
        tree.set(1, b"k1".to_vec(), b"v1".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 1);
        tree.set(2, b"k2".to_vec(), b"v2".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 2);
        tree.set(3, b"k3".to_vec(), b"v3".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 3);
    }

    #[tokio::test]
    async fn test_last_applied_index_tracks_raft_index_after_delete() {
        let mut tree = make_tree();
        tree.set(1, b"k".to_vec(), b"v".to_vec()).await;
        assert_eq!(tree.last_applied_index(), 1);
        tree.delete(2, b"k").await;
        assert_eq!(tree.last_applied_index(), 2);
    }

    #[tokio::test]
    async fn test_last_applied_index_tracks_delete_of_missing_key() {
        let mut tree = make_tree();
        tree.delete(1, b"nonexistent").await;
        assert_eq!(tree.last_applied_index(), 1);
    }

    #[tokio::test]
    async fn test_last_applied_index_restored_after_recover() {
        let wal = RecordingWal::default();
        wal.entries.lock().unwrap().extend([
            WalEntry::set(1, b"k1".to_vec(), b"v1".to_vec()),
            WalEntry::set(2, b"k2".to_vec(), b"v2".to_vec()),
            WalEntry::delete(3, b"k1".to_vec()),
        ]);
        let mut tree = LSMTree::with_wal(wal);
        tree.recover().await;
        // Last WAL entry has raft index 3 → last_applied_index = 3.
        assert_eq!(tree.last_applied_index(), 3);
    }

    #[tokio::test]
    async fn test_last_applied_index_zero_after_recover_from_empty_wal() {
        let mut tree = make_tree();
        tree.recover().await;
        assert_eq!(tree.last_applied_index(), 0);
    }
}
