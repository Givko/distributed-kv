use crate::storage::entry::Entry;
use std::collections::BTreeMap;

#[derive(Clone, PartialEq, Debug)]
pub enum MemTableEntry {
    Value {
        logical_index: u64, // Logical index of the last WAL entry that set this value. Used to resolve conflicts during recovery.
        value: Vec<u8>,
    },
    Tombstone {
        logical_index: u64, // Logical index of the last WAL entry that deleted this key. Used to resolve conflicts during recovery.
    },
}

pub trait MemTable {
    fn set(&mut self, logical_index: u64, key: Vec<u8>, value: Vec<u8>);
    fn delete(&mut self, logical_index: u64, key: &[u8]);
    fn get(&self, key: &[u8]) -> Option<&MemTableEntry>;
    fn to_entries(&self) -> Vec<Entry>; // ordered snapshot for flushing — owns the flush conversion logic
    fn clear(&mut self);
    fn is_empty(&self) -> bool;
    fn size_in_bytes(&self) -> usize; // used to decide when to trigger a flush
    fn extend(&mut self, entries: Vec<Entry>);
}

pub struct BTreeMapMemTable {
    map: BTreeMap<Vec<u8>, MemTableEntry>,
    cur_size_bytes: usize, // Track the size of the memtable in bytes for flush triggering
}

impl BTreeMapMemTable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            cur_size_bytes: 0,
        }
    }

    fn entry_size(key: &[u8], entry: &MemTableEntry) -> usize {
        let key_size = key.len();
        let value_size = match entry {
            MemTableEntry::Value { value, .. } => value.len(),
            MemTableEntry::Tombstone { .. } => 0,
        };

        key_size + value_size + std::mem::size_of::<u64>() + std::mem::size_of::<u8>() // logical_index + op code
    }

    fn set_internal(&mut self, key: Vec<u8>, entry: MemTableEntry) {
        let size = Self::entry_size(&key, &entry);

        if let Some(old_entry) = self.map.insert(key.clone(), entry) {
            self.cur_size_bytes -= Self::entry_size(&key, &old_entry);
        }
        self.cur_size_bytes += size;
    }
}

impl MemTable for BTreeMapMemTable {
    fn set(&mut self, logical_index: u64, key: Vec<u8>, value: Vec<u8>) {
        let entry = MemTableEntry::Value {
            logical_index,
            value,
        };
        self.set_internal(key, entry);
    }

    fn delete(&mut self, logical_index: u64, key: &[u8]) {
        let entry = MemTableEntry::Tombstone { logical_index };
        self.set_internal(key.to_vec(), entry);
    }

    fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        self.map.get(key)
    }

    fn to_entries(&self) -> Vec<Entry> {
        self.map
            .iter()
            .map(|(key, entry)| match entry {
                MemTableEntry::Value {
                    logical_index,
                    value,
                } => Entry::set(*logical_index, key.clone(), value.clone()),
                MemTableEntry::Tombstone { logical_index } => {
                    Entry::delete(*logical_index, key.clone())
                }
            })
            .collect()
    }

    fn clear(&mut self) {
        self.map.clear();
        self.cur_size_bytes = 0;
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn size_in_bytes(&self) -> usize {
        self.cur_size_bytes
    }

    fn extend(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            match entry.op {
                crate::storage::entry::OP_SET => self.set(entry.index, entry.key, entry.value),
                crate::storage::entry::OP_DELETE => self.delete(entry.index, &entry.key),
                _ => eprintln!("Unknown entry op in extend: {}", entry.op),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_bytes_grows_with_each_entry() {
        let mut table = BTreeMapMemTable::new();
        let empty_size = table.size_in_bytes();

        table.set(1, b"key1".to_vec(), b"value1".to_vec());
        let one_entry_size = table.size_in_bytes();
        let expected_size = empty_size
            + BTreeMapMemTable::entry_size(
                b"key1",
                &MemTableEntry::Value {
                    logical_index: 1,
                    value: b"value1".to_vec(),
                },
            );

        assert_eq!(
            one_entry_size, expected_size,
            "size should increase by the size of the new entry"
        );

        table.set(2, b"key2".to_vec(), b"value2".to_vec());
        let two_entries_size = table.size_in_bytes();
        let expected_size = expected_size
            + BTreeMapMemTable::entry_size(
                b"key2",
                &MemTableEntry::Value {
                    logical_index: 2,
                    value: b"value2".to_vec(),
                },
            );

        assert_eq!(
            two_entries_size, expected_size,
            "size should increase by the size of the second entry"
        );
    }

    #[test]
    fn size_bytes_accounts_for_overwrite() {
        let mut table = BTreeMapMemTable::new();

        table.set(1, b"key".to_vec(), b"small".to_vec());
        let after_first = table.size_in_bytes();

        // overwrite with a larger value — size should grow by the diff
        table.set(2, b"key".to_vec(), b"a_much_larger_value".to_vec());
        let expected_after_overwrite = BTreeMapMemTable::entry_size(
            b"key",
            &MemTableEntry::Value {
                logical_index: 2,
                value: b"a_much_larger_value".to_vec(),
            },
        );
        assert_eq!(
            table.size_in_bytes(),
            expected_after_overwrite,
            "size should reflect the new larger value"
        );
        assert!(
            table.size_in_bytes() > after_first,
            "overwriting with a larger value should increase size"
        );

        // overwrite with a smaller value — size should shrink
        table.set(3, b"key".to_vec(), b"x".to_vec());
        let expected_after_shrink = BTreeMapMemTable::entry_size(
            b"key",
            &MemTableEntry::Value {
                logical_index: 3,
                value: b"x".to_vec(),
            },
        );
        assert_eq!(
            table.size_in_bytes(),
            expected_after_shrink,
            "size should reflect the new smaller value"
        );
        assert!(
            table.size_in_bytes() < after_first,
            "overwriting with a smaller value should decrease size"
        );
    }

    #[test]
    fn size_bytes_resets_to_zero_after_clear() {
        let mut table = BTreeMapMemTable::new();

        table.set(1, b"key1".to_vec(), b"value1".to_vec());
        table.set(2, b"key2".to_vec(), b"value2".to_vec());
        assert!(
            table.size_in_bytes() > 0,
            "size should be non-zero before clear"
        );

        table.clear();
        assert_eq!(table.size_in_bytes(), 0, "size should be zero after clear");
    }

    #[test]
    fn size_bytes_tracks_delete_tombstone() {
        let mut table = BTreeMapMemTable::new();

        table.set(1, b"key".to_vec(), b"some_value".to_vec());
        let after_set = table.size_in_bytes();

        // replace with a tombstone — key stays the same, value bytes disappear
        table.delete(2, b"key");
        let expected_after_delete =
            BTreeMapMemTable::entry_size(b"key", &MemTableEntry::Tombstone { logical_index: 2 });
        assert_eq!(
            table.size_in_bytes(),
            expected_after_delete,
            "size should reflect tombstone entry"
        );
        assert!(
            table.size_in_bytes() < after_set,
            "tombstone should be smaller than a value entry"
        );
    }

    #[test]
    fn to_entries_returns_keys_in_lexicographic_order() {
        let mut table = BTreeMapMemTable::new();

        // logical indices are deliberately inverted relative to key order
        // to prove sorting is by key, not by raft index
        table.set(1, b"zebra".to_vec(), b"z".to_vec());
        table.set(2, b"apple".to_vec(), b"a".to_vec());
        table.set(3, b"mango".to_vec(), b"m".to_vec());

        let entries = table.to_entries();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"apple"); // index 2 — comes first by key
        assert_eq!(entries[1].key, b"mango"); // index 3
        assert_eq!(entries[2].key, b"zebra"); // index 1 — comes last by key
    }

    #[test]
    fn to_entries_preserves_logical_index_and_op() {
        let mut table = BTreeMapMemTable::new();

        table.set(10, b"key1".to_vec(), b"val1".to_vec());
        table.delete(20, b"key2");

        let entries = table.to_entries();

        assert_eq!(entries.len(), 2);

        let set_entry = entries.iter().find(|e| e.key == b"key1").unwrap();
        assert_eq!(set_entry.index, 10);
        assert_eq!(set_entry.op, crate::storage::entry::OP_SET);
        assert_eq!(set_entry.value, b"val1");

        let del_entry = entries.iter().find(|e| e.key == b"key2").unwrap();
        assert_eq!(del_entry.index, 20);
        assert_eq!(del_entry.op, crate::storage::entry::OP_DELETE);
        assert!(del_entry.value.is_empty());
    }

    #[test]
    fn to_entries_lexicographic_order_with_byte_keys() {
        // Verify byte-level ordering: b"b" < b"ba" < b"bb" < b"c"
        // This matters for SSTable binary search and merge correctness.
        // Logical indices are inverted vs key order to confirm key-sorted output.
        let mut table = BTreeMapMemTable::new();

        table.set(1, b"c".to_vec(), b"4".to_vec());
        table.set(2, b"ba".to_vec(), b"2".to_vec());
        table.set(3, b"b".to_vec(), b"1".to_vec());
        table.set(4, b"bb".to_vec(), b"3".to_vec());

        let entries = table.to_entries();
        let keys: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();

        assert_eq!(
            keys,
            vec![b"b".to_vec(), b"ba".to_vec(), b"bb".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn to_entries_empty_table_returns_empty_vec() {
        let table = BTreeMapMemTable::new();
        assert!(table.to_entries().is_empty());
    }
}
