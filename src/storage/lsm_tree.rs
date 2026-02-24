use std::collections::BTreeMap;
use crate::raft::state_machine::StorageEngine;

pub struct LSMTree {
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl LSMTree {
    pub fn init() -> Self {
        LSMTree {
            memtable: BTreeMap::new(),
        }
        //TODO: use WAL to load existing data from disk on startup
    }

}

impl StorageEngine for LSMTree {
    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.memtable.get(key).map(|v| v.as_slice())
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.insert(key, value);
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        self.memtable.remove(key).is_some()
        //TODO: add tombstone entry in BTreeMap and WAL to ensure deletion is persisted
    }
}