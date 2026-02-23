use std::collections::HashMap;

use crate::raft::storage_engine::StorageEngine;

/// In-memory key-value store backed by a `HashMap<String, String>`.
#[derive(Debug, Default)]
pub struct MemKv {
    data: HashMap<String, String>,
}

impl StorageEngine for MemKv {
    /// Apply a command of the form `SET <key> <value>`.
    fn apply(&mut self, command: &str) {
        let args: Vec<&str> = command.split_whitespace().collect();
        if args.len() < 2 {
            return;
        }
        let cmd = args[0];
        let key = args[1];
        if cmd.to_lowercase() == "set" && args.len() >= 3 {
            let val = args[2];
            self.data.insert(key.to_owned(), val.to_owned());
        }
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }
}

impl MemKv {
    /// Direct insert — intended for test setup only.
    pub fn insert(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }
}
