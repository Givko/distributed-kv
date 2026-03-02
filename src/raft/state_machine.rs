use crate::storage::lsm_tree::MemTableEntry;

#[async_trait::async_trait]
pub trait StorageEngine {
    async fn set(&mut self, raft_index: u64, key: Vec<u8>, value: Vec<u8>);

    async fn delete(&mut self, raft_index: u64, key: &[u8]) -> bool;

    /// Returns `None` if the key has never been set, `Some(Entry::Value(_))` if
    /// it exists, or `Some(Entry::Tombstone)` if it was deleted.
    async fn get(&self, key: &[u8]) -> Option<MemTableEntry>;

    async fn recover(&mut self);

    fn last_applied_index(&self) -> u64;
}

pub struct StateMachine<SM: StorageEngine> {
    engine: SM,
}

impl<SM: StorageEngine> StateMachine<SM> {
    pub fn new(engine: SM) -> Self {
        StateMachine { engine }
    }

    pub fn last_applied_index(&self) -> u64 {
        // For simplicity, we won't track the last applied index in this example.
        self.engine.last_applied_index()
    }

    pub async fn recover(&mut self) {
        self.engine.recover().await;
        eprintln!(
            "State machine with last applied index {}",
            self.last_applied_index()
        );
    }

    pub async fn apply(&mut self, raft_index: u64, command: String) -> anyhow::Result<()> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            eprintln!("Received empty command, ignoring");
            return Ok(());
        }

        let cmd = parts[0].to_lowercase();
        match cmd.as_str() {
            "set" if parts.len() == 3 => {
                self.engine
                    .set(raft_index, parts[1].as_bytes().to_vec(), parts[2].as_bytes().to_vec())
                    .await;
                Ok(())
            }
            "delete" if parts.len() == 2 => {
                self.engine.delete(raft_index, parts[1].as_bytes()).await;
                Ok(())
            }
            _ => {
                //Return error for unknown command instead of silently ignoring it
                Err(anyhow::anyhow!("Unknown command: {}", command))
            }
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        match self.engine.get(key.as_bytes()).await {
            Some(MemTableEntry::Value { logical_index: _, value }) => String::from_utf8(value).ok(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Default)]
    struct MockEngine {
        data: HashMap<Vec<u8>, Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl StorageEngine for MockEngine {
        async fn set(&mut self, _raft_index: u64, key: Vec<u8>, value: Vec<u8>) {
            self.data.insert(key, value);
        }

        async fn delete(&mut self, _raft_index: u64, key: &[u8]) -> bool {
            self.data.remove(key).is_some()
        }

        async fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
            self.data.get(key).map(|v| MemTableEntry::Value { logical_index: 0, value: v.clone() })
        }

        async fn recover(&mut self) {}

        fn last_applied_index(&self) -> u64 {
            0
        }
    }

    fn make_sm() -> StateMachine<MockEngine> {
        StateMachine::new(MockEngine::default())
    }

    #[tokio::test]
    async fn test_apply_set_stores_value() {
        let mut sm = make_sm();
        sm.apply(1, "set key1 val1".to_string()).await.unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val1"));
    }

    #[tokio::test]
    async fn test_apply_set_uppercase_stores_value() {
        let mut sm = make_sm();
        sm.apply(1, "SET key1 val1".to_string()).await.unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val1"));
    }

    #[tokio::test]
    async fn test_apply_set_mixed_case_stores_value() {
        let mut sm = make_sm();
        sm.apply(1, "Set key1 val1".to_string()).await.unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val1"));
    }

    #[tokio::test]
    async fn test_apply_delete_uppercase_removes_key() {
        let mut sm = make_sm();
        sm.apply(1, "set key1 val1".to_string()).await.unwrap();
        sm.apply(2, "DELETE key1".to_string()).await.unwrap();
        assert_eq!(sm.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_apply_delete_mixed_case_removes_key() {
        let mut sm = make_sm();
        sm.apply(1, "set key1 val1".to_string()).await.unwrap();
        sm.apply(2, "Delete key1".to_string()).await.unwrap();
        assert_eq!(sm.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_apply_set_overwrites_existing_key() {
        let mut sm = make_sm();
        sm.apply(1, "set key1 val1".to_string()).await.unwrap();
        sm.apply(2, "set key1 val2".to_string()).await.unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val2"));
    }

    #[tokio::test]
    async fn test_apply_delete_removes_key() {
        let mut sm = make_sm();
        sm.apply(1, "set key1 val1".to_string()).await.unwrap();
        sm.apply(2, "delete key1".to_string()).await.unwrap();
        assert_eq!(sm.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_apply_delete_nonexistent_key_does_not_error() {
        let mut sm = make_sm();
        assert!(sm.apply(1, "delete missing".to_string()).await.is_ok());
    }

    #[tokio::test]
    async fn test_apply_empty_command_is_ok() {
        let mut sm = make_sm();
        assert!(sm.apply(1, "".to_string()).await.is_ok());
    }

    #[tokio::test]
    async fn test_apply_unknown_command_returns_error() {
        let mut sm = make_sm();
        assert!(sm.apply(1, "get key1".to_string()).await.is_err());
    }

    #[tokio::test]
    async fn test_apply_set_missing_value_returns_error() {
        let mut sm = make_sm();
        assert!(sm.apply(1, "set key1".to_string()).await.is_err());
    }

    #[tokio::test]
    async fn test_get_missing_key_returns_none() {
        let sm = make_sm();
        assert_eq!(sm.get("missing").await, None);
    }
}
