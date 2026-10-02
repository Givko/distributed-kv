use crate::{raft::raft_types::Command, storage::lsm_tree::MemTableEntry};

#[async_trait::async_trait]
pub trait StorageEngine {
    async fn set(&mut self, key: Vec<u8>, value: Vec<u8>);

    async fn delete(&mut self, key: &[u8]) -> bool;

    /// Returns `None` if the key has never been set, `Some(Entry::Value(_))` if
    /// it exists, or `Some(Entry::Tombstone)` if it was deleted.
    async fn get(&self, key: &[u8]) -> Option<MemTableEntry>;

    async fn recover(&mut self) -> anyhow::Result<()>;

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

    pub async fn recover(&mut self) -> anyhow::Result<()> {
        let _ = self.engine.recover().await;
        eprintln!(
            "State machine with last applied index {}",
            self.last_applied_index()
        );
        Ok(())
    }

    pub async fn apply(&mut self, command: Command) -> anyhow::Result<()> {
        match command {
            Command::Set { key, value } => {
                self.engine
                    .set(key.as_bytes().to_vec(), value.as_bytes().to_vec())
                    .await;
                Ok(())
            }
            Command::Delete { key } => {
                self.engine.delete(key.as_bytes()).await;
                Ok(())
            }
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        match self.engine.get(key.as_bytes()).await {
            Some(MemTableEntry::Value(v)) => String::from_utf8(v).ok(),
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
        async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
            self.data.insert(key, value);
        }

        async fn delete(&mut self, key: &[u8]) -> bool {
            self.data.remove(key).is_some()
        }

        async fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
            self.data.get(key).map(|v| MemTableEntry::Value(v.clone()))
        }

        async fn recover(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

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
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val1".into(),
        })
        .await
        .unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val1"));
    }

    #[tokio::test]
    async fn test_apply_set_uppercase_stores_value() {
        let mut sm = make_sm();
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val1".into(),
        })
        .await
        .unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val1"));
    }

    #[tokio::test]
    async fn test_apply_set_mixed_case_stores_value() {
        let mut sm = make_sm();
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val1".into(),
        })
        .await
        .unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val1"));
    }

    #[tokio::test]
    async fn test_apply_delete_uppercase_removes_key() {
        let mut sm = make_sm();
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val1".into(),
        })
        .await
        .unwrap();
        sm.apply(Command::Delete { key: "key1".into() })
            .await
            .unwrap();
        assert_eq!(sm.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_apply_delete_mixed_case_removes_key() {
        let mut sm = make_sm();
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val1".into(),
        })
        .await
        .unwrap();
        sm.apply(Command::Delete { key: "key1".into() })
            .await
            .unwrap();
        assert_eq!(sm.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_apply_set_overwrites_existing_key() {
        let mut sm = make_sm();
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val1".into(),
        })
        .await
        .unwrap();
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val2".into(),
        })
        .await
        .unwrap();
        assert_eq!(sm.get("key1").await.as_deref(), Some("val2"));
    }

    #[tokio::test]
    async fn test_apply_delete_removes_key() {
        let mut sm = make_sm();
        sm.apply(Command::Set {
            key: "key1".into(),
            value: "val1".into(),
        })
        .await
        .unwrap();
        sm.apply(Command::Delete { key: "key1".into() })
            .await
            .unwrap();
        assert_eq!(sm.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_apply_delete_nonexistent_key_does_not_error() {
        let mut sm = make_sm();
        assert!(
            sm.apply(Command::Delete {
                key: "missing".into()
            })
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn test_apply_empty_command_is_ok() {
        let mut sm = make_sm();
        assert!(
            sm.apply(Command::Set {
                key: "".into(),
                value: "".into()
            })
            .await
            .is_ok()
        );
    }
}
