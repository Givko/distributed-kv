pub trait StorageEngine {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);

    fn delete(&mut self, key: &[u8]) -> bool;

    fn get(&self, key: &[u8]) -> Option<&[u8]>;
}

pub struct StateMachine<SM: StorageEngine> {
    engine: SM,
}

impl <SM: StorageEngine> StateMachine<SM> {
    pub fn new(engine: SM) -> Self {
        StateMachine { engine }
    }

    pub fn apply(&mut self, command: String) -> anyhow::Result<()> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            eprintln!("Received empty command, ignoring");
            return Ok(()); 
        }

        let cmd = parts[0].to_lowercase();
        match cmd.as_str() {
            "set" if parts.len() == 3 => {
                self.engine.set(parts[1].as_bytes().to_vec(), parts[2].as_bytes().to_vec());
                return Ok(());
            }
            "delete" if parts.len() == 2 => {
                self.engine.delete(parts[1].as_bytes());
                return Ok(());
            }
            _ => {
                //Return error for unknown command instead of silently ignoring it
                return Err(anyhow::anyhow!("Unknown command: {}", command));
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.engine.get(key.as_bytes()).and_then(|v| std::str::from_utf8(v).ok())
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

    impl StorageEngine for MockEngine {
        fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
            self.data.insert(key, value);
        }

        fn delete(&mut self, key: &[u8]) -> bool {
            self.data.remove(key).is_some()
        }

        fn get(&self, key: &[u8]) -> Option<&[u8]> {
            self.data.get(key).map(|v| v.as_slice())
        }
    }

    fn make_sm() -> StateMachine<MockEngine> {
        StateMachine::new(MockEngine::default())
    }

    #[test]
    fn test_apply_set_stores_value() {
        let mut sm = make_sm();
        sm.apply("set key1 val1".to_string()).unwrap();
        assert_eq!(sm.get("key1"), Some("val1"));
    }

    #[test]
    fn test_apply_set_uppercase_stores_value() {
        let mut sm = make_sm();
        sm.apply("SET key1 val1".to_string()).unwrap();
        assert_eq!(sm.get("key1"), Some("val1"));
    }

    #[test]
    fn test_apply_set_mixed_case_stores_value() {
        let mut sm = make_sm();
        sm.apply("Set key1 val1".to_string()).unwrap();
        assert_eq!(sm.get("key1"), Some("val1"));
    }

    #[test]
    fn test_apply_delete_uppercase_removes_key() {
        let mut sm = make_sm();
        sm.apply("set key1 val1".to_string()).unwrap();
        sm.apply("DELETE key1".to_string()).unwrap();
        assert_eq!(sm.get("key1"), None);
    }

    #[test]
    fn test_apply_delete_mixed_case_removes_key() {
        let mut sm = make_sm();
        sm.apply("set key1 val1".to_string()).unwrap();
        sm.apply("Delete key1".to_string()).unwrap();
        assert_eq!(sm.get("key1"), None);
    }

    #[test]
    fn test_apply_set_overwrites_existing_key() {
        let mut sm = make_sm();
        sm.apply("set key1 val1".to_string()).unwrap();
        sm.apply("set key1 val2".to_string()).unwrap();
        assert_eq!(sm.get("key1"), Some("val2"));
    }

    #[test]
    fn test_apply_delete_removes_key() {
        let mut sm = make_sm();
        sm.apply("set key1 val1".to_string()).unwrap();
        sm.apply("delete key1".to_string()).unwrap();
        assert_eq!(sm.get("key1"), None);
    }

    #[test]
    fn test_apply_delete_nonexistent_key_does_not_error() {
        let mut sm = make_sm();
        assert!(sm.apply("delete missing".to_string()).is_ok());
    }

    #[test]
    fn test_apply_empty_command_is_ok() {
        let mut sm = make_sm();
        assert!(sm.apply("".to_string()).is_ok());
    }

    #[test]
    fn test_apply_unknown_command_returns_error() {
        let mut sm = make_sm();
        assert!(sm.apply("get key1".to_string()).is_err());
    }

    #[test]
    fn test_apply_set_missing_value_returns_error() {
        let mut sm = make_sm();
        assert!(sm.apply("set key1".to_string()).is_err());
    }

    #[test]
    fn test_get_missing_key_returns_none() {
        let sm = make_sm();
        assert_eq!(sm.get("missing"), None);
    }
}




