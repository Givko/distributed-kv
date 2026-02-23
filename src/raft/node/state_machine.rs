use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct StateMachine {
    data: HashMap<String, String>,
}

impl StateMachine {
    pub fn new() -> Self {
        StateMachine {
            data: HashMap::new(),
        }
    }

    /// Apply a command string of the form "SET key value" to the state machine.
    pub fn apply(&mut self, command: &str) {
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

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    /// Direct insert — intended for test setup only.
    pub fn insert(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }
}
