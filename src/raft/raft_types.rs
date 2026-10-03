use crate::common::entry::Entry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

impl LogEntry {
    pub fn to_entry(&self) -> Entry<'_> {
        match &self.command {
            Command::Set { key, value } => Entry::set(self.term, key.as_bytes(), value.as_bytes()),
            Command::Delete { key } => Entry::delete(self.term, key.as_bytes()),
        }
    }
}

#[derive(Debug)]
pub struct RequestVoteData {
    pub term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub candidate: String,
}

#[derive(Debug)]
pub struct AppendEntriesData {
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub leader_commit: u64,
    pub leader_id: String,
    pub entries: Vec<LogEntry>,
}

#[derive(Debug)]
pub struct AppendEntriesReplyData {
    pub term: u64,
    pub success: bool,
    pub peer: String,
    pub entries_count: u64,
}

#[derive(Debug)]
pub struct RequestVoteReplyData {
    pub term: u64,
    pub vote: bool,
}

#[derive(Debug)]
pub struct ChangeStateReply {
    pub success: bool,
    pub leader: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

pub enum RaftMsg {
    RequestVoteReply {
        vote_reply: RequestVoteReplyData,
        reply_channel: Option<tokio::sync::oneshot::Sender<()>>,
    },
    VoteRequest {
        vote_request: RequestVoteData,
        reply_channel: Option<tokio::sync::oneshot::Sender<RequestVoteReplyData>>,
    },
    AppendEntries {
        append_request: AppendEntriesData,
        reply_channel: Option<tokio::sync::oneshot::Sender<AppendEntriesReplyData>>,
    },
    AppendEntriesReply {
        append_reply: AppendEntriesReplyData,
        reply_channel: Option<tokio::sync::oneshot::Sender<()>>,
    },
    ChangeState {
        command: Command,
        reply_channel: Option<tokio::sync::oneshot::Sender<ChangeStateReply>>,
    },
    GetState {
        key: String,
        reply_channel: tokio::sync::oneshot::Sender<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::entry::{OP_DELETE, OP_SET};

    #[test]
    fn test_to_entry_converts_set_command() {
        let log_entry = LogEntry {
            term: 3,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        };

        let entry = log_entry.to_entry();

        assert_eq!(entry.index, 3);
        assert_eq!(entry.op, OP_SET);
        assert_eq!(entry.key, b"key1".to_vec());
        assert_eq!(entry.value, b"val1".to_vec());
    }

    #[test]
    fn test_to_entry_converts_delete_command() {
        let log_entry = LogEntry {
            term: 7,
            command: Command::Delete {
                key: "key1".to_string(),
            },
        };

        let entry = log_entry.to_entry();

        assert_eq!(entry.index, 7);
        assert_eq!(entry.op, OP_DELETE);
        assert_eq!(entry.key, b"key1".to_vec());
        assert!(entry.value.is_empty());
    }

    #[test]
    fn test_to_entry_is_case_insensitive() {
        let log_entry = LogEntry {
            term: 1,
            command: Command::Set {
                key: "key1".to_string(),
                value: "val1".to_string(),
            },
        };

        assert_eq!(log_entry.to_entry(), Entry::set(1, b"key1", b"val1"));
    }

    #[test]
    fn test_to_entry_preserves_spaces_in_key_and_value() {
        let log_entry = LogEntry {
            term: 2,
            command: Command::Set {
                key: "user name".to_string(),
                value: "John Doe".to_string(),
            },
        };

        let entry = log_entry.to_entry();

        assert_eq!(entry.key, b"user name".to_vec());
        assert_eq!(entry.value, b"John Doe".to_vec());
    }

    #[test]
    fn test_to_entry_preserves_empty_value() {
        let log_entry = LogEntry {
            term: 2,
            command: Command::Set {
                key: "key1".to_string(),
                value: String::new(),
            },
        };

        let entry = log_entry.to_entry();

        assert_eq!(entry.op, OP_SET);
        assert_eq!(entry.key, b"key1".to_vec());
        assert!(entry.value.is_empty());
    }

    #[test]
    fn test_to_entry_delete_leaves_value_empty() {
        let log_entry = LogEntry {
            term: 9,
            command: Command::Delete {
                key: "user name".to_string(),
            },
        };

        let entry = log_entry.to_entry();

        assert_eq!(entry.op, OP_DELETE);
        assert_eq!(entry.key, b"user name".to_vec());
        assert!(entry.value.is_empty());
    }
}
