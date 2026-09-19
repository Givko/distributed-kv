use crate::common::entry::Entry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

impl LogEntry {
    /// Maps this entry onto the WAL record layout so the Raft log can be
    /// persisted with the same `Encoder` as the storage engine: the term
    /// becomes the record index and the command is split into op/key/value.
    pub fn to_entry(&self) -> anyhow::Result<Entry> {
        let parts: Vec<&str> = self.command.split_whitespace().collect();
        match parts.as_slice() {
            [cmd, key, value] if cmd.eq_ignore_ascii_case("set") => Ok(Entry::set(
                self.term,
                key.as_bytes().to_vec(),
                value.as_bytes().to_vec(),
            )),
            [cmd, key] if cmd.eq_ignore_ascii_case("delete") => {
                Ok(Entry::delete(self.term, key.as_bytes().to_vec()))
            }
            _ => Err(anyhow::anyhow!("Unknown command: {}", self.command)),
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
        command: String,
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
            command: "set key1 val1".to_string(),
        };

        let entry = log_entry.to_entry().unwrap();

        assert_eq!(entry.index, 3);
        assert_eq!(entry.op, OP_SET);
        assert_eq!(entry.key, b"key1".to_vec());
        assert_eq!(entry.value, b"val1".to_vec());
    }

    #[test]
    fn test_to_entry_converts_delete_command() {
        let log_entry = LogEntry {
            term: 7,
            command: "delete key1".to_string(),
        };

        let entry = log_entry.to_entry().unwrap();

        assert_eq!(entry.index, 7);
        assert_eq!(entry.op, OP_DELETE);
        assert_eq!(entry.key, b"key1".to_vec());
        assert!(entry.value.is_empty());
    }

    #[test]
    fn test_to_entry_is_case_insensitive() {
        let log_entry = LogEntry {
            term: 1,
            command: "SET key1 val1".to_string(),
        };

        assert_eq!(
            log_entry.to_entry().unwrap(),
            Entry::set(1, b"key1".to_vec(), b"val1".to_vec())
        );
    }

    #[test]
    fn test_to_entry_rejects_unknown_command() {
        let log_entry = LogEntry {
            term: 1,
            command: "increment key1".to_string(),
        };

        assert!(log_entry.to_entry().is_err());
    }

    #[test]
    fn test_to_entry_rejects_wrong_arity() {
        let set_without_value = LogEntry {
            term: 1,
            command: "set key1".to_string(),
        };
        let delete_with_value = LogEntry {
            term: 1,
            command: "delete key1 val1".to_string(),
        };

        assert!(set_without_value.to_entry().is_err());
        assert!(delete_with_value.to_entry().is_err());
    }

    #[test]
    fn test_to_entry_rejects_empty_command() {
        let log_entry = LogEntry {
            term: 1,
            command: String::new(),
        };

        assert!(log_entry.to_entry().is_err());
    }
}
