use super::raft_types::LogEntry;

pub enum OutMsg {
    RequestVote {
        term: u64,
        peer: String,
        last_log_index: u64,
        last_log_term: u64,
        candidate: String,
    },
    AppendEntries {
        term: u64,
        peer: String,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
        leader_id: String,
        entries: Vec<LogEntry>,
    },
}
