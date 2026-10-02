use super::proto::raft_server::Raft;
use super::proto::{
    AppendEntriesMessage, AppendEntriesReply, RequestVoteMessage, RequestVoteReply,
};
use super::raft_types::{
    AppendEntriesData, AppendEntriesReplyData, LogEntry, RaftMsg, RequestVoteData,
    RequestVoteReplyData,
};
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct RaftService {
    mailbox: Sender<RaftMsg>,
}

impl RaftService {
    pub fn new(mailbox: Sender<RaftMsg>) -> Self {
        RaftService { mailbox }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteMessage>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let (snd, rcv) = tokio::sync::oneshot::channel::<RequestVoteReplyData>();
        let message_data = RequestVoteData {
            term: request.get_ref().term,
            last_log_index: request.get_ref().last_log_index,
            last_log_term: request.get_ref().last_log_term,
            candidate: request.get_ref().candidate.clone(),
        };
        let vote_message = RaftMsg::VoteRequest {
            vote_request: message_data,
            reply_channel: Some(snd),
        };

        self.mailbox
            .send(vote_message)
            .await
            .expect("Failed to send vote message");
        let vote_reply = rcv.await.expect("Failed to receive vote reply");

        let reply = RequestVoteReply {
            term: vote_reply.term,
            vote: vote_reply.vote,
        };
        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesMessage>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let message = request.into_inner();
        let (snd, rcv) = tokio::sync::oneshot::channel::<AppendEntriesReplyData>();
        let mut entries = Vec::<LogEntry>::new();
        for entry in message.entries.iter() {
            let log_entry = LogEntry {
                term: entry.term,
                command: match entry.command.clone() {
                    Some(command) => match command.kind {
                        Some(super::proto::command::Kind::Set(set)) => {
                            super::raft_types::Command::Set {
                                key: set.key.clone(),
                                value: set.value.clone(),
                            }
                        }
                        Some(super::proto::command::Kind::Delete(delete)) => {
                            super::raft_types::Command::Delete {
                                key: delete.key.clone(),
                            }
                        }
                        None => return Err(Status::invalid_argument("Missing command kind")),
                    },
                    None => return Err(Status::invalid_argument("Log entry without command")),
                },
            };
            entries.push(log_entry);
        }

        let append_entries_daata = AppendEntriesData {
            term: message.term,
            prev_log_index: message.prev_log_index,
            prev_log_term: message.prev_log_term,
            leader_commit: message.leader_commit,
            leader_id: message.leader_id.clone(),
            entries,
        };
        let append_message = RaftMsg::AppendEntries {
            append_request: append_entries_daata,
            reply_channel: Some(snd),
        };
        self.mailbox
            .send(append_message)
            .await
            .expect("Failed to send append entries message");
        let reply_data = rcv.await.expect("Failed to receive append entries reply");
        let reply = AppendEntriesReply {
            term: reply_data.term,
            success: reply_data.success,
        };

        Ok(Response::new(reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::proto::{
        Command as ProtoCommand, Delete, Entry as ProtoEntry, Set, command::Kind as ProtoKind,
    };
    use crate::raft::raft_types::Command;

    fn append_entries_message(entries: Vec<ProtoEntry>) -> AppendEntriesMessage {
        AppendEntriesMessage {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_id: "node2".to_string(),
            entries,
        }
    }

    fn set_entry(term: u64, key: &str, value: &str) -> ProtoEntry {
        ProtoEntry {
            term,
            command: Some(ProtoCommand {
                kind: Some(ProtoKind::Set(Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })),
            }),
        }
    }

    // ------------------------------------------------------------
    // A malformed entry is a client error, not a reason to crash the
    // whole gRPC server task.
    // ------------------------------------------------------------

    #[tokio::test]
    async fn test_append_entries_rejects_entry_without_command() {
        let (mailbox, mut inbox) = tokio::sync::mpsc::channel(1);
        let service = RaftService::new(mailbox);

        let status = service
            .append_entries(Request::new(append_entries_message(vec![ProtoEntry {
                term: 1,
                command: None,
            }])))
            .await
            .expect_err("entry without a command must be rejected");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            inbox.try_recv().is_err(),
            "a malformed request must not reach the Raft node"
        );
    }

    #[tokio::test]
    async fn test_append_entries_rejects_command_without_kind() {
        let (mailbox, mut inbox) = tokio::sync::mpsc::channel(1);
        let service = RaftService::new(mailbox);

        let status = service
            .append_entries(Request::new(append_entries_message(vec![ProtoEntry {
                term: 1,
                command: Some(ProtoCommand { kind: None }),
            }])))
            .await
            .expect_err("command without a kind must be rejected");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            inbox.try_recv().is_err(),
            "a malformed request must not reach the Raft node"
        );
    }

    #[tokio::test]
    async fn test_append_entries_rejects_malformed_entry_among_valid_ones() {
        let (mailbox, mut inbox) = tokio::sync::mpsc::channel(1);
        let service = RaftService::new(mailbox);

        let status = service
            .append_entries(Request::new(append_entries_message(vec![
                set_entry(1, "key1", "val1"),
                ProtoEntry {
                    term: 2,
                    command: None,
                },
            ])))
            .await
            .expect_err("one malformed entry must reject the whole batch");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            inbox.try_recv().is_err(),
            "no entry from a partially malformed batch may be applied"
        );
    }

    // ------------------------------------------------------------
    // Well-formed entries decode into typed commands
    // ------------------------------------------------------------

    #[tokio::test]
    async fn test_append_entries_forwards_typed_commands() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = tokio::sync::mpsc::channel(1);
        let service = RaftService::new(mailbox);

        let node = tokio::spawn(async move {
            let message = inbox.recv().await.expect("mailbox message");
            let RaftMsg::AppendEntries {
                append_request,
                reply_channel,
            } = message
            else {
                panic!("expected an AppendEntries message");
            };
            reply_channel
                .expect("reply channel")
                .send(AppendEntriesReplyData {
                    term: 2,
                    success: true,
                    peer: "node1".to_string(),
                    entries_count: append_request.entries.len() as u64,
                })
                .expect("reply must be delivered");
            append_request
        });

        let reply = service
            .append_entries(Request::new(append_entries_message(vec![
                set_entry(1, "user name", "John Doe"),
                ProtoEntry {
                    term: 2,
                    command: Some(ProtoCommand {
                        kind: Some(ProtoKind::Delete(Delete {
                            key: "key1".to_string(),
                        })),
                    }),
                },
            ])))
            .await
            .expect("well-formed request must be accepted")
            .into_inner();

        assert!(reply.success);
        assert_eq!(reply.term, 2);

        let append_request = node.await?;
        assert_eq!(append_request.term, 2);
        assert_eq!(append_request.leader_id, "node2");
        assert_eq!(append_request.entries.len(), 2);
        assert_eq!(append_request.entries[0].term, 1);
        // The value keeps its space -- the old string encoding split on
        // whitespace and would have lost everything after "John".
        assert_eq!(
            append_request.entries[0].command,
            Command::Set {
                key: "user name".to_string(),
                value: "John Doe".to_string(),
            }
        );
        assert_eq!(append_request.entries[1].term, 2);
        assert_eq!(
            append_request.entries[1].command,
            Command::Delete {
                key: "key1".to_string(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_append_entries_accepts_empty_heartbeat() -> anyhow::Result<()> {
        let (mailbox, mut inbox) = tokio::sync::mpsc::channel(1);
        let service = RaftService::new(mailbox);

        let node = tokio::spawn(async move {
            let message = inbox.recv().await.expect("mailbox message");
            let RaftMsg::AppendEntries { reply_channel, .. } = message else {
                panic!("expected an AppendEntries message");
            };
            reply_channel
                .expect("reply channel")
                .send(AppendEntriesReplyData {
                    term: 2,
                    success: true,
                    peer: "node1".to_string(),
                    entries_count: 0,
                })
                .expect("reply must be delivered");
        });

        let reply = service
            .append_entries(Request::new(append_entries_message(vec![])))
            .await
            .expect("heartbeat must be accepted")
            .into_inner();

        assert!(reply.success);
        node.await?;
        Ok(())
    }
}
