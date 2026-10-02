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
