use super::network_types::OutMsg;
use super::raft_types::{AppendEntriesReplyData, RaftMsg, RequestVoteReplyData};
use crate::raft::proto::raft_client::RaftClient;
use crate::raft::proto::{AppendEntriesMessage, Entry, RequestVoteMessage};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;
use tonic::transport::Channel;

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

pub async fn network_worker(mut outbox: Receiver<OutMsg>, raft_inbox: Sender<RaftMsg>) {
    while let Some(msg) = outbox.recv().await {
        match msg {
            OutMsg::RequestVote {
                term,
                peer,
                last_log_index,
                last_log_term,
                candidate,
            } => {
                let vote_request = RequestVoteMessage {
                    term,
                    last_log_index,
                    last_log_term,
                    candidate,
                };
                let request = Request::new(vote_request);
                tokio::spawn(send_request_vote(peer, request, raft_inbox.clone()));
            }
            OutMsg::AppendEntries {
                term,
                peer,
                prev_log_index,
                prev_log_term,
                leader_commit,
                leader_id,
                entries,
            } => {
                let log_entries: Vec<Entry> = entries
                    .into_iter()
                    .map(|entry| Entry {
                        term: entry.term,
                        command: entry.command,
                    })
                    .collect();
                let message = AppendEntriesMessage {
                    term,
                    prev_log_index,
                    prev_log_term,
                    leader_commit,
                    leader_id,
                    entries: log_entries,
                };
                let entries_count = message.entries.len() as u64;
                let request = Request::new(message);
                tokio::spawn(send_append_entries(
                    peer,
                    request,
                    entries_count,
                    raft_inbox.clone(),
                ));
            }
        }
    }
}

async fn connect_to_peer(peer: &str) -> Option<RaftClient<Channel>> {
    let Ok(connect_result) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        RaftClient::connect(format!("http://{}", peer)),
    )
    .await
    else {
        return None;
    };

    connect_result.ok()
}

async fn send_request_vote(
    peer: String,
    request: Request<RequestVoteMessage>,
    raft_inbox: Sender<RaftMsg>,
) {
    let Some(mut peer_client) = connect_to_peer(&peer).await else {
        eprintln!("Failed to connect to peer {}", peer);
        return;
    };

    let vote_reply = peer_client
        .request_vote(request)
        .await
        .expect("Failed to send request vote");
    let vote_reply = vote_reply.into_inner();
    let vote_reply_message = RaftMsg::RequestVoteReply {
        vote_reply: RequestVoteReplyData {
            term: vote_reply.term,
            vote: vote_reply.vote,
        },
        reply_channel: None,
    };

    raft_inbox
        .send(vote_reply_message)
        .await
        .expect("Failed to send message to raft");
}

async fn send_append_entries(
    peer: String,
    request: Request<AppendEntriesMessage>,
    entries_count: u64,
    raft_inbox: Sender<RaftMsg>,
) {
    let Some(mut peer_client) = connect_to_peer(&peer).await else {
        return;
    };

    let Ok(reply) = peer_client.append_entries(request).await else {
        return;
    };

    let reply_inner = reply.into_inner();
    let append = RaftMsg::AppendEntriesReply {
        append_reply: AppendEntriesReplyData {
            term: reply_inner.term,
            success: reply_inner.success,
            peer,
            entries_count,
        },
        reply_channel: None,
    };

    raft_inbox
        .send(append)
        .await
        .expect("Failed to send new leader message");
}
