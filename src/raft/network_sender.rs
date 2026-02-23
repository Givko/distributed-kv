use crate::raft::proto::raft_client::RaftClient;
use crate::raft::proto::{AppendEntriesMessage, Entry, RequestVoteMessage};
use super::network_types::OutMsg;
use super::raft_types::{AppendEntriesReplyData, RaftMsg, RequestVoteReplyData};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;

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
                let raft_inbox_clone = raft_inbox.clone();
                tokio::spawn(async move {
                    let timeout_duration = std::time::Duration::from_millis(100);
                    let Ok(timeout_result) = tokio::time::timeout(
                        timeout_duration,
                        RaftClient::connect(format!("http://{}", peer)),
                    )
                    .await
                    else {
                        eprintln!("Timeout connecting to peer {}", peer);
                        return;
                    };

                    let Ok(mut peer_client) = timeout_result else {
                        eprintln!("Failed to connect to peer {}", peer);
                        return;
                    };

                    let vote_reply = peer_client
                        .request_vote(request)
                        .await
                        .expect("Failed to send request vote");
                    let vote_reply = vote_reply.into_inner();
                    let vote_reply_inner = RequestVoteReplyData {
                        term: vote_reply.term,
                        vote: vote_reply.vote,
                    };
                    let vote_reply_message = RaftMsg::RequestVoteReply {
                        vote_reply: vote_reply_inner,
                        reply_channel: None,
                    };

                    raft_inbox_clone
                        .send(vote_reply_message)
                        .await
                        .expect("Failed to send message to raft");
                });
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
                let entries_count = message.entries.len();
                let append_entries_request = Request::new(message);
                let raft_inbox_clone = raft_inbox.clone();
                tokio::spawn(async move {
                    let timeout_duration = std::time::Duration::from_millis(100);
                    let Ok(timeout_result) = tokio::time::timeout(
                        timeout_duration,
                        RaftClient::connect(format!("http://{}", peer)),
                    )
                    .await
                    else {
                        return;
                    };

                    let Ok(mut peer_client) = timeout_result else {
                        return;
                    };

                    let Ok(reply) = peer_client.append_entries(append_entries_request).await else {
                        return;
                    };

                    let reply_inner = reply.into_inner();
                    let append_entries_reply_data = AppendEntriesReplyData {
                        term: reply_inner.term,
                        success: reply_inner.success,
                        peer: peer.clone(),
                        entries_count: entries_count as u64,
                    };
                    let append = RaftMsg::AppendEntriesReply {
                        append_reply: append_entries_reply_data,
                        reply_channel: None,
                    };
                    raft_inbox_clone
                        .send(append)
                        .await
                        .expect("Failed to send new leader message");
                });
            }
        }
    }
}
