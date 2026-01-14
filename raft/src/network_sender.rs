use crate::raft::raft_client::RaftClient;
use crate::raft::{AppendEntriesMessage, RequestVoteMessage};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;

use crate::raft_node::{AppendEntriesReplyData, RaftMsg, RequestVoteReplyData};
pub enum OutMsg {
    RequestVote { term: u64, peer: String, last_log_index: u64, last_log_term: u64 },
    AppendEntries { term: u64, peer: String },
}

pub async fn network_worker(mut outbox: Receiver<OutMsg>, raft_inbox: Sender<RaftMsg>) {
    while let Some(msg) = outbox.recv().await {
        match msg {
            OutMsg::RequestVote { term, peer, last_log_index, last_log_term } => {
                let vote_request = RequestVoteMessage { 
                    term,
                    last_log_index,
                    last_log_term,
                };
                let request = Request::new(vote_request);
                let raft_inbox_clone = raft_inbox.clone();
                tokio::spawn(async move {
                    let timeout_duration = std::time::Duration::from_millis(100);
                    let Ok(timeout_result) = tokio::time::timeout(
                        timeout_duration,
                        RaftClient::connect(format!("http://{}", peer)),
                    ).await else {
                        //eprintln!("Timeout connecting to peer {}", peer);
                        return;
                    };

                    let Ok(mut peer_client) = timeout_result else {
                        //eprintln!("Failed to connect to peer {}", peer);
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
            OutMsg::AppendEntries { term, peer } => {
                let append_entries_request = Request::new(AppendEntriesMessage { term });
                let raft_inbox_clone = raft_inbox.clone();
                tokio::spawn(async move {
                    let timeout_duration = std::time::Duration::from_millis(100);
                    let Ok(timeout_result) = tokio::time::timeout(
                        timeout_duration,
                        RaftClient::connect(format!("http://{}", peer)),
                    ).await else {
                        //eprintln!("Timeout connecting to peer {}", peer);
                        return;
                    };

                    let Ok(mut peer_client) = timeout_result else {
                        //eprintln!("Failed to connect to peer {}", peer);
                        return;
                    };

                    let Ok(reply) = peer_client.append_entries(append_entries_request).await else {
                        eprintln!("Failed to recieve reply from peer {}", peer);
                        return;
                    };

                    let reply_inner = reply.into_inner();
                    let append_entries_reply_data = AppendEntriesReplyData {
                        term: reply_inner.term,
                        success: reply_inner.success,
                    };

                    let eppend = RaftMsg::AppendEntriesReply {
                        append_reply: append_entries_reply_data,
                        reply_channel: None,
                    };
                    raft_inbox_clone
                        .send(eppend)
                        .await
                        .expect("Failed to send new leader message");

                });
            }
        }
    }
}
