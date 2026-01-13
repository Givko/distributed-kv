use std::time::Duration;
use rand::Rng;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::network_sender::OutMsg;

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
enum State {
    Candidate {
        votes: usize,
    },
    Leader,
    #[default]
    Follower,
}

#[derive(Debug)]
pub struct RequestVoteData {
    pub term: u64,
}

#[derive(Debug)]
pub struct AppendEntriesData {
    pub term: u64,
}

#[derive(Debug)]
pub struct AppendEntriesReplyData {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug)]
pub struct RequestVoteReplyData {
    pub term: u64,
    pub vote: bool,
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
}

#[derive(Debug)]
pub struct Node {
    current_term: usize,
    state: State,
    peers: Vec<String>,
    voted_for: bool,
    network_inbox: Sender<OutMsg>,
}

impl Node {
    pub fn new(peers: Vec<String>, network_inbox: Sender<OutMsg>) -> Self {
        Node {
            current_term: 0,
            state: State::default(),
            peers,
            voted_for: false,
            network_inbox,
        }
    }

    pub async fn run(mut self, mut inbox: Receiver<RaftMsg>) {
        loop {
            let mut duration: u64 = rand::rng().random_range(150..300);
            if self.state == State::Leader {
                duration = 50;
            }

            tokio::select! {
                Some(msg) = inbox.recv() => {
                    self.handle_message(msg).await;
                }
                _ = tokio::time::sleep(Duration::from_millis(duration)) => {
                    if self.state == State::Leader {
                        self.send_heartbeat().await;
                        continue;
                    }

                    self.start_election().await;
                }
            }
        }
    }

    async fn send_heartbeat(&self) {
        // Placeholder for heartbeat logic
        for peer in self.peers.clone() {
            // If we're the leader, we don't need to run the election timer
            let out_msg = OutMsg::AppendEntries {
                term: self.current_term as u64,
                peer: peer.clone(),
            };

            self.network_inbox
                .send(out_msg)
                .await
                .expect(format!("Failed to send heartbeat to {}", peer).as_str());
        }
    }

    async fn start_election(&mut self) {
        eprintln!("Election timeout, starting election");
        self.current_term += 1;
        self.state = State::Candidate { votes: 1 }; // vote for self
        self.voted_for = true;

        eprintln!("Current node state: {:?}, term: {}", self.state, self.current_term);
        let peers = self.peers.clone();
        for peer in peers {
            eprintln!("Sending requestVote to {}", peer);
            let out_msg = OutMsg::RequestVote {
                term: self.current_term as u64,
                peer: peer.clone(),
            };
            self.network_inbox
                .send(out_msg)
                .await
                .expect(format!("Failed to send request vote to {}", peer).as_str());
        }
    }

    async fn handle_vote_request(&mut self, vote_request: RequestVoteData) -> RequestVoteReplyData {
        if self.current_term < vote_request.term as usize {
            eprintln!("Stepping down as follower due to higher term in vote request");
            self.current_term = vote_request.term as usize;
            self.voted_for = false;
            self.state = State::Follower;
        }

        if self.voted_for || self.current_term > vote_request.term as usize {
            let vote = RequestVoteReplyData {
                term: self.current_term as u64,
                vote: false,
            };

            return vote;
        }

        let vote_reply = RequestVoteReplyData {
            term: self.current_term as u64,
            vote: true,
        };

        self.current_term = vote_request.term as usize;
        self.voted_for = true;
        self.state = State::Follower;

        vote_reply
    }

    async fn handle_append_entries(
        &mut self,
        append_request: AppendEntriesData,
    ) -> AppendEntriesReplyData {
        if self.current_term > append_request.term as usize {
            let reply = AppendEntriesReplyData {
                term: self.current_term as u64,
                success: false,
            };

            return reply;
        }

        if self.state != State::Follower {
            eprintln!("Received append entries, stepping down to follower");
            self.state = State::Follower;
        }

        // Reset voted_for on receiving heartbeat
        if self.current_term < append_request.term as usize {
            self.current_term = append_request.term as usize;
            self.voted_for = false;
        }

        let reply = AppendEntriesReplyData {
            term: self.current_term as u64,
            success: true,
        };

        return reply;
    }

    async fn handle_request_vote_reply(&mut self, vote_reply: RequestVoteReplyData) {
        if vote_reply.term as usize > self.current_term {
            self.current_term = vote_reply.term as usize;
            self.state = State::Follower;
            self.voted_for = false;
            eprintln!("Stepping down to follower due to higher term in vote reply");
            return;
        }
        // Ignore stale votes from old terms
        if vote_reply.term as usize != self.current_term {
            eprintln!("Ignoring stale vote from term {}", vote_reply.term);
            return;
        }

        let mut votes = match self.state {
            State::Candidate { votes } => votes,
            _ => {
                eprintln!("Received vote reply while not a candidate, ignoring");
                return;
            }
        };

        votes += if vote_reply.vote { 1 } else { 0 };
        self.state = State::Candidate { votes };

        eprintln!("Total votes received: {}", votes);
        if votes <= (self.peers.len() + 1) / 2 {
            eprintln!("Did not receive majority votes, remaining candidate");
            return;
        }

        eprintln!(
            "Received majority votes, becoming leader with term {}",
            self.current_term
        );
        self.state = State::Leader;
    }

    async fn handle_append_entries_reply(
        &mut self,
        append_entries_reply_data: AppendEntriesReplyData,
    ) {
        if append_entries_reply_data.success {
            return;
        }

        if self.current_term < append_entries_reply_data.term as usize {
            self.current_term = append_entries_reply_data.term as usize;
            self.state = State::Follower;
            eprint!(
                "Stepping down to follower due to new leader with higher term {}",
                append_entries_reply_data.term
            );
            self.voted_for = false;
        }
    }

    async fn handle_message(&mut self, msg: RaftMsg) {
        match msg {
            RaftMsg::VoteRequest {
                vote_request,
                reply_channel,
            } => {
                eprintln!("Got requestVote in node");
                let vote_reply = self.handle_vote_request(vote_request).await;
                if let Some(reply_channel) = reply_channel {
                    reply_channel
                        .send(vote_reply)
                        .expect("Failed to send vote reply");
                }
            }
            RaftMsg::AppendEntries {
                append_request,
                reply_channel,
            } => {
                let reply = self.handle_append_entries(append_request).await;
                if let Some(reply_channel) = reply_channel {
                    reply_channel
                        .send(reply)
                        .expect("Failed to send append entries reply");
                }
            }
            RaftMsg::AppendEntriesReply {
                append_reply,
                reply_channel,
            } => {
                self.handle_append_entries_reply(append_reply).await;
                if let Some(reply_channel) = reply_channel {
                    reply_channel
                        .send(())
                        .expect("Failed to send ack for append entries reply");
                }
            }
            RaftMsg::RequestVoteReply {
                vote_reply,
                reply_channel,
            } => {
                eprintln!("Received vote reply: {:?}", vote_reply);
                self.handle_request_vote_reply(vote_reply).await;
                if let Some(reply_channel) = reply_channel {
                    reply_channel
                        .send(())
                        .expect("Failed to send ack for vote reply");
                }
            }
        }
    }
}

