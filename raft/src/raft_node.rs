use crate::network_sender::OutMsg;
use rand::Rng;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum State {
    Candidate {
        votes: usize,
    },
    Leader,
    #[default]
    Follower,
}

#[derive(Debug)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: String,
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
pub struct Entry {
    pub term: u64,
    pub command: String,
}

#[derive(Debug)]
pub struct Node {
    current_term: usize,
    state: State,
    peers: Vec<String>,
    voted_for: Option<String>,
    entries: Vec<Entry>,
    network_inbox: Sender<OutMsg>,
    candidate_id: String,
    commit_index: u64,
}

impl Node {
    pub fn new(peers: Vec<String>, network_inbox: Sender<OutMsg>, candidate_id: String) -> Self {
        Node {
            current_term: 0,
            state: State::default(),
            peers,
            voted_for: None,
            network_inbox,
            entries: vec![],
            candidate_id,
            commit_index: 0,
        }
    }

    pub fn get_state(&self) -> &State {
        &self.state
    }

    pub fn get_current_term(&self) -> usize {
        self.current_term
    }

    pub fn has_voted(&self) -> bool {
        self.voted_for.is_some()
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
                leader_id: self.candidate_id.clone(),
                entries: vec![],
                prev_log_index: self.entries.len() as u64,
                prev_log_term: if self.entries.is_empty() {
                    0
                } else {
                    self.entries[self.entries.len() - 1].term
                },
                leader_commit: self.commit_index,
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
        self.voted_for = Some(self.candidate_id.clone());
        eprintln!(
            "Current node state: {:?}, term: {}",
            self.state, self.current_term
        );

        let peers = self.peers.clone();
        let last_log_index = self.entries.len() as u64;
        let last_log_term = if self.entries.is_empty() {
            0
        } else {
            self.entries[last_log_index as usize - 1].term
        };

        for peer in peers {
            eprintln!("Sending requestVote to {}", peer);
            let out_msg = OutMsg::RequestVote {
                term: self.current_term as u64,
                peer: peer.clone(),
                last_log_index: last_log_index as u64,
                last_log_term,
                candidate: self.candidate_id.clone(),
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
            self.voted_for.take();
            self.state = State::Follower;
        }

        let voted_for_candidate = match &self.voted_for {
            Some(candidate) => candidate == &vote_request.candidate,
            None => false,
        };
        if (self.voted_for.is_some() && !voted_for_candidate)
            || self.current_term > vote_request.term as usize
            || (self.entries.last().map_or(0, |e| e.term) == vote_request.last_log_term
                && self.entries.len() > vote_request.last_log_index as usize)
            || self.entries.last().map_or(0, |e| e.term) > vote_request.last_log_term
        {
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

        self.voted_for = Some(vote_request.candidate.clone());
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
            self.state = State::Follower;
        }

        // Reset voted_for on receiving heartbeat
        if self.current_term < append_request.term as usize {
            eprintln!("Received append entries, stepping down to follower");
            self.current_term = append_request.term as usize;
            self.voted_for = None;
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
            self.voted_for = None;
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
            self.voted_for = None;
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

#[cfg(test)]
mod raft_node_tests {
    use super::*;

    #[tokio::test]
    async fn test_handle_vote_request() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        let vote_request = RequestVoteData {
            term: 1,
            last_log_index: 0,
            last_log_term: 0,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(vote_reply.vote);
        assert_eq!(node.get_current_term(), 1);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_vote_request_already_voted() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.voted_for = Some(String::from("node2"));
        let vote_request = RequestVoteData {
            term: 0,
            last_log_index: 0,
            last_log_term: 0,
            candidate: String::from("node3"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(!vote_reply.vote);
        assert_eq!(node.get_current_term(), 0);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_not_up_to_date() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(Entry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 0,
            last_log_term: 0,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(vote_reply.vote == false);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_index_up_to_date() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(Entry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 2,
            last_log_term: 1,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(vote_reply.vote);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_vote_request_voted_for_same_candidate() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.voted_for = Some(String::from("node2"));
        let vote_request = RequestVoteData {
            term: 1,
            last_log_index: 0,
            last_log_term: 0,
            candidate: String::from("node2"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(vote_reply.vote);
        assert_eq!(node.get_current_term(), 1);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_vote_request_higher_log_index_not_up_to_date() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(Entry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(Entry {
            term: 1,
            command: "cmd2".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 1,
            last_log_term: 1,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(vote_reply.vote == false);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_mismatch() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(Entry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 1,
            last_log_term: 0,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(vote_reply.vote == false);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_vote_request_stale_term() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        let vote_request = RequestVoteData {
            term: 1,
            last_log_index: 0,
            last_log_term: 0,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(!vote_reply.vote);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_request_vote_reply_become_leader() {
        let peers = vec!["node1".to_string(), "node2".to_string()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };
        let vote_reply = RequestVoteReplyData {
            term: 1,
            vote: true,
        };

        node.handle_request_vote_reply(vote_reply).await;
        assert_eq!(*node.get_state(), State::Leader);
        assert_eq!(node.get_current_term(), 1);
    }

    #[tokio::test]
    async fn test_handle_vote_request_candidate_has_higher_term() {
        let peers = vec![];
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(Entry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(Entry {
            term: 1,
            command: "cmd2".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 3,
            last_log_index: 1,
            last_log_term: 2,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await;
        assert!(vote_reply.vote);
        assert_eq!(node.get_current_term(), 3);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_append_entries() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 1;
        node.state = State::Leader;
        let append_request = AppendEntriesData {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await;
        assert!(reply.success);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_term() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 1;
        node.state = State::Leader;
        let append_request = AppendEntriesData {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await;
        assert!(reply.success);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_request() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Leader;
        let append_request = AppendEntriesData {
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await;
        assert!(!reply.success);
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Leader);
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_step_down() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 1;
        node.state = State::Leader;
        let append_reply = AppendEntriesReplyData {
            term: 2,
            success: false,
        };

        node.handle_append_entries_reply(append_reply).await;
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Follower);
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_no_step_down() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Leader;
        let append_reply = AppendEntriesReplyData {
            term: 1,
            success: false,
        };

        node.handle_append_entries_reply(append_reply).await;
        assert_eq!(node.get_current_term(), 2);
        assert_eq!(*node.get_state(), State::Leader);
    }
}
