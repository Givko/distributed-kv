use crate::network_sender::OutMsg;
use rand::Rng;
use std::{collections::HashMap, time::Duration};
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
}

#[derive(Debug)]
pub struct Node {
    current_term: u64,
    state: State,
    peers: Vec<String>,
    voted_for: Option<String>,
    entries: Vec<LogEntry>,
    network_inbox: Sender<OutMsg>,
    id: String,
    commit_index: u64,

    last_applied: u64,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,

    state_machine: HashMap<String, String>,
    pending_clients: HashMap<u64, tokio::sync::oneshot::Sender<ChangeStateReply>>,
}

impl Node {
    pub fn new(peers: Vec<String>, network_inbox: Sender<OutMsg>, candidate_id: String) -> Self {
        let mut next_index_map = HashMap::new();
        let mut match_index_map = HashMap::new();
        for peer in peers.clone() {
            next_index_map.insert(peer.clone(), 0);
            match_index_map.insert(peer, 0);
        }
        Node {
            current_term: 0,
            state: State::default(),
            peers,
            voted_for: None,
            network_inbox,
            entries: vec![],
            id: candidate_id,
            commit_index: 0,
            last_applied: 0,
            next_index: next_index_map,
            match_index: match_index_map,
            state_machine: HashMap::new(),
            pending_clients: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut inbox: Receiver<RaftMsg>) -> anyhow::Result<()> {
        loop {
            let mut duration: u64 = rand::rng().random_range(150..300);
            if self.state == State::Leader {
                duration = 50;
            }

            tokio::select! {
                option = inbox.recv() => {
                    let Some(msg) = option else {return Ok(()) }; // TODO: gracefull shutdown
                    self.handle_message(msg).await?;
                },
                _ = tokio::time::sleep(Duration::from_millis(duration)) => {
                    if self.state == State::Leader {
                        self.send_heartbeat().await?;
                        continue;
                    }

                    self.start_election().await?;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: RaftMsg) -> anyhow::Result<()> {
        match msg {
            RaftMsg::VoteRequest {
                vote_request,
                reply_channel,
            } => {
                eprintln!("Got requestVote in node");
                let vote_reply = self.handle_vote_request(vote_request).await?;
                self.send_to_reply_channel(reply_channel, vote_reply)?;
            }
            RaftMsg::AppendEntries {
                append_request,
                reply_channel,
            } => {
                let reply = self.handle_append_entries(append_request).await?;
                self.send_to_reply_channel(reply_channel, reply)?;
            }
            RaftMsg::AppendEntriesReply {
                append_reply,
                reply_channel,
            } => {
                self.handle_append_entries_reply(append_reply).await?;
                self.send_to_reply_channel(reply_channel, ())?;
            }
            RaftMsg::RequestVoteReply {
                vote_reply,
                reply_channel,
            } => {
                eprintln!("Received vote reply: {:?}", vote_reply);
                self.handle_request_vote_reply(vote_reply).await?;
                self.send_to_reply_channel(reply_channel, ())?;
            }
            RaftMsg::ChangeState {
                command,
                reply_channel,
            } => {
                if !matches!(self.state, State::Leader) {
                    let reply = ChangeStateReply { success: false };
                    self.send_to_reply_channel(reply_channel, reply)?;
                } else {
                    for peer in &self.peers {
                        let append_entries = OutMsg::AppendEntries {
                            term: self.current_term,
                            peer: peer.clone(),
                            prev_log_index: self.entries.len() as u64,
                            prev_log_term: self.entries.last().map_or(0, |e| e.term),
                            leader_commit: self.commit_index,
                            leader_id: self.id.clone(),
                            entries: vec![crate::network_sender::LogEntry {
                                term: self.current_term,
                                command: command.clone(),
                            }],
                        };
                        eprintln!("replicating to {}", peer.clone());
                        self.network_inbox.send(append_entries).await?;
                    }
                    self.entries.push(LogEntry {
                        term: self.current_term,
                        command,
                    });
                    let log_index = self.entries.len() as u64;
                    if let Some(reply_channel) = reply_channel {
                        self.pending_clients.insert(log_index, reply_channel);
                    }
                }
            }
        }

        self.apply_commands().await?;
        Ok(())
    }

    async fn apply_commands(&mut self) -> anyhow::Result<()> {
        if self.commit_index <= self.last_applied {
            return Ok(());
        }

        for i in self.last_applied + 1..=self.commit_index {
            let args: Vec<&str> = self.entries[i as usize - 1]
                .command
                .split_whitespace()
                .collect();
            let command = args[0];
            let key = args[1];

            if command.to_lowercase() == "set" {
                let val = args[2];
                self.state_machine.insert(key.to_owned(), val.to_owned());
            }

            self.last_applied = i;
            let reply_channel = self.pending_clients.remove(&i);
            eprintln!("replying to {}", i);
            let reply = ChangeStateReply { success: true };
            self.send_to_reply_channel(reply_channel, reply)?;
        }

        Ok(())
    }

    async fn send_heartbeat(&self) -> anyhow::Result<()> {
        // Placeholder for heartbeat logic
        for peer in &self.peers {
            let prev_log_index = *(self.next_index.get(peer).expect("cant get next_index"));
            // If we're the leader, we don't need to run the election timer
            let out_msg = OutMsg::AppendEntries {
                term: self.current_term as u64,
                leader_id: self.id.clone(),
                entries: vec![],
                prev_log_index,
                prev_log_term: if self.entries.is_empty() {
                    0
                } else {
                    self.entries[self.entries.len() - 1].term
                },
                leader_commit: self.commit_index,
                peer: peer.clone(),
            };

            self.network_inbox.send(out_msg).await?;
        }
        Ok(())
    }

    async fn start_election(&mut self) -> anyhow::Result<()> {
        eprintln!("Election timeout, starting election");
        self.current_term += 1;
        self.state = State::Candidate { votes: 1 }; // vote for self
        self.voted_for = Some(self.id.clone());
        eprintln!(
            "Current node state: {:?}, term: {}",
            self.state, self.current_term
        );

        let peers = &self.peers;
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
                candidate: self.id.clone(),
            };
            self.network_inbox.send(out_msg).await?;
        }
        Ok(())
    }

    async fn handle_vote_request(
        &mut self,
        vote_request: RequestVoteData,
    ) -> anyhow::Result<RequestVoteReplyData> {
        if self.current_term < vote_request.term {
            eprintln!("Stepping down as follower due to higher term in vote request");
            self.current_term = vote_request.term;
            self.voted_for.take();
            self.state = State::Follower;
        }

        let voted_for_candidate = match &self.voted_for {
            Some(candidate) => candidate == &vote_request.candidate,
            None => false,
        };
        if (self.voted_for.is_some() && !voted_for_candidate)
            || self.current_term > vote_request.term
            || (self.entries.last().map_or(0, |e| e.term) == vote_request.last_log_term
                && self.entries.len() > vote_request.last_log_index as usize)
            || self.entries.last().map_or(0, |e| e.term) > vote_request.last_log_term
        {
            let vote = RequestVoteReplyData {
                term: self.current_term as u64,
                vote: false,
            };

            return Ok(vote);
        }

        let vote_reply = RequestVoteReplyData {
            term: self.current_term as u64,
            vote: true,
        };

        self.voted_for = Some(vote_request.candidate.clone());
        self.state = State::Follower;

        Ok(vote_reply)
    }

    async fn handle_append_entries(
        &mut self,
        append_request: AppendEntriesData,
    ) -> anyhow::Result<AppendEntriesReplyData> {
        if self.current_term > append_request.term
            || self.entries.len() < append_request.prev_log_index as usize
            || (append_request.prev_log_index != 0
                && self
                    .entries
                    .get(append_request.prev_log_index as usize - 1)
                    .map_or(0, |e| e.term)
                    != append_request.prev_log_term)
        {
            self.state = if self.current_term < append_request.term {
                State::Follower
            } else {
                self.state
            };
            self.current_term = std::cmp::max(self.current_term, append_request.term);

            let reply = AppendEntriesReplyData {
                term: self.current_term as u64,
                success: false,

                peer: self.id.clone(),
                entries_count: 0,
            };

            return Ok(reply);
        }

        // Reset voted_for on receiving heartbeat
        if self.current_term <= append_request.term {
            self.current_term = append_request.term;
            self.voted_for = None;
        }

        if self.state != State::Follower {
            eprintln!("Received append entries, stepping down to follower");
            self.state = State::Follower;
        }

        // Remove conflicting entries
        if append_request.prev_log_index != 0
            && self.entries.len() > append_request.prev_log_index as usize
        {
            self.entries
                .truncate(append_request.prev_log_index as usize);
        }

        let entries_count = append_request.entries.len();
        if entries_count > 0 {
            eprintln!("commiting new entries {}", entries_count);
        }
        // Append new entries
        self.entries.extend(append_request.entries);

        // Update commit index
        if append_request.leader_commit > self.commit_index {
            self.commit_index =
                std::cmp::min(self.entries.len() as u64, append_request.leader_commit);
        }

        let reply = AppendEntriesReplyData {
            term: self.current_term as u64,
            success: true,

            peer: self.id.clone(),
            entries_count: entries_count as u64,
        };

        return Ok(reply);
    }

    async fn handle_request_vote_reply(
        &mut self,
        vote_reply: RequestVoteReplyData,
    ) -> anyhow::Result<()> {
        if vote_reply.term > self.current_term {
            self.current_term = vote_reply.term;
            self.state = State::Follower;
            self.voted_for = None;
            eprintln!("Stepping down to follower due to higher term in vote reply");
            return Ok(());
        }
        // Ignore stale votes from old terms
        if vote_reply.term != self.current_term {
            eprintln!("Ignoring stale vote from term {}", vote_reply.term);
            return Ok(());
        }

        let mut votes = match self.state {
            State::Candidate { votes } => votes,
            _ => {
                eprintln!("Received vote reply while not a candidate, ignoring");
                return Ok(());
            }
        };

        votes += if vote_reply.vote { 1 } else { 0 };
        self.state = State::Candidate { votes };

        eprintln!("Total votes received: {}", votes);
        if !self.is_majority(votes) {
            eprintln!("Did not receive majority votes, remaining candidate");
            return Ok(());
        }

        eprintln!(
            "Received majority votes, becoming leader with term {}",
            self.current_term
        );

        let next_index = self.entries.len() + 1;
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), next_index as u64);
            self.match_index.insert(peer.clone(), 0);
        }

        self.state = State::Leader;
        return Ok(());
    }

    async fn handle_append_entries_reply(
        &mut self,
        append_entries_reply_data: AppendEntriesReplyData,
    ) -> anyhow::Result<()> {
        if append_entries_reply_data.success {
            let prev_log_index = *self
                .next_index
                .get(&append_entries_reply_data.peer)
                .expect("no peer found")
                - 1;

            let match_index = prev_log_index + append_entries_reply_data.entries_count;
            let next_index = match_index + 1;
            self.next_index
                .insert(append_entries_reply_data.peer.clone(), next_index);
            self.match_index
                .insert(append_entries_reply_data.peer.clone(), match_index);

            for log_index in self.commit_index + 1..=self.entries.len() as u64 {
                let mut count = 1; // self
                for (_, value) in self.match_index.iter() {
                    if *value >= log_index as u64 {
                        count += 1;
                    }
                }

                if self.is_majority(count)
                    && self.entries[log_index as usize - 1].term == self.current_term
                {
                    self.commit_index = log_index as u64
                }
            }

            return Ok(());
        }

        if self.current_term < append_entries_reply_data.term {
            self.current_term = append_entries_reply_data.term;
            self.state = State::Follower;
            eprint!(
                "Stepping down to follower due to new leader with higher term {}",
                append_entries_reply_data.term
            );
            self.voted_for = None;

            return Ok(());
        }

        let mut next_index = *self
            .next_index
            .get(&append_entries_reply_data.peer)
            .expect("no peer found in state");

        if next_index <= 1 {
            return Ok(());
        }

        next_index = next_index - 1;
        let prev_log_index = next_index - 1;
        let mut prev_log_term = 0;
        if prev_log_term > 0 {
            prev_log_term = self
                .entries
                .get(prev_log_index as usize - 1)
                .map_or(0, |e| e.term);
        }

        let entries_to_send = self.entries[prev_log_index as usize..]
            .iter()
            .map(|e| crate::network_sender::LogEntry {
                term: e.term,
                command: e.command.clone(),
            })
            .collect();

        _ = self
            .next_index
            .insert(append_entries_reply_data.peer.clone(), next_index);

        let append_entries = OutMsg::AppendEntries {
            term: self.current_term as u64,
            peer: append_entries_reply_data.peer,
            prev_log_index,
            prev_log_term,
            leader_commit: self.commit_index,
            leader_id: self.id.clone(),
            entries: entries_to_send,
        };

        self.network_inbox.send(append_entries).await?;
        Ok(())
    }

    fn is_majority(&self, count: usize) -> bool {
        count > (self.peers.len() + 1) / 2
    }

    fn send_to_reply_channel<T>(
        &self,
        reply_channel: Option<tokio::sync::oneshot::Sender<T>>,
        reply: T,
    ) -> anyhow::Result<()> {
        let Some(reply_channel) = reply_channel else {
            return Ok(());
        };

        reply_channel
            .send(reply)
            .map_err(|_| anyhow::anyhow!("reply receiver dropped"))
    }
}

#[cfg(test)]
mod raft_node_tests {
    use super::*;

    #[tokio::test]
    async fn test_handle_vote_request() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        let vote_request = RequestVoteData {
            term: 1,
            last_log_index: 0,
            last_log_term: 0,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(vote_reply.vote);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_already_voted() -> anyhow::Result<()> {
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

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(!vote_reply.vote);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_not_up_to_date() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 0,
            last_log_term: 0,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(vote_reply.vote == false);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_index_up_to_date() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 2,
            last_log_term: 1,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(vote_reply.vote);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_voted_for_same_candidate() -> anyhow::Result<()> {
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

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(vote_reply.vote);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_higher_log_index_not_up_to_date() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd2".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 1,
            last_log_term: 1,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(vote_reply.vote == false);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_mismatch() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 2,
            last_log_index: 1,
            last_log_term: 0,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(vote_reply.vote == false);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_stale_term() -> anyhow::Result<()> {
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

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(!vote_reply.vote);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_request_vote_reply_become_leader() -> anyhow::Result<()> {
        let peers = vec!["node1".to_string(), "node2".to_string()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };
        let vote_reply = RequestVoteReplyData {
            term: 1,
            vote: true,
        };

        node.handle_request_vote_reply(vote_reply).await?;
        assert_eq!(node.state, State::Leader);
        assert_eq!(node.current_term, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_candidate_has_higher_term() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd2".to_string(),
        });
        let vote_request = RequestVoteData {
            term: 3,
            last_log_index: 1,
            last_log_term: 2,
            candidate: String::from("node1"),
        };

        let vote_reply = node.handle_vote_request(vote_request).await?;
        assert!(vote_reply.vote);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_reply_no_majority() -> anyhow::Result<()> {
        let peers = vec!["node1".to_string(), "node2".to_string()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };
        let vote_reply = RequestVoteReplyData {
            term: 1,
            vote: false,
        };

        node.handle_request_vote_reply(vote_reply).await?;
        assert_eq!(node.state, State::Candidate { votes: 1 });
        assert_eq!(node.current_term, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries() -> anyhow::Result<()> {
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

        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_term() -> anyhow::Result<()> {
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

        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_stale_request() -> anyhow::Result<()> {
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

        let reply = node.handle_append_entries(append_request).await?;
        assert!(!reply.success);
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Leader);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_prev_log_index_high() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Leader;
        let append_request = AppendEntriesData {
            term: 3,
            prev_log_index: 1,
            prev_log_term: 0,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await?;
        assert!(!reply.success);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_prev_log_term_mismatch() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Leader;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let append_request = AppendEntriesData {
            term: 3,
            prev_log_index: 1,
            prev_log_term: 0,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await?;
        assert!(!reply.success);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_successful_append() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let append_request = AppendEntriesData {
            term: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![LogEntry {
                term: 2,
                command: "cmd2".to_string(),
            }],
        };
        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[1].term, 2);
        assert_eq!(node.entries[1].command, "cmd2".to_string());
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        assert_eq!(node.commit_index, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_conflicting_entries() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        let append_request = AppendEntriesData {
            term: 3,
            prev_log_index: 1,
            prev_log_term: 1,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![LogEntry {
                term: 3,
                command: "cmd3".to_string(),
            }],
        };
        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2, "Conflicting entry was not removed");
        assert_eq!(node.entries[1].term, 3, "New entry was not appended");
        assert_eq!(
            node.entries[1].command,
            "cmd3".to_string(),
            "New entry command mismatch"
        );
        assert_eq!(node.current_term, 3, "Term was not updated");
        assert_eq!(node.state, State::Follower, "State should be follower");
        assert_eq!(node.commit_index, 0, "Commit index should remain unchanged");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_heartbeat_update_commit_index_with_leader()
    -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        let append_request = AppendEntriesData {
            term: 3,
            prev_log_index: 2,
            prev_log_term: 2,
            leader_commit: 2,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2, "Entries are changed");
        assert_eq!(node.state, State::Follower, "State should be follower");
        assert_eq!(node.commit_index, 2, "Commit index should be updated");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_update_commit_with_entries_length() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        let append_request = AppendEntriesData {
            term: 3,
            prev_log_index: 2,
            prev_log_term: 2,
            leader_commit: 4,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 2, "Entries are changed");
        assert_eq!(node.state, State::Follower, "State should be follower");

        assert_eq!(node.commit_index, 2, "Commit index should be updated");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_update_commit_with_new_entries_length() -> anyhow::Result<()>
    {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Follower;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        let append_request = AppendEntriesData {
            term: 3,
            prev_log_index: 2,
            prev_log_term: 2,
            leader_commit: 4,
            leader_id: String::from("node2"),
            entries: vec![LogEntry {
                term: 3,
                command: "cmd3".to_string(),
            }],
        };

        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.entries.len(), 3, "Entries are changed");
        assert_eq!(node.entries[2].term, 3);
        assert_eq!(node.entries[2].command, "cmd3".to_string());
        assert_eq!(node.state, State::Follower, "State should be follower");

        assert_eq!(node.commit_index, 3, "Commit index should be updated");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_step_down() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 1;
        node.state = State::Leader;
        let append_reply = AppendEntriesReplyData {
            term: 2,
            success: false,
            peer: "test".to_owned(),
            entries_count: 0,
        };

        node.handle_append_entries_reply(append_reply).await?;
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_no_step_down() -> anyhow::Result<()> {
        let peers = vec!["test".to_owned()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("node1"));
        node.current_term = 2;
        node.state = State::Leader;
        node.next_index.insert("test".to_owned(), 1);
        let append_reply = AppendEntriesReplyData {
            term: 1,
            success: false,

            peer: "test".to_owned(),
            entries_count: 0,
        };

        node.handle_append_entries_reply(append_reply).await?;
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Leader);
        assert_eq!(*node.next_index.get("test").expect("no peer found"), 1);
        Ok(())
    }
}
