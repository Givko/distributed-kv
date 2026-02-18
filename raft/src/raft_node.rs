use crate::network_types::OutMsg;
use crate::raft_types::LogEntry;
pub use crate::raft_types::{
    AppendEntriesData, AppendEntriesReplyData, ChangeStateReply, RaftMsg, RequestVoteData,
    RequestVoteReplyData,
};
use crate::state_persister::{PersistentState, Persister};
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
pub struct Node<T> {
    current_term: u64,
    state: State,
    peers: Vec<String>,
    voted_for: Option<String>,
    entries: Vec<LogEntry>,
    network_inbox: Sender<OutMsg>,
    id: String,
    commit_index: u64,

    leader: String,
    last_applied: u64,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,

    state_machine: HashMap<String, String>,
    pending_clients: HashMap<u64, tokio::sync::oneshot::Sender<ChangeStateReply>>,

    state_persister: T,
}

impl<T: Persister + Send + Sync> Node<T> {
    pub async fn new(
        peers: Vec<String>,
        network_inbox: Sender<OutMsg>,
        id: String,
        state_persister: T,
    ) -> anyhow::Result<Self> {
        let mut next_index_map = HashMap::new();
        let mut match_index_map = HashMap::new();
        for peer in peers.clone() {
            next_index_map.insert(peer.clone(), 0);
            match_index_map.insert(peer, 0);
        }
        let mut node = Node {
            leader: String::new(),
            current_term: 0,
            state: State::default(),
            peers,
            voted_for: None,
            network_inbox,
            entries: vec![],
            id,
            commit_index: 0,
            last_applied: 0,
            next_index: next_index_map,
            match_index: match_index_map,
            state_machine: HashMap::new(),
            pending_clients: HashMap::new(),
            state_persister: state_persister,
        };
        let init_state = node.state_persister.load_state().await?;
        node.current_term = init_state.current_term;
        node.voted_for = init_state.voted_for;
        node.entries = init_state.entries;
        Ok(node)
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
    async fn persist_state(&self) -> anyhow::Result<()> {
        let persistent_state = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            entries: self.entries.clone(),
        };

        self.state_persister.save_state(&persistent_state).await?;
        Ok(())
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
                    let reply = ChangeStateReply {
                        success: false,
                        leader: self.leader.clone(),
                    };
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
                            entries: vec![LogEntry {
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
            RaftMsg::GetState { key, reply_channel } => {
                if let Some(val) = self.state_machine.get(&key) {
                    self.send_to_reply_channel(Some(reply_channel), val.to_owned())?;
                } else {
                    self.send_to_reply_channel(Some(reply_channel), "".to_owned())?;
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
            let command = &self.entries[i as usize - 1].command;

            let args: Vec<&str> = command.split_whitespace().collect();
            let command = args[0];
            let key = args[1];

            if command.to_lowercase() == "set" {
                let val = args[2];
                self.state_machine.insert(key.to_owned(), val.to_owned());
            }

            self.last_applied = i;
            let reply_channel = self.pending_clients.remove(&i);
            let reply = ChangeStateReply {
                success: true,
                leader: self.id.clone(),
            };
            self.send_to_reply_channel(reply_channel, reply)?;
        }

        Ok(())
    }

    async fn send_heartbeat(&self) -> anyhow::Result<()> {
        // Placeholder for heartbeat logic
        for peer in &self.peers {
            let prev_log_index = self.entries.len() as u64;
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

        self.persist_state().await?;
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
        self.persist_state().await?;

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
            eprintln!("current_term {}", self.current_term.clone());
            eprintln!("prev_log_index {}", append_request.prev_log_index.clone());
            eprintln!("prev_log_term {}", append_request.prev_log_term);
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

            self.persist_state().await?;
            return Ok(reply);
        }

        // Reset voted_for on receiving heartbeat
        if self.current_term < append_request.term {
            self.current_term = append_request.term;
            self.voted_for = None;
        }

        if self.state != State::Follower {
            eprintln!("Received append entries, stepping down to follower");
            self.leader = append_request.leader_id;
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

        for entry in append_request.entries {
            self.entries.push(entry);
        }

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

        self.persist_state().await?;
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
            self.persist_state().await?;
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

            self.persist_state().await?;
            return Ok(());
        }

        let mut next_index = *self
            .next_index
            .get(&append_entries_reply_data.peer)
            .expect("no peer found in state");

        eprintln!(
            "next_index {} for peer {}",
            next_index, append_entries_reply_data.peer
        );
        if next_index > 1 {
            next_index = next_index - 1;
        }

        let prev_log_index = next_index - 1;
        let mut prev_log_term = 0;
        if prev_log_index > 0 {
            prev_log_term = self
                .entries
                .get(prev_log_index as usize - 1)
                .map_or(0, |e| e.term);
        }

        let entries_to_send = self.entries[prev_log_index as usize..]
            .iter()
            .map(|e| LogEntry {
                term: e.term,
                command: e.command.clone(),
            })
            .collect();

        eprintln!(
            "setting new next_index {} for {}",
            next_index,
            append_entries_reply_data.peer.clone()
        );
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

    fn send_to_reply_channel<R>(
        &self,
        reply_channel: Option<tokio::sync::oneshot::Sender<R>>,
        reply: R,
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

    struct TestPersister;
    struct LoadedStatePersister {
        state: PersistentState,
    }
    struct FailingLoadPersister;

    #[async_trait::async_trait]
    impl Persister for TestPersister {
        async fn save_state(&self, _state: &PersistentState) -> anyhow::Result<()> {
            Ok(())
        }

        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Ok(PersistentState {
                current_term: 0,
                voted_for: None,
                entries: vec![],
            })
        }
    }

    #[async_trait::async_trait]
    impl Persister for LoadedStatePersister {
        async fn save_state(&self, _state: &PersistentState) -> anyhow::Result<()> {
            Ok(())
        }

        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Ok(PersistentState {
                current_term: self.state.current_term,
                voted_for: self.state.voted_for.clone(),
                entries: self.state.entries.clone(),
            })
        }
    }

    #[async_trait::async_trait]
    impl Persister for FailingLoadPersister {
        async fn save_state(&self, _state: &PersistentState) -> anyhow::Result<()> {
            Ok(())
        }

        async fn load_state(&self) -> anyhow::Result<PersistentState> {
            Err(anyhow::anyhow!("failed to load state"))
        }
    }

    #[tokio::test]
    async fn test_new_loads_persistent_state() -> anyhow::Result<()> {
        let peers = vec!["node2".to_string()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let persister = LoadedStatePersister {
            state: PersistentState {
                current_term: 7,
                voted_for: Some("node3".to_string()),
                entries: vec![
                    LogEntry {
                        term: 5,
                        command: "set key1 val1".to_string(),
                    },
                    LogEntry {
                        term: 7,
                        command: "set key2 val2".to_string(),
                    },
                ],
            },
        };

        let node = Node::new(peers, network_inbox, String::from("node1"), persister).await?;

        assert_eq!(node.current_term, 7);
        assert_eq!(node.voted_for, Some("node3".to_string()));
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[0].term, 5);
        assert_eq!(node.entries[0].command, "set key1 val1");
        assert_eq!(node.entries[1].term, 7);
        assert_eq!(node.entries[1].command, "set key2 val2");
        Ok(())
    }

    #[tokio::test]
    async fn test_new_returns_error_when_state_loading_fails() {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);

        let result = Node::new(
            peers,
            network_inbox,
            String::from("node1"),
            FailingLoadPersister,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_vote_request() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let file_persister = TestPersister {};
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), file_persister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
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

    #[tokio::test]
    async fn test_handle_append_entries_reply_unsuccess_decrement_next_index() -> anyhow::Result<()>
    {
        let peers = vec!["test".to_owned()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
        node.current_term = 2;
        node.next_index.insert("test".to_owned(), 3);
        node.state = State::Leader;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd3".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd4".to_string(),
        });
        let append_reply = AppendEntriesReplyData {
            term: 2,
            success: false,

            peer: "test".to_owned(),
            entries_count: 0,
        };

        node.handle_append_entries_reply(append_reply).await?;
        assert_eq!(node.current_term, 2);
        assert_eq!(node.state, State::Leader);
        assert_eq!(*node.next_index.get("test").expect("no peer found"), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_reply_unsuccess_next_index_stays_1() -> anyhow::Result<()> {
        let peers = vec!["test".to_owned()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
        node.current_term = 2;
        node.next_index.insert("test".to_owned(), 1);
        node.state = State::Leader;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd3".to_string(),
        });
        node.entries.push(LogEntry {
            term: 2,
            command: "cmd4".to_string(),
        });
        let append_reply = AppendEntriesReplyData {
            term: 2,
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

    #[tokio::test]
    async fn test_apply_commands_applies_everything_after_commit_index() -> anyhow::Result<()> {
        let peers = vec!["test".to_owned()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("self"), TestPersister).await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.commit_index = 3;
        node.last_applied = 0;
        node.entries.push(LogEntry {
            term: 1,
            command: "set key1 val1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key2 val2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key1 val3".to_string(),
        });

        let (snd1, rcv1) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd2, rcv2) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd3, rcv3) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        node.pending_clients.insert(1, snd1);
        node.pending_clients.insert(2, snd2);
        node.pending_clients.insert(3, snd3);
        node.apply_commands().await?;

        let res1 = rcv1.await?;
        let res2 = rcv2.await?;
        let res3 = rcv3.await?;

        let val1 = node
            .state_machine
            .get("key1")
            .expect("missing value")
            .to_owned();
        let val2 = node
            .state_machine
            .get("key2")
            .expect("missing value")
            .to_owned();

        assert_eq!(val1, "val3".to_owned());
        assert_eq!(val2, "val2".to_owned());

        assert_eq!(res1.success, true);
        assert_eq!(res1.leader, node.id);
        assert_eq!(res2.success, true);
        assert_eq!(res2.leader, node.id);
        assert_eq!(res3.success, true);
        assert_eq!(res3.leader, node.id);

        assert_eq!(node.current_term, 1);
        assert_eq!(node.state, State::Leader);
        assert_eq!(node.last_applied, node.commit_index);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_commands_has_state_applies_everything_after_commit_index()
    -> anyhow::Result<()> {
        let peers = vec!["test".to_owned()];
        let (network_inbox, _network_outbox) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(peers, network_inbox, String::from("self"), TestPersister).await?;
        node.current_term = 1;
        node.state = State::Leader;
        node.commit_index = 3;
        node.last_applied = 1;
        node.state_machine
            .insert("key1".to_owned(), "val1".to_owned());
        node.entries.push(LogEntry {
            term: 1,
            command: "set key1 val3".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key2 val2".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "set key3 val3".to_string(),
        });

        let (snd2, rcv2) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let (snd3, rcv3) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        node.pending_clients.insert(2, snd2);
        node.pending_clients.insert(3, snd3);
        node.apply_commands().await?;

        let res2 = rcv2.await?;
        let res3 = rcv3.await?;

        let val2 = node
            .state_machine
            .get("key2")
            .expect("missing value")
            .to_owned();
        let val3 = node
            .state_machine
            .get("key3")
            .expect("missing value")
            .to_owned();
        let val1 = node
            .state_machine
            .get("key1")
            .expect("no val found")
            .to_owned();

        assert_eq!(val1, "val1".to_owned());
        assert_eq!(val2, "val2".to_owned());
        assert_eq!(val3, "val3".to_owned());

        assert_eq!(res2.success, true);
        assert_eq!(res2.leader, node.id);
        assert_eq!(res3.success, true);
        assert_eq!(res3.leader, node.id);

        assert_eq!(node.current_term, 1);
        assert_eq!(node.state, State::Leader);
        assert_eq!(node.last_applied, node.commit_index);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_append_entries_same_term_does_not_reset_voted_for() -> anyhow::Result<()> {
        let peers = vec![];
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node =
            Node::new(peers, network_inbox, String::from("node1"), TestPersister).await?;
        node.current_term = 2;
        node.voted_for = Some(String::from("node2")); // already voted this term
        node.state = State::Follower;

        let append_request = AppendEntriesData {
            term: 2, // same term — heartbeat from current leader
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            leader_id: String::from("node2"),
            entries: vec![],
        };

        let reply = node.handle_append_entries(append_request).await?;
        assert!(reply.success);
        assert_eq!(node.voted_for, Some(String::from("node2"))); // must not be cleared
        assert_eq!(node.current_term, 2);
        Ok(())
    }
}
