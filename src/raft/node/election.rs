use super::{Node, State};
use crate::raft::network_types::OutMsg;
use crate::raft::raft_types::{RequestVoteData, RequestVoteReplyData};
use crate::raft::state_persister::Persister;
use crate::raft::storage_engine::StorageEngine;

impl<T: Persister + Send + Sync, SM: StorageEngine + std::fmt::Debug> Node<T, SM> {
    pub(super) async fn start_election(&mut self) -> anyhow::Result<()> {
        eprintln!("Election timeout, starting election");
        self.current_term += 1;
        self.state = State::Candidate { votes: 1 }; // vote for self
        self.voted_for = Some(self.id.clone());
        eprintln!(
            "Current node state: {:?}, term: {}",
            self.state, self.current_term
        );

        let peers = &self.peers;
        let last_log_index = self.last_log_index();
        let last_log_term = self
            .entries
            .last()
            .map_or(self.snapshot_last_term, |e| e.term);

        self.persist_state().await?;
        for peer in peers {
            eprintln!("Sending requestVote to {}", peer);
            let out_msg = OutMsg::RequestVote {
                term: self.current_term as u64,
                peer: peer.clone(),
                last_log_index,
                last_log_term,
                candidate: self.id.clone(),
            };
            self.network_inbox.send(out_msg).await?;
        }

        Ok(())
    }

    pub(super) async fn handle_vote_request(
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
        let last_log_term = self
            .entries
            .last()
            .map_or(self.snapshot_last_term, |e| e.term);

        if (self.voted_for.is_some() && !voted_for_candidate)
            || self.current_term > vote_request.term
            || (last_log_term == vote_request.last_log_term
                && self.last_log_index() > vote_request.last_log_index)
            || last_log_term > vote_request.last_log_term
        {
            return Ok(RequestVoteReplyData {
                term: self.current_term as u64,
                vote: false,
            });
        }

        self.voted_for = Some(vote_request.candidate.clone());
        self.state = State::Follower;
        self.persist_state().await?;

        Ok(RequestVoteReplyData {
            term: self.current_term as u64,
            vote: true,
        })
    }

    pub(super) async fn handle_request_vote_reply(
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

        let next_index = self.last_log_index() + 1;
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), next_index);
            self.match_index.insert(peer.clone(), 0);
        }

        self.state = State::Leader;
        Ok(())
    }
}
