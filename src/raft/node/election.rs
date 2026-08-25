use super::{Node, State};
use crate::raft::network_types::OutMsg;
use crate::raft::raft_types::{RequestVoteData, RequestVoteReplyData};
use crate::raft::state_machine::StorageEngine;
use crate::raft::state_persister::Persister;

impl<T: Persister + Send + Sync, SM: StorageEngine> Node<T, SM> {
    pub(super) async fn start_election(&mut self) -> anyhow::Result<()> {
        self.current_term += 1;
        self.state = State::Candidate { votes: 1 }; // vote for self
        self.voted_for = Some(self.id.clone());
        let peers = &self.peers;
        let last_log_index = self.last_log_index();
        let last_log_term = self
            .entries
            .last()
            .map_or(self.snapshot_last_term, |e| e.term);

        self.persist_state().await?;
        for peer in peers {
            let out_msg = OutMsg::RequestVote {
                term: self.current_term,
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
            self.step_down(vote_request.term);
            self.persist_state().await?;
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
                term: self.current_term,
                vote: false,
            });
        }

        self.voted_for = Some(vote_request.candidate.clone());
        self.state = State::Follower;
        self.persist_state().await?;

        Ok(RequestVoteReplyData {
            term: self.current_term,
            vote: true,
        })
    }

    pub(super) async fn handle_request_vote_reply(
        &mut self,
        vote_reply: RequestVoteReplyData,
    ) -> anyhow::Result<()> {
        if vote_reply.term > self.current_term {
            self.step_down(vote_reply.term);
            self.persist_state().await?;
            return Ok(());
        } else if vote_reply.term < self.current_term {
            // Ignore stale replies
            return Ok(());
        }

        let mut votes = match self.state {
            State::Candidate { votes } => votes,
            _ => {
                return Ok(());
            }
        };

        votes += if vote_reply.vote { 1 } else { 0 };
        self.state = State::Candidate { votes };

        if !self.is_majority(votes) {
            return Ok(());
        }

        self.become_leader();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::node::test_helpers::{LSMTree, TestPersister};
    use crate::raft::raft_types::LogEntry;

    #[tokio::test]
    async fn test_handle_vote_request() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 1,
                last_log_index: 0,
                last_log_term: 0,
                candidate: "node1".to_string(),
            })
            .await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_already_voted() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.voted_for = Some("node2".to_string());
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 0,
                last_log_index: 0,
                last_log_term: 0,
                candidate: "node3".to_string(),
            })
            .await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_not_up_to_date() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 2,
                last_log_index: 0,
                last_log_term: 0,
                candidate: "node1".to_string(),
            })
            .await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_index_up_to_date() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 2,
                last_log_index: 2,
                last_log_term: 1,
                candidate: "node1".to_string(),
            })
            .await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_voted_for_same_candidate() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.voted_for = Some("node2".to_string());
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 1,
                last_log_index: 0,
                last_log_term: 0,
                candidate: "node2".to_string(),
            })
            .await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_higher_log_index_not_up_to_date() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd2".to_string(),
        });
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 2,
                last_log_index: 1,
                last_log_term: 1,
                candidate: "node1".to_string(),
            })
            .await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_log_term_mismatch() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 2,
                last_log_index: 1,
                last_log_term: 0,
                candidate: "node1".to_string(),
            })
            .await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_stale_term() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.current_term = 2;
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 1,
                last_log_index: 0,
                last_log_term: 0,
                candidate: "node1".to_string(),
            })
            .await?;
        assert!(!reply.vote);
        assert_eq!(node.current_term, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_candidate_has_higher_term() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec![],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd1".to_string(),
        });
        node.entries.push(LogEntry {
            term: 1,
            command: "cmd2".to_string(),
        });
        let reply = node
            .handle_vote_request(RequestVoteData {
                term: 3,
                last_log_index: 1,
                last_log_term: 2,
                candidate: "node1".to_string(),
            })
            .await?;
        assert!(reply.vote);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.state, State::Follower);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_request_vote_reply_become_leader() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["node1".to_string(), "node2".to_string()],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };
        node.handle_request_vote_reply(RequestVoteReplyData {
            term: 1,
            vote: true,
        })
        .await?;
        assert_eq!(node.state, State::Leader);
        assert_eq!(node.current_term, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_vote_request_reply_no_majority() -> anyhow::Result<()> {
        let (network_inbox, _) = tokio::sync::mpsc::channel(100);
        let mut node = Node::new(
            vec!["node1".to_string(), "node2".to_string()],
            network_inbox,
            "node1".to_string(),
            TestPersister,
            LSMTree::new(),
        )
        .await?;
        node.current_term = 1;
        node.state = State::Candidate { votes: 1 };
        node.handle_request_vote_reply(RequestVoteReplyData {
            term: 1,
            vote: false,
        })
        .await?;
        assert_eq!(node.state, State::Candidate { votes: 1 });
        Ok(())
    }
}
