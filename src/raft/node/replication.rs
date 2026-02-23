use super::{Node, State};
use crate::raft::network_types::OutMsg;
use crate::raft::raft_types::{AppendEntriesData, AppendEntriesReplyData, LogEntry};
use crate::raft::state_persister::Persister;

impl<T: Persister + Send + Sync> Node<T> {
    pub(super) async fn send_heartbeat(&self) -> anyhow::Result<()> {
        for peer in &self.peers {
            let prev_log_index = self.last_log_index();
            let prev_log_term = self
                .entries
                .last()
                .map_or(self.snapshot_last_term, |e| e.term);
            let out_msg = OutMsg::AppendEntries {
                term: self.current_term as u64,
                leader_id: self.id.clone(),
                entries: vec![],
                prev_log_index,
                prev_log_term,
                leader_commit: self.commit_index,
                peer: peer.clone(),
            };
            self.network_inbox.send(out_msg).await?;
        }
        Ok(())
    }

    pub(super) async fn handle_append_entries(
        &mut self,
        append_request: AppendEntriesData,
    ) -> anyhow::Result<AppendEntriesReplyData> {
        if self.current_term > append_request.term
            || self.last_log_index() < append_request.prev_log_index
            || (append_request.prev_log_index != 0
                && self.get_log_term(append_request.prev_log_index)
                    != append_request.prev_log_term)
        {
            eprintln!("current_term {}", self.current_term);
            eprintln!("prev_log_index {}", append_request.prev_log_index);
            eprintln!("prev_log_term {}", append_request.prev_log_term);
            self.state = if self.current_term < append_request.term {
                State::Follower
            } else {
                self.state
            };
            self.current_term = std::cmp::max(self.current_term, append_request.term);

            self.persist_state().await?;
            return Ok(AppendEntriesReplyData {
                term: self.current_term as u64,
                success: false,
                peer: self.id.clone(),
                entries_count: 0,
            });
        }

        // Reset voted_for on receiving heartbeat from a higher term
        if self.current_term < append_request.term {
            self.current_term = append_request.term;
            self.voted_for = None;
        }

        if self.state != State::Follower {
            eprintln!("Received append entries, stepping down to follower");
            self.leader = append_request.leader_id.clone();
            self.state = State::Follower;
        }

        // Remove conflicting entries
        if append_request.prev_log_index != 0
            && self.last_log_index() > append_request.prev_log_index
            && append_request.prev_log_index >= self.snapshot_last_index
        {
            let truncate_to =
                (append_request.prev_log_index - self.snapshot_last_index) as usize;
            self.entries.truncate(truncate_to);
        }

        let entries_count = append_request.entries.len();
        if entries_count > 0 {
            eprintln!("committing new entries {}", entries_count);
        }

        for entry in append_request.entries {
            self.entries.push(entry);
        }

        // Update commit index
        if append_request.leader_commit > self.commit_index {
            self.commit_index =
                std::cmp::min(self.last_log_index(), append_request.leader_commit);
        }

        self.persist_state().await?;
        Ok(AppendEntriesReplyData {
            term: self.current_term as u64,
            success: true,
            peer: self.id.clone(),
            entries_count: entries_count as u64,
        })
    }

    pub(super) async fn handle_append_entries_reply(
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

            for log_index in self.commit_index + 1..=self.last_log_index() {
                let mut count = 1; // self
                for (_, value) in self.match_index.iter() {
                    if *value >= log_index {
                        count += 1;
                    }
                }

                if self.is_majority(count)
                    && self.get_log_entry(log_index).map_or(0, |e| e.term)
                        == self.current_term
                {
                    self.commit_index = log_index;
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
            next_index -= 1;
        }

        let prev_log_index = next_index - 1;
        let prev_log_term = self.get_log_term(prev_log_index);

        // TODO: if prev_log_index < snapshot_last_index, we need InstallSnapshot instead
        let start_index = (prev_log_index.saturating_sub(self.snapshot_last_index) as usize)
            .min(self.entries.len());

        let entries_to_send: Vec<LogEntry> = self.entries[start_index..]
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
        self.next_index
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
}
