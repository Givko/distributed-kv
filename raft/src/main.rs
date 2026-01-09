use std::time::Duration;

use clap::Parser;
use rand::Rng;
use raft::raft_client::RaftClient;
use raft::raft_server::{Raft, RaftServer};
use raft::{AppendEntriesMessage, AppendEntriesReply, RequestVoteMessage, RequestVoteReply};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::{Request, Response, Status, transport::Server};

pub mod raft {
    tonic::include_proto!("raft");
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    // Port the node will use
    #[arg(short, long)]
    port: usize,

    #[arg(short, long, num_args = 1.., value_delimiter = ',')]
    nodes: Vec<String>,
}

#[derive(Debug)]
pub struct RaftService {
    mailbox: Sender<RaftMsg>,
}

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
enum State {
    Candidate {
        votes: usize,
    },
    Leader,
    #[default]
    Follower,
}

enum RaftMsg {
    RequestVoteReply {
        vote_reply: RequestVoteReply,
        reply_channel: tokio::sync::oneshot::Sender<()>,
    },
    HandleVoteRequest {
        vote_request: RequestVoteMessage, 
        reply_channel: tokio::sync::oneshot::Sender<RequestVoteReply>
    },
    EntriesAppended {
        append_request: AppendEntriesMessage,
        reply_channel: tokio::sync::oneshot::Sender<AppendEntriesReply>,
    },
    NewLeader{
        leader_term: usize,
    },
}

#[derive(Debug)]
struct Node {
    current_term: usize,
    state: State,
    peers: Vec<String>,
    voted_for: bool, 
}

impl Node {
    async fn run(mut self, mut inbox: Receiver<RaftMsg>, mailbox_sender: Sender<RaftMsg>) {
        loop{

            let mut duration: u64 = rand::rng().random_range(150..300);
            if self.is_leader() {
                duration = 50;
            }

            tokio::select! {
                Some(msg) = inbox.recv() => {
                    self.handle_message(msg).await;
                }
                _ = tokio::time::sleep(Duration::from_millis(duration)) => {
                    if self.is_leader() {
                        for peer in self.peers.clone() {
                            // If we're the leader, we don't need to run the election timer
                            let current_term = self.current_term;
                            let append_entries_request = Request::new(AppendEntriesMessage {
                                term: self.current_term as u64,
                            });

                            if let Ok(mut peer_client) = RaftClient::connect(format!("http://{}", peer)).await {
                                let inbox_sender = mailbox_sender.clone();
                                tokio::spawn(async move {
                                    if let Ok(reply) = peer_client.append_entries(append_entries_request).await {
                                        let reply_inner = reply.into_inner();
                                        if reply_inner.success {
                                            return;
                                        } 

                                        eprintln!("Heartbeat rejected by {}", peer);
                                        if reply_inner.term as usize > current_term {
                                            let new_leader_msg = RaftMsg::NewLeader {
                                                leader_term: reply_inner.term as usize,
                                            };
                                            inbox_sender.send(new_leader_msg).await.expect("Failed to send new leader message");
                                        }
                                    } 
                                });
                            }
                        }

                        continue;
                    }

                    eprintln!("Election timeout, starting election");
                    self.current_term += 1;
                    self.state = State::Candidate { votes: 1 }; // vote for self
                    self.voted_for = true;

                    eprintln!("Current node state: {:?}", self.state);
                    let cur_term = self.current_term;
                    let peers = self.peers.clone();
                    for peer in peers {
                        eprintln!("Sending requestVote to {}", peer);
                        let vote_request = RequestVoteMessage {
                            term: cur_term as u64
                        };
                        let request = Request::new(vote_request);
                        let mailbox_sender = mailbox_sender.clone();
                        tokio::spawn(async move {
                            if let Ok(mut peer_client) = RaftClient::connect(format!("http://{}", peer)).await {
                                let vote_reply = peer_client.request_vote(request).await.expect("Failed to send request vote");
                                let vote_reply_inner = vote_reply.into_inner();
                                let (snd, rcv) = tokio::sync::oneshot::channel::<()>();
                                let vote_reply_message = RaftMsg::RequestVoteReply {
                                    vote_reply: vote_reply_inner,
                                    reply_channel: snd,
                                };
                                mailbox_sender.send(vote_reply_message).await.expect("Failed to send vote reply to node");
                                rcv.await.expect(format!("Failed to receive ack for vote reply for peer {peer}").as_str());
                            }
                        });
                    }
                }
            }
            
        }
    }

    async fn handle_message(&mut self, msg: RaftMsg) {
        match msg {
            RaftMsg::HandleVoteRequest { vote_request, reply_channel } => {
                eprintln!("Got requestVote in node");

                if self.current_term < vote_request.term as usize {
                    self.current_term = vote_request.term as usize;
                    self.voted_for = false;
                    self.state = State::Follower;
                }

                if self.voted_for || self.current_term > vote_request.term as usize {
                    let vote = RequestVoteReply {
                        term: self.current_term as u64,
                        vote: false,
                    };

                    reply_channel.send(vote).expect("Failed to send vote reply");
                    return; 
                }

                let vote_reply = RequestVoteReply {
                    term: self.current_term as u64,
                    vote: true,
                };

                self.current_term = vote_request.term as usize;
                self.voted_for = true;
                self.state = State::Follower;

                reply_channel.send(vote_reply).expect("Failed to send vote reply");
            }
            RaftMsg::EntriesAppended {append_request, reply_channel} => {
                if self.current_term > append_request.term as usize {
                    let vote = AppendEntriesReply {
                        term: self.current_term as u64,
                        success: false,
                    };

                    reply_channel.send(vote).expect("Failed to send append entries reply not successful");
                    return;
                }

                let reply = AppendEntriesReply {
                    term: self.current_term as u64,
                    success: true,
                };

                self.state = State::Follower;

                // Reset voted_for on receiving heartbeat
                if self.current_term < reply.term as usize {
                    self.current_term = reply.term as usize;
                    self.voted_for = false;
                }

                reply_channel.send(reply).expect("Failed to send append entries reply");
            }
            RaftMsg::NewLeader { leader_term } => {
                if self.current_term < leader_term {
                    self.current_term = leader_term;
                    self.state = State::Follower;
                    eprint!("Stepping down to follower due to new leader with higher term {}", leader_term);
                    self.voted_for = false;
                }
            }
            RaftMsg::RequestVoteReply { vote_reply, reply_channel } => {
                eprintln!("Received vote reply: {:?}", vote_reply);
                if vote_reply.term as usize > self.current_term {
                    self.current_term = vote_reply.term as usize;
                    self.state = State::Follower;
                    self.voted_for = false;
                    eprintln!("Stepping down to follower due to higher term in vote reply");
                    reply_channel.send(()).expect("Failed to send ack for vote reply");
                    return;
                }

                let mut votes = match self.state {
                    State::Candidate { votes } => votes,
                    _ => {
                        eprintln!("Received vote reply while not a candidate, ignoring");
                        reply_channel.send(()).expect("Failed to send ack for vote reply");
                        return;
                    }
                }; 
                
                votes += if vote_reply.vote { 1 } else { 0 };
                self.state = State::Candidate { votes };
                
                eprintln!("Total votes received: {}", votes);
                if votes <= (self.peers.len() + 1) / 2 {
                    eprintln!("Did not receive majority votes, remaining candidate");
                    reply_channel.send(()).expect("Failed to send ack for vote reply");
                    return;
                }
                
                // Get write lock in order to prevent state update while updating state
                eprintln!("cur state before becoming leader: {:?}", self.state);
                if self.is_leader() || self.is_follower() {
                    eprintln!("Node became leader or follower during election, aborting election process");
                    reply_channel.send(()).expect("Failed to send ack for vote reply");
                    return;
                }

                eprintln!("Received majority votes, becoming leader with term {}", self.current_term);
                self.state = State::Leader;

                // Here we would handle the vote reply, e.g., count votes
                reply_channel.send(()).expect("Failed to send ack for vote reply");
            }
        }
    }

    fn is_leader(&self) -> bool {
        self.state == State::Leader
    }

    fn is_follower(&self) -> bool {
        self.state == State::Follower
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteMessage>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        
        let (snd, rcv) = tokio::sync::oneshot::channel::<RequestVoteReply>();
        let vote_message = RaftMsg::HandleVoteRequest {
            vote_request: request.into_inner(),
            reply_channel: snd,
        };

        self.mailbox.send(vote_message).await.expect("Failed to send vote message");
        let vote_reply = rcv.await.expect("Failed to receive vote reply");
        
        Ok(Response::new(vote_reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesMessage>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let message= request.into_inner();
        let (snd, rcv) = tokio::sync::oneshot::channel::<AppendEntriesReply>();
        let append_message = RaftMsg::EntriesAppended {
            append_request: message,
            reply_channel: snd,
        };
        self.mailbox.send(append_message).await.expect("Failed to send append entries message");
        let reply = rcv.await.expect("Failed to receive append entries reply");

        Ok(Response::new(reply))
    }
}

impl RaftService {
    fn new(mailbox: Sender<RaftMsg>) -> Self {
        RaftService {
            mailbox, 
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    eprintln!("{}", args.port);
    eprintln!("{:?}", args.nodes);

    let addr = format!("127.0.0.1:{}", args.port).parse()?;
    let (mailbox_snd, mailbox_rcv) = tokio::sync::mpsc::channel::<RaftMsg>(100);
    
    let node: Node = Node {
        current_term: 0,
        state: State::default(),
        peers:args.nodes,
        voted_for: false,
    };
    let raft = RaftService::new(mailbox_snd.clone());

    _ = tokio::spawn(async move { node.run(mailbox_rcv, mailbox_snd).await });
    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
