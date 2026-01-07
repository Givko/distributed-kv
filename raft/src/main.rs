use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rand::Rng;
use raft::raft_client::RaftClient;
use raft::raft_server::{Raft, RaftServer};
use raft::{AppendEntriesMessage, AppendEntriesReply, RequestVoteMessage, RequestVoteReply};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
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
    elect_timer_reset_tx: Sender<()>,
}

#[derive(Debug, PartialEq, Eq, Default)]
enum State {
    Candidate,
    Leader,
    #[default]
    Follower,
}

enum RaftMsg {
    RequestVote {
        vote_request: RequestVoteMessage, 
        reply_channel: tokio::sync::oneshot::Sender<RequestVoteReply>
    },
    AppendEntries {
        append_request: AppendEntriesMessage,
        reply_channel: tokio::sync::oneshot::Sender<AppendEntriesReply>,
    },
}

#[derive(Debug)]
struct Node {
    current_term: usize,
    state: State,
    peers: Vec<String>,
    voted_for: bool, 
    election_handle: Option<JoinHandle<()>>,
}

impl Node {

    async fn run(mut self, mut inbox: Receiver<RaftMsg>) {
        while let Some(msg) = inbox.recv().await {
            match msg {
                RaftMsg::RequestVote { vote_request, reply_channel } => {
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
                RaftMsg::AppendEntries {append_request, reply_channel} => {
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
            }
        }
    }

    fn is_leader(&self) -> bool {
        self.state == State::Leader
    }

    fn is_candidate(&self) -> bool {
        self.state == State::Candidate
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
        let vote_message = RaftMsg::RequestVote {
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
        let append_message = RaftMsg::AppendEntries {
            append_request: message,
            reply_channel: snd,
        };
        self.mailbox.send(append_message).await.expect("Failed to send append entries message");
        let reply = rcv.await.expect("Failed to receive append entries reply");
        self.elect_timer_reset_tx.send(()).await.expect("Failed to send election timer reset");

        Ok(Response::new(reply))
    }
}

impl RaftService {
    fn new(elect_timer_reset_tx: Sender<()>, mailbox: Sender<RaftMsg>) -> Self {
        RaftService {
            mailbox, 
            elect_timer_reset_tx,
        }
    }
}

async fn election_timer(raft_service: RaftService, mut reset_rx: Receiver<()>) {
    loop {
        let duration: u64 = rand::rng().random_range(150..300);
        let timeout_duration: Duration = Duration::from_millis(duration);

        if node.read().await.is_leader() {
            // If we're the leader, we don't need to run the election timer
            //TODO send heartbeat messages
            tokio::time::sleep(Duration::from_millis(50)).await;
            for peer in node.read().await.peers.clone() {
                if let Ok(mut peer_client) = RaftClient::connect(format!("http://{}", peer)).await {
                    let node = node.clone();
                    tokio::spawn(async move {
                        let append_entries_request = Request::new(AppendEntriesMessage {
                            term: node.read().await.current_term as u64,
                        });

                        if let Ok(reply) = peer_client.append_entries(append_entries_request).await {
                            let reply_inner = reply.into_inner();
                            if reply_inner.success {
                                eprintln!("Heartbeat acknowledged by {}", peer);
                                return;
                            } 

                            eprintln!("Heartbeat rejected by {}", peer);
                            if reply_inner.term as usize > node.read().await.current_term {
                                let mut node_guard = node.write().await;
                                node_guard.current_term = reply_inner.term as usize;
                                node_guard.state = State::Follower;
                                eprintln!("Stepping down to follower due to higher term from {}", peer);
                            }
                        } 
                    });
                }
            }

            continue;
        }

        tokio::select! {
            _ = tokio::time::sleep(timeout_duration) => {
                // Election timeout elapsed, start a new election
                if node.read().await.is_candidate() {
                    eprintln!("Node became candidate during timeout, aborting election");
                    if let Some(handle) = node.write().await.election_handle.take() {
                        handle.abort();
                    }
                }

                let node_clone = node.clone();
                let election_handle = tokio::spawn(async move {
                    election_process(node_clone).await;
                });

                let mut node_guard = node.write().await;
                node_guard.election_handle = Some(election_handle);
            }
            _ = reset_rx.recv() => {
                // Received a reset signal, restart the timer
                continue;
            }
        }

    }
}

async fn election_process(node: Arc<RwLock<Node>>) {
    eprintln!("Starting election process");
    {
        let mut node_guard = node.write().await;
        node_guard.current_term += 1;
        node_guard.state = State::Candidate; 
        node_guard.voted_for = true;
    }

    eprintln!("Current node state: {:?}", node.read().await.state);
    let mut join_handles: Vec<JoinHandle<Result<Response<RequestVoteReply>, Status>>> = vec![];
    let cur_term = node.read().await.current_term;
    //TODO: borrow dont clone if possible to void performance hit
    let peers = node.read().await.peers.clone();
    for peer in peers {
        eprintln!("Sending requestVote to {}", peer);
        if let Ok(mut peer_client) = RaftClient::connect(format!("http://{}", peer)).await {
            let vote_request = Request::new(RequestVoteMessage {
                term: cur_term as u64,
            });

            let handle: JoinHandle<Result<Response<RequestVoteReply>, Status>> = tokio::spawn(async move {
                let vote_reply = peer_client.request_vote(vote_request).await;
                eprintln!("Got vote reply from {}: {:?}", peer, vote_reply);
                vote_reply
            });

            join_handles.push(handle);
        }
    }

    let mut votes = vec![];

    //TODO: this is sequential, make it parallel use channels
    for handle in join_handles {
        match handle.await {
            Ok(Ok(vote_reply)) => {
                votes.push(vote_reply.into_inner());
            }
            Ok(Err(e)) => {
                eprintln!("Error getting vote reply: {}", e);
            }
            Err(e) => {
                eprintln!("Join error: {}", e);
            } 
        }
    }
    
    eprintln!("cur state after votes collected: {:?}", node.read().await.state);
    let mut votes_recieved = 1; // vote for self
    for vote in votes {
        eprintln!("Vote: {:?}", vote);
        if vote.vote {
            votes_recieved += 1;
        }
    }

    eprintln!("Total votes received: {}", votes_recieved);
    if votes_recieved <= (node.read().await.peers.len() + 1) / 2 {
        eprintln!("Did not receive majority votes, remaining candidate");
        return;
    }
    
    {
        // Get write lock in order to prevent state update while updating state
        let mut node_guard = node.write().await;
        eprintln!("cur state before becoming leader: {:?}", node_guard.state);
        if node_guard.is_leader() || node_guard.is_follower() {
            eprintln!("Node became leader or follower during election, aborting election process");
            return;
        }

        eprintln!("Received majority votes, becoming leader");
        node_guard.state = State::Leader;
    }

    // Reset election timer
    node.read().await.elect_timer_reset_tx.send(()).await.expect("Failed to send reset signal");
    return;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    eprintln!("{}", args.port);
    eprintln!("{:?}", args.nodes);

    let addr = format!("127.0.0.1:{}", args.port).parse()?;
    let (elect_rst_sn, elect_rst_rcv) = tokio::sync::mpsc::channel::<()>(1);
    let (mailbox_snd, mailbox_rcv) = tokio::sync::mpsc::channel::<RaftMsg>(100);
    let raft = RaftService::new(elect_rst_sn,mailbox_snd);

    let node: Node = Node {
        current_term: 0,
        state: State::default(),
        peers:args.nodes,
        voted_for: false,
        election_handle: None,
    };

    _ = tokio::spawn(async move { node.run(mailbox_rcv).await });
    _ = tokio::spawn(election_timer(
        raft, 
        elect_rst_rcv
    ));
    
    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
