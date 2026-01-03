use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rand::Rng;
use raft::raft_client::RaftClient;
use raft::raft_server::{Raft, RaftServer};
use raft::{AppendEntriesMessage, AppendEntriesReply, RequestVoteMessage, RequestVoteReply};
use tokio::sync::RwLock;
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
    node: Arc<RwLock<Node>>,
    elect_timer_reset_tx: Sender<()>,
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Candidate {
        votes: usize,
    },
    Leader,
    Follower {
        leader: String,
    },
}

impl Default for State {
    fn default() -> Self {
        State::Follower {
            leader: String::new(),
        }
    }
    
}

#[derive(Debug, Default)]
struct Node {
    current_term: usize,
    state: State,
    peers: Vec<String>,
}

impl Node {
    fn is_leader(&self) -> bool {
        self.state == State::Leader
    }

    fn is_candidate(&self) -> bool {
        match self.state {
            State::Candidate { .. } => true,
            _ => false,
        }
    }

    fn is_follower(&self) -> bool {
        match self.state {
            State::Follower { .. } => true,
            _ => false,
        }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteMessage>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        eprintln!("Got requestVote");

        let vote = RequestVoteReply {
            term: 1,
            vote: true,
        };

        Ok(Response::new(vote))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesMessage>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        eprintln!("got append entries");

        let reply = AppendEntriesReply {
            term: 1,
            success: true,
        };

        self.elect_timer_reset_tx.send(()).await.unwrap();

        Ok(Response::new(reply))
    }
}

impl RaftService {
    fn new(snd: Sender<()>, peers: Vec<String>) -> Self {
        let node: Node = Node {
            current_term: 0,
            state: State::default(),
            peers,
        };

        RaftService {
            node: Arc::new(RwLock::new(node)),
            elect_timer_reset_tx: snd,
        }
    }
}

async fn election_timer(node: Arc<RwLock<Node>>, mut reset_rx: Receiver<()>) {
    loop {
        let duration: u64 = rand::rng().random_range(150..300);
        let timeout_duration: Duration = Duration::from_millis(duration);

        if node.read().await.is_leader() {
            // If we're the leader, we don't need to run the election timer
            eprintln!("Node is leader, skipping election timer");
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        tokio::select! {
            _ = tokio::time::sleep(timeout_duration) => {
                // Election timeout elapsed, start a new election
                //eprintln!("Election timeout elapsed, starting new election");
            }
            _ = reset_rx.recv() => {
                // Received a reset signal, restart the timer
                eprintln!("Election timer reset");
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
        node_guard.state = State::Candidate { votes: 1 };
    }

    //TODO: Send requestVote to all other nodes

    // Before counting votes, check if we became leader already
    if node.read().await.is_leader() || node.read().await.is_follower() {
        eprintln!("Node became leader or follower during election, aborting election process");
        return;
    }

    //TODO: Count votes and become leader if majority is reached
    {
        // take write lock
        // count votes
        // if majority, become leader
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    eprintln!("{}", args.port);
    eprintln!("{:?}", args.nodes);

    let addr = format!("127.0.0.1:{}", args.port).parse()?;
    let (snd, rcv) = tokio::sync::mpsc::channel::<()>(1);
    let raft = RaftService::new(snd, args.nodes);

    _ = tokio::spawn(election_timer(
        raft.node.clone(), 
        rcv
    ));
    
    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
