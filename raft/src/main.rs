use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rand::Rng;
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
}

#[derive(Debug)]
pub struct RaftService {
    node: Arc<RwLock<Node>>,
    elect_timer_reset_tx: Sender<()>,
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Candidate {
        voted_for: usize,
        votes: usize,
    },
    Leader,
    Follower {
        leader: String,
        current_timer: Duration,
    },
}

impl Default for State {
    fn default() -> Self {
        let mut rng = rand::rng();
        let duration  = rng.random_range(150..300);
        State::Follower {
            leader: String::new(),
            current_timer: Duration::from_millis(duration),
        }
    }
    
}

#[derive(Debug, Default)]
struct Node {
    current_term: usize,
    state: State,
}

impl Node {
    fn is_leader(&self) -> bool {
        self.state == State::Leader
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
    fn new(snd: Sender<()>) -> Self {
        RaftService {
            node: Arc::new(RwLock::new(Node::default())),
            elect_timer_reset_tx: snd,
        }
    }
}

async fn election_timer(mut node: Arc<RwLock<Node>>, mut reset_rx: Receiver<()>) {
    loop {
        let duration = rand::rng().random_range(150..300);
        let timeout_duration = Duration::from_millis(duration);

        if node.read().await.is_leader() {
            // If we're the leader, we don't need to run the election timer
            continue;
        }

        tokio::select! {
            _ = tokio::time::sleep(timeout_duration) => {
                // Election timeout elapsed, start a new election
                //eprintln!("Election timeout elapsed, starting new election");
                // Here you would typically transition to Candidate state and start the election process
            }
            _ = reset_rx.recv() => {
                // Received a reset signal, restart the timer
                eprintln!("Election timer reset");
                continue;
            }
        }

    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    eprintln!("{}", args.port);

    let addr = "127.0.0.1:5050".parse()?;
    let (snd, rcv) = tokio::sync::mpsc::channel::<()>(1);
    let raft = RaftService::new(snd);

    let election_timer = tokio::spawn(election_timer(
        raft.node.clone(), 
        rcv
        /* reset_rx */,
    ));
    
    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
