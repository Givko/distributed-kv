use clap::Parser;
use raft::raft::raft_server::{Raft, RaftServer};
use raft::raft::{AppendEntriesMessage, AppendEntriesReply, RequestVoteMessage, RequestVoteReply};
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status, transport::Server};

use raft::raft_node::{
    AppendEntriesData, AppendEntriesReplyData, LogEntry, Node, RaftMsg, RequestVoteData,
    RequestVoteReplyData,
};

use raft::network_sender::{OutMsg, network_worker};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    // Port the node will use
    #[arg(short, long)]
    port: usize,

    #[arg(short, long, num_args = 1.., value_delimiter = ',')]
    nodes: Vec<String>,

    #[arg(short, long)]
    id: String,
}

#[derive(Debug)]
pub struct RaftService {
    mailbox: Sender<RaftMsg>,
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteMessage>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let (snd, rcv) = tokio::sync::oneshot::channel::<RequestVoteReplyData>();
        let message_data = RequestVoteData {
            term: request.get_ref().term,
            last_log_index: request.get_ref().last_log_index,
            last_log_term: request.get_ref().last_log_term,
            candidate: request.get_ref().candidate.clone(),
        };
        let vote_message = RaftMsg::VoteRequest {
            vote_request: message_data,
            reply_channel: Some(snd),
        };

        self.mailbox
            .send(vote_message)
            .await
            .expect("Failed to send vote message");
        let vote_reply = rcv.await.expect("Failed to receive vote reply");

        let reply = RequestVoteReply {
            term: vote_reply.term,
            vote: vote_reply.vote,
        };
        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesMessage>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let message = request.into_inner();
        let (snd, rcv) = tokio::sync::oneshot::channel::<AppendEntriesReplyData>();
        let append_entries_daata = AppendEntriesData {
            term: message.term,
            prev_log_index: message.prev_log_index,
            prev_log_term: message.prev_log_term,
            leader_commit: message.leader_commit,
            leader_id: message.leader_id.clone(),
            entries: message
                .entries
                .iter()
                .map(|e| LogEntry {
                    term: e.term,
                    command: e.command.clone(),
                })
                .collect(),
        };
        let append_message = RaftMsg::AppendEntries {
            append_request: append_entries_daata,
            reply_channel: Some(snd),
        };
        self.mailbox
            .send(append_message)
            .await
            .expect("Failed to send append entries message");
        let reply_data = rcv.await.expect("Failed to receive append entries reply");
        let reply = AppendEntriesReply {
            term: reply_data.term,
            success: reply_data.success,
        };

        Ok(Response::new(reply))
    }
}

impl RaftService {
    fn new(mailbox: Sender<RaftMsg>) -> Self {
        RaftService { mailbox }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    eprintln!("{}", args.port);
    eprintln!("{:?}", args.nodes);

    let addr = format!("127.0.0.1:{}", args.port).parse()?;
    let (mailbox_snd, mailbox_rcv) = tokio::sync::mpsc::channel::<RaftMsg>(100);
    let (outbox_snd, outbox_rcv) = tokio::sync::mpsc::channel::<OutMsg>(100);
    let mailbox_clone = mailbox_snd.clone();
    _ = tokio::spawn(async move { network_worker(outbox_rcv, mailbox_clone).await });

    let node: Node = Node::new(args.nodes, outbox_snd, args.id);
    _ = tokio::spawn(async move { node.run(mailbox_rcv).await });

    let raft = RaftService::new(mailbox_snd.clone());
    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
