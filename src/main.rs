use clap::Parser;
use distributed_kv::raft::proto::raft_server::{Raft, RaftServer};
use distributed_kv::raft::proto::{
    AppendEntriesMessage, AppendEntriesReply, GetMessage, GetReply, RequestVoteMessage,
    RequestVoteReply, SetMessage, SetReply,
};
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status, transport::Server};

use distributed_kv::raft::network_types::OutMsg;
use distributed_kv::LsmTreeNode;
use distributed_kv::raft::raft_types::{
    AppendEntriesData, AppendEntriesReplyData, ChangeStateReply, LogEntry, RaftMsg,
    RequestVoteData, RequestVoteReplyData,
};
use distributed_kv::raft::network_sender::network_worker;

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
    async fn get(&self, request: Request<GetMessage>) -> Result<Response<GetReply>, Status> {
        let get_command = request.into_inner();
        let (snd, rcv) = tokio::sync::oneshot::channel::<String>();
        let raft_msg = RaftMsg::GetState {
            key: get_command.key,
            reply_channel: snd,
        };
        self.mailbox
            .send(raft_msg)
            .await
            .expect("failed to send raft msg");
        let value = rcv.await.expect("did not rcv from reply channel");
        Ok(Response::new(GetReply { value }))
    }
    async fn set(&self, _request: Request<SetMessage>) -> Result<Response<SetReply>, Status> {
        let msg = _request.into_inner();
        eprintln!("set {}: {}", msg.key, msg.value);

        let command = format!("SET {} {}", msg.key, msg.value);
        let (snd, rcv) = tokio::sync::oneshot::channel::<ChangeStateReply>();
        let raft_msg = RaftMsg::ChangeState {
            command,
            reply_channel: Some(snd),
        };
        self.mailbox
            .send(raft_msg)
            .await
            .expect("failed to send to raft");
        let reply = rcv.await.expect("failed to rcv reply");
        if !reply.success {
            return Err(Status::cancelled("node is not leader")); // TODO: return leader URL 
        }
        Ok(Response::new(SetReply {
            success: true,
            leader: String::new(), // TODO: Add leader to response
        }))
    }
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
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("{}", args.port);
    eprintln!("{:?}", args.nodes);

    let addr = format!("127.0.0.1:{}", args.port).parse()?;
    let (mailbox_snd, mailbox_rcv) = tokio::sync::mpsc::channel::<RaftMsg>(100);
    let (outbox_snd, outbox_rcv) = tokio::sync::mpsc::channel::<OutMsg>(100);
    let mailbox_clone = mailbox_snd.clone();
    _ = tokio::spawn(async move { network_worker(outbox_rcv, mailbox_clone).await });

    let persister = distributed_kv::raft::state_persister::FilePersistentStorage::new(args.id.clone());
    let storage_engine = distributed_kv::storage::lsm_tree::LSMTree::with_node_id(&args.id).await;
    let node = LsmTreeNode::new(args.nodes, outbox_snd, args.id, persister, storage_engine).await?;
    _ = tokio::spawn(async move { node.run(mailbox_rcv).await });

    let raft = RaftService::new(mailbox_snd.clone());
    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
