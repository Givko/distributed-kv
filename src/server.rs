//! Assembly root: builds a running node out of its parts and serves it.
//!
//! This is the one place that knows how the layers fit together — consensus
//! (`raft`), persistence (`storage`), and the two transports (`raft::network_*`
//! for peers, `api::http` for clients). Everything below is unaware of the
//! others; nothing here implements protocol or algorithm logic itself.

use crate::LsmTreeNode;
use crate::api::http;
use crate::raft::network_receiver::RaftService;
use crate::raft::network_sender::network_worker;
use crate::raft::network_types::OutMsg;
use crate::raft::proto::raft_server::RaftServer;
use crate::raft::raft_types::RaftMsg;
use crate::raft::state_persister::FilePersistentStorage;
use crate::storage::lsm_tree::LSMTree;
use std::net::SocketAddr;
use tonic::transport::Server;

const CHANNEL_CAPACITY: usize = 100;

pub struct ServerConfig {
    pub id: String,
    pub peers: Vec<String>,
    pub grpc_addr: SocketAddr,
    pub http_addr: SocketAddr,
}

pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    let (mailbox_snd, mailbox_rcv) = tokio::sync::mpsc::channel::<RaftMsg>(CHANNEL_CAPACITY);
    let (outbox_snd, outbox_rcv) = tokio::sync::mpsc::channel::<OutMsg>(CHANNEL_CAPACITY);

    let worker_mailbox = mailbox_snd.clone();
    _ = tokio::spawn(async move { network_worker(outbox_rcv, worker_mailbox).await });

    let persister = FilePersistentStorage::new(config.id.clone());
    let storage_engine = LSMTree::with_node_id(&config.id).await;
    let node = LsmTreeNode::new(
        config.peers,
        outbox_snd,
        config.id,
        persister,
        storage_engine,
    )
    .await?;
    _ = tokio::spawn(async move { node.run(mailbox_rcv).await });

    let grpc_server = Server::builder()
        .add_service(RaftServer::new(RaftService::new(mailbox_snd.clone())))
        .serve(config.grpc_addr);
    let http_server = http::serve(config.http_addr, mailbox_snd);

    tokio::try_join!(
        async { grpc_server.await.map_err(anyhow::Error::from) },
        http_server
    )?;

    Ok(())
}
