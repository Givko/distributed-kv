pub mod raft {
    tonic::include_proto!("raft");
}

pub mod raft_node;
pub mod network_sender;