pub mod raft {
    tonic::include_proto!("proto");
}

pub mod network_sender;
pub mod network_types;
pub mod raft_node;
pub mod raft_types;
pub mod state_persister;
