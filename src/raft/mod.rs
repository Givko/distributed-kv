pub mod network_sender;
pub mod network_types;
pub mod node;
pub mod raft_types;
pub mod state_persister;
pub mod state_machine;

pub use state_machine::StorageEngine;

pub mod proto {
    tonic::include_proto!("proto");
}