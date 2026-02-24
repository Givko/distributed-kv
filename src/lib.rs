pub mod raft;
pub mod storage;

/// Concrete node wired with file-backed persistence and the in-memory KV engine.
pub type LsmTreeNode = raft::node::Node<
    raft::state_persister::FilePersistentStorage,
    storage::lsm_tree::LSMTree,
>;