//! Client-facing API layer.
//!
//! Everything here sits *above* consensus: it translates external requests into
//! `RaftMsg`s for the node and turns the replies back into protocol responses.
//! The peer-to-peer Raft transport lives in `raft::network_sender` instead.

pub mod http;
