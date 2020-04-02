//! Events for Raft instance

use crate::raft::rpc::RaftRPC;

pub enum RaftEvent {
    RPC((u64, u64, RaftRPC)),
    Log,
    Commit,
}
