//! Define RPC calls and RPC service for Raft

use crate::raft::instance::Raft;
use crate::raft::log::Log;

/// AppendEntries RPC
pub struct AppendEntries {
    /// leader's term
    pub term: i64,
    /// for followers redirect clients
    pub leader_id: u64,
    /// index of log entry immediately preceding new ones
    pub prev_log_index: i64,
    /// term of prev_log_index entry
    pub prev_log_term: i64,
    /// log entries to store (empty for heartbeat)
    pub entries: Vec<Log>,
    /// leader's commit index
    pub leader_commit: i64,
}

/// AppendEntries RPC Reply
pub struct AppendEntriesReply {
    /// current term, for leader to update itself
    pub term: i64,
    /// true if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,
}

/// RequestVote RPC
pub struct RequestVote {
    /// candidate's term
    pub term: i64,
    /// candidate requesting vote
    pub candidate_id: u64,
    /// index of candidate's last log entry
    pub last_log_index: i64,
    /// term of candidate's last log entry
    pub last_log_term: i64,
}

/// RequestVote RPC Reply
pub struct RequestVoteReply {
    /// current term, for candidate to update itself
    pub term: i64,
    /// true means candidate received vote
    pub vote_granted: bool,
}

impl Into<RaftRPC> for AppendEntriesReply {
    fn into(self) -> RaftRPC {
        RaftRPC::AppendEntriesReply(self)
    }
}

impl Into<RaftRPC> for AppendEntries {
    fn into(self) -> RaftRPC {
        RaftRPC::AppendEntries(self)
    }
}

impl Into<RaftRPC> for RequestVote {
    fn into(self) -> RaftRPC {
        RaftRPC::RequestVote(self)
    }
}

impl Into<RaftRPC> for RequestVoteReply {
    fn into(self) -> RaftRPC {
        RaftRPC::RequestVoteReply(self)
    }
}

pub enum RaftRPC {
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}

pub struct MockRPCService {
    pub rpc_log: Vec<(u64, RaftRPC)>,
}

impl MockRPCService {
    pub fn new() -> Self {
        Self { rpc_log: vec![] }
    }
    pub fn send(&mut self, peer: u64, log: RaftRPC) {
        self.rpc_log.push((peer, log));
    }
}
