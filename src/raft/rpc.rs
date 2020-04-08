//! Define RPC calls and RPC service for Raft

use crate::raft::log::Log;
use slog::info;

/// AppendEntries RPC
#[derive(Debug)]
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
    pub entries: Vec<(i64, Log)>,
    /// leader's commit index
    pub leader_commit: i64,
}

/// AppendEntries RPC Reply
#[derive(Debug)]
pub struct AppendEntriesReply {
    /// current term, for leader to update itself
    pub term: i64,
    /// true if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,
}

impl AppendEntriesReply {
    pub fn new(term: i64, success: bool) -> Self {
        Self { term, success }
    }

    pub fn reply(self, msg_id: u64) -> RaftRPC {
        RaftRPC::AppendEntriesReply(msg_id, self)
    }
}

/// RequestVote RPC
#[derive(Debug)]
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
#[derive(Debug)]
pub struct RequestVoteReply {
    /// current term, for candidate to update itself
    pub term: i64,
    /// true means candidate received vote
    pub vote_granted: bool,
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

#[derive(Debug)]
pub enum RaftRPC {
    AppendEntries(AppendEntries),
    AppendEntriesReply(u64, AppendEntriesReply),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}

impl RaftRPC {
    pub fn term(&self) -> i64 {
        match self {
            RaftRPC::AppendEntries(AppendEntries { term, .. }) => *term,
            RaftRPC::AppendEntriesReply(_, AppendEntriesReply { term, .. }) => *term,
            RaftRPC::RequestVote(RequestVote { term, .. }) => *term,
            RaftRPC::RequestVoteReply(RequestVoteReply { term, .. }) => *term,
        }
    }
}

/// RPC Service used by Raft instance
pub trait RPCService : Send + Sync {
    /// send `msg` to `peer`, returns RPC id for this request
    fn send(&mut self, peer: u64, msg: RaftRPC) -> u64;
}

