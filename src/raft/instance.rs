//! Define Raft instance

use crate::raft::log::Log;
use crate::raft::rpc::{MockRPCService, RequestVote};
use std::collections::HashMap;

/// Raft role
#[derive(PartialEq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// A Raft instance
pub struct Raft {
    /// latest term server has seen (initialized to 0 on first boot)
    /// This is a persistent state.
    current_term: i64,
    /// candidate_id that received vote in current term
    /// This is a persistent state.
    voted_for: Option<u64>,
    /// log entries; each entry contains command for state machine, and term when
    /// entry was received by leader (first index is 1)
    /// This is a persistent state.
    log: Vec<(i64, Log)>,
    /// index of highest log entry known to be committed (initialized to 0)
    /// This is a volatile state.
    commit_index: i64,
    /// index of highest log entry applied to state machine (initialized to 0)
    /// This is a volatile state.
    last_applied: i64,
    /// for each server, index of the next log entry to send to that serve
    /// (initialized to leader last log index + 1)
    /// This is a volatile state for leaders.
    next_index: HashMap<i64, i64>,
    /// for each server, index of highest log entry known to be replicated on server
    ///(initialized to 0)
    /// This is a volatile state for leaders.
    match_index: HashMap<i64, i64>,

    /// RPC service for Raft
    /// This is raft-kvs internal state.
    rpc: MockRPCService,
    /// role of Raft instance
    /// This is raft-kvs internal state.
    role: Role,
    /// follower will start election after this time
    /// This is raft-kvs internal state.
    election_expire_at: u64,
    /// known peers, must include `self_id`
    /// This is raft-kvs internal state.
    known_peers: Vec<u64>,
    /// id of myself
    id: u64,
}

impl Raft {
    /// create new raft instance
    pub fn new() -> Self {
        Raft {
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            rpc: MockRPCService::new(),
            role: Role::Follower,
            election_expire_at: 0,
            known_peers: vec![1, 2, 3, 4, 5],
            id: 1,
        }
    }
    /// timer tick
    /// tick is current system time
    pub fn tick(&mut self, tick: u64) {
        match self.role {
            Role::Follower => {
                if tick > self.election_expire_at {
                    self.become_follower();
                }
            }
            Role::Candidate => {}
            Role::Leader => {}
        }
    }

    /// become a follower
    fn become_follower(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.begin_election();
    }

    /// begin election
    fn begin_election(&mut self) {
        self.voted_for = Some(self.id);
        for peer in self.known_peers.iter() {
            let peer = *peer;
            if peer != self.id {
                self.rpc.send(
                    peer,
                    RequestVote {
                        term: self.current_term,
                        candidate_id: self.id,
                        last_log_index: self.last_log_index(),
                        last_log_term: self.last_log_term(),
                    }.into(),
                );
            }
        }
    }

    fn last_log_index(&self) -> i64 {
        self.log.len() as i64
    }

    fn last_log_term(&self) -> i64 {
        match self.log.last() {
            Some(x) => x.0,
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::rpc::RaftRPC;

    #[test]
    fn test_new() {
        let r = Raft::new();
    }

    fn inspect_has_request_vote_to(rpc: &MockRPCService, to: u64) -> bool {
        for log in rpc.rpc_log.iter() {
            match log {
                (log_to, RaftRPC::RequestVote(_)) => {
                    if *log_to == to {
                        return true;
                    }
                }
                _ => {}
            };
        }
        return false;
    }

    #[test]
    fn test_become_candidate() {
        let mut r = Raft::new();
        r.tick(1000);
        assert_eq!(r.role, Role::Candidate);
        assert_eq!(r.current_term, 1);
        assert!(inspect_has_request_vote_to(&r.rpc, 2));
        assert!(inspect_has_request_vote_to(&r.rpc, 3));
        assert!(inspect_has_request_vote_to(&r.rpc, 4));
        assert!(inspect_has_request_vote_to(&r.rpc, 5));
    }
}
