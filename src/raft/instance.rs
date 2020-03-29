//! Define Raft instance

use crate::raft::event::RaftEvent;
use crate::raft::log::Log;
use crate::raft::rpc::{MockRPCService, RaftRPC, RequestVote};
use rand::Rng;
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
    /// known peers, must include `self_id`
    /// This is raft-kvs internal state.
    known_peers: Vec<u64>,
    /// id of myself
    /// This is raft-kvs internal state.
    id: u64,

    /// follower will start election after this time
    /// This is raft-kvs internal state. Should be reset when become follower.
    election_start_at: u64,

    /// candidate fails election after this time, and starts new election
    /// This is raft-kvs internal state. Should be reset when become candidate.
    election_timeout_at: u64,

    /// number of votes a candidate gets
    /// This is raft-kvs internal state. Should be reset when become candidate.
    vote_from: HashMap<u64, ()>,
}

impl Raft {
    /// create new raft instance
    pub fn new(known_peers: Vec<u64>) -> Self {
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
            election_start_at: 0,
            election_timeout_at: 0,
            known_peers,
            id: 1,
            vote_from: HashMap::new(),
        }
    }

    /// timer tick
    /// `current_tick` is current system time
    pub fn tick(&mut self, current_tick: u64) {
        match self.role {
            Role::Follower => {
                if current_tick > self.election_start_at {
                    self.become_follower(current_tick);
                }
            }
            Role::Candidate => {
                if current_tick > self.election_timeout_at {
                    self.begin_election(current_tick);
                }
            }
            Role::Leader => {}
        }
    }

    /// become a follower
    fn become_follower(&mut self, current_tick: u64) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.begin_election(current_tick);
    }

    // become a leader
    fn become_leader(&mut self) {
        self.role = Role::Leader;
    }

    /// begin election
    fn begin_election(&mut self, current_tick: u64) {
        self.voted_for = Some(self.id);
        // TODO: persistent
        self.vote_from = HashMap::new();
        self.vote_from.insert(self.id, ());
        self.election_timeout_at = current_tick + Self::tick_election_fail_at();
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
                    }
                        .into(),
                );
            }
        }
    }

    /// get last log index
    /// returns 0 if there's no log
    fn last_log_index(&self) -> i64 {
        self.log.len() as i64
    }

    /// get last log term
    /// returns 0 if there's no log
    fn last_log_term(&self) -> i64 {
        match self.log.last() {
            Some(x) => x.0,
            None => 0,
        }
    }

    /// process Raft event
    pub fn on_event(&mut self, event: RaftEvent, current_tick: u64) {
        match event {
            RaftEvent::RPC((from, event)) => match event {
                RaftRPC::RequestVote(request) => {}
                RaftRPC::RequestVoteReply(reply) => {
                    if reply.vote_granted {
                        self.vote_from.insert(from, ());
                        if self.vote_from.len() * 2 >= self.known_peers.len() {
                            self.become_leader();
                        }
                    }
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }

    /// generate random election timeout
    fn tick_election_fail_at() -> u64 {
        rand::thread_rng().gen_range(200, 300)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::rpc::{RaftRPC, RequestVoteReply};

    #[test]
    fn test_new() {
        let r = test_raft_instance();
    }

    fn inspect_request_vote(rpc: &MockRPCService) -> HashMap<u64, u64> {
        let mut m: HashMap<u64, u64> = HashMap::new();
        for log in rpc.rpc_log.iter() {
            match log {
                (log_to, RaftRPC::RequestVote(_)) => {
                    let log_to = *log_to;
                    match m.get_mut(&log_to) {
                        Some(x) => {
                            *x += 1;
                        }
                        None => {
                            m.insert(log_to, 1);
                        }
                    }
                }
                _ => {}
            };
        }
        return m;
    }

    fn inspect_request_vote_to(rpc: &MockRPCService, to: u64) -> u64 {
        match inspect_request_vote(rpc).get(&to) {
            None => 0,
            Some(x) => *x,
        }
    }

    fn inspect_has_request_vote_to(rpc: &MockRPCService, to: u64) -> bool {
        match inspect_request_vote(rpc).get(&to) {
            None => false,
            Some(_) => true,
        }
    }

    fn test_raft_instance() -> Raft {
        Raft::new(vec![1, 2, 3, 4, 5])
    }

    #[test]
    fn test_become_candidate() {
        let mut r = test_raft_instance();
        r.tick(1000);
        // should change role and increase term number
        assert_eq!(r.role, Role::Candidate);
        assert_eq!(r.current_term, 1);
        // should begin election
        assert!(inspect_has_request_vote_to(&r.rpc, 2));
        assert!(inspect_has_request_vote_to(&r.rpc, 3));
        assert!(inspect_has_request_vote_to(&r.rpc, 4));
        assert!(inspect_has_request_vote_to(&r.rpc, 5));
        assert!(!inspect_has_request_vote_to(&r.rpc, 1));
    }

    #[test]
    fn test_restart_election() {
        let mut r = test_raft_instance();
        r.tick(1000);
        // should have started election
        assert_eq!(r.role, Role::Candidate);
        r.tick(2000);
        // should have started another election
        assert_eq!(r.role, Role::Candidate);
        assert_eq!(inspect_request_vote_to(&r.rpc, 2), 2);
        assert_eq!(inspect_request_vote_to(&r.rpc, 3), 2);
        assert_eq!(inspect_request_vote_to(&r.rpc, 4), 2);
        assert_eq!(inspect_request_vote_to(&r.rpc, 5), 2);
    }

    #[test]
    fn test_win_election() {
        let mut r = test_raft_instance();
        r.tick(1000);
        // should have started election
        assert_eq!(r.role, Role::Candidate);
        // send mock RPC to raft instance
        for i in 2..=3 {
            r.on_event(
                RaftEvent::RPC((
                    i as u64,
                    RequestVoteReply {
                        term: 1,
                        vote_granted: true,
                    }
                        .into(),
                )),
                100 + i,
            );
        }
        assert_eq!(r.role, Role::Leader);
    }

    #[test]
    fn test_election_not_enough_vote() {
        let mut r = test_raft_instance();
        r.tick(1000);
        // should have started election
        assert_eq!(r.role, Role::Candidate);
        // send mock RPC to raft instance
        for i in 1..=5 {
            r.on_event(
                RaftEvent::RPC((
                    2,
                    RequestVoteReply {
                        term: 1,
                        vote_granted: true,
                    }
                        .into(),
                )),
                100 + i,
            );
        }
        assert_eq!(r.role, Role::Candidate);
    }
}
