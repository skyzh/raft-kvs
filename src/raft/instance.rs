//! Define Raft instance

use crate::raft::event::RaftEvent;
use crate::raft::log::Log;
use crate::raft::rpc::{
    AppendEntries, AppendEntriesReply, RPCService, RaftRPC, RequestVote, RequestVoteReply,
};
use rand::Rng;
use slog::{info, trace};
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
    pub current_term: i64,
    /// candidate_id that received vote in current term
    /// This is a persistent state.
    pub voted_for: Option<u64>,
    /// log entries; each entry contains command for state machine, and term when
    /// entry was received by leader (first index is 1)
    /// This is a persistent state.
    pub log: Vec<(i64, Log)>,

    /// index of highest log entry known to be committed (initialized to 0)
    /// This is a volatile state.
    pub commit_index: i64,
    /// index of highest log entry applied to state machine (initialized to 0)
    /// This is a volatile state.
    pub last_applied: i64,

    /// for each server, index of the next log entry to send to that serve
    /// (initialized to leader last log index + 1)
    /// This is a volatile state for leaders.
    pub next_index: HashMap<u64, i64>,
    /// for each server, index of highest log entry known to be replicated on server
    ///(initialized to 0)
    /// This is a volatile state for leaders.
    pub match_index: HashMap<u64, i64>,

    /// RPC service for Raft
    /// This is raft-kvs internal state.
    pub rpc: Box<dyn RPCService>,
    /// role of Raft instance
    /// This is raft-kvs internal state.
    pub role: Role,
    /// known peers, must include `self_id`
    /// This is raft-kvs internal state.
    pub known_peers: Vec<u64>,
    /// id of myself
    /// This is raft-kvs internal state.
    pub id: u64,

    /// follower will start election after this time
    /// This is raft-kvs internal state. Should be reset when become follower.
    election_start_at: u64,

    /// candidate fails election after this time, and starts new election
    /// This is raft-kvs internal state. Should be reset when become candidate.
    election_timeout_at: u64,

    /// number of votes a candidate gets
    /// This is raft-kvs internal state. Should be reset when become candidate.
    vote_from: HashMap<u64, ()>,

    /// used to pair RPC request with RPC response. Will be periodically cleared.
    /// This is a raft-kvs internal state.
    pub rpc_append_entries_log_idx: HashMap<u64, (i64, u64, u64)>,

    /// logger
    /// This is raft-kvs internal state.
    logger: slog::Logger,
}

impl Raft {
    /// create new raft instance
    pub fn new(
        known_peers: Vec<u64>,
        logger: slog::Logger,
        rpc: Box<dyn RPCService>,
        id: u64,
        current_tick: u64,
    ) -> Self {
        let mut instance = Raft {
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            rpc,
            role: Role::Follower,
            election_start_at: 0,
            election_timeout_at: 0,
            known_peers,
            id,
            vote_from: HashMap::new(),
            logger,
            rpc_append_entries_log_idx: HashMap::new(),
        };
        instance.become_follower(current_tick);
        instance
    }

    /// timer tick
    /// `current_tick` is current system time
    pub fn tick(&mut self, current_tick: u64) {
        match self.role {
            Role::Follower => {
                // start election is there's no reply from leader
                if current_tick > self.election_start_at {
                    self.become_candidate(current_tick);
                }
            }
            Role::Candidate => {
                // restart election if there's not enough vote and there's no leader
                if current_tick > self.election_timeout_at {
                    self.become_candidate(current_tick);
                }
            }
            Role::Leader => {
                // periodically send heartbeats to followers
                self.heartbeats(current_tick);
            }
        }
        self.update_rpc_cache(current_tick);
    }

    /// update RPC request-response pairing cache
    fn update_rpc_cache(&mut self, current_tick: u64) {
        let expired = std::mem::replace(&mut self.rpc_append_entries_log_idx, HashMap::new());
        for (req, (idx, timestamp, length)) in expired.into_iter() {
            if timestamp + 10000 >= current_tick {
                self.rpc_append_entries_log_idx
                    .insert(req, (idx, timestamp, length));
            }
        }
    }

    /// send heartbeats to followers
    fn heartbeats(&mut self, current_tick: u64) {
        for peer in self.known_peers.clone().iter() {
            let peer = *peer;
            if peer != self.id {
                self.sync_log_with(peer, current_tick);
            }
        }
    }

    /// send heartbeat to peer
    fn heartbeat(&mut self, peer: u64) {
        let entries = vec![];
        self.rpc.send(
            peer,
            AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_term: self.last_log_term(),
                prev_log_index: self.last_log_index(),
                entries,
                leader_commit: self.commit_index,
            }
                .into(),
        );
    }

    /// sync log to peer
    fn sync_log_with(&mut self, peer: u64, current_tick: u64) {
        let next_idx = *self.next_index.get(&peer).unwrap();
        if self.last_log_index() < next_idx {
            self.heartbeat(peer);
            return;
        }
        let prev_log_index = next_idx - 1;
        // TODO: limit maximum entries
        let entries: Vec<(i64, Log)> = self.log[next_idx as usize - 1..].iter().map(|x| x.clone()).collect();
        let entries_length = entries.len();
        let rpc_id = self.rpc.send(
            peer,
            AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_term: self.log_term_of(next_idx - 1),
                prev_log_index,
                entries,
                leader_commit: self.commit_index,
            }
                .into(),
        );
        self.rpc_append_entries_log_idx
            .insert(rpc_id, (prev_log_index, current_tick, entries_length as u64));
    }

    /// become a follower
    fn become_follower(&mut self, current_tick: u64) {
        info!(self.logger, "role transition"; "role" => format!("{:?}->{:?}", self.role, Role::Follower));
        self.role = Role::Follower;
        self.voted_for = None;
        self.election_start_at = current_tick + Self::tick_election_start_at();
    }

    /// become a candidate
    fn become_candidate(&mut self, current_tick: u64) {
        info!(self.logger, "role transition"; "role" => format!("{:?}->{:?}", self.role, Role::Candidate));
        self.current_term += 1;
        self.role = Role::Candidate;
        self.begin_election(current_tick);
    }

    // become a leader
    fn become_leader(&mut self, current_tick: u64) {
        info!(self.logger, "role transition"; "role" => format!("{:?}->{:?}", self.role, Role::Leader));
        self.role = Role::Leader;
        // initialize leader-related data structure
        self.match_index = HashMap::new();
        self.next_index = HashMap::new();
        for peer in self.known_peers.iter() {
            let peer = *peer;
            if peer != self.id {
                self.match_index.insert(peer, 0);
                self.next_index.insert(peer, self.last_log_index() + 1);
            }
        }
        // send heartbeats to followers
        self.heartbeats(current_tick);
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
        self.log_term_of(self.last_log_index())
    }

    /// get term of log given id
    fn log_term_of(&self, id: i64) -> i64 {
        if id == 0 {
            0
        } else {
            self.log[id as usize - 1].0 as i64
        }
    }

    /// candidate rpc event
    fn candidate_rpc_event(&mut self, from: u64, _msg_id: u64, event: RaftRPC, current_tick: u64) {
        match event {
            RaftRPC::RequestVoteReply(reply) => {
                if reply.vote_granted {
                    self.vote_from.entry(from).or_insert(());
                    if self.vote_from.len() * 2 >= self.known_peers.len() {
                        self.become_leader(current_tick);
                    }
                }
            }
            RaftRPC::AppendEntries(request) => {
                if request.term == self.current_term {
                    self.become_follower(current_tick);
                    return;
                }
            }
            _ => {}
        }
    }

    /// follower rpc event
    fn follower_rpc_event(&mut self, from: u64, msg_id: u64, event: RaftRPC, current_tick: u64) {
        match event {
            RaftRPC::RequestVote(request) => {
                if request.term < self.current_term {
                    self.rpc.send(
                        from,
                        RequestVoteReply {
                            term: self.current_term,
                            vote_granted: false,
                        }
                            .into(),
                    );
                    return;
                }
                let vote_granted = match self.voted_for {
                    Some(candidate_id) => candidate_id == request.candidate_id,
                    None => {
                        // TODO: how to check up-to-date?
                        request.last_log_index >= self.last_log_index()
                    }
                };
                if vote_granted {
                    self.voted_for = Some(request.candidate_id);
                }
                self.election_start_at = current_tick + Self::tick_election_start_at();
                self.rpc.send(
                    from,
                    RequestVoteReply {
                        term: self.current_term,
                        vote_granted,
                    }
                        .into(),
                );
            }
            RaftRPC::AppendEntries(request) => {
                // reset election timer
                self.election_start_at = current_tick + Self::tick_election_start_at();

                let mut ok = false;
                if request.term < self.current_term {
                    info!(self.logger, "append entries failed"; "reason" => "lower term");
                } else if request.prev_log_index > self.last_log_index() {
                    info!(self.logger, "append entries failed"; "reason" => "log not found");
                } else if self.log_term_of(request.prev_log_index) == request.prev_log_term {
                    ok = true;
                } else {
                    info!(self.logger, "append entries failed"; "reason" => "term not match");
                }

                if ok {
                    let length = request.entries.len();
                    for (idx, log) in request.entries.into_iter().enumerate() {
                        let log_idx = request.prev_log_index as usize + idx;
                        if log_idx < self.log.len() {
                            self.log[log_idx] = log;
                        } else {
                            self.log.push(log);
                        }
                    }
                    trace!(self.logger, "append entries success";
                            "entries_processed" => length,
                            "log_length" => self.log.len());
                }
                self.rpc.send(
                    from,
                    AppendEntriesReply::new(self.current_term, ok).reply(msg_id),
                );
            }
            _ => {}
        }
    }

    /// leader rpc event
    fn leader_rpc_event(&mut self, from: u64, _msg_id: u64, event: RaftRPC, _current_tick: u64) {
        if let RaftRPC::AppendEntriesReply(reply_to, response) = event {
            let prev_match_index = self.rpc_append_entries_log_idx.get(&reply_to);
            if prev_match_index.is_none() {
                return;
            }
            let (prev_match_index, _, length) = prev_match_index.unwrap();
            let length = *length as i64;
            let prev_match_index = *prev_match_index;
            self.rpc_append_entries_log_idx.remove(&reply_to);
            if response.success {
                *self.match_index.get_mut(&from).unwrap() = prev_match_index + length;
                *self.next_index.get_mut(&from).unwrap() = prev_match_index + length + 1;
                self.try_commit();
            } else {
                *self.next_index.get_mut(&from).unwrap() = prev_match_index;
                info!(self.logger, "append failed";
                            "prev_match_index" => prev_match_index, "from" => from);
            }
        }
    }

    /// commit log if agree by majority of followers
    fn try_commit(&mut self) {
        let mut latest_match: Vec<i64> = self.match_index.iter().map(|x| *x.1).collect();
        latest_match.push(self.last_log_index());
        latest_match.sort();
        let commit_idx = latest_match[self.known_peers.len() / 2]; // 4 -> 2, 5 -> 2
        self.commit_index = commit_idx;
    }

    /// process Raft event
    pub fn on_event(&mut self, event: RaftEvent, current_tick: u64) {
        match event {
            RaftEvent::RPC((from, msg_id, event)) => {
                if event.term() > self.current_term {
                    self.current_term = event.term();
                    self.become_follower(current_tick);
                }
                match self.role {
                    Role::Follower => self.follower_rpc_event(from, msg_id, event, current_tick),
                    Role::Candidate => self.candidate_rpc_event(from, msg_id, event, current_tick),
                    Role::Leader => self.leader_rpc_event(from, msg_id, event, current_tick),
                }
            }
            _ => unimplemented!(),
        }
    }

    /// append log
    /// returns (term, idx) if is leader
    pub fn append_log(&mut self, log: Log, current_tick: u64) -> Option<(i64, i64)> {
        if self.role != Role::Leader {
            return None;
        }
        self.log.push((self.current_term, log));
        let term = self.last_log_term();
        let idx = self.last_log_index();
        for peer in self.known_peers.clone().iter() {
            let peer = *peer;
            if peer != self.id {
                self.sync_log_with(peer, current_tick);
            }
        }
        return Some((term, idx));
    }

    /// generate random election timeout
    fn tick_election_fail_at() -> u64 {
        rand::thread_rng().gen_range(150, 300)
    }

    /// generate random election start
    fn tick_election_start_at() -> u64 {
        rand::thread_rng().gen_range(150, 300)
    }
}
