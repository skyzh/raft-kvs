#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use futures::sync::mpsc::UnboundedSender;
use futures::{Future, Sink};
use fxhash::{FxHashMap, FxHashSet};
use labrpc::RpcFuture;
use rand::Rng;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

/// Raft role
#[derive(PartialEq, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub enum RPCEvents {
    RequestVoteReply(RequestVoteReply),
    AppendEntriesReply(AppendEntriesReply),
}

impl RPCEvents {
    pub fn term(&self) -> u64 {
        match self {
            RPCEvents::RequestVoteReply(x) => x.term,
            RPCEvents::AppendEntriesReply(x) => x.term,
        }
    }
}

pub struct RPCSequencer {
    rpc_id: u64,
}

impl Default for RPCSequencer {
    fn default() -> Self {
        RPCSequencer { rpc_id: 0 }
    }
}

impl RPCSequencer {
    pub fn send(&mut self) -> u64 {
        let rpc_id = self.rpc_id;
        self.rpc_id += 1;
        rpc_id
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct PersistCore {
    /// latest term server has seen (initialized to 0 on first boot)
    /// This is a persistent state.
    term: u64,
    /// candidate_id that received vote in current term
    /// This is a persistent state.
    voted_for: Option<u64>,
    /// log entries; each entry contains command for state machine, and term when
    /// entry was received by leader (first index is 1)
    /// This is a persistent state.
    log: Vec<(u64, Vec<u8>)>,
}

struct PersistState {
    /// core persistent data
    core: PersistCore,
    /// check if we should persist data before sending RPC
    dirty: bool,
}

impl PersistState {
    fn new(core: PersistCore) -> Self {
        Self { core, dirty: false }
    }
    fn term_mut(&mut self) -> &mut u64 {
        self.dirty = true;
        &mut self.core.term
    }

    fn voted_for_mut(&mut self) -> &mut Option<u64> {
        self.dirty = true;
        &mut self.core.voted_for
    }

    fn log_mut(&mut self) -> &mut Vec<(u64, Vec<u8>)> {
        self.dirty = true;
        &mut self.core.log
    }

    fn term(&self) -> u64 {
        self.core.term
    }
    fn voted_for(&self) -> Option<u64> {
        self.core.voted_for
    }
    fn log(&self) -> &Vec<(u64, Vec<u8>)> {
        &self.core.log
    }

    fn clean(&mut self) -> bool {
        let prev_dirty = self.dirty;
        self.dirty = false;
        prev_dirty
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: u64,

    /// state that should be persistent
    persist_state: PersistState,

    /// role of Raft instance
    /// This is raft-kvs internal state.
    role: Role,

    /// index of highest log entry known to be committed (initialized to 0)
    /// This is a volatile state.
    commit_index: u64,
    /// index of highest log entry applied to state machine (initialized to 0)
    /// This is a volatile state.
    last_applied: u64,
    /// for each server, index of the next log entry to send to that serve
    /// (initialized to leader last log index + 1)
    /// This is a volatile state for leaders.
    next_index: Vec<u64>,
    /// for each server, index of highest log entry known to be replicated on server
    ///(initialized to 0)
    /// This is a volatile state for leaders.
    match_index: Vec<u64>,
    /// if a node only requires a heartbeat, don't send it too frequently
    /// This is a volatile state for leaders.
    next_heartbeat: Vec<u128>,

    /// follower will start election after this time
    /// This is raft-kvs internal state. Should be reset when become follower.
    election_start_at: u128,

    /// candidate fails election after this time, and starts new election
    /// This is raft-kvs internal state. Should be reset when become candidate.
    election_timeout_at: u128,

    /// number of votes a candidate gets
    /// This is raft-kvs internal state. Should be reset when become candidate.
    vote_from: FxHashSet<u64>,

    /// time of booting Raft instance
    /// This is raft-kvs internal state.
    boot_time: Instant,

    /// rpc message sequence number
    /// This is raft-kvs internal state.
    rpc_sequence: RPCSequencer,

    /// rpc completion channel
    /// background thread should send events to this instance
    rpc_channel_tx: Option<Sender<(u64, u64, RPCEvents)>>,

    /// used to pair RPC request with RPC response. Will be periodically cleared.
    /// prev_log_index, current_tick, entries_length, failed attempt
    /// This is a raft-kvs internal state.
    rpc_append_entries_log_idx: FxHashMap<u64, (u64, u128, u64, u64)>,

    /// used to update RPC cache
    cache_next_update: u128,

    /// apply channel
    apply_ch: UnboundedSender<ApplyMsg>,

    /// last known leader
    lst_leader: Option<u64>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        let persist_core = PersistCore {
            log: vec![],
            term: 0,
            voted_for: None,
        };

        let mut rf = Raft {
            peers,
            persister,
            me: me as u64,
            role: Role::Follower,
            election_start_at: 0,
            election_timeout_at: 0,
            vote_from: FxHashSet::default(),
            boot_time: Instant::now(),
            persist_state: PersistState::new(persist_core),
            rpc_sequence: Default::default(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
            rpc_append_entries_log_idx: FxHashMap::default(),
            rpc_channel_tx: None,
            next_heartbeat: Vec::new(),
            cache_next_update: 0,
            apply_ch,
            lst_leader: None,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf.as_follower();

        rf
    }

    /// get time from instance start
    fn current_tick(&self) -> u128 {
        self.boot_time.elapsed().as_millis()
    }

    /// become follower
    fn as_follower(&mut self) {
        debug!(
            "@{} role transition: {:?} -> {:?}",
            self.me,
            self.role,
            Role::Follower
        );
        self.role = Role::Follower;
        *self.persist_state.voted_for_mut() = None;
        self.election_start_at = self.current_tick() + Self::tick_election_start_at();
    }

    /// become candidate
    fn as_candidate(&mut self) {
        debug!(
            "@{} role transition: {:?} -> {:?}",
            self.me,
            self.role,
            Role::Candidate
        );
        *self.persist_state.term_mut() += 1;
        self.role = Role::Candidate;
        self.begin_election();
    }

    /// become leader
    fn as_leader(&mut self) {
        debug!(
            "@{} role transition: {:?} -> {:?}",
            self.me,
            self.role,
            Role::Leader
        );
        self.role = Role::Leader;
        // initialize leader-related data structure
        self.match_index = Vec::new();
        self.next_index = Vec::new();
        self.next_heartbeat = Vec::new();
        for _ in 0..self.peers.len() {
            self.next_heartbeat.push(self.current_tick());
            self.match_index.push(0);
            self.next_index.push(self.last_log_index() + 1);
        }
        // send heartbeats to followers
        self.heartbeats();
    }

    /// begin election
    fn begin_election(&mut self) {
        *self.persist_state.voted_for_mut() = Some(self.me);
        self.vote_from = {
            let mut hashset = FxHashSet::default();
            hashset.insert(self.me);
            hashset
        };
        self.election_timeout_at = self.current_tick() + Self::tick_election_fail_at();
        for peer in 0..self.peers.len() {
            let peer = peer as u64;
            if peer != self.me {
                self.send_request_vote(
                    peer,
                    &RequestVoteArgs {
                        term: self.persist_state.term(),
                        candidate_id: self.me,
                        last_log_index: self.last_log_index(),
                        last_log_term: self.last_log_term(),
                    },
                );
            }
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        if self.persist_state.clean() {
            let mut buf = vec![];
            self.persist_state
                .core
                .serialize(&mut Serializer::new(&mut buf))
                .unwrap();
            self.persister.save_raft_state(buf);
        }
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        self.persist_state.core = rmp_serde::from_read(data).unwrap();
    }

    fn send_request_vote(&mut self, peer: u64, args: &RequestVoteArgs) -> u64 {
        // persist before sending RPC
        self.persist();

        let rpc_id = self.rpc_sequence.send();
        let rpc_peer = &self.peers[peer as usize];
        let tx_channel = self.rpc_channel_tx.clone();

        if let Some(tx_channel) = tx_channel {
            rpc_peer.spawn(
                rpc_peer
                    .request_vote(&args)
                    .map_err(Error::Rpc)
                    .then(move |res| {
                        if let Ok(res) = res {
                            tx_channel
                                .send((rpc_id, peer, RPCEvents::RequestVoteReply(res)))
                                .ok();
                        }
                        Ok(())
                    }),
            );
        }
        rpc_id
    }

    fn send_append_entries(&mut self, peer: u64, args: &AppendEntriesArgs) -> u64 {
        // persist before sending RPC
        self.persist();

        let rpc_id = self.rpc_sequence.send();
        let rpc_peer = &self.peers[peer as usize];
        let tx_channel = self.rpc_channel_tx.clone();

        if let Some(tx_channel) = tx_channel {
            rpc_peer.spawn(
                rpc_peer
                    .append_entries(&args)
                    .map_err(Error::Rpc)
                    .then(move |res| {
                        if let Ok(res) = res {
                            tx_channel
                                .send((rpc_id, peer, RPCEvents::AppendEntriesReply(res)))
                                .ok();
                        }
                        Ok(())
                    }),
            );
        }
        rpc_id
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role == Role::Leader {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            let term = self.persist_state.term();
            self.persist_state.log_mut().push((term, buf));
            self.debug_log();
            let index = self.last_log_index();
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    /// commit log if agree by majority of followers
    fn try_commit(&mut self) {
        let mut latest_match: Vec<u64> = self.match_index.clone();
        latest_match[self.me as usize] = self.last_log_index();
        latest_match.sort();
        let commit_idx = latest_match[self.peers.len() / 2]; // 4 -> 2, 5 -> 2
        debug!("@{} match index {:?}", self.me, self.match_index);
        if commit_idx > 0
            && commit_idx > self.commit_index
            && self.persist_state.log()[commit_idx as usize - 1].0 == self.persist_state.term()
        {
            self.commit_index = commit_idx;
            debug!(
                "@{} leader commit {:?}=>{}",
                self.me, self.match_index, self.commit_index
            );
            self.apply_message();
        }
    }

    /// apply log message
    fn apply_message(&mut self) {
        for idx in self.last_applied + 1..=self.commit_index {
            self.apply_ch
                .unbounded_send(ApplyMsg {
                    command_valid: true,
                    command_index: idx,
                    command: self.persist_state.log()[idx as usize - 1].1.clone(),
                })
                .unwrap();
        }
    }

    /// send heartbeats to followers
    fn heartbeats(&mut self) {
        for peer in 0..self.peers.len() {
            let peer = peer as u64;
            if peer != self.me {
                self.sync_log_with(peer, 0);
            }
        }
    }

    /// send heartbeat to peer
    fn heartbeat(&mut self, peer: u64) {
        let current_tick = self.current_tick();
        let next_heartbeat = &mut self.next_heartbeat[peer as usize];
        let send_heartbeat = if current_tick - *next_heartbeat >= Self::heartbeat_interval() {
            *next_heartbeat = current_tick;
            true
        } else {
            false
        };

        if send_heartbeat {
            self.send_append_entries(
                peer,
                &AppendEntriesArgs {
                    term: self.persist_state.term(),
                    leader_id: self.me,
                    prev_log_term: self.last_log_term(),
                    prev_log_index: self.last_log_index(),
                    entries: vec![],
                    entries_term: vec![],
                    leader_commit: self.commit_index,
                },
            );
        }
    }

    /// sync log to peer
    fn sync_log_with(&mut self, peer: u64, failed_attempt: u64) {
        let next_idx = self.next_index[peer as usize];
        if self.last_log_index() < next_idx {
            self.heartbeat(peer);
            return;
        }
        let prev_log_index = next_idx - 1;
        const MAX_ENTRY: usize = 100;
        let log = self.persist_state.log();
        let idx_last = if next_idx as usize + MAX_ENTRY <= log.len() {
            next_idx as usize + MAX_ENTRY
        } else {
            log.len()
        };
        let entries_iter = log[next_idx as usize - 1..idx_last].iter();
        let entries_iter_term = entries_iter.clone();
        let entries_term: Vec<u64> = entries_iter_term.map(|x| x.0).collect();
        let entries: Vec<Vec<u8>> = entries_iter.map(|x| x.1.clone()).collect();
        let entries_length = entries.len();
        let rpc_id = self.send_append_entries(
            peer,
            &AppendEntriesArgs {
                term: self.persist_state.term(),
                leader_id: self.me,
                prev_log_term: self.log_term_of(next_idx - 1),
                prev_log_index,
                entries,
                entries_term,
                leader_commit: self.commit_index,
            },
        );
        let current_tick = self.current_tick();
        self.rpc_append_entries_log_idx.insert(
            rpc_id,
            (
                prev_log_index,
                current_tick,
                entries_length as u64,
                failed_attempt,
            ),
        );
    }

    /// handle routine tasks
    fn tick(&mut self) {
        let current_tick = self.current_tick();
        match self.role {
            Role::Follower => {
                // start election is there's no reply from leader
                if current_tick > self.election_start_at {
                    self.as_candidate();
                }
            }
            Role::Candidate => {
                // restart election if there's not enough vote and there's no leader
                if current_tick > self.election_timeout_at {
                    self.as_candidate();
                }
            }
            Role::Leader => {
                // periodically send heartbeats to followers
                self.heartbeats();
            }
        }

        self.update_rpc_cache();
    }

    /// update RPC request-response pairing cache
    fn update_rpc_cache(&mut self) {
        let current_tick = self.current_tick();
        if current_tick > self.cache_next_update {
            self.cache_next_update = current_tick + 1000;
            let expired = std::mem::take(&mut self.rpc_append_entries_log_idx);
            for (req, (idx, timestamp, length, failed_attempt)) in expired.into_iter() {
                if timestamp + 3000 >= current_tick {
                    self.rpc_append_entries_log_idx
                        .insert(req, (idx, timestamp, length, failed_attempt));
                }
            }
        }
    }

    /// proceses request vote rpc
    fn on_rpc_request_vote(&mut self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        if args.term > self.persist_state.term() {
            self.as_follower();
            *self.persist_state.term_mut() = args.term;
        }
        let reply = match self.role {
            Role::Follower => {
                let vote_granted = match self.persist_state.voted_for() {
                    Some(candidate_id) => candidate_id == args.candidate_id,
                    None => {
                        args.last_log_term > self.last_log_term()
                            || (args.last_log_term == self.last_log_term()
                                && args.last_log_index >= self.last_log_index())
                    }
                };
                if vote_granted {
                    *self.persist_state.voted_for_mut() = Some(args.candidate_id);
                }
                self.election_start_at =
                    self.boot_time.elapsed().as_millis() + Self::tick_election_start_at();
                Box::new(futures::future::ok(RequestVoteReply {
                    term: self.persist_state.term(),
                    vote_granted,
                }))
            }
            _ => Box::new(futures::future::ok(RequestVoteReply {
                term: self.persist_state.term(),
                vote_granted: false,
            })),
        };
        // persist before replying to RPC
        self.persist();
        reply
    }

    fn debug_log(&self) {
        if log_enabled!(log::Level::Trace) {
            let mut x = String::new();
            for (term, log) in self.persist_state.log().iter() {
                x += format!("{} {} ({:?}), ", term, log.len(), log).as_ref();
            }
            trace!("@{} commit={} log={}", self.me, self.commit_index, x);
        }
    }

    /// proceess append entries rpc
    fn on_rpc_append_entries(&mut self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        if args.term > self.persist_state.term() {
            self.as_follower();
            *self.persist_state.term_mut() = args.term;
        }
        if args.term < self.persist_state.term() {
            return Box::new(futures::future::ok(AppendEntriesReply {
                term: self.persist_state.term(),
                success: false,
            }));
        }
        // degrade to follower if there's a leader
        if self.role == Role::Candidate && args.term == self.persist_state.term() {
            self.as_follower();
        }
        let reply = match self.role {
            Role::Follower => {
                self.lst_leader = Some(args.leader_id);
                self.election_start_at = self.current_tick() + Self::tick_election_start_at();

                let mut ok = false;
                if args.term < self.persist_state.term() {
                    trace!("@{} append entries failed: lower term", self.me);
                } else if args.prev_log_index > self.last_log_index() {
                    trace!("@{} append entries failed: log not found", self.me);
                } else if self.log_term_of(args.prev_log_index) == args.prev_log_term {
                    ok = true;
                } else {
                    trace!("@{} append entries failed: term not match", self.me);
                }
                if ok {
                    let log_entries = self.persist_state.log_mut();
                    let length = args.entries.len();
                    for (idx, log) in args
                        .entries_term
                        .into_iter()
                        .zip(args.entries.into_iter())
                        .enumerate()
                    {
                        let log_idx = args.prev_log_index as usize + idx;
                        if log_idx < log_entries.len() {
                            if log_entries[log_idx].0 != log.0 {
                                log_entries.drain(log_idx..);
                                log_entries.push(log);
                                trace!("@{} drain log {}", self.me, log_entries.len());
                            } else {
                                log_entries[log_idx] = log;
                            }
                        } else {
                            log_entries.push(log);
                        }
                    }
                    trace!(
                        "@{} append entries success {}, {}",
                        self.me,
                        length,
                        log_entries.len()
                    );
                    self.debug_log();
                    if args.leader_commit > self.commit_index {
                        self.commit_index = self.last_log_index().min(args.leader_commit);
                        self.apply_message();
                        debug!("@{} leader commit: {}", self.me, self.commit_index);
                    }
                }
                Box::new(futures::future::ok(AppendEntriesReply {
                    term: self.persist_state.term(),
                    success: ok,
                }))
            }
            _ => Box::new(futures::future::ok(AppendEntriesReply {
                term: self.persist_state.term(),
                success: false,
            })),
        };
        self.persist();
        reply
    }

    /// candidate rpc event
    fn candidate_rpc_event(&mut self, _rpc_id: u64, from: u64, event: RPCEvents) {
        if let RPCEvents::RequestVoteReply(reply) = event {
            if reply.vote_granted {
                self.vote_from.insert(from);
                if self.vote_from.len() * 2 >= self.peers.len() {
                    self.as_leader();
                }
            }
        }
    }

    fn leader_rpc_event(&mut self, rpc_id: u64, from: u64, event: RPCEvents) {
        if let RPCEvents::AppendEntriesReply(reply) = event {
            let prev_match_index = self.rpc_append_entries_log_idx.get(&rpc_id);
            if prev_match_index.is_none() {
                return;
            }
            let (prev_match_index, _, length, failed_attempt) = prev_match_index.unwrap();
            let length = *length;
            let prev_match_index = *prev_match_index;
            let failed_attempt = *failed_attempt;
            self.rpc_append_entries_log_idx.remove(&rpc_id);
            if reply.success {
                self.match_index[from as usize] = prev_match_index + length;
                self.next_index[from as usize] = prev_match_index + length + 1;
                self.try_commit();
            } else {
                let subtract_size = 2_u64.pow(failed_attempt as u32) - 1;
                let prev_match_index = if prev_match_index >= subtract_size {
                    prev_match_index - subtract_size
                } else {
                    0
                };
                self.next_index[from as usize] = prev_match_index.max(1);
                self.sync_log_with(from, failed_attempt + 1);
                debug!(
                    "@{} -> {} append failed, prev_match_index={}, from={}, attempt={}",
                    self.me, from, prev_match_index, from, failed_attempt
                );
            }
        }
    }

    /// called when there's RPC reply
    pub fn on_event(&mut self, rpc_id: u64, from: u64, event: RPCEvents) {
        if event.term() > self.persist_state.term() {
            self.as_follower();
            *self.persist_state.term_mut() = event.term();
        }
        match self.role {
            Role::Candidate => self.candidate_rpc_event(rpc_id, from, event),
            Role::Leader => self.leader_rpc_event(rpc_id, from, event),
            _ => {}
        }
    }

    /// generate random election timeout
    fn tick_election_fail_at() -> u128 {
        rand::thread_rng().gen_range::<u64>(150, 300) as u128
    }

    /// generate random election start
    fn tick_election_start_at() -> u128 {
        rand::thread_rng().gen_range::<u64>(150, 300) as u128
    }

    /// next heartbeat time
    fn heartbeat_interval() -> u128 {
        100
    }

    /// get last log index
    /// returns 0 if there's no log
    fn last_log_index(&self) -> u64 {
        self.persist_state.log().len() as u64
    }

    /// get last log term
    /// returns 0 if there's no log
    fn last_log_term(&self) -> u64 {
        self.log_term_of(self.last_log_index())
    }

    /// get term of log given id
    fn log_term_of(&self, id: u64) -> u64 {
        if id == 0 {
            0
        } else {
            self.persist_state.log()[id as usize - 1].0
        }
    }

    /// get who is believed to be leader
    fn believed_leader(&self) -> Option<u64> {
        if self.role == Role::Leader {
            Some(self.me)
        } else {
            self.lst_leader
        }
    }
}

#[derive(Clone)]
pub struct Node {
    /// Raft instance
    raft: Arc<Mutex<Option<Raft>>>,
    cancel: Arc<AtomicBool>,
    ticker: Arc<Option<JoinHandle<()>>>,
    poll_ticker: Arc<Option<JoinHandle<()>>>,
    executor: futures_cpupool::CpuPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        let me = raft.me;
        let (tx, rx) = channel();
        raft.rpc_channel_tx = Some(tx);
        let raft = Arc::new(Mutex::new(Some(raft)));
        let cancel = Arc::new(AtomicBool::new(false));

        let mut node = Node {
            raft,
            cancel,
            ticker: Arc::new(None),
            poll_ticker: Arc::new(None),
            executor: futures_cpupool::CpuPool::new_num_cpus(),
        };
        let cancel = node.cancel.clone();
        let raft = node.raft.clone();
        node.ticker = Arc::new(Some(std::thread::spawn(move || {
            info!("@{} start ticking task", me);
            while !cancel.load(SeqCst) {
                {
                    let mut raft = raft.lock().unwrap();
                    if let Some(raft) = raft.as_mut() {
                        raft.tick();
                    } else {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            info!("@{} stop ticking task", me);
        })));

        let raft = node.raft.clone();
        node.poll_ticker = Arc::new(Some(std::thread::spawn(move || {
            info!("@{} start polling rpc event", me);
            for (id, from, event) in rx.iter() {
                {
                    let mut raft = raft.lock().unwrap();
                    if let Some(raft) = raft.as_mut() {
                        raft.on_event(id, from, event);
                    } else {
                        break;
                    }
                }
                std::thread::yield_now();
            }
            info!("@{} stop polling rpc event", me);
        })));
        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if let Some(raft) = self.raft.lock().unwrap().as_mut() {
            raft.start(command)
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let mut raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            State {
                is_leader: raft.role == Role::Leader,
                term: raft.persist_state.term(),
            }
        } else {
            State {
                is_leader: false,
                term: 0,
            }
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        self.cancel.store(true, SeqCst);
        let mut raft = self.raft.lock().unwrap();
        *raft = None;
    }

    pub fn believed_leader(&self) -> Option<u64> {
        self.raft
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .believed_leader()
    }
}

impl RaftService for Node {
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let raft = self.raft.clone();
        Box::new(self.executor.spawn_fn(move || {
            let mut raft = raft.lock().unwrap();
            if let Some(raft) = raft.as_mut() {
                raft.on_rpc_request_vote(args)
            } else {
                Box::new(futures::future::err(labrpc::Error::Stopped))
            }
        }))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let raft = self.raft.clone();
        Box::new(self.executor.spawn_fn(move || {
            let mut raft = raft.lock().unwrap();
            if let Some(raft) = raft.as_mut() {
                raft.on_rpc_append_entries(args)
            } else {
                Box::new(futures::future::err(labrpc::Error::Stopped))
            }
        }))
    }
}
