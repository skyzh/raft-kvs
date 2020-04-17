use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

use futures::sync::mpsc::UnboundedSender;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use futures::{Future, Async};
use rand::Rng;
use std::collections::{HashSet, HashMap};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
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
    pub voted_for: Option<u64>,
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
    /// candidate_id that received vote in current term
    pub fn voted_for(&self) -> Option<u64> {
        self.voted_for
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

pub enum RPCEventsFuture {
    RequestVoteReply(RpcFuture<RequestVoteReply>),
    AppendEntriesReply(RpcFuture<AppendEntriesReply>),
}

pub struct RPCSequencer {
    rpc_id: u64,
    rpc_futures: HashMap<u64, (u64, RPCEventsFuture)>,
    // events: Vec<(u64, u64, RPCEvents)>
}

impl RPCSequencer {
    pub fn new(peer_len: usize) -> Self {
        RPCSequencer { rpc_id: 0, rpc_futures: HashMap::new(), events: Vec::new() }
    }

    pub fn send(&mut self, to: u64, f: RPCEventsFuture) -> u64 {
        let rpc_id = self.rpc_id;
        self.rpc_id += 1;

        self.rpc_futures.insert(rpc_id, (to, f));

        return rpc_id;
    }

    pub fn poll(&mut self) -> Vec<(u64, u64, RPCEvents)> {
        let current_rpc = std::mem::replace(&mut self.rpc_futures, HashMap::new());
        let mut events = vec![];
        for (id, (from, mut future)) in current_rpc {
            match future {
                RPCEventsFuture::RequestVoteReply(mut future) => {
                    match future.poll() {
                        Ok(Async::Ready(t)) => { events.push((id, from, RPCEvents::RequestVoteReply(t))); }
                        Ok(Async::NotReady) => { self.rpc_futures.insert(id, (from, RPCEventsFuture::RequestVoteReply(future))); }
                        Err(e) => { warn!("rpc error: {:?}", e); }
                    }
                }
                RPCEventsFuture::AppendEntriesReply(mut future) => {
                    match future.poll() {
                        Ok(Async::Ready(t)) => { events.push((id, from, RPCEvents::AppendEntriesReply(t))); }
                        Ok(Async::NotReady) => { self.rpc_futures.insert(id, (from, RPCEventsFuture::AppendEntriesReply(future))); }
                        Err(e) => { warn!("rpc error: {:?}", e); }
                    }
                }
            }
        }
        events
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

    /// role of Raft instance
    /// This is raft-kvs internal state.
    role: Role,

    /// index of highest log entry known to be committed (initialized to 0)
    /// This is a volatile state.
    commit_index: u64,
    /// index of highest log entry applied to state machine (initialized to 0)
    /// This is a volatile state.
    last_applied: u64,

    /// follower will start election after this time
    /// This is raft-kvs internal state. Should be reset when become follower.
    election_start_at: u128,

    /// candidate fails election after this time, and starts new election
    /// This is raft-kvs internal state. Should be reset when become candidate.
    election_timeout_at: u128,

    /// number of votes a candidate gets
    /// This is raft-kvs internal state. Should be reset when become candidate.
    vote_from: HashSet<u64>,

    /// time of booting Raft instance
    /// This is raft-kvs internal state.
    boot_time: Instant,

    /// rpc message sequence number
    /// This is raft-kvs internal state.
    rpc_sequence: RPCSequencer,
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
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let peers_len = peers.len();

        let mut rf = Raft {
            peers,
            persister,
            me: me as u64,
            log: vec![],
            role: Role::Follower,
            election_start_at: 0,
            election_timeout_at: 0,
            vote_from: HashSet::new(),
            boot_time: Instant::now(),
            term: 0,
            voted_for: None,
            rpc_sequence: RPCSequencer::new(peers_len),
            commit_index: 0,
            last_applied: 0,
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
        info!("{} role transition: {:?} -> {:?}", self.me, self.role, Role::Follower);
        self.role = Role::Follower;
        self.voted_for = None;
        self.election_start_at = self.current_tick() + Self::tick_election_start_at();
    }

    /// become candidate
    fn as_candidate(&mut self) {
        info!("{} role transition: {:?} -> {:?}", self.me, self.role, Role::Candidate);
        self.term += 1;
        self.role = Role::Candidate;
        self.begin_election();
    }

    /// become leader
    fn as_leader(&mut self) {
        info!("{} role transition: {:?} -> {:?}", self.me, self.role, Role::Leader);
        self.role = Role::Leader;
    }

    /// begin election
    fn begin_election(&mut self) {
        self.voted_for = Some(self.me);
        self.vote_from = HashSet::new();
        self.vote_from.insert(self.me);
        self.election_timeout_at = self.current_tick() + Self::tick_election_fail_at();
        for peer in 0..self.peers.len() {
            let peer = peer as u64;
            if peer != self.me {
                self.send_request_vote(peer, &RequestVoteArgs {
                    term: self.term,
                    candidate_id: self.me,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                });
            }
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        self.persister.save_raft_state(vec![]);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    fn send_request_vote(&mut self, peer: u64, args: &RequestVoteArgs) -> u64 {
        self.rpc_sequence.send(peer, RPCEventsFuture::RequestVoteReply(self.peers[peer as usize].request_vote(&args)))
    }

    fn send_append_entries(&mut self, peer: u64, args: &AppendEntriesArgs) -> u64 {
        self.rpc_sequence.send(peer, RPCEventsFuture::AppendEntriesReply(self.peers[peer as usize].append_entries(&args)))
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
        where
            M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    /// send heartbeats to followers
    fn heartbeats(&mut self) {
        for peer in 0..self.peers.len() {
            let peer = peer as u64;
            if peer != self.me {
                self.sync_log_with(peer);
            }
        }
    }

    /// sync log to peer
    fn sync_log_with(&mut self, peer: u64) {
        self.send_append_entries(peer, &AppendEntriesArgs {
            term: self.term,
            leader_id: self.me,
            prev_log_term: 0,
            prev_log_index: 0,
            entries: vec![],
            entries_term: vec![],
            leader_commit: self.commit_index,
        });
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
    }

    /// proceses request vote rpc
    fn on_rpc_request_vote(&mut self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        if args.term > self.term {
            self.as_follower();
            self.term = args.term;
        }
        match self.role {
            Role::Follower => {
                let vote_granted = match self.voted_for {
                    Some(candidate_id) => candidate_id == args.candidate_id,
                    None => {
                        // TODO: how to check up-to-date?
                        args.last_log_index >= self.last_log_index()
                    }
                };
                if vote_granted {
                    self.voted_for = Some(args.candidate_id);
                }
                self.election_start_at =
                    self.boot_time.elapsed().as_millis() + Self::tick_election_start_at();
                Box::new(futures::future::ok(RequestVoteReply {
                    term: self.term,
                    vote_granted,
                }))
            }
            _ => {
                Box::new(futures::future::ok(RequestVoteReply {
                    term: self.term,
                    vote_granted: false,
                }))
            }
        }
    }

    /// proceess append entries rpc
    fn on_rpc_append_entries(&mut self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        match self.role {
            Role::Follower => {
                self.election_start_at =
                    self.boot_time.elapsed().as_millis() + Self::tick_election_start_at();
                Box::new(futures::future::ok(AppendEntriesReply {
                    term: self.term,
                    success: false,
                }))
            }
            _ => {
                Box::new(futures::future::ok(AppendEntriesReply {
                    term: self.term,
                    success: false,
                }))
            }
        }
    }

    /// candidate rpc event
    fn candidate_rpc_event(&mut self, _rpc_id: u64, from: u64, event: RPCEvents) {
        match event {
            RPCEvents::RequestVoteReply(reply) => {
                if reply.vote_granted {
                    self.vote_from.insert(from);
                    if self.vote_from.len() * 2 >= self.peers.len() {
                        self.as_leader();
                    }
                }
            }
            _ => {}
        }
    }

    pub fn on_event(&mut self, rpc_id: u64, from: u64, event: RPCEvents) {
        match self.role {
            Role::Candidate => self.candidate_rpc_event(rpc_id, from, event),
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

    /// get last log index
    /// returns 0 if there's no log
    fn last_log_index(&self) -> u64 {
        self.log.len() as u64
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
            self.log[id as usize - 1].0
        }
    }
}

#[derive(Clone)]
pub struct Node {
    /// Raft instance
    raft: Arc<Mutex<Raft>>,
    cancel: Arc<AtomicBool>,
    ticker: Arc<Option<JoinHandle<()>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let raft = Arc::new(Mutex::new(raft));
        let cancel = Arc::new(AtomicBool::new(false));
        let mut node = Node {
            raft,
            cancel,
            ticker: Arc::new(None),
        };
        let cancel = node.cancel.clone();
        let raft = node.raft.clone();
        node.ticker = Arc::new(Some(std::thread::spawn(move || {
            while !cancel.load(SeqCst) {
                {
                    let mut raft = raft.lock().unwrap();
                    for (id, from, event) in raft.rpc_sequence.poll() {
                        raft.on_event(id, from, event);
                    }
                    raft.tick();
                }
                std::thread::sleep(Duration::from_millis(10));
            }
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
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.lock().unwrap().term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let raft = self.raft.lock().unwrap();
        State {
            is_leader: raft.role == Role::Leader,
            term: raft.term,
            voted_for: raft.voted_for,
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
        self.raft.lock().unwrap().persist();
        self.cancel.store(true, SeqCst);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // self.cancel.store(true, SeqCst);
    }
}

impl RaftService for Node {
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let mut raft = self.raft.lock().unwrap();
        raft.on_rpc_request_vote(args)
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        raft.on_rpc_append_entries(args)
    }
}
