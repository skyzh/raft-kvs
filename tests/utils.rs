use raft_kvs::raft::{event::*, instance::*, log::*, rpc::*};
use slog::{info, o, Drain};
use std::collections::HashMap;
use lazy_static::lazy_static;

lazy_static! {
    static ref LOGGER: slog::Logger = {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let log = slog::Logger::root(drain, o!());
        log
    };
}

pub fn new_test_raft_instance() -> Raft {
    Raft::new(vec![1, 2, 3, 4, 5], LOGGER.clone(), 1)
}

pub fn inspect_request_vote(rpc: &MockRPCService) -> HashMap<u64, u64> {
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

pub fn inspect_append_entries(rpc: &MockRPCService) -> HashMap<u64, u64> {
    let mut m: HashMap<u64, u64> = HashMap::new();
    for log in rpc.rpc_log.iter() {
        match log {
            (log_to, RaftRPC::AppendEntries(_)) => {
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

pub fn inspect_request_vote_to(rpc: &MockRPCService, to: u64) -> u64 {
    match inspect_request_vote(rpc).get(&to) {
        None => 0,
        Some(x) => *x,
    }
}

pub fn inspect_has_request_vote_to(rpc: &MockRPCService, to: u64) -> bool {
    match inspect_request_vote(rpc).get(&to) {
        None => false,
        Some(_) => true,
    }
}

pub fn inspect_request_vote_reply(rpc: &MockRPCService) -> HashMap<u64, u64> {
    let mut m: HashMap<u64, u64> = HashMap::new();
    for log in rpc.rpc_log.iter() {
        match log {
            (
                log_to,
                RaftRPC::RequestVoteReply(RequestVoteReply {
                    vote_granted: true, ..
                }),
            ) => {
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

pub fn inspect_request_vote_reply_to(rpc: &MockRPCService, to: u64) -> u64 {
    match inspect_request_vote_reply(rpc).get(&to) {
        None => 0,
        Some(x) => *x,
    }
}

pub fn inspect_has_request_vote_reply_to(rpc: &MockRPCService, to: u64) -> bool {
    match inspect_request_vote_reply(rpc).get(&to) {
        None => false,
        Some(_) => true,
    }
}

pub fn get_leader_instance() -> Raft {
    let mut r = new_test_raft_instance();
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
    r
}

pub fn inspect_append_entries_content(rpc: &MockRPCService, find_log: &Log) -> HashMap<u64, ()> {
    let mut m: HashMap<u64, ()> = HashMap::new();
    for log in rpc.rpc_log.iter() {
        match log {
            (log_to, RaftRPC::AppendEntries(AppendEntries { entries, .. })) => {
                if entries
                    .into_iter()
                    .map(|x| x.1.clone())
                    .collect::<Vec<Log>>()
                    .contains(find_log)
                {
                    m.insert(*log_to, ());
                }
            }
            _ => {}
        };
    }
    return m;
}

pub fn inspect_has_append_entries_content_to(
    rpc: &MockRPCService,
    find_log: &Log,
    to: u64,
) -> bool {
    match inspect_append_entries_content(rpc, find_log).get(&to) {
        Some(_) => true,
        None => false,
    }
}

pub fn random_log() -> Log {
    Log::Get("2333".into())
}
