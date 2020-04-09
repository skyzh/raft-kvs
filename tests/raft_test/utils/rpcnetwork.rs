use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::HashMap;
use raft_kvs::raft::rpc::RaftRPC;
use lazy_static::lazy_static;
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use slog::debug;

lazy_static! {
    static ref GLOBAL_TIME: Instant = { Instant::now() };
}

pub fn now() -> u64 {
    GLOBAL_TIME.elapsed().as_millis() as u64
}

pub struct RPCNetwork {
    msg_queue: PriorityQueue<u64, Reverse<u64>>,
    msg: HashMap<u64, (u64, u64, u64, RaftRPC)>,
    total_msg_id: u64,
    disabled: HashMap<u64, ()>,
    log: slog::Logger,
}

impl RPCNetwork {
    pub fn new(log: slog::Logger) -> Self {
        Self {
            msg_queue: PriorityQueue::new(),
            disabled: HashMap::new(),
            msg: HashMap::new(),
            total_msg_id: 0,
            log,
        }
    }

    pub fn deliver(&mut self, from: u64, msg_id: u64, to: u64, msg: RaftRPC) {
        if self.disabled.contains_key(&from) || self.disabled.contains_key(&to) {
            return;
        }
        let time: u64 = now();
        self.msg.insert(self.total_msg_id, (from, msg_id, to, msg));
        self.msg_queue.push(self.total_msg_id, Reverse(time));
        self.total_msg_id += 1;
    }

    pub fn disable(&mut self, id: u64) {
        self.disabled.insert(id, ());
    }

    pub fn enable(&mut self, id: u64) {
        self.disabled.remove(&id);
    }

    pub fn disabled(&self, id: u64) -> bool {
        self.disabled.contains_key(&id)
    }

    pub fn process(&mut self) -> Vec<(u64, u64, u64, RaftRPC)> {
        let time: u64 = now();
        let mut q = vec![];
        loop {
            let result = match self.msg_queue.peek() {
                Some((_, x)) => x.0 <= time,
                None => false,
            };

            if result {
                let e = self.msg_queue.pop().unwrap().0;
                let e = self.msg.remove(&e).unwrap();
                if !self.disabled(e.2) {
                    debug!(self.log, "rpc message"; "from" => e.0, "to" => e.2, "seq" => e.1, "msg" => format!("{:?}", e.3));
                    q.push(e);
                } else {
                    debug!(self.log, "rpc message (dropped)"; "from" => e.0, "to" => e.2, "seq" => e.1, "msg" => format!("{:?}", e.3));
                }
            } else {
                break;
            }
        }
        q
    }
}
