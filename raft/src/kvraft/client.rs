use std::fmt;

use crate::proto::kvraftpb::*;
use futures::Future;
use futures_timer::FutureExt;
use rand::Rng;
use std::io::Error;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    leader: Arc<AtomicUsize>,
    client_id: u64,
    request_seq: Arc<AtomicU64>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        Self {
            name,
            servers,
            leader: Arc::new(AtomicUsize::new(0)),
            client_id: rand::thread_rng().gen(),
            request_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_from_server(&self, idx: usize, arg: &GetRequest) -> Option<String> {
        debug!("->{} get", idx);
        let server = &self.servers[idx];
        if let Ok(reply) = server
            .get(arg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            .timeout(Duration::from_secs(1))
            .wait()
        {
            if !reply.wrong_leader && reply.err == "" {
                info!("get <- {} {}", idx, reply.value);
                Some(reply.value)
            } else {
                debug!("->{} {}", idx, reply.err);
                None
            }
        } else {
            debug!("->{} timeout", idx);
            None
        }
    }

    pub fn put_append_to_server(&self, idx: usize, arg: &PutAppendRequest) -> Option<()> {
        debug!("->{} put append", idx);
        let server = &self.servers[idx];
        if let Ok(reply) = server
            .put_append(arg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            .timeout(Duration::from_secs(1))
            .wait()
        {
            if !reply.wrong_leader && reply.err == "" {
                Some(())
            } else {
                debug!("->{} {}", idx, reply.err);
                None
            }
        } else {
            debug!("->{} timeout", idx);
            None
        }
    }

    pub fn get(&self, key: String) -> String {
        let arg = GetRequest { key };
        let leader = self.leader.load(Ordering::SeqCst);
        if let Some(x) = self.get_from_server(leader, &arg) {
            return x;
        }
        loop {
            for idx in 0..self.servers.len() {
                if let Some(x) = self.get_from_server(idx, &arg) {
                    self.leader.store(idx, Ordering::SeqCst);
                    return x;
                }
            }
            std::thread::yield_now();
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        let arg = {
            let client_id = self.client_id;
            let request_id = self.request_seq.fetch_add(1, Ordering::SeqCst);
            match op {
                Op::Put(key, value) => PutAppendRequest {
                    key,
                    value,
                    client_id,
                    request_id,
                    op: crate::proto::kvraftpb::Op::Put as i32,
                },
                Op::Append(key, value) => PutAppendRequest {
                    key,
                    value,
                    client_id,
                    request_id,
                    op: crate::proto::kvraftpb::Op::Append as i32,
                },
            }
        };
        let leader = self.leader.load(Ordering::SeqCst);
        if self.put_append_to_server(leader, &arg).is_some() {
            return;
        }
        loop {
            for idx in 0..self.servers.len() {
                if self.put_append_to_server(idx, &arg).is_some() {
                    self.leader.store(idx, Ordering::SeqCst);
                    return;
                }
            }
            std::thread::yield_now();
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
