use std::fmt;

use crate::proto::kvraftpb::*;
use futures::Future;
use std::time::Duration;
use std::sync::{Arc, Mutex};

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    leader: Arc<Mutex<u64>>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        Self { name, servers, leader: Arc::new(Mutex::new(0)) }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        let leader = *self.leader.lock().unwrap();
        let arg = GetRequest { key };
        for idx in 0..=self.servers.len() {
            let server = if idx == 0 {
                &self.servers[leader as usize]
            } else {
                &self.servers[idx - 1]
            };
            if let Ok(reply) = server.get(&arg).wait() {
                return reply.value;
            }
        }
        info!("key not found");
        "".to_string()
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        let leader = *self.leader.lock().unwrap();
        let arg = match op {
            Op::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: crate::proto::kvraftpb::Op::Put as i32,
            },
            Op::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: crate::proto::kvraftpb::Op::Append as i32,
            },
        };
        loop {
            for idx in 0..=self.servers.len() {
                let server = if idx == 0 {
                    &self.servers[leader as usize]
                } else {
                    &self.servers[idx - 1]
                };
                if let Ok(reply) = server.put_append(&arg).wait() {
                    if !reply.wrong_leader && reply.err == "" {
                        if idx != 0 { *self.leader.lock().unwrap() = idx as u64 - 1; }
                        return;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
