use futures::sync::mpsc::{unbounded, UnboundedReceiver};

use labrpc::RpcFuture;

use crate::proto::kvraftpb::{kv_log::*, *};
use crate::raft;
use crate::raft::ApplyMsg;
use futures::future::Executor;
use futures::sync::oneshot;
use futures::{Future, Stream};
use fxhash::{FxHashMap, FxHashSet};
use std::sync::{Arc, Mutex};

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    maxraftstate: Option<usize>,
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,
    kvstore: FxHashMap<String, String>,
    client_store: FxHashMap<u64, u64>,
    apply_queue: FxHashMap<u64, (oneshot::Sender<Option<String>>, KvLog)>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let rf = raft::Node::new(rf);

        Self {
            rf,
            maxraftstate,
            me,
            apply_ch: Some(apply_ch),
            kvstore: FxHashMap::default(),
            client_store: FxHashMap::default(),
            apply_queue: FxHashMap::default(),
        }
    }

    pub fn get_apply_channel(&mut self) -> Option<UnboundedReceiver<ApplyMsg>> {
        std::mem::take(&mut self.apply_ch)
    }
}
pub type OpFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send + 'static>;

impl KvServer {
    fn op(&mut self, op: KvLog) -> OpFuture<String, String> {
        match self.rf.start(&op) {
            Ok((idx, _)) => {
                let (tx, rx) = oneshot::channel();
                let out = self.apply_queue.insert(idx, (tx, op));
                if let Some((tx, _)) = out {
                    tx.send(None).unwrap();
                }
                Box::new(
                    rx.map_err(|_| "error while waiting for commit".to_string())
                        .and_then(move |result| {
                            if let Some(value) = result {
                                futures::future::ok(value)
                            } else {
                                futures::future::err("unmatched log".to_string())
                            }
                        }),
                )
            }
            Err(raft::errors::Error::NotLeader) => {
                trace!("error when append entries: not leader");
                Box::new(futures::future::err("not leader".to_string()))
            }
            Err(e) => {
                trace!("error when append entries: {:?}", e);
                Box::new(futures::future::err(e.to_string()))
            }
        }
    }

    fn apply_log(&mut self, log: KvLog) -> Option<String> {
        if log.op == LogOp::Get as i32 {
            Some(match self.kvstore.get(&log.key) {
                Some(v) => {
                    trace!("@{} {}={}", self.me, log.key, v);
                    v.clone()
                }
                None => String::new(),
            })
        } else {
            let seq = self.client_store.entry(log.client_id).or_insert(0);
            if log.request_id > *seq {
                *seq = log.request_id;
                if log.op == LogOp::Put as i32 {
                    self.kvstore.insert(log.key, log.value);
                } else if log.op == LogOp::Append as i32 {
                    let entry = self
                        .kvstore
                        .entry(log.key.clone())
                        .or_insert_with(String::new);
                    *entry += &log.value;
                }
                trace!("@{} kv={:?}", self.me, self.kvstore);
            }
            None
        }
    }
}

#[derive(Clone)]
pub struct Node {
    server: Arc<Mutex<Option<KvServer>>>,
    executor: futures_cpupool::CpuPool,
}

impl Node {
    pub fn new(mut kv: KvServer) -> Node {
        let apply_ch = kv.get_apply_channel().unwrap();
        let me = kv.me;
        let server = Arc::new(Mutex::new(Some(kv)));
        let node = Self {
            server: server.clone(),
            executor: futures_cpupool::CpuPool::new_num_cpus(),
        };

        let apply = apply_ch
            .for_each(move |x| {
                let result = {
                    let mut server_lock = server.lock().unwrap();
                    if let Some(server) = server_lock.as_mut() {
                        // apply log first
                        debug!("@{} applying {}", server.me, x.command_index);
                        let log_to_apply = labcodec::decode(&x.command).unwrap();
                        let value = server.apply_log(log_to_apply).unwrap_or_default();
                        // then check if there's pending request
                        if let Some((tx, log_sent)) = server.apply_queue.remove(&x.command_index) {
                            let mut log_sent_encoded = vec![];
                            labcodec::encode(&log_sent, &mut log_sent_encoded).unwrap();
                            if server.rf.is_leader() && x.command == log_sent_encoded {
                                debug!("@{} responding {}", server.me, x.command_index);
                                tx.send(Some(value)).ok();
                            } else {
                                tx.send(None).ok();
                            }
                        }
                        Ok(())
                    } else {
                        Err(())
                    }
                };
                result
            })
            .map_err(move |e| info!("@{} stop applying", me));

        node.executor.spawn(apply).forget();

        node
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        let mut server = self.server.lock().unwrap();
        server.as_mut().unwrap().rf.kill();
        *server = None;
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        let mut server = self.server.lock().unwrap();
        server.as_mut().unwrap().rf.get_state()
    }

    fn op_to_log_op(x: i32) -> i32 {
        if x == Op::Put as i32 {
            LogOp::Put as i32
        } else if x == Op::Append as i32 {
            LogOp::Append as i32
        } else {
            x
        }
    }
}

impl KvService for Node {
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        let server = self.server.clone();
        Box::new(self.executor.spawn_fn(move || {
            {
                let mut server = server.lock().unwrap();
                if let Some(server) = server.as_mut() {
                    trace!("@{} get {:?}", server.me, arg);
                    server.op(KvLog {
                        key: arg.key,
                        value: String::new(),
                        op: LogOp::Get as i32,
                        client_id: 0,
                        request_id: 0,
                    })
                } else {
                    Box::new(futures::future::err("already killed".to_string()))
                }
            }
            .map(|value| GetReply {
                err: String::new(),
                value,
                wrong_leader: false,
            })
            .or_else(|err| {
                let err_str = err.to_string();
                if err_str == "not leader" {
                    futures::future::ok(GetReply {
                        err: err_str,
                        value: String::new(),
                        wrong_leader: true,
                    })
                } else {
                    futures::future::ok(GetReply {
                        err,
                        value: String::new(),
                        wrong_leader: false,
                    })
                }
            })
        }))
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        let server = self.server.clone();
        Box::new(self.executor.spawn_fn(move || {
            {
                let mut server = server.lock().unwrap();
                if let Some(server) = server.as_mut() {
                    trace!("@{} put {:?}", server.me, arg);
                    server.op(KvLog {
                        key: arg.key,
                        value: arg.value,
                        op: Self::op_to_log_op(arg.op),
                        client_id: arg.client_id,
                        request_id: arg.request_id,
                    })
                } else {
                    Box::new(futures::future::err("already killed".to_string()))
                }
            }
            .map(|_| PutAppendReply {
                err: String::new(),
                wrong_leader: false,
            })
            .or_else(|err| {
                let err_str = err.to_string();
                if err_str == "not leader" {
                    futures::future::ok(PutAppendReply {
                        err: err_str,
                        wrong_leader: true,
                    })
                } else {
                    futures::future::ok(PutAppendReply {
                        err,
                        wrong_leader: false,
                    })
                }
            })
        }))
    }
}
