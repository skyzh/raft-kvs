use futures::sync::mpsc::{unbounded, UnboundedReceiver};

use labrpc::RpcFuture;

use crate::proto::kvraftpb::*;
use crate::raft;
use crate::raft::ApplyMsg;
use futures::future::FutureResult;
use futures::sync::oneshot;
use futures::{Async, Future, Poll, Stream};
use fxhash::FxHashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    maxraftstate: Option<usize>,
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,
    kvstore: FxHashMap<String, String>,
    apply_queue: FxHashMap<u64, (oneshot::Sender<bool>, KvLog)>,
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
            apply_queue: FxHashMap::default(),
        }
    }

    pub fn get_apply_channel(&mut self) -> Option<UnboundedReceiver<ApplyMsg>> {
        std::mem::take(&mut self.apply_ch)
    }
}
pub type OpFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send + 'static>;

impl KvServer {
    fn op(&mut self, op: KvLog) -> OpFuture<bool, String> {
        match self.rf.start(&op) {
            Ok((idx, _)) => {
                let (tx, rx) = oneshot::channel();
                let out = self.apply_queue.insert(idx, (tx, op));
                if let Some((tx, _)) = out {
                    tx.send(false).unwrap();
                }
                let me = self.me;
                Box::new(
                    rx.map(move |x| {
                        info!("@{} commit: {}", me, idx);
                        x
                    })
                    .map_err(|_| "error while waiting for commit".to_string()),
                )
            }
            Err(raft::errors::Error::NotLeader) => {
                info!("error when append entries: not leader");
                Box::new(futures::future::err("not leader".to_string()))
            }
            Err(e) => {
                info!("error when append entries: {:?}", e);
                Box::new(futures::future::err(e.to_string()))
            }
        }
    }
}

#[derive(Clone)]
pub struct Node {
    server: Arc<Mutex<KvServer>>,
    executor: futures_cpupool::CpuPool,
}

impl Node {
    pub fn new(mut kv: KvServer) -> Node {
        let apply_ch = kv.get_apply_channel().unwrap();
        let me = kv.me;
        let server = Arc::new(Mutex::new(kv));
        let mut node = Self {
            server: server.clone(),
            executor: futures_cpupool::CpuPool::new_num_cpus(),
        };

        std::thread::spawn(move || {
            info!("@{} looping...", me);
            apply_ch
                .for_each(|x| {
                    {
                        let mut server = server.lock().unwrap();
                        // info!("@{} apply {}", server.me, x.command_index);
                        match server.apply_queue.remove(&x.command_index) {
                            Some((tx, log_sent)) => {
                                let mut log_sent_encoded = vec![];
                                labcodec::encode(&log_sent, &mut log_sent_encoded).unwrap();
                                tx.send(x.command == log_sent_encoded);
                            }
                            None => {
                                // info!("@{} failed to apply {}", me, x.command_index);
                            }
                        }
                    }
                    Ok(())
                })
                .wait();
            info!("@{} exit", me);
        });

        node
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
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
        self.server.lock().unwrap().rf.get_state()
    }
}

impl KvService for Node {
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        let message = {
            let mut server = self.server.lock().unwrap();
            info!("@{} get {:?}", server.me, arg);
            server
                .op(KvLog {
                    key: arg.key,
                    value: "".to_string(),
                    op: LogOp::Get as i32,
                })
                .map_err(|_| labrpc::Error::Stopped)
        };
        Box::new(message.map(|x| {
            if x {
                GetReply {
                    err: "".to_string(),
                    value: "here".to_string(),
                    wrong_leader: false,
                }
            } else {
                GetReply {
                    err: "true".to_string(),
                    value: "".to_string(),
                    wrong_leader: false,
                }
            }
        }))
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        let message = {
            let mut server = self.server.lock().unwrap();
            info!("@{} put {:?}", server.me, arg);
            server
                .op(KvLog {
                    key: arg.key,
                    value: arg.value,
                    op: LogOp::PutAppend as i32,
                })
                .map_err(|_| labrpc::Error::Stopped)
        };
        Box::new(message.map(|x| {
            if x {
                PutAppendReply {
                    err: "".to_string(),
                    wrong_leader: false,
                }
            } else {
                PutAppendReply {
                    err: "true".to_string(),
                    wrong_leader: false,
                }
            }
        }))
    }
}
