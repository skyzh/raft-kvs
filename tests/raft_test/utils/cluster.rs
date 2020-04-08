use crate::system_utils::RaftSystem;
use crate::utils::LOGGER;
use lazy_static::lazy_static;
use priority_queue::PriorityQueue;
use raft_kvs::raft::event::RaftEvent;
use raft_kvs::raft::event::RaftEvent::{Log, RPC};
use raft_kvs::raft::instance::Raft;
use raft_kvs::raft::rpc::{RPCService, RaftRPC};
use slog::{debug, info, o, trace, Logger};
use std::cmp::Reverse;
use std::collections::HashMap;
use std::panic;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

lazy_static! {
    static ref global_time: Instant = { Instant::now() };
}

pub fn now() -> u64 {
    global_time.elapsed().as_millis() as u64
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

pub fn deliver_all_message(cluster: &mut Vec<Arc<Mutex<Raft>>>, network: Arc<Mutex<RPCNetwork>>) {
    let time: u64 = now();
    let msg = network.lock().unwrap().process();
    for (from, msg_id, to, msg) in msg {
        if 0 <= to && to < cluster.len() as u64 {
            let mut instance = cluster[to as usize].lock().unwrap();
            instance.on_event(RaftEvent::RPC((from, msg_id, msg)), time);
        }
    }
}

pub fn tick_cluster(cluster: &mut Vec<Raft>) {
    for instance in cluster {
        instance.tick(now());
    }
}

pub struct RPCClient {
    rpc_network: Arc<Mutex<RPCNetwork>>,
    id: u64,
    msg_id: u64,
}

impl RPCClient {
    pub fn new(rpc_network: Arc<Mutex<RPCNetwork>>, id: u64) -> Self {
        Self {
            rpc_network,
            id,
            msg_id: 0,
        }
    }
}

impl RPCService for RPCClient {
    fn send(&mut self, peer: u64, msg: RaftRPC) -> u64 {
        let mut rpc = self.rpc_network.lock().unwrap();
        rpc.deliver(self.id, self.msg_id, peer, msg);
        let prev = self.msg_id;
        self.msg_id += 1;
        prev
    }
}

pub fn with_cluster<F>(number: usize, func: F)
where
    F: FnOnce(RaftSystem) + std::marker::Send + 'static,
{
    let mut raft_instance = vec![];
    let known_peers: Vec<u64> = (0..number).map(|x| x as u64).collect();
    let network = Arc::new(Mutex::new(RPCNetwork::new(LOGGER.clone())));
    for id in 0..number {
        let id = id as u64;
        let rpc = Box::new(RPCClient::new(network.clone(), id));
        let logger = LOGGER.new(o!("id" => id));
        let instance = Arc::new(Mutex::new(Raft::new(
            known_peers.clone(),
            logger,
            rpc,
            id,
            now(),
        )));
        raft_instance.push(instance);
    }
    let system = RaftSystem::new(raft_instance, network, LOGGER.clone());
    let cancel = Arc::new(AtomicBool::new(false));
    let handles = spawn_cluster(&system.cluster, system.network.clone(), cancel.clone());
    thread::spawn(move || func(system)).join().ok();
    cancel.store(true, std::sync::atomic::Ordering::SeqCst);
    drop(handles);
}

pub struct ClusterHandles {
    handles: Vec<Option<JoinHandle<()>>>,
}

impl ClusterHandles {
    pub fn new(handles: Vec<Option<JoinHandle<()>>>) -> Self {
        Self { handles }
    }
}

impl Drop for ClusterHandles {
    fn drop(&mut self) {
        for i in self.handles.iter_mut() {
            let handle = std::mem::replace(i, None).unwrap();
            handle.join().unwrap();
        }
    }
}

pub fn spawn_cluster(
    cluster: &Vec<Arc<Mutex<Raft>>>,
    network: Arc<Mutex<RPCNetwork>>,
    cancel: Arc<AtomicBool>,
) -> ClusterHandles {
    let mut x: Vec<Option<JoinHandle<()>>> = cluster
        .iter()
        .map(|instance| {
            let cancel = cancel.clone();
            let instance = instance.clone();
            thread::spawn(move || loop {
                if cancel.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                instance.lock().unwrap().tick(now());
                thread::sleep(Duration::from_millis(10));
            })
        })
        .map(|x| Some(x))
        .collect();
    let cancel = cancel.clone();
    let network = network.clone();
    let mut cluster = cluster.clone();
    x.push(Some(thread::spawn(move || loop {
        if cancel.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        deliver_all_message(&mut cluster, network.clone());
        thread::yield_now();
    })));
    ClusterHandles::new(x)
}
