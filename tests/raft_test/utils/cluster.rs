use crate::system_utils::RaftSystem;
use crate::utils::LOGGER;
use raft_kvs::raft::event::RaftEvent;
use raft_kvs::raft::event::RaftEvent::{Log, RPC};
use raft_kvs::raft::instance::Raft;
use slog::{debug, info, o, trace, Logger};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use raft_kvs::raft::rpc::{RaftRPC, RPCService};
use crate::utils::rpcnetwork::{now, RPCNetwork};
use std::time::Duration;

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
