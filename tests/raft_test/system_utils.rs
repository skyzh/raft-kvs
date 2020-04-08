use crate::utils::cluster::RPCNetwork;
use raft_kvs::raft::instance::{Raft, Role};
use slog::info;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct RaftSystem {
    pub cluster: Vec<Arc<Mutex<Raft>>>,
    pub network: Arc<Mutex<RPCNetwork>>,
    disconnected: HashSet<u64>,
    log: slog::Logger,
}

impl RaftSystem {
    pub fn new(
        cluster: Vec<Arc<Mutex<Raft>>>,
        network: Arc<Mutex<RPCNetwork>>,
        log: slog::Logger,
    ) -> Self {
        Self {
            cluster,
            network,
            log,
            disconnected: HashSet::new(),
        }
    }

    pub fn online_instance(&self) -> Vec<Arc<Mutex<Raft>>> {
        self.cluster
            .iter()
            .enumerate()
            .filter(|(x, _)| !self.disconnected.contains(&(*x as u64)))
            .map(|(_, x)| x.clone())
            .collect()
    }

    pub fn check_one_leader(&self) -> Option<u64> {
        let mut leader = None;
        for instance in self.online_instance() {
            let instance = instance.lock().unwrap();
            if instance.role == Role::Leader {
                if leader.is_some() {
                    return None;
                } else {
                    leader = Some(instance.id)
                }
            }
        }
        leader
    }

    pub fn check_no_leader(&self) -> bool {
        self.online_instance()
            .iter()
            .all(|x| x.lock().unwrap().role != Role::Leader)
    }

    pub fn check_terms(&self) -> i64 {
        let terms: Vec<i64> = self
            .online_instance()
            .iter()
            .map(|x| x.lock().unwrap().current_term)
            .collect();
        for i in 1..terms.len() {
            assert_eq!(terms[i], terms[0]);
        }
        terms[0]
    }

    pub fn disconnect(&mut self, id: u64) {
        self.disconnected.insert(id);
        self.network.lock().unwrap().disable(id);
        // TODO: close all connection
        std::thread::sleep(Duration::from_secs(1));
        info!(self.log, "disconnect"; "id" => id);
    }

    pub fn connect(&mut self, id: u64) {
        self.network.lock().unwrap().enable(id);
        self.disconnected.remove(&id);
        std::thread::sleep(Duration::from_secs(1));
        info!(self.log, "connect"; "id" => id);
    }
}
