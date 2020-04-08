use raft_kvs::raft::rpc::*;
use slog::{debug, info};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

/// A RPC Service for testing purpose
pub struct MockRPCService {
    /// RPC requests from client
    pub rpc_log: Vec<(u64, RaftRPC)>,
    /// Logger
    log: slog::Logger,
    /// instance id of Raft instance using this RPC service
    instance_id: u64,
}

impl MockRPCService {
    /// create new MockRPCService
    pub fn new(logger: slog::Logger, instance_id: u64) -> Self {
        Self {
            rpc_log: vec![],
            log: logger,
            instance_id,
        }
    }
}

impl RPCService for MockRPCService {
    fn send(&mut self, peer: u64, msg: RaftRPC) -> u64 {
        debug!(self.log, "send"; "msg" => format!("{:?}", msg));
        self.rpc_log.push((peer, msg));
        self.rpc_log.len() as u64 - 1
    }
}

pub struct MockRPCServiceWrapper {
    rpc: Arc<Mutex<MockRPCService>>,
}

impl MockRPCServiceWrapper {
    pub fn new(rpc: Arc<Mutex<MockRPCService>>) -> Self {
        Self { rpc }
    }
}

impl RPCService for MockRPCServiceWrapper {
    fn send(&mut self, peer: u64, msg: RaftRPC) -> u64 {
        let mut rpc = self.rpc.lock().unwrap();
        rpc.send(peer, msg)
    }
}
