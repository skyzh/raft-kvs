use crate::utils::cluster::{with_cluster, tick_cluster, spawn_cluster};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use raft_kvs::raft::instance::Role;

#[test]
fn test_create_cluster() {
    with_cluster(3, |cluster, network| {
        assert_eq!(cluster.len(), 3);
        for i in 0..3 {
            assert_eq!(cluster[i].lock().unwrap().id, i as u64);
        }
    });
}

#[test]
fn test_initial_election_2a() {
    with_cluster(3, |cluster, network| {
        let cancel = Arc::new(AtomicBool::new(false));
        let handles = spawn_cluster(&cluster, network.clone(), cancel.clone());
        std::thread::sleep(Duration::from_secs(3));
        cancel.store(true, Ordering::SeqCst);
        drop(handles);
        assert!(cluster.iter().any(|x| x.lock().unwrap().role == Role::Leader));
    });
}
