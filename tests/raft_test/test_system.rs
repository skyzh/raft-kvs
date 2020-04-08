use crate::system_utils::*;
use crate::utils::cluster::{spawn_cluster, tick_cluster, with_cluster, now};
use raft_kvs::raft::instance::Role;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

#[test]
fn test_create_cluster() {
    with_cluster(3, |cfg| {
        assert_eq!(cfg.cluster.len(), 3);
        for i in 0..3 {
            assert_eq!(cfg.cluster[i].lock().unwrap().id, i as u64);
        }
    });
}

#[test]
fn test_initial_election_2a() {
    with_cluster(3, | cfg| {
        sleep(Duration::from_secs(1));
        assert!(cfg.check_one_leader().is_some());
        sleep(Duration::from_millis(50));
        let term = cfg.check_terms();
        assert!(term >= 1);
        // if no network failure, the leader should stay the same
        sleep(Duration::from_millis(1000));
        assert_eq!(cfg.check_terms(), term);
        assert!(cfg.check_one_leader().is_some());
    });
}

#[test]
fn test_re_election_2a() {
    with_cluster(3, | mut cfg| {
        sleep(Duration::from_secs(1));
        let leader = cfg.check_one_leader();
        assert!(leader.is_some());
        let leader = leader.unwrap();
        // disconnect leader
        cfg.disconnect(leader);
        // a new leader should be elected
        let new_leader = cfg.check_one_leader();
        assert!(new_leader.is_some());
        let new_leader = new_leader.unwrap();
        // old leader rejoins shouldn't disturb new leader
        cfg.connect(leader);
        assert_eq!(cfg.check_one_leader().unwrap(), new_leader);
        // no leader should be elected
        cfg.disconnect(new_leader);
        cfg.disconnect((new_leader + 1 ) % 3);
        assert!(cfg.check_one_leader().is_none());
        // there should be leader with new quorum
        cfg.connect(new_leader);
        assert!(cfg.check_one_leader().is_some());
        // rejoin shouldn't prevent leader from existing
        cfg.connect((new_leader + 1 ) % 3);
        assert!(cfg.check_one_leader().is_some());
    });
}
