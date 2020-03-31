use std::collections::HashMap;
use raft_kvs::raft::{rpc::*, instance::*, log::*, event::*};
mod utils;
use utils::*;

#[test]
fn test_new() {
    let r = new_test_raft_instance();
}


#[test]
fn test_candidate_restart_election() {
    let mut r = new_test_raft_instance();
    r.tick(1000);
    // should have started election
    assert_eq!(r.role, Role::Candidate);
    r.tick(2000);
    // should have started another election
    assert_eq!(r.role, Role::Candidate);
    assert_eq!(inspect_request_vote_to(&r.rpc, 2), 2);
    assert_eq!(inspect_request_vote_to(&r.rpc, 3), 2);
    assert_eq!(inspect_request_vote_to(&r.rpc, 4), 2);
    assert_eq!(inspect_request_vote_to(&r.rpc, 5), 2);
}

#[test]
fn test_leader_become_follower_term() {
    let mut r = new_test_raft_instance();
    r.tick(1000);
    r.role = Role::Leader;
    r.on_event(
        RaftEvent::RPC((
            2,
            AppendEntries {
                term: r.current_term + 3,
                leader_id: 2,
                prev_log_index: 233,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 200,
            }
                .into(),
        )),
        1005,
    );
    assert_eq!(r.role, Role::Follower);
}

#[test]
fn test_leader_heartbeat() {
    let mut r = get_leader_instance();
    assert_eq!(inspect_append_entries(&r.rpc).len(), 4);
}
