use raft_kvs::raft::{event::*, instance::*, log::*, rpc::*};
use std::collections::HashMap;

use crate::utils::*;

#[test]
fn test_win_election() {
    get_leader_instance();
}

#[test]
fn test_election_not_enough_vote() {
    let (mut r, rpc) = new_test_raft_instance();
    r.tick(1000);
    // should have started election
    assert_eq!(r.role, Role::Candidate);
    // send mock RPC to raft instance
    for i in 1..=5 {
        r.on_event(
            RaftEvent::RPC((
                2,
                0,
                RequestVoteReply {
                    term: 1,
                    vote_granted: true,
                }
                .into(),
            )),
            100 + i,
        );
    }
    r.tick(1100);
    assert_eq!(r.role, Role::Candidate);
}

#[test]
fn test_become_follower_append() {
    let (mut r, rpc) = new_test_raft_instance();
    r.tick(1000);
    assert_eq!(r.role, Role::Candidate);
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
            AppendEntries {
                term: r.current_term,
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
fn test_become_follower_term() {
    let (mut r, rpc) = new_test_raft_instance();
    r.tick(1000);
    assert_eq!(r.role, Role::Candidate);
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
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
fn test_restart_election() {
    let (mut r, rpc) = new_test_raft_instance();
    r.tick(1000);
    // should have started election
    assert_eq!(r.role, Role::Candidate);
    r.tick(2000);
    // should have started another election
    assert_eq!(r.role, Role::Candidate);
    assert_eq!(inspect_request_vote_to(&rpc.lock().unwrap(), 2), 2);
    assert_eq!(inspect_request_vote_to(&rpc.lock().unwrap(), 3), 2);
    assert_eq!(inspect_request_vote_to(&rpc.lock().unwrap(), 4), 2);
    assert_eq!(inspect_request_vote_to(&rpc.lock().unwrap(), 5), 2);
}


#[test]
fn test_no_append() {
    let (mut r, rpc) = new_test_raft_instance();
    assert_eq!(r.append_log(random_log(), 100), None);
}
