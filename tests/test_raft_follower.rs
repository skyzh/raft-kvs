use std::collections::HashMap;
use raft_kvs::raft::{rpc::*, instance::*, log::*, event::*};
mod utils;
use utils::*;

#[test]
fn test_follower_become_candidate() {
    let mut r = new_test_raft_instance();
    r.tick(1000);
    // should change role and increase term number
    assert_eq!(r.role, Role::Candidate);
    assert_eq!(r.current_term, 1);
    // should begin election
    assert!(inspect_has_request_vote_to(&r.rpc, 2));
    assert!(inspect_has_request_vote_to(&r.rpc, 3));
    assert!(inspect_has_request_vote_to(&r.rpc, 4));
    assert!(inspect_has_request_vote_to(&r.rpc, 5));
    assert!(!inspect_has_request_vote_to(&r.rpc, 1));
}

#[test]
fn test_begin_as_follower() {
    let r = new_test_raft_instance();
    assert_eq!(r.role, Role::Follower);
}

#[test]
fn test_follower_respond_to_one_vote() {
    let mut r = new_test_raft_instance();
    r.current_term = 1;
    r.on_event(
        RaftEvent::RPC((
            2,
            RequestVote {
                term: 1,
                candidate_id: 2,
                last_log_term: 0,
                last_log_index: 0,
            }
                .into(),
        )),
        100,
    );
    r.on_event(
        RaftEvent::RPC((
            3,
            RequestVote {
                term: 1,
                candidate_id: 3,
                last_log_term: 0,
                last_log_index: 0,
            }
                .into(),
        )),
        101,
    );
    r.on_event(
        RaftEvent::RPC((
            2,
            RequestVote {
                term: 1,
                candidate_id: 2,
                last_log_term: 0,
                last_log_index: 0,
            }
                .into(),
        )),
        105,
    );
    assert_eq!(inspect_request_vote_reply_to(&r.rpc, 2), 2);
    assert!(!inspect_has_request_vote_reply_to(&r.rpc, 3));
}

#[test]
fn test_follower_respond_to_lower_term_vote() {
    let mut r = new_test_raft_instance();
    r.current_term = 233;
    r.on_event(
        RaftEvent::RPC((
            2,
            RequestVote {
                term: 1,
                candidate_id: 2,
                last_log_term: 0,
                last_log_index: 0,
            }
                .into(),
        )),
        100,
    );
    assert!(!inspect_has_request_vote_reply_to(&r.rpc, 2));
}

#[test]
fn test_follower_respond_to_vote_stale_log() {
    let mut r = new_test_raft_instance();
    r.current_term = 1;
    r.log.push((1, Log::Get("233".into())));
    r.on_event(
        RaftEvent::RPC((
            2,
            RequestVote {
                term: 1,
                candidate_id: 2,
                last_log_term: 0,
                last_log_index: 0,
            }
                .into(),
        )),
        100,
    );
    assert!(!inspect_has_request_vote_reply_to(&r.rpc, 2));
}
