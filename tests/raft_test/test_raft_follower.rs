use crate::utils::*;
use raft_kvs::raft::{event::*, instance::*, log::*, rpc::*};
use std::collections::HashMap;

#[test]
fn test_become_candidate() {
    let (mut r, rpc) = new_test_raft_instance();
    r.tick(1000);
    // should change role and increase term number
    assert_eq!(r.role, Role::Candidate);
    assert_eq!(r.current_term, 1);
    // should begin election
    assert!(inspect_has_request_vote_to(&rpc.lock().unwrap(), 2));
    assert!(inspect_has_request_vote_to(&rpc.lock().unwrap(), 3));
    assert!(inspect_has_request_vote_to(&rpc.lock().unwrap(), 4));
    assert!(inspect_has_request_vote_to(&rpc.lock().unwrap(), 5));
    assert!(!inspect_has_request_vote_to(&rpc.lock().unwrap(), 1));
}

#[test]
fn test_begin_as_follower() {
    let (r, rpc) = new_test_raft_instance();
    assert_eq!(r.role, Role::Follower);
}

#[test]
fn test_respond_to_one_vote() {
    let (mut r, rpc) = new_test_raft_instance();
    r.current_term = 1;
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
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
            0,
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
            0,
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
    assert_eq!(inspect_request_vote_reply_to(&rpc.lock().unwrap(), 2), 2);
    assert!(!inspect_has_request_vote_reply_to(&rpc.lock().unwrap(), 3));
}

#[test]
fn test_respond_to_lower_term_vote() {
    let (mut r, rpc) = new_test_raft_instance();
    r.current_term = 233;
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
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
    assert!(!inspect_has_request_vote_reply_to(&rpc.lock().unwrap(), 2));
}

#[test]
fn test_respond_to_vote_stale_log() {
    let (mut r, rpc) = new_test_raft_instance();
    r.current_term = 1;
    r.log.push((1, Log::Get("233".into())));
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
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
    assert!(!inspect_has_request_vote_reply_to(&rpc.lock().unwrap(), 2));
}

#[test]
fn test_append_log() {
    let (mut r, rpc) = new_test_raft_instance();
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
            AppendEntries {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![(1, random_log()), (1, random_log())],
                leader_commit: 0,
            }
            .into(),
        )),
        100,
    );
    assert_eq!(r.log.len(), 2);
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
            AppendEntries {
                term: 1,
                leader_id: 2,
                prev_log_index: 2,
                prev_log_term: 1,
                entries: vec![(1, random_log()), (1, random_log())],
                leader_commit: 0,
            }
            .into(),
        )),
        105,
    );
    assert_eq!(r.log.len(), 4);
}

#[test]
fn test_append_log_purge() {
    let (mut r, rpc) = new_test_raft_instance();
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
            AppendEntries {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![(1, random_log()), (1, random_log())],
                leader_commit: 0,
            }
            .into(),
        )),
        100,
    );
    assert_eq!(r.log.len(), 2);
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
            AppendEntries {
                term: 1,
                leader_id: 2,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![(1, random_log()), (1, random_log())],
                leader_commit: 0,
            }
            .into(),
        )),
        105,
    );
    assert_eq!(r.log.len(), 3);
}

#[test]
fn test_reject_log_term_id() {
    let (mut r, rpc) = new_test_raft_instance();
    r.current_term = 200;
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
            AppendEntries {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![(1, random_log()), (1, random_log())],
                leader_commit: 0,
            }
            .into(),
        )),
        100,
    );
    assert_eq!(r.log.len(), 0);
}
