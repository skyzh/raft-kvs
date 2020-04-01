use raft_kvs::raft::{event::*, instance::*, log::*, rpc::*};
use std::collections::HashMap;

pub mod utils;

use raft_kvs::raft::log::Log::Get;
use utils::*;

#[test]
fn test_become_follower_term() {
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
fn test_heartbeat() {
    let mut r = get_leader_instance();
    assert_eq!(inspect_append_entries(&r.rpc).len(), 4);
}

#[test]
fn test_sync_log() {
    let r = get_leader_instance();
}

#[test]
fn test_append_log() {
    let mut r = get_leader_instance();
    let entry = Get("23333".into());
    r.append_log(entry.clone());
    assert!(inspect_has_append_entries_content_to(&r.rpc, &entry, 2));
    assert!(inspect_has_append_entries_content_to(&r.rpc, &entry, 3));
    assert!(inspect_has_append_entries_content_to(&r.rpc, &entry, 4));
    assert!(inspect_has_append_entries_content_to(&r.rpc, &entry, 5));
}
