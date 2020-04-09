use raft_kvs::raft::{event::*, instance::*, log::*, rpc::*};
use std::collections::HashMap;

use crate::utils::*;
use raft_kvs::raft::log::Log::Get;
use std::time::Duration;

#[test]
fn test_become_follower_term() {
    let (mut r, rpc) = new_test_raft_instance();
    r.tick(1000);
    r.role = Role::Leader;
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
fn test_heartbeat() {
    let (mut r, rpc) = get_leader_instance();
    assert_eq!(inspect_append_entries(&rpc.lock().unwrap()).len(), 4);
}

#[test]
fn test_sync_log() {
    let (mut r, rpc) = get_leader_instance();
    let mut tick = 1000;
    for i in 0..100 {
        r.append_log(random_log(), tick);
        let reply_id = {
            let rpc = rpc.lock().unwrap();
            let x = rpc
                .rpc_log
                .iter()
                .enumerate()
                .map(|x| match x.1 {
                    (2, RaftRPC::AppendEntries(xx)) => {
                        if xx.entries.len() != 0 && xx.prev_log_index == i {
                            Some((x.0 as u64, xx))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .find(|x| x.is_some());
            assert!(x.is_some());
            let x = x.unwrap().unwrap();
            x.0
        };
        r.on_event(
            RaftEvent::RPC((
                2,
                0,
                AppendEntriesReply {
                    term: r.current_term,
                    success: true,
                }
                    .reply(reply_id),
            )),
            tick + 5,
        );
        tick += 10;
        r.tick(tick);
        tick += 10;
    }
}

#[test]
fn test_sync_log_not_match() {
    let (mut r, rpc) = get_leader_instance();
    let mut tick = 1000;
    // sync 20 logs first
    for i in 0..20 {
        r.append_log(random_log(), tick);
        let reply_id = {
            let rpc = rpc.lock().unwrap();
            let x = rpc
                .rpc_log
                .iter()
                .enumerate()
                .map(|x| match x.1 {
                    (2, RaftRPC::AppendEntries(xx)) => {
                        if xx.entries.len() != 0 && xx.prev_log_index == i {
                            Some((x.0 as u64, xx))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .find(|x| x.is_some());
            assert!(x.is_some());
            let x = x.unwrap().unwrap();
            x.0
        };
        r.on_event(
            RaftEvent::RPC((
                2,
                0,
                AppendEntriesReply {
                    term: r.current_term,
                    success: true,
                }
                    .reply(reply_id),
            )),
            tick + 5,
        );
        tick += 10;
        r.tick(tick);
        tick += 10;
    }
    let mut lst_scanned_idx = rpc.lock().unwrap().rpc_log.len();
    // reject 10 logs
    for i in 0..10 {
        tick += 100;
        r.append_log(random_log(), tick);
        let reply_id = {
            let rpc = rpc.lock().unwrap();
            let x = rpc
                .rpc_log
                .iter()
                .enumerate()
                .map(|x| match x.1 {
                    (2, RaftRPC::AppendEntries(xx)) => {
                        if xx.entries.len() != 0 && x.0 >= lst_scanned_idx {
                            Some((x.0 as u64, xx))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .find(|x| x.is_some());
            assert!(x.is_some());
            let x = x.unwrap().unwrap();
            lst_scanned_idx = rpc.rpc_log.len();
            x.0
        };
        tick += 5;
        r.on_event(
            RaftEvent::RPC((
                2,
                0,
                AppendEntriesReply {
                    term: r.current_term,
                    success: false,
                }
                    .reply(reply_id),
            )),
            tick + 5,
        );
    }
    r.append_log(random_log(), tick);
    {
        let rpc = rpc.lock().unwrap();
        let x = rpc
            .rpc_log
            .iter()
            .enumerate()
            .map(|x| match x.1 {
                (2, RaftRPC::AppendEntries(xx)) => {
                    if xx.entries.len() != 0 && x.0 >= lst_scanned_idx {
                        Some((x.0 as u64, xx))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .find(|x| x.is_some());
        assert!(x.is_some());
        let x = x.unwrap().unwrap();
        assert!(x.1.prev_log_index <= 10);
    }
}

#[test]
fn test_append_log() {
    let (mut r, rpc) = get_leader_instance();
    let entry = Get("23333".into());
    r.append_log(entry.clone(), 1000);
    assert!(inspect_has_append_entries_content_to(
        &rpc.lock().unwrap(),
        &entry,
        2,
    ));
    assert!(inspect_has_append_entries_content_to(
        &rpc.lock().unwrap(),
        &entry,
        3,
    ));
    assert!(inspect_has_append_entries_content_to(
        &rpc.lock().unwrap(),
        &entry,
        4,
    ));
    assert!(inspect_has_append_entries_content_to(
        &rpc.lock().unwrap(),
        &entry,
        5,
    ));
}

#[test]
fn test_expire_rpc() {
    let (mut r, _) = get_leader_instance();
    r.append_log(random_log(), 1000);
    r.tick(2000);
    assert!(r.rpc_append_entries_log_idx.len() > 0);
    let x = r.rpc_append_entries_log_idx.clone();
    r.tick(1000000);
    for (k, v) in x.iter() {
        assert!(!r.rpc_append_entries_log_idx.contains_key(k));
    }
}

#[test]
fn test_commit() {
    let (mut r, rpc) = get_leader_instance();
    assert_eq!(r.match_index.len(), 4);
    r.match_index.insert(2, 100);
    r.match_index.insert(3, 100);
    for i in 0..100 { r.append_log(random_log(), i); }
    r.tick(200);
    let msg_id = {
        let rpc = rpc.lock().unwrap();
        let x = rpc.rpc_log
            .iter()
            .enumerate()
            .map(|x| match x.1 {
                (2, RaftRPC::AppendEntries(xx)) => {
                    if xx.entries.len() == 100 {
                        Some((x.0 as u64, xx))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .find(|x| x.is_some());
        assert!(x.is_some());
        x.unwrap().unwrap().0
    };
    r.on_event(
        RaftEvent::RPC((
            2,
            0,
            AppendEntriesReply {
                term: r.current_term,
                success: true,
            }
                .reply(msg_id),
        )),
        300
    );
    assert_eq!(r.commit_index, 100);
}
