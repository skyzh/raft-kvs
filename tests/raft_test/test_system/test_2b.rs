use crate::system_utils::*;
use crate::utils::cluster::{spawn_cluster, tick_cluster, with_cluster};
use crate::utils::rpcnetwork::now;
use raft_kvs::raft::instance::Role;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
