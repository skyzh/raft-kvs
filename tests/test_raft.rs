use raft_kvs::raft::{event::*, instance::*, log::*, rpc::*};
use std::collections::HashMap;

mod utils;

use utils::*;

#[test]
fn test_new() {
    let r = new_test_raft_instance();
}
