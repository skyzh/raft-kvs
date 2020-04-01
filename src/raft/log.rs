//! Define log used in Raft

/// Log in Raft
#[derive(PartialEq, Clone)]
pub enum Log {
    Get(String),
    Set(String, String),
    Remove(String),
}
