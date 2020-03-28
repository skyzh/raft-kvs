//! Define log used in Raft

/// Log in Raft
pub enum Log {
    Get(String),
    Set(String, String),
    Remove(String),
}
