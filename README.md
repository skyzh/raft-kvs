# raft-kvs

A distributed key-value store based on Raft.

## Roadmap

- Implement Raft consensus algorithm
    - [x] Leader election
    - [x] Candidate fallback
    - [x] Leader append log
    - [x] Follower verify log
    - [ ] Leader sync log
    - [ ] Batch append log
    - [ ] Batch re-send log
    - [ ] Membership changes (TBD)
    - [ ] Snapshot (TBD)
    - [ ] System test
    - [ ] Implement gRPC
    - [ ] Config
    - [ ] Provide abstraction for application
    - [ ] Migrate test from skyzh/raft
    - [ ] Migrate test from 6.824
- Implement KVS
    - [ ] Migrate test from 6.824
