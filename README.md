# raft-kvs

![build](https://github.com/skyzh/raft-kvs/workflows/build/badge.svg)

A distributed key-value store based on Raft.

## Roadmap

- Implement Raft consensus algorithm
    - [x] Leader election
    - [x] Candidate fallback
    - [x] Leader append log
    - [x] Follower verify log
    - [x] Leader sync log
    - [ ] Migrate test from 6.824
        - [x] 2A
        - [ ] 2B
        - [ ] 2C
    - [ ] Batch append log
    - [ ] Batch rollback log
    - [ ] Membership changes (TBD)
    - [ ] Snapshot (TBD)
    - [ ] System test
    - [ ] Implement gRPC
    - [ ] Config
    - [ ] Provide abstraction for application
    - [ ] Migrate test from skyzh/raft
- Implement KVS
    - [ ] Migrate test from 6.824
