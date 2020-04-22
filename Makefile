# export RUSTFLAGS=-Dwarnings
export RUST_TEST_THREADS=1
export RUST_BACKTRACE=1
CARGO_FLAG ?=
LOG_LEVEL ?= raft=info,percolator=info

check:
	cargo fmt --all -- --check
	cargo clippy --all --tests -- -D clippy::all

test: test_others test_2 test_3

test_2: test_2a test_2b test_2c

test_2a: cargo_test_2a

test_2b: cargo_test_2b

test_2c: cargo_test_2c

test_3: test_3a test_3b

test_3a: cargo_test_3a

test_3b: cargo_test_3b

build:
	cargo build $(CARGO_FLAG)

cargo_test_%: check
	RUST_LOG=${LOG_LEVEL} cargo test $(CARGO_FLAG) -p raft -- --nocapture --test $*

ci_test_%:
	RUST_LOG=${LOG_LEVEL} cargo test $(CARGO_FLAG) -p raft -- --nocapture --test $*

test_others: check
	RUST_LOG=${LOG_LEVEL} cargo test $(CARGO_FLAG)  -p labrpc -p labcodec -- --nocapture

test_percolator: check
	RUST_LOG=${LOG_LEVEL} cargo test $(CARGO_FLAG)  -p percolator -- --nocapture

test_alex: FORCE
	RUST_LOG=${LOG_LEVEL} cargo test $(CARGO_FLAG)  -p raft --lib tests::test_persist_partition_unreliable_linearizable_3a -- --nocapture

.PHONY: FORCE
FORCE:
