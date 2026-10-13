//! Sans-IO Raft: leader election (with PreVote and CheckQuorum), log replication,
//! snapshots / InstallSnapshot, single-server membership changes and ReadIndex reads.
//! The caller owns time, the network and the state machine; see `RaftNode`.

mod node;
mod rng;
mod storage;
mod types;

pub use node::{Config, RaftNode, Status};
pub use rng::Rng;
pub use storage::{MemStorage, Storage, bootstrap};
pub use types::*;
