//! Raft-replicated key-value store: gRPC (tonic) API, file-backed Raft log, peer streams.

#[allow(clippy::all)]
pub mod pb {
    tonic::include_proto!("raftkv");
}

pub mod client;
pub mod cluster;
mod convert;
pub mod node;
mod service;
pub mod storage;
pub mod transport;

pub use client::{Client, ClientError};
pub use cluster::{ClusterOpts, LocalCluster};
pub use node::{NodeConfig, serve};
pub use transport::Faults;
