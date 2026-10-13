//! In-process cluster for tests and benchmarks. Every node runs on its own Tokio runtime, so
//! `crash` really kills it: tasks, sockets and unsynced buffers go away together and only
//! what `FileStorage` made durable survives `restart`.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use raft::{Member, NodeId};
use tokio::runtime::Runtime;

use crate::node::{self, AliveToken, NodeConfig};
use crate::pb;
use crate::transport::Faults;

#[derive(Clone, Debug)]
pub struct ClusterOpts {
    pub tick: Duration,
    pub election_ticks: u64,
    pub heartbeat_ticks: u64,
    pub snapshot_every: u64,
    pub fsync: bool,
    pub threads: usize,
}

impl Default for ClusterOpts {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(10),
            election_ticks: 15,
            heartbeat_ticks: 3,
            snapshot_every: 10_000,
            fsync: true,
            threads: 2,
        }
    }
}

pub struct LocalCluster {
    pub faults: Faults,
    root: PathBuf,
    opts: ClusterOpts,
    addrs: BTreeMap<NodeId, String>,
    running: BTreeMap<NodeId, (Runtime, AliveToken)>,
}

impl LocalCluster {
    pub async fn start(n: u64, root: &Path, opts: ClusterOpts) -> io::Result<Self> {
        let mut c = Self {
            faults: Faults::default(),
            root: root.to_path_buf(),
            opts,
            addrs: BTreeMap::new(),
            running: BTreeMap::new(),
        };
        for id in 1..=n {
            c.addrs.insert(id, free_addr()?);
        }
        let members: Vec<Member> = c
            .addrs
            .iter()
            .map(|(&id, a)| Member {
                id,
                addr: a.clone(),
            })
            .collect();
        for id in 1..=n {
            c.start_node(id, Some(members.clone())).await?;
        }
        Ok(c)
    }

    async fn start_node(&mut self, id: NodeId, bootstrap: Option<Vec<Member>>) -> io::Result<()> {
        let addr = self.addrs[&id].clone();
        let mut cfg = NodeConfig::new(id, addr.clone(), self.root.join(format!("node-{id}")));
        cfg.bootstrap = bootstrap;
        cfg.tick = self.opts.tick;
        cfg.election_ticks = self.opts.election_ticks;
        cfg.heartbeat_ticks = self.opts.heartbeat_ticks;
        cfg.snapshot_every = self.opts.snapshot_every;
        cfg.fsync = self.opts.fsync;
        cfg.faults = self.faults.clone();
        let alive = cfg.alive.clone();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.opts.threads)
            .thread_name(format!("node-{id}"))
            .enable_all()
            .build()?;
        // The previous incarnation's listener may still be closing.
        let mut tries = 0;
        let std_listener = loop {
            match std::net::TcpListener::bind(&addr) {
                Ok(l) => break l,
                Err(e) if tries < 100 => {
                    let _ = e;
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => return Err(e),
            }
        };
        std_listener.set_nonblocking(true)?;
        let listener = {
            let _guard = rt.enter();
            tokio::net::TcpListener::from_std(std_listener)?
        };
        rt.spawn(node::serve(cfg, listener));
        self.running.insert(id, (rt, alive));
        Ok(())
    }

    /// Kills the node's runtime and waits until its tasks (and storage) are gone.
    pub async fn crash(&mut self, id: NodeId) {
        if let Some((rt, alive)) = self.running.remove(&id) {
            rt.shutdown_background();
            while Arc::strong_count(&alive) > 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    pub async fn restart(&mut self, id: NodeId) -> io::Result<()> {
        self.start_node(id, None).await
    }

    /// Starts an empty node that waits to be added with `AddMember`.
    pub async fn spawn_joiner(&mut self, id: NodeId) -> io::Result<String> {
        let addr = free_addr()?;
        self.addrs.insert(id, addr.clone());
        self.start_node(id, None).await?;
        Ok(addr)
    }

    pub fn is_running(&self, id: NodeId) -> bool {
        self.running.contains_key(&id)
    }

    pub fn addr(&self, id: NodeId) -> String {
        self.addrs[&id].clone()
    }

    pub fn addrs(&self) -> Vec<String> {
        self.addrs.values().cloned().collect()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.addrs.keys().copied().collect()
    }

    pub async fn admin(
        &self,
        id: NodeId,
    ) -> Option<pb::admin_client::AdminClient<tonic::transport::Channel>> {
        pb::admin_client::AdminClient::connect(format!("http://{}", self.addrs[&id]))
            .await
            .ok()
    }

    pub async fn status(&self, id: NodeId) -> Option<pb::StatusResponse> {
        let mut a = self.admin(id).await?;
        let s = tokio::time::timeout(Duration::from_millis(500), a.status(pb::StatusRequest {}));
        Some(s.await.ok()?.ok()?.into_inner())
    }

    /// Polls until some running node reports itself leader.
    pub async fn leader(&self, timeout: Duration) -> Option<NodeId> {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            for &id in self.running.keys() {
                if let Some(s) = self.status(id).await
                    && s.role == "leader"
                {
                    return Some(id);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }
}

impl Drop for LocalCluster {
    fn drop(&mut self) {
        for (_, (rt, _)) in std::mem::take(&mut self.running) {
            rt.shutdown_background();
        }
    }
}

fn free_addr() -> io::Result<String> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .to_string())
}
