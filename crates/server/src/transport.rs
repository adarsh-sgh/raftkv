//! Peer transport: one long-lived gRPC client stream per peer, fed by a bounded queue.
//! Sends never block the Raft loop: a full queue or a broken stream drops messages, which
//! Raft tolerates. `Faults` injects partitions and random loss at this layer for tests.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use raft::{NodeId, Rng};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;

use crate::pb;
use crate::pb::raft_client::RaftClient;

#[derive(Default)]
struct FaultState {
    blocked: HashSet<(NodeId, NodeId)>,
    drop_rate: f64,
    rng: Option<Rng>,
}

/// Shared fault switchboard (a no-op unless a test configures it).
#[derive(Clone, Default)]
pub struct Faults(Arc<Mutex<FaultState>>);

impl Faults {
    pub fn allow(&self, from: NodeId, to: NodeId) -> bool {
        let mut f = self.0.lock().expect("faults lock");
        if f.blocked.contains(&(from, to)) {
            return false;
        }
        let p = f.drop_rate;
        p == 0.0 || !f.rng.get_or_insert_with(|| Rng::new(7)).chance(p)
    }

    /// Cuts every link between `a` and `b` (both directions).
    pub fn partition(&self, a: &[NodeId], b: &[NodeId]) {
        let mut f = self.0.lock().expect("faults lock");
        f.blocked.clear();
        for &x in a {
            for &y in b {
                f.blocked.insert((x, y));
                f.blocked.insert((y, x));
            }
        }
    }

    pub fn set_drop_rate(&self, p: f64) {
        self.0.lock().expect("faults lock").drop_rate = p;
    }

    pub fn heal(&self) {
        let mut f = self.0.lock().expect("faults lock");
        f.blocked.clear();
        f.drop_rate = 0.0;
    }
}

pub struct Peer {
    pub addr: String,
    tx: mpsc::Sender<pb::RaftMessage>,
}

impl Peer {
    pub fn spawn(addr: String) -> Self {
        let (tx, rx) = mpsc::channel(4096);
        tokio::spawn(run(addr.clone(), rx));
        Self { addr, tx }
    }

    pub fn send(&self, m: pb::RaftMessage) {
        let _ = self.tx.try_send(m);
    }
}

async fn run(addr: String, mut rx: mpsc::Receiver<pb::RaftMessage>) {
    let Ok(endpoint) = Endpoint::from_shared(format!("http://{addr}")) else {
        return;
    };
    let endpoint = endpoint
        .connect_timeout(Duration::from_millis(500))
        .tcp_nodelay(true);
    loop {
        let channel = match endpoint.connect().await {
            Ok(c) => c,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                // Drop what queued up while the peer was unreachable; it is stale by now.
                while rx.try_recv().is_ok() {}
                if rx.is_closed() {
                    return;
                }
                continue;
            }
        };
        let (stx, srx) = mpsc::channel(4096);
        let mut client = RaftClient::new(channel);
        let call = tokio::spawn(async move { client.send(ReceiverStream::new(srx)).await });
        loop {
            let Some(m) = rx.recv().await else {
                call.abort();
                return;
            };
            if stx.send(m).await.is_err() {
                break; // stream ended: reconnect
            }
        }
        let _ = call.await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
