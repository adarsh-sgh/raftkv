//! The node: one Tokio task owns the `RaftNode` and the `KvStore` and is the only writer to
//! either. gRPC handlers talk to it over channels; each loop iteration drains a batch of
//! requests and messages, then calls `ready()` once, so a batch shares one fsync and one
//! AppendEntries round.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kv::{CmdResult, Command, KvStore, Message as _};
use raft::{ConfChange, Member, NodeId, Payload, ProposeError, RaftNode, Role};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::TcpListenerStream;

use crate::convert::*;
use crate::pb;
use crate::service::{AdminService, KvService, RaftService};
use crate::storage::FileStorage;
use crate::transport::{Faults, Peer};

#[derive(Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    /// Address peers and clients dial.
    pub addr: String,
    pub data_dir: PathBuf,
    /// Initial members of a new cluster. `None` joins (or restarts) with what is on disk.
    pub bootstrap: Option<Vec<Member>>,
    pub tick: Duration,
    pub election_ticks: u64,
    pub heartbeat_ticks: u64,
    pub snapshot_every: u64,
    pub fsync: bool,
    pub faults: Faults,
    /// Held by the node's tasks; `LocalCluster` waits for it to drop after a crash.
    pub alive: AliveToken,
}

impl NodeConfig {
    pub fn new(id: NodeId, addr: String, data_dir: PathBuf) -> Self {
        Self {
            id,
            addr,
            data_dir,
            bootstrap: None,
            tick: Duration::from_millis(20),
            election_ticks: 10,
            heartbeat_ticks: 2,
            snapshot_every: 10_000,
            fsync: true,
            faults: Faults::default(),
            alive: AliveToken::default(),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    NotLeader(Option<String>),
    Unavailable(String),
    Invalid(String),
}

pub type Reply<T> = oneshot::Sender<Result<T, Error>>;
type GetReply = Reply<Option<Vec<u8>>>;

pub enum Request {
    Get(Vec<u8>, GetReply),
    Write(Command, Reply<CmdResult>),
    Conf(ConfChange, Reply<()>),
    Status(oneshot::Sender<pb::StatusResponse>),
}

/// Serves gRPC on `listener` and runs the node until the task is dropped.
pub async fn serve(cfg: NodeConfig, listener: TcpListener) -> std::io::Result<()> {
    let mut storage = FileStorage::open(&cfg.data_dir, cfg.fsync)?;
    if let Some(members) = &cfg.bootstrap {
        raft::bootstrap(&mut storage, members.clone());
    }
    let mut rc = raft::Config::new(cfg.id);
    rc.election_ticks = cfg.election_ticks;
    rc.heartbeat_ticks = cfg.heartbeat_ticks;
    rc.seed = cfg.id ^ std::process::id() as u64 ^ now_nanos();
    let raft = RaftNode::new(rc, storage);

    let (req_tx, req_rx) = mpsc::channel(8192);
    let (peer_tx, peer_rx) = mpsc::channel(8192);
    let mut addrs: HashMap<NodeId, String> = raft
        .members()
        .iter()
        .map(|m| (m.id, m.addr.clone()))
        .collect();
    addrs.insert(cfg.id, cfg.addr.clone());
    let driver = Driver {
        raft,
        kv: KvStore::default(),
        applied: 0,
        net: Net {
            id_addr: cfg.addr.clone(),
            faults: cfg.faults.clone(),
            addrs,
            peers: HashMap::new(),
            snapshots_sent: 0,
        },
        writes: HashMap::new(),
        reads: HashMap::new(),
        ready_reads: Vec::new(),
        next_token: 0,
        confs: Vec::new(),
        was_leader: false,
        snapshots_installed: 0,
        cfg: cfg.clone(),
    };
    let driver = tokio::spawn(driver.run(req_rx, peer_rx));

    let server = tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .add_service(pb::kv_server::KvServer::new(KvService(req_tx.clone())))
        .add_service(pb::admin_server::AdminServer::new(AdminService(req_tx)))
        .add_service(pb::raft_server::RaftServer::new(RaftService(peer_tx)))
        .serve_with_incoming(TcpListenerStream::new(listener));
    let res = server.await;
    driver.abort();
    res.map_err(std::io::Error::other)
}

struct Driver {
    cfg: NodeConfig,
    raft: RaftNode<FileStorage>,
    kv: KvStore,
    applied: u64,
    net: Net,
    writes: HashMap<(u64, u64), Vec<Reply<CmdResult>>>,
    /// ReadIndex token -> (key, reply).
    reads: HashMap<u64, (Vec<u8>, GetReply)>,
    /// Confirmed reads waiting for `applied >= index`: (index, key, reply).
    ready_reads: Vec<(u64, Vec<u8>, GetReply)>,
    next_token: u64,
    confs: Vec<(u64, u64, Reply<()>)>,
    was_leader: bool,
    snapshots_installed: u64,
}

/// Outbound side, split from `Driver` so the Raft core can send through it mid-`ready`.
struct Net {
    id_addr: String,
    faults: Faults,
    addrs: HashMap<NodeId, String>,
    peers: HashMap<NodeId, Peer>,
    snapshots_sent: u64,
}

const BATCH: usize = 1024;

impl Driver {
    async fn run(
        mut self,
        mut reqs: mpsc::Receiver<Request>,
        mut msgs: mpsc::Receiver<pb::RaftMessage>,
    ) {
        let mut ticker = tokio::time::interval(self.cfg.tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.raft.tick(),
                Some(m) = msgs.recv() => {
                    self.step(m);
                    for _ in 0..BATCH {
                        let Ok(m) = msgs.try_recv() else { break };
                        self.step(m);
                    }
                }
                Some(r) = reqs.recv() => {
                    self.handle(r);
                    for _ in 0..BATCH {
                        let Ok(r) = reqs.try_recv() else { break };
                        self.handle(r);
                    }
                }
                else => return,
            }
            self.process_ready();
        }
    }

    fn step(&mut self, m: pb::RaftMessage) {
        if !m.from_addr.is_empty() {
            self.net.addrs.insert(m.from, m.from_addr.clone());
        }
        if let Some(m) = message_from_pb(m) {
            self.raft.step(m);
        }
    }

    fn not_leader(&self, e: ProposeError) -> Error {
        match e {
            ProposeError::NotLeader(hint) => {
                Error::NotLeader(hint.and_then(|id| self.net.addrs.get(&id).cloned()))
            }
            ProposeError::ConfChangeInProgress => Error::Unavailable(e.to_string()),
            ProposeError::Invalid(s) => Error::Invalid(s.to_string()),
        }
    }

    fn handle(&mut self, r: Request) {
        match r {
            Request::Get(key, tx) => {
                self.next_token += 1;
                match self.raft.read_index(self.next_token) {
                    Ok(()) => {
                        self.reads.insert(self.next_token, (key, tx));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(self.not_leader(e)));
                    }
                }
            }
            Request::Write(cmd, tx) => match self.raft.propose(cmd.encode_to_vec()) {
                Ok(_) => self
                    .writes
                    .entry((cmd.client, cmd.seq))
                    .or_default()
                    .push(tx),
                Err(e) => {
                    let _ = tx.send(Err(self.not_leader(e)));
                }
            },
            Request::Conf(change, tx) => {
                if let ConfChange::Add(m) = &change {
                    self.net.addrs.insert(m.id, m.addr.clone());
                }
                match self.raft.propose_conf_change(change) {
                    Ok(index) => self.confs.push((index, self.raft.term(), tx)),
                    Err(e) => {
                        let _ = tx.send(Err(self.not_leader(e)));
                    }
                }
            }
            Request::Status(tx) => {
                let _ = tx.send(self.status());
            }
        }
    }

    fn process_ready(&mut self) {
        let net = &mut self.net;
        let rd = self.raft.ready_with(|m| net.send(m));
        for m in rd.messages {
            self.net.send(m);
        }
        if let Some(s) = rd.snapshot {
            self.kv = KvStore::restore(&s.data);
            self.applied = s.meta.index;
            self.snapshots_installed += 1;
        }
        for e in rd.committed {
            match &e.payload {
                Payload::Command(data) => {
                    let cmd = Command::decode(data.as_slice()).expect("command written by us");
                    if let Some(res) = self.kv.apply(&cmd) {
                        for tx in self
                            .writes
                            .remove(&(cmd.client, cmd.seq))
                            .unwrap_or_default()
                        {
                            let _ = tx.send(Ok(res.clone()));
                        }
                    }
                }
                Payload::Config(members) => {
                    for m in members {
                        self.net.addrs.insert(m.id, m.addr.clone());
                    }
                }
                Payload::Noop => {}
            }
            for (_, term, tx) in extract(&mut self.confs, |(i, ..)| *i == e.index) {
                let ok = term == e.term && matches!(e.payload, Payload::Config(_));
                let _ = tx.send(if ok { Ok(()) } else { Err(lost()) });
            }
            self.applied = e.index;
        }
        // Skipped by an installed snapshot: outcome unknown.
        let applied = self.applied;
        for (_, _, tx) in extract(&mut self.confs, |(i, ..)| *i <= applied) {
            let _ = tx.send(Err(lost()));
        }
        for (token, index) in rd.reads {
            if let Some((key, tx)) = self.reads.remove(&token) {
                self.ready_reads.push((index, key, tx));
            }
        }
        for token in rd.aborted_reads {
            if let Some((_, tx)) = self.reads.remove(&token) {
                let _ = tx.send(Err(Error::NotLeader(None)));
            }
        }
        for (_, key, tx) in extract(&mut self.ready_reads, |(i, ..)| *i <= applied) {
            let _ = tx.send(Ok(self.kv.get(&key).cloned()));
        }

        let leader = self.raft.is_leader();
        if self.was_leader && !leader {
            // Outcome unknown; the client retries with the same (client, seq).
            for (_, txs) in self.writes.drain() {
                for tx in txs {
                    let _ = tx.send(Err(Error::NotLeader(None)));
                }
            }
        }
        self.was_leader = leader;

        if self.applied >= self.raft.snapshot_index() + self.cfg.snapshot_every {
            self.raft.compact(self.applied, self.kv.snapshot());
        }
    }

    fn status(&self) -> pb::StatusResponse {
        let s = self.raft.status();
        pb::StatusResponse {
            id: s.id,
            role: match s.role {
                Role::Leader => "leader",
                Role::Candidate => "candidate",
                Role::PreCandidate => "pre-candidate",
                Role::Follower => "follower",
            }
            .into(),
            term: s.term,
            leader: s.leader.unwrap_or(0),
            commit: s.commit,
            applied: self.applied,
            last_index: s.last_index,
            snapshot_index: s.snapshot_index,
            members: s.members.iter().map(member_to_pb).collect(),
            snapshots_sent: self.net.snapshots_sent,
            snapshots_installed: self.snapshots_installed,
            keys: self.kv.len() as u64,
        }
    }
}

impl Net {
    fn send(&mut self, m: raft::Message) {
        if !self.faults.allow(m.from, m.to) {
            return;
        }
        let Some(addr) = self.addrs.get(&m.to) else {
            return;
        };
        if matches!(m.msg, raft::Msg::InstallSnapshot { .. }) {
            self.snapshots_sent += 1;
        }
        let peer = match self.peers.get(&m.to) {
            Some(p) if &p.addr == addr => p,
            _ => {
                self.peers.insert(m.to, Peer::spawn(addr.clone()));
                &self.peers[&m.to]
            }
        };
        peer.send(message_to_pb(m, &self.id_addr));
    }
}

fn lost() -> Error {
    Error::Unavailable("config change outcome unknown".into())
}

fn extract<T>(v: &mut Vec<T>, pred: impl Fn(&T) -> bool) -> Vec<T> {
    let (hit, keep) = std::mem::take(v).into_iter().partition(|x| pred(x));
    *v = keep;
    hit
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

pub type AliveToken = Arc<()>;
