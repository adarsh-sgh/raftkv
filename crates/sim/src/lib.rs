//! Deterministic, single-threaded cluster simulation in the spirit of Jepsen: real
//! `RaftNode`s and `KvStore`s exchange messages over a simulated network that drops,
//! duplicates, delays and reorders them, while a nemesis partitions the cluster, crashes and
//! restarts nodes (keeping only their durable storage) and changes membership. Clients record
//! a history that is checked for linearizability. A run is a pure function of its seed.

use std::collections::{BTreeMap, BTreeSet};

use kv::{CmdResult, Command, KvStore, Message as _};
use lincheck::kv::{Input, Output, Register, Value};
use lincheck::{Operation, Outcome, check_partitioned};
use raft::{
    ConfChange, Entry, MemStorage, Member, Message, Msg, NodeId, Payload, ProposeError, RaftNode,
    Rng, bootstrap,
};

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: u64,
    pub clients: usize,
    pub keys: usize,
    /// Ticks of workload + faults; a quiet period follows so pending ops can finish.
    pub ticks: u64,
    pub faults: bool,
    pub membership: bool,
    /// Compact the log once this many entries are applied past the last snapshot.
    pub snapshot_every: u64,
    /// Bug switch: leaders answer reads from local state without ReadIndex.
    pub unsafe_local_reads: bool,
    pub check_quorum: bool,
    pub election_ticks: u64,
}

impl SimConfig {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            nodes: 3,
            clients: 6,
            keys: 4,
            ticks: 3000,
            faults: true,
            membership: false,
            snapshot_every: 64,
            unsafe_local_reads: false,
            check_quorum: true,
            election_ticks: 20,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub ops_ok: usize,
    pub ops_unknown: usize,
    pub max_term: u64,
    pub messages: u64,
    pub dropped: u64,
    pub partitions: u64,
    pub crashes: u64,
    pub conf_changes: u64,
    pub snapshots_sent: u64,
    pub compactions: u64,
    /// Longest stretch of ticks (during the workload) in which no operation completed.
    pub max_stall: u64,
    /// Ticks from healing all faults until every pending client op completed (liveness).
    pub heal_ticks: u64,
}

impl Stats {
    pub fn add(&mut self, o: &Stats) {
        self.ops_ok += o.ops_ok;
        self.ops_unknown += o.ops_unknown;
        self.max_term = self.max_term.max(o.max_term);
        self.messages += o.messages;
        self.dropped += o.dropped;
        self.partitions += o.partitions;
        self.crashes += o.crashes;
        self.conf_changes += o.conf_changes;
        self.snapshots_sent += o.snapshots_sent;
        self.compactions += o.compactions;
        self.max_stall = self.max_stall.max(o.max_stall);
        self.heal_ticks = self.heal_ticks.max(o.heal_ticks);
    }
}

#[derive(Debug)]
pub struct Report {
    pub outcome: Outcome,
    pub stats: Stats,
}

#[derive(Clone, Debug)]
enum Req {
    Write(Command),
    Read { seq: u64, key: String },
}

#[derive(Clone, Debug)]
enum Resp {
    Write { seq: u64, res: CmdResult },
    Read { seq: u64, value: Value },
    NotLeader { seq: u64, hint: Option<NodeId> },
}

#[derive(Clone, Debug)]
enum Envelope {
    Raft(Message),
    Request {
        client: usize,
        node: NodeId,
        req: Req,
    },
    Response {
        client: usize,
        resp: Resp,
    },
}

struct Node {
    raft: RaftNode<MemStorage>,
    kv: KvStore,
    applied: u64,
    writes: BTreeMap<(u64, u64), usize>,
    reads: BTreeMap<u64, (usize, u64, String)>,
    ready_reads: Vec<(u64, usize, u64, String)>,
    next_token: u64,
}

struct Pending {
    req: Req,
    hist: usize,
    deadline: u64,
    timeouts: u32,
}

struct Client {
    id: u64,
    seq: u64,
    target: NodeId,
    op: Option<Pending>,
    next_op_at: u64,
    seen: BTreeMap<String, Value>,
    /// History indexes of writes this client gave up on.
    abandoned: Vec<usize>,
}

struct Sim {
    cfg: SimConfig,
    rng: Rng,
    now: u64,
    /// Durable state of every node ever started; `nodes` holds the live ones.
    disks: BTreeMap<NodeId, MemStorage>,
    /// Crashed nodes come back at this tick (kill -9, then restart).
    restart_at: BTreeMap<NodeId, u64>,
    nodes: BTreeMap<NodeId, Node>,
    next_id: NodeId,
    net: Vec<(u64, u64, Envelope)>,
    net_seq: u64,
    blocked: BTreeSet<(NodeId, NodeId)>,
    drop_rate: f64,
    dup_rate: f64,
    clients: Vec<Client>,
    clock: u64,
    history: Vec<(String, Operation<Input, Output>)>,
    leaders: BTreeMap<u64, NodeId>,
    last_completion: u64,
    applied_log: BTreeMap<u64, Entry>,
    stats: Stats,
}

const CLIENT_TIMEOUT: u64 = 30;
const GIVE_UP_AFTER: u32 = 3;
const MAX_LATENCY: u64 = 4;

pub fn run(cfg: SimConfig) -> Report {
    let mut sim = Sim::new(cfg);
    sim.run()
}

impl Sim {
    fn new(cfg: SimConfig) -> Self {
        let mut s = Self {
            rng: Rng::new(cfg.seed.wrapping_mul(0x9E37_79B9) ^ 0xDEAD_BEEF),
            now: 0,
            disks: BTreeMap::new(),
            restart_at: BTreeMap::new(),
            nodes: BTreeMap::new(),
            next_id: cfg.nodes + 1,
            net: Vec::new(),
            net_seq: 0,
            blocked: BTreeSet::new(),
            drop_rate: 0.0,
            dup_rate: 0.0,
            clients: Vec::new(),
            clock: 0,
            history: Vec::new(),
            leaders: BTreeMap::new(),
            last_completion: 0,
            applied_log: BTreeMap::new(),
            stats: Stats::default(),
            cfg,
        };
        let members: Vec<Member> = (1..=s.cfg.nodes)
            .map(|id| Member {
                id,
                addr: String::new(),
            })
            .collect();
        for id in 1..=s.cfg.nodes {
            let mut disk = MemStorage::default();
            bootstrap(&mut disk, members.clone());
            s.disks.insert(id, disk);
            s.start(id);
        }
        for i in 0..s.cfg.clients {
            s.clients.push(Client {
                id: i as u64 + 1,
                seq: 0,
                target: 1 + i as u64 % s.cfg.nodes,
                op: None,
                next_op_at: 0,
                seen: BTreeMap::new(),
                abandoned: Vec::new(),
            });
        }
        s
    }

    fn start(&mut self, id: NodeId) {
        self.restart_at.remove(&id);
        let disk = self.disks.remove(&id).expect("disk exists");
        let mut rc = raft::Config::new(id);
        rc.election_ticks = self.cfg.election_ticks;
        rc.heartbeat_ticks = 3;
        rc.check_quorum = self.cfg.check_quorum;
        rc.seed = self.rng.next_u64();
        self.nodes.insert(
            id,
            Node {
                raft: RaftNode::new(rc, disk),
                kv: KvStore::default(),
                applied: 0,
                writes: BTreeMap::new(),
                reads: BTreeMap::new(),
                ready_reads: Vec::new(),
                next_token: 0,
            },
        );
    }

    /// Kills a node. With `mid_fsync`, it dies inside its next `ready`: the leader's early
    /// AppendEntries are already on the wire but its own unsynced writes are lost.
    fn crash(&mut self, id: NodeId, mid_fsync: bool) {
        let Some(mut n) = self.nodes.remove(&id) else {
            return;
        };
        if mid_fsync {
            n.raft.storage_mut().fail_sync = true;
            let mut early = Vec::new();
            drop(n.raft.ready_with(|m| early.push(m)));
            for m in early {
                self.send(Envelope::Raft(m));
            }
        }
        let mut disk = n.raft.into_storage();
        disk.crash();
        self.disks.insert(id, disk);
        self.restart_at
            .insert(id, self.now + self.rng.range(20, 300));
        self.stats.crashes += 1;
    }

    fn all_ids(&self) -> Vec<NodeId> {
        self.nodes
            .keys()
            .chain(self.disks.keys())
            .copied()
            .collect()
    }

    fn run(&mut self) -> Report {
        let end = self.cfg.ticks;
        let mut next_fault = 50;
        while self.now < end + 3000 {
            if self.now < end && self.cfg.faults && self.now >= next_fault {
                self.nemesis();
                next_fault = self.now + self.rng.range(30, 150);
            }
            if self.now == end {
                // Heal everything and let clients finish.
                self.blocked.clear();
                self.drop_rate = 0.0;
                self.dup_rate = 0.0;
                let down: Vec<NodeId> = self.disks.keys().copied().collect();
                for id in down {
                    self.start(id);
                }
            }
            let due: Vec<NodeId> = (self.restart_at.iter())
                .filter(|&(_, &t)| t <= self.now)
                .map(|(&id, _)| id)
                .collect();
            for id in due {
                self.start(id);
            }
            self.deliver();
            let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            for &id in &ids {
                self.nodes.get_mut(&id).expect("live").raft.tick();
            }
            for &id in &ids {
                self.process_ready(id);
            }
            self.drive_clients(self.now < end);
            self.check_election_safety();
            if self.now > end && self.clients.iter().all(|c| c.op.is_none()) {
                break;
            }
            self.now += 1;
        }
        self.stats.heal_ticks = self.now - end;
        self.check_convergence();

        for c in &self.clients {
            if let Some(p) = &c.op {
                let op = &mut self.history[p.hist].1;
                op.output = Output::Unknown;
                op.ret = u64::MAX;
            }
        }
        self.stats.ops_unknown += self.clients.iter().filter(|c| c.op.is_some()).count();
        self.stats.ops_ok = self.history.len() - self.stats.ops_unknown;
        // A read that never returned constrains nothing.
        let history = self
            .history
            .drain(..)
            .filter(|(_, op)| !(op.input == Input::Get && op.output == Output::Unknown));
        let outcome = check_partitioned(&Register, history, 20_000_000);
        Report {
            outcome,
            stats: self.stats.clone(),
        }
    }

    // ---- network ------------------------------------------------------------------------

    fn send(&mut self, env: Envelope) {
        self.stats.messages += 1;
        if let Envelope::Raft(m) = &env {
            if self.blocked.contains(&(m.from, m.to)) {
                self.stats.dropped += 1;
                return;
            }
            if matches!(m.msg, Msg::InstallSnapshot { .. }) {
                self.stats.snapshots_sent += 1;
            }
        }
        if self.rng.chance(self.drop_rate) {
            self.stats.dropped += 1;
            return;
        }
        let copies = if self.rng.chance(self.dup_rate) { 2 } else { 1 };
        for _ in 0..copies {
            let mut delay = self.rng.range(1, MAX_LATENCY + 1);
            if self.rng.chance(0.01) {
                delay += self.rng.range(10, 40);
            }
            self.net_seq += 1;
            self.net.push((self.now + delay, self.net_seq, env.clone()));
        }
    }

    fn deliver(&mut self) {
        let now = self.now;
        let (mut due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.net)
            .into_iter()
            .partition(|(at, _, _)| *at <= now);
        self.net = rest;
        due.sort_by_key(|(at, seq, _)| (*at, *seq));
        for (_, _, env) in due {
            match env {
                Envelope::Raft(m) => {
                    if let Some(n) = self.nodes.get_mut(&m.to) {
                        n.raft.step(m);
                    }
                }
                Envelope::Request { client, node, req } => self.handle_request(client, node, req),
                Envelope::Response { client, resp } => self.handle_response(client, resp),
            }
        }
    }

    // ---- nodes --------------------------------------------------------------------------

    fn respond(&mut self, client: usize, resp: Resp) {
        self.send(Envelope::Response { client, resp });
    }

    fn handle_request(&mut self, client: usize, id: NodeId, req: Req) {
        let unsafe_reads = self.cfg.unsafe_local_reads;
        let Some(n) = self.nodes.get_mut(&id) else {
            return;
        };
        let resp = match req {
            Req::Write(cmd) => {
                let key = (cmd.client, cmd.seq);
                let seq = cmd.seq;
                match n.raft.propose(cmd.encode_to_vec()) {
                    Ok(_) => {
                        n.writes.insert(key, client);
                        return;
                    }
                    Err(e) => Resp::NotLeader { seq, hint: hint(e) },
                }
            }
            Req::Read { seq, key } => {
                if unsafe_reads && n.raft.is_leader() {
                    Resp::Read {
                        seq,
                        value: get(&n.kv, &key),
                    }
                } else {
                    n.next_token += 1;
                    let token = n.next_token;
                    match n.raft.read_index(token) {
                        Ok(()) => {
                            n.reads.insert(token, (client, seq, key));
                            return;
                        }
                        Err(e) => Resp::NotLeader { seq, hint: hint(e) },
                    }
                }
            }
        };
        self.respond(client, resp);
    }

    fn process_ready(&mut self, id: NodeId) {
        let n = self.nodes.get_mut(&id).expect("live");
        let rd = n.raft.ready();
        let mut out: Vec<(usize, Resp)> = Vec::new();
        if let Some(s) = rd.snapshot {
            n.kv = KvStore::restore(&s.data);
            n.applied = s.meta.index;
        }
        for e in rd.committed {
            // State machine safety: every node applies the same entry at each index.
            match self.applied_log.get(&e.index) {
                Some(prev) => assert_eq!(
                    prev, &e,
                    "seed {}: node {id} applied a different entry at {}",
                    self.cfg.seed, e.index
                ),
                None => {
                    self.applied_log.insert(e.index, e.clone());
                }
            }
            if let Payload::Command(data) = &e.payload {
                let cmd = Command::decode(data.as_slice()).expect("valid command");
                if let Some(res) = n.kv.apply(&cmd)
                    && let Some(client) = n.writes.remove(&(cmd.client, cmd.seq))
                {
                    out.push((client, Resp::Write { seq: cmd.seq, res }));
                }
            }
            n.applied = e.index;
        }
        for (token, index) in rd.reads {
            if let Some((client, seq, key)) = n.reads.remove(&token) {
                n.ready_reads.push((index, client, seq, key));
            }
        }
        for token in rd.aborted_reads {
            if let Some((client, seq, _)) = n.reads.remove(&token) {
                out.push((client, Resp::NotLeader { seq, hint: None }));
            }
        }
        let applied = n.applied;
        let (now, later): (Vec<_>, Vec<_>) =
            n.ready_reads.drain(..).partition(|(i, ..)| *i <= applied);
        n.ready_reads = later;
        for (_, client, seq, key) in now {
            out.push((
                client,
                Resp::Read {
                    seq,
                    value: get(&n.kv, &key),
                },
            ));
        }
        if n.applied >= n.raft.snapshot_index() + self.cfg.snapshot_every {
            n.raft.compact(n.applied, n.kv.snapshot());
            self.stats.compactions += 1;
        }
        self.stats.max_term = self.stats.max_term.max(n.raft.term());
        for m in rd.messages {
            self.send(Envelope::Raft(m));
        }
        for (client, resp) in out {
            self.respond(client, resp);
        }
    }

    // ---- clients ------------------------------------------------------------------------

    fn drive_clients(&mut self, new_ops: bool) {
        let ids = self.all_ids();
        for i in 0..self.clients.len() {
            let now = self.now;
            let c = &self.clients[i];
            if let Some(p) = &c.op {
                if now >= p.deadline && p.timeouts >= GIVE_UP_AFTER && new_ops {
                    // Indeterminate (Jepsen's `info`): the write may still take effect, but
                    // only until this client's next write applies (sessions drop older seqs).
                    let c = &mut self.clients[i];
                    let p = c.op.take().expect("pending");
                    if matches!(p.req, Req::Write(_)) {
                        c.abandoned.push(p.hist);
                    }
                    self.stats.ops_unknown += 1;
                } else if now >= p.deadline {
                    let target = ids[self.rng.range(0, ids.len() as u64) as usize];
                    self.clients[i].op.as_mut().expect("pending").timeouts += 1;
                    self.resend(i, target);
                }
            } else if new_ops && now >= c.next_op_at {
                self.invoke(i);
            }
        }
    }

    fn invoke(&mut self, i: usize) {
        let key = format!("k{}", self.rng.range(0, self.cfg.keys as u64));
        let roll = self.rng.range(0, 10);
        let c = &mut self.clients[i];
        c.seq += 1;
        let fresh = Some(format!("{}.{}", c.id, c.seq));
        let (input, req) = match roll {
            0..5 => (
                Input::Get,
                Req::Read {
                    seq: c.seq,
                    key: key.clone(),
                },
            ),
            5..9 => {
                let v = if roll == 8 { None } else { fresh };
                (Input::Put(v.clone()), write(c, &key, v, None))
            }
            _ => {
                let expected = c.seen.get(&key).cloned().flatten();
                (
                    Input::Cas {
                        expected: expected.clone(),
                        new: fresh.clone(),
                    },
                    write(c, &key, fresh, Some(expected)),
                )
            }
        };
        self.clock += 1;
        self.history.push((
            key,
            Operation {
                input,
                output: Output::Unknown,
                call: self.clock,
                ret: u64::MAX,
            },
        ));
        let target = self.clients[i].target;
        self.clients[i].op = Some(Pending {
            req,
            hist: self.history.len() - 1,
            deadline: 0,
            timeouts: 0,
        });
        self.resend(i, target);
    }

    fn resend(&mut self, i: usize, target: NodeId) {
        let deadline = self.now + CLIENT_TIMEOUT;
        let c = &mut self.clients[i];
        c.target = target;
        let p = c.op.as_mut().expect("pending op");
        p.deadline = deadline;
        let req = p.req.clone();
        self.send(Envelope::Request {
            client: i,
            node: target,
            req,
        });
    }

    fn handle_response(&mut self, i: usize, resp: Resp) {
        let c = &mut self.clients[i];
        let seq = match &resp {
            Resp::Write { seq, .. } | Resp::Read { seq, .. } | Resp::NotLeader { seq, .. } => *seq,
        };
        if c.seq != seq || c.op.is_none() {
            return; // stale response for an earlier op
        }
        let p = c.op.as_ref().expect("checked");
        let (key, op) = &mut self.history[p.hist];
        let output = match (resp, &p.req) {
            (Resp::NotLeader { hint, .. }, _) => {
                let ids = self.all_ids();
                let target =
                    hint.unwrap_or_else(|| ids[self.rng.range(0, ids.len() as u64) as usize]);
                // Back off briefly so an election can finish.
                let c = &mut self.clients[i];
                c.target = target;
                c.op.as_mut().expect("checked").deadline = self.now + self.rng.range(2, 8);
                return;
            }
            (Resp::Read { value, .. }, _) => {
                c.seen.insert(key.clone(), value.clone());
                Output::Get(value)
            }
            (Resp::Write { res, .. }, Req::Write(cmd)) if cmd.cas => {
                let now = if res.ok { cmd.value.clone() } else { res.prev };
                c.seen.insert(key.clone(), now.map(utf8));
                Output::Cas(res.ok)
            }
            (Resp::Write { .. }, _) => Output::Put,
        };
        self.clock += 1;
        let output_kind = output.clone();
        op.output = output;
        op.ret = self.clock;
        if matches!(output_kind, Output::Put | Output::Cas(_)) {
            for h in c.abandoned.drain(..) {
                self.history[h].1.ret = self.clock;
            }
        }
        if self.now <= self.cfg.ticks {
            self.stats.max_stall = self.stats.max_stall.max(self.now - self.last_completion);
        }
        self.last_completion = self.now;
        c.op = None;
        c.next_op_at = self.now + self.rng.range(0, 4);
    }

    // ---- nemesis ------------------------------------------------------------------------

    fn nemesis(&mut self) {
        let max_down = if self.cfg.membership {
            1
        } else {
            (self.cfg.nodes as usize - 1) / 2
        };
        let actions = if self.cfg.membership { 7 } else { 6 };
        match self.rng.range(0, actions) {
            0 => self.blocked.clear(),
            1 => {
                let mut ids = self.all_ids();
                shuffle(&mut self.rng, &mut ids);
                let cut = self.rng.range(1, ids.len() as u64) as usize;
                self.partition(&ids[..cut], &ids[cut..]);
            }
            2 => {
                if let Some(l) = self.leader() {
                    let rest: Vec<NodeId> =
                        self.all_ids().into_iter().filter(|&i| i != l).collect();
                    self.partition(&[l], &rest);
                }
            }
            3 => {
                if self.disks.len() < max_down {
                    let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
                    let id = ids[self.rng.range(0, ids.len() as u64) as usize];
                    let mid_fsync = self.rng.chance(0.5);
                    self.crash(id, mid_fsync);
                }
            }
            4 => {
                if let Some(&id) = self.disks.keys().next() {
                    self.start(id);
                }
            }
            5 => {
                self.drop_rate = [0.0, 0.05, 0.2][self.rng.range(0, 3) as usize];
                self.dup_rate = [0.0, 0.05][self.rng.range(0, 2) as usize];
            }
            _ => self.change_membership(),
        }
    }

    fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        self.blocked.clear();
        for &x in a {
            for &y in b {
                self.blocked.insert((x, y));
                self.blocked.insert((y, x));
            }
        }
        self.stats.partitions += 1;
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.raft.is_leader())
            .max_by_key(|(_, n)| n.raft.term())
            .map(|(&id, _)| id)
    }

    fn change_membership(&mut self) {
        let Some(l) = self.leader() else { return };
        let voters: Vec<NodeId> = self.nodes[&l].raft.members().iter().map(|m| m.id).collect();
        let add = voters.len() < 5 && (voters.len() <= 3 || self.rng.chance(0.5));
        let change = if add {
            ConfChange::Add(Member {
                id: self.next_id,
                addr: String::new(),
            })
        } else {
            ConfChange::Remove(voters[self.rng.range(0, voters.len() as u64) as usize])
        };
        let leader = self.nodes.get_mut(&l).expect("live");
        if leader.raft.propose_conf_change(change).is_ok() {
            self.stats.conf_changes += 1;
            if add {
                // A fresh, empty node that waits for the leader to replicate to it.
                self.disks.insert(self.next_id, MemStorage::default());
                self.start(self.next_id);
                self.next_id += 1;
            }
        }
    }

    // ---- invariants ---------------------------------------------------------------------

    fn check_election_safety(&mut self) {
        for (&id, n) in &self.nodes {
            if n.raft.is_leader() {
                let prev = *self.leaders.entry(n.raft.term()).or_insert(id);
                assert_eq!(
                    prev,
                    id,
                    "seed {}: two leaders in term {}",
                    self.cfg.seed,
                    n.raft.term()
                );
            }
        }
    }

    fn check_convergence(&self) {
        let mut by_index: BTreeMap<u64, &KvStore> = BTreeMap::new();
        for (id, n) in &self.nodes {
            if let Some(other) = by_index.insert(n.applied, &n.kv) {
                assert_eq!(other, &n.kv, "seed {}: node {id} diverged", self.cfg.seed);
            }
        }
    }
}

fn hint(e: ProposeError) -> Option<NodeId> {
    match e {
        ProposeError::NotLeader(h) => h,
        _ => None,
    }
}

fn utf8(v: Vec<u8>) -> String {
    String::from_utf8(v).expect("sim values are utf8")
}

fn get(kv: &KvStore, key: &str) -> Value {
    kv.get(key.as_bytes()).cloned().map(utf8)
}

fn write(c: &Client, key: &str, value: Value, expected: Option<Value>) -> Req {
    Req::Write(Command {
        client: c.id,
        seq: c.seq,
        key: key.as_bytes().to_vec(),
        value: value.map(String::into_bytes),
        cas: expected.is_some(),
        expected: expected.flatten().map(String::into_bytes),
    })
}

fn shuffle(rng: &mut Rng, v: &mut [NodeId]) {
    for i in (1..v.len()).rev() {
        v.swap(i, rng.range(0, i as u64 + 1) as usize);
    }
}
