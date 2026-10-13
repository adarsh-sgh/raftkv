use crate::types::{Entry, HardState, Member, Snapshot, SnapshotMeta};

/// Durable Raft state. Writes may be buffered until `sync`; the core calls `sync` before
/// releasing any message or committed entry, so nothing is acknowledged before it is durable.
pub trait Storage {
    /// Hard state, latest snapshot, and log entries after the snapshot index.
    fn initial_state(&mut self) -> (HardState, Snapshot, Vec<Entry>);
    fn set_hard_state(&mut self, hs: HardState);
    fn append(&mut self, entries: &[Entry]);
    /// Removes every entry with index >= `from`.
    fn truncate(&mut self, from: u64);
    /// Persists `snap` and drops entries with index <= `snap.meta.index`.
    fn save_snapshot(&mut self, snap: &Snapshot);
    fn sync(&mut self);
}

/// Writes the initial config of a brand-new cluster. No-op if `storage` already has state.
pub fn bootstrap<S: Storage>(storage: &mut S, members: Vec<Member>) {
    let (hs, snap, entries) = storage.initial_state();
    if hs == HardState::default() && snap.meta.members.is_empty() && entries.is_empty() {
        let meta = SnapshotMeta {
            index: 0,
            term: 0,
            members,
        };
        storage.save_snapshot(&Snapshot {
            meta,
            data: Vec::new(),
        });
        storage.sync();
    }
}

#[derive(Clone, Debug, Default)]
struct MemState {
    hs: HardState,
    snapshot: Snapshot,
    entries: Vec<Entry>,
}

#[derive(Clone, Debug)]
enum Op {
    HardState(HardState),
    Append(Vec<Entry>),
    Truncate(u64),
    Snapshot(Snapshot),
}

impl MemState {
    fn apply(&mut self, op: Op) {
        match op {
            Op::HardState(hs) => self.hs = hs,
            Op::Append(e) => self.entries.extend(e),
            Op::Truncate(from) => self.entries.retain(|e| e.index < from),
            Op::Snapshot(s) => {
                self.entries.retain(|e| e.index > s.meta.index);
                self.snapshot = s;
            }
        }
    }
}

/// In-memory storage with an explicit durability boundary: writes are journaled and become
/// durable on `sync`; [`MemStorage::crash`] drops the journal. The simulator keeps it across
/// a node crash (see [`crate::RaftNode::into_storage`]).
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    durable: MemState,
    pending: Vec<Op>,
    /// Makes `sync` a no-op, to model a crash during fsync.
    pub fail_sync: bool,
}

impl MemStorage {
    /// Loses everything written since the last successful `sync`.
    pub fn crash(&mut self) {
        self.pending.clear();
        self.fail_sync = false;
    }
}

impl Storage for MemStorage {
    fn initial_state(&mut self) -> (HardState, Snapshot, Vec<Entry>) {
        let mut s = self.durable.clone();
        for op in self.pending.iter().cloned() {
            s.apply(op);
        }
        (s.hs, s.snapshot, s.entries)
    }

    fn set_hard_state(&mut self, hs: HardState) {
        self.pending.push(Op::HardState(hs));
    }

    fn append(&mut self, entries: &[Entry]) {
        self.pending.push(Op::Append(entries.to_vec()));
    }

    fn truncate(&mut self, from: u64) {
        self.pending.push(Op::Truncate(from));
    }

    fn save_snapshot(&mut self, snap: &Snapshot) {
        self.pending.push(Op::Snapshot(snap.clone()));
    }

    fn sync(&mut self) {
        if !self.fail_sync {
            for op in self.pending.drain(..) {
                self.durable.apply(op);
            }
        }
    }
}
