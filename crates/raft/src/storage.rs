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

/// In-memory storage with an explicit durability boundary: writes become durable on `sync`,
/// and [`MemStorage::crash`] throws away everything since. The simulator keeps it across a
/// node crash (see [`crate::RaftNode::into_storage`]).
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    state: MemState,
    durable: MemState,
    /// Makes `sync` a no-op, to model a crash during fsync.
    pub fail_sync: bool,
}

impl MemStorage {
    /// Reverts to the last synced state.
    pub fn crash(&mut self) {
        self.state = self.durable.clone();
        self.fail_sync = false;
    }
}

impl Storage for MemStorage {
    fn initial_state(&mut self) -> (HardState, Snapshot, Vec<Entry>) {
        let s = &self.state;
        (s.hs, s.snapshot.clone(), s.entries.clone())
    }

    fn set_hard_state(&mut self, hs: HardState) {
        self.state.hs = hs;
    }

    fn append(&mut self, entries: &[Entry]) {
        self.state.entries.extend_from_slice(entries);
    }

    fn truncate(&mut self, from: u64) {
        self.state.entries.retain(|e| e.index < from);
    }

    fn save_snapshot(&mut self, snap: &Snapshot) {
        self.state.entries.retain(|e| e.index > snap.meta.index);
        self.state.snapshot = snap.clone();
    }

    fn sync(&mut self) {
        if !self.fail_sync {
            self.durable = self.state.clone();
        }
    }
}
