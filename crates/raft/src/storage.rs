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

/// In-memory storage. Survives a simulated crash because the simulator keeps it when it
/// drops the node (see [`crate::RaftNode::into_storage`]).
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    pub hs: HardState,
    pub snapshot: Snapshot,
    pub entries: Vec<Entry>,
}

impl Storage for MemStorage {
    fn initial_state(&mut self) -> (HardState, Snapshot, Vec<Entry>) {
        (self.hs, self.snapshot.clone(), self.entries.clone())
    }

    fn set_hard_state(&mut self, hs: HardState) {
        self.hs = hs;
    }

    fn append(&mut self, entries: &[Entry]) {
        self.entries.extend_from_slice(entries);
    }

    fn truncate(&mut self, from: u64) {
        self.entries.retain(|e| e.index < from);
    }

    fn save_snapshot(&mut self, snap: &Snapshot) {
        self.entries.retain(|e| e.index > snap.meta.index);
        self.snapshot = snap.clone();
    }

    fn sync(&mut self) {}
}
