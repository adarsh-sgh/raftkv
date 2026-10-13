//! Durable Raft storage on the local filesystem.
//!
//! `log`: append-only records `[len u32][crc32 u32][pb::Entry]`; a torn tail is cut on open.
//! `hardstate`, `snapshot`: replaced atomically (write temp, fsync, rename).
//! Writes are buffered and made durable in `sync`, which the Raft core calls once per
//! `ready()`, so a batch of proposals costs one fsync.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use prost::Message as _;
use raft::{Entry, HardState, Snapshot, Storage};

use crate::convert::*;
use crate::pb;

pub struct FileStorage {
    dir: PathBuf,
    fsync: bool,
    log: File,
    /// File offset of each entry, entry `first_index + i` at `offsets[i]`.
    offsets: Vec<u64>,
    first_index: u64,
    len: u64,
    buf: Vec<u8>,
    hard_state: Option<HardState>,
    initial: Option<State>,
}

impl FileStorage {
    pub fn open(dir: impl AsRef<Path>, fsync: bool) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let (state, offsets, valid) = load(&dir)?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("log"))?;
        log.set_len(valid)?; // drop a torn tail
        let first_index = state.2.first().map_or(state.1.meta.index + 1, |e| e.index);
        Ok(Self {
            dir,
            fsync,
            log,
            offsets,
            first_index,
            len: valid,
            buf: Vec::new(),
            hard_state: None,
            initial: Some(state),
        })
    }

    fn flush_log(&mut self) {
        if !self.buf.is_empty() {
            self.log.write_all(&self.buf).expect("write raft log");
            self.len += self.buf.len() as u64;
            self.buf.clear();
        }
    }

    fn write_atomic(&self, name: &str, data: &[u8]) {
        let tmp = self.dir.join(format!("{name}.tmp"));
        let mut f = File::create(&tmp).expect("create temp file");
        f.write_all(data).expect("write temp file");
        if self.fsync {
            f.sync_all().expect("fsync temp file");
        }
        fs::rename(&tmp, self.dir.join(name)).expect("rename");
        if self.fsync {
            File::open(&self.dir)
                .and_then(|d| d.sync_all())
                .expect("fsync dir");
        }
    }
}

impl Storage for FileStorage {
    fn initial_state(&mut self) -> (HardState, Snapshot, Vec<Entry>) {
        self.sync();
        // Cached from `open` for the first call; later calls re-read the (synced) files.
        self.initial
            .take()
            .unwrap_or_else(|| load(&self.dir).expect("reload raft state").0)
    }

    fn set_hard_state(&mut self, hs: HardState) {
        self.hard_state = Some(hs);
    }

    fn append(&mut self, entries: &[Entry]) {
        for e in entries {
            debug_assert_eq!(e.index, self.first_index + self.offsets.len() as u64);
            self.offsets.push(self.len + self.buf.len() as u64);
            let body = entry_to_pb(e).encode_to_vec();
            self.buf
                .extend_from_slice(&(body.len() as u32).to_le_bytes());
            self.buf
                .extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
            self.buf.extend_from_slice(&body);
        }
    }

    fn truncate(&mut self, from: u64) {
        self.flush_log();
        let keep = from.saturating_sub(self.first_index) as usize;
        if keep < self.offsets.len() {
            self.len = self.offsets[keep];
            self.offsets.truncate(keep);
            self.log.set_len(self.len).expect("truncate raft log");
        }
    }

    fn save_snapshot(&mut self, snap: &Snapshot) {
        self.flush_log();
        self.write_atomic("snapshot", &snapshot_to_pb(snap).encode_to_vec());
        // Rewrite the log without the compacted prefix.
        let skip = (snap.meta.index + 1).saturating_sub(self.first_index) as usize;
        let tail = if skip < self.offsets.len() {
            let start = self.offsets[skip];
            let mut f = File::open(self.dir.join("log")).expect("open log");
            f.seek(SeekFrom::Start(start)).expect("seek log");
            let mut tail = Vec::new();
            f.read_to_end(&mut tail).expect("read log");
            self.offsets = self.offsets[skip..].iter().map(|o| o - start).collect();
            self.first_index = snap.meta.index + 1;
            tail
        } else {
            self.offsets.clear();
            self.first_index = snap.meta.index + 1;
            Vec::new()
        };
        self.len = tail.len() as u64;
        self.write_atomic("log", &tail);
        self.log = OpenOptions::new()
            .append(true)
            .open(self.dir.join("log"))
            .expect("reopen log");
    }

    fn sync(&mut self) {
        if let Some(hs) = self.hard_state.take() {
            self.write_atomic("hardstate", &hard_state_to_pb(hs).encode_to_vec());
        }
        if !self.buf.is_empty() {
            self.flush_log();
            if self.fsync {
                self.log.sync_data().expect("fsync raft log");
            }
        }
    }
}

type State = (HardState, Snapshot, Vec<Entry>);

/// Reads persisted state; also returns entry offsets and the length of the valid log prefix.
fn load(dir: &Path) -> io::Result<(State, Vec<u64>, u64)> {
    let hs = read_pb::<pb::HardState>(&dir.join("hardstate"))?
        .map(hard_state_from_pb)
        .unwrap_or_default();
    let snap = read_pb::<pb::Snapshot>(&dir.join("snapshot"))?
        .map(snapshot_from_pb)
        .unwrap_or_default();
    let bytes = match fs::read(dir.join("log")) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    let (mut entries, mut offsets, mut pos) = (Vec::new(), Vec::new(), 0usize);
    while let Some((entry, next)) = decode_record(&bytes, pos) {
        // Entries covered by the snapshot can survive a crash during compaction.
        if entry.index > snap.meta.index {
            offsets.push(pos as u64);
            entries.push(entry);
        }
        pos = next;
    }
    Ok(((hs, snap, entries), offsets, pos as u64))
}

fn read_pb<M: prost::Message + Default>(path: &Path) -> io::Result<Option<M>> {
    match fs::read(path) {
        Ok(b) => Ok(Some(M::decode(b.as_slice()).map_err(io::Error::other)?)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn decode_record(b: &[u8], pos: usize) -> Option<(Entry, usize)> {
    let header = b.get(pos..pos + 8)?;
    let len = u32::from_le_bytes(header[..4].try_into().ok()?) as usize;
    let crc = u32::from_le_bytes(header[4..].try_into().ok()?);
    let body = b.get(pos + 8..pos + 8 + len)?;
    if crc32fast::hash(body) != crc {
        return None;
    }
    let e = pb::Entry::decode(body).ok()?;
    Some((entry_from_pb(e), pos + 8 + len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::{Member, Payload, SnapshotMeta};

    fn entry(term: u64, index: u64) -> Entry {
        Entry {
            term,
            index,
            payload: Payload::Command(vec![index as u8; 10]),
        }
    }

    /// Append, truncate, compact, torn tail: everything acknowledged by `sync` survives reopen.
    #[test]
    fn survives_reopen_through_truncate_compaction_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path(), true).unwrap();
        assert_eq!(s.initial_state(), Default::default());
        s.set_hard_state(HardState {
            term: 3,
            vote: Some(2),
        });
        s.append(&(1..=10).map(|i| entry(1, i)).collect::<Vec<_>>());
        s.truncate(8);
        s.append(&[entry(2, 8), entry(2, 9)]);
        s.sync();
        let snap = Snapshot {
            meta: SnapshotMeta {
                index: 5,
                term: 1,
                members: vec![Member {
                    id: 1,
                    addr: "a".into(),
                }],
            },
            data: b"state".to_vec(),
        };
        s.save_snapshot(&snap);
        s.append(&[entry(2, 10)]);
        s.sync();
        drop(s);

        // Simulate a crash mid-write: garbage after the last record.
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.path().join("log"))
            .unwrap();
        f.write_all(&[9, 0, 0, 0, 1, 2]).unwrap();
        drop(f);

        let mut s = FileStorage::open(dir.path(), true).unwrap();
        let (hs, got_snap, entries) = s.initial_state();
        assert_eq!(hs.term, 3);
        assert_eq!(hs.vote, Some(2));
        assert_eq!(got_snap, snap);
        let ids: Vec<(u64, u64)> = entries.iter().map(|e| (e.term, e.index)).collect();
        assert_eq!(ids, vec![(1, 6), (1, 7), (2, 8), (2, 9), (2, 10)]);
        // Appends after reopen land after the cut tail.
        s.truncate(9);
        s.append(&[entry(3, 9)]);
        s.sync();
        drop(s);
        let mut s = FileStorage::open(dir.path(), true).unwrap();
        let last = s.initial_state().2.last().cloned().unwrap();
        assert_eq!((last.term, last.index), (3, 9));
    }
}
