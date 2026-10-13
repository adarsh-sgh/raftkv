//! Replicated key-value state machine. Writes carry `(client, seq)` so a retried command that
//! lands in the log twice is applied once and the original result is returned (thesis §6.3).

use std::collections::BTreeMap;

pub use prost::Message;

/// A write. `cas == false`: set `key` to `value` (`None` deletes). `cas == true`: do the same
/// only if the current value equals `expected` (`None` = absent).
#[derive(Clone, PartialEq, Message)]
pub struct Command {
    #[prost(uint64, tag = "1")]
    pub client: u64,
    #[prost(uint64, tag = "2")]
    pub seq: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub key: Vec<u8>,
    #[prost(bytes = "vec", optional, tag = "4")]
    pub value: Option<Vec<u8>>,
    #[prost(bool, tag = "5")]
    pub cas: bool,
    #[prost(bytes = "vec", optional, tag = "6")]
    pub expected: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CmdResult {
    /// False only for a CAS whose comparison failed.
    #[prost(bool, tag = "1")]
    pub ok: bool,
    /// Value before the command.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub prev: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct Pair {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct Session {
    #[prost(uint64, tag = "1")]
    client: u64,
    #[prost(uint64, tag = "2")]
    seq: u64,
    #[prost(message, optional, tag = "3")]
    result: Option<CmdResult>,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotData {
    #[prost(message, repeated, tag = "1")]
    pairs: Vec<Pair>,
    #[prost(message, repeated, tag = "2")]
    sessions: Vec<Session>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct KvStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    /// client -> (last applied seq, its result). Client 0 opts out of deduplication.
    sessions: BTreeMap<u64, (u64, CmdResult)>,
}

impl KvStore {
    /// Applies `cmd`, or returns the cached result for a duplicate. `None` means the command is
    /// older than the client's latest one (the client already moved on).
    pub fn apply(&mut self, cmd: &Command) -> Option<CmdResult> {
        if cmd.client != 0
            && let Some((seq, res)) = self.sessions.get(&cmd.client)
        {
            if cmd.seq == *seq {
                return Some(res.clone());
            }
            if cmd.seq < *seq {
                return None;
            }
        }
        let prev = self.data.get(&cmd.key).cloned();
        let ok = !cmd.cas || prev == cmd.expected;
        if ok {
            match &cmd.value {
                Some(v) => self.data.insert(cmd.key.clone(), v.clone()),
                None => self.data.remove(&cmd.key),
            };
        }
        let res = CmdResult { ok, prev };
        if cmd.client != 0 {
            self.sessions.insert(cmd.client, (cmd.seq, res.clone()));
        }
        Some(res)
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn snapshot(&self) -> Vec<u8> {
        SnapshotData {
            pairs: self
                .data
                .iter()
                .map(|(k, v)| Pair {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            sessions: self
                .sessions
                .iter()
                .map(|(&client, (seq, r))| Session {
                    client,
                    seq: *seq,
                    result: Some(r.clone()),
                })
                .collect(),
        }
        .encode_to_vec()
    }

    pub fn restore(data: &[u8]) -> Self {
        let s = SnapshotData::decode(data).expect("snapshot written by KvStore::snapshot");
        Self {
            data: s.pairs.into_iter().map(|p| (p.key, p.value)).collect(),
            sessions: s
                .sessions
                .into_iter()
                .map(|s| (s.client, (s.seq, s.result.unwrap_or_default())))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_cas_and_snapshot_roundtrip() {
        let mut kv = KvStore::default();
        let put = |seq, v: &str| Command {
            client: 7,
            seq,
            key: b"k".to_vec(),
            value: Some(v.into()),
            ..Default::default()
        };
        assert!(kv.apply(&put(1, "a")).unwrap().ok);
        // A retry of seq 1 after seq 2 applied must not resurrect "a".
        kv.apply(&put(2, "b"));
        assert_eq!(kv.apply(&put(1, "a")), None);
        assert_eq!(
            kv.apply(&put(2, "b")).unwrap().prev.as_deref(),
            Some(&b"a"[..])
        );

        let cas = Command {
            client: 8,
            seq: 1,
            key: b"k".to_vec(),
            value: None,
            cas: true,
            expected: Some(b"zzz".to_vec()),
        };
        assert!(!kv.apply(&cas).unwrap().ok);
        assert_eq!(kv.get(b"k").map(Vec::as_slice), Some(&b"b"[..]));

        let restored = KvStore::restore(&kv.snapshot());
        assert_eq!(restored, kv);
        assert_eq!(KvStore::restore(&[]), KvStore::default());
    }
}
