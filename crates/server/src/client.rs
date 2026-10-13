//! Client with leader discovery and retries. Writes keep their `(client, seq)` across retries,
//! so a write that was applied but whose reply was lost is not applied twice.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::pb;
use crate::pb::kv_client::KvClient;
use crate::service::LEADER_HEADER;

#[derive(Debug, PartialEq, Eq)]
pub enum ClientError {
    /// No reply within the op timeout. The write may or may not have taken effect.
    Timeout,
    Rejected(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ClientError {}

pub struct Client {
    addrs: Vec<String>,
    leader: usize,
    conns: HashMap<String, KvClient<Channel>>,
    id: u64,
    seq: u64,
    pub attempt_timeout: Duration,
    pub op_timeout: Duration,
}

enum Call {
    Get(pb::GetRequest),
    Write(pb::WriteRequest),
}

enum Reply {
    Get(pb::GetResponse),
    Write(pb::WriteResponse),
}

impl Client {
    pub fn new(addrs: Vec<String>) -> Self {
        // The id keys this client's session, so it must be unique: clock alone is not (macOS
        // clocks tick in microseconds), hence the per-process counter.
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
            ^ ((std::process::id() as u64) << 32)
            ^ NEXT
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        Self {
            leader: 0,
            addrs,
            conns: HashMap::new(),
            id: raft::Rng::new(seed).next_u64() | 1,
            seq: 0,
            attempt_timeout: Duration::from_millis(500),
            op_timeout: Duration::from_secs(5),
        }
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self
            .call(Call::Get(pb::GetRequest { key: key.to_vec() }))
            .await?
        {
            Reply::Get(r) => Ok(r.value),
            Reply::Write(_) => unreachable!(),
        }
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        self.write(key, Some(value), None).await.map(|_| ())
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<(), ClientError> {
        self.write(key, None, None).await.map(|_| ())
    }

    /// Sets `key` to `new` (None deletes) iff it currently equals `expected` (None = absent).
    /// Returns whether it swapped and the value before.
    pub async fn cas(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<(bool, Option<Vec<u8>>), ClientError> {
        let r = self.write(key, new, Some(expected)).await?;
        Ok((r.ok, r.prev))
    }

    async fn write(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        expected: Option<Option<&[u8]>>,
    ) -> Result<pb::WriteResponse, ClientError> {
        self.seq += 1;
        let req = pb::WriteRequest {
            client: self.id,
            seq: self.seq,
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            cas: expected.is_some(),
            expected: expected.flatten().map(<[u8]>::to_vec),
        };
        match self.call(Call::Write(req)).await? {
            Reply::Write(r) => Ok(r),
            Reply::Get(_) => unreachable!(),
        }
    }

    async fn call(&mut self, call: Call) -> Result<Reply, ClientError> {
        let deadline = tokio::time::Instant::now() + self.op_timeout;
        while tokio::time::Instant::now() < deadline {
            let addr = self.addrs[self.leader].clone();
            let mut c = self.conn(&addr)?;
            let fut = async {
                match &call {
                    Call::Get(r) => c.get(r.clone()).await.map(|r| Reply::Get(r.into_inner())),
                    Call::Write(r) => c
                        .write(r.clone())
                        .await
                        .map(|r| Reply::Write(r.into_inner())),
                }
            };
            match tokio::time::timeout(self.attempt_timeout, fut).await {
                Ok(Ok(reply)) => return Ok(reply),
                Ok(Err(s)) if s.code() == Code::InvalidArgument => {
                    return Err(ClientError::Rejected(s.message().into()));
                }
                Ok(Err(s)) if s.code() == Code::FailedPrecondition => {
                    self.follow_hint(&s);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                _ => {
                    self.leader = (self.leader + 1) % self.addrs.len();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        Err(ClientError::Timeout)
    }

    fn follow_hint(&mut self, s: &Status) {
        let hint = s
            .metadata()
            .get(LEADER_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        match hint {
            Some(h) => {
                self.leader = self.addrs.iter().position(|a| *a == h).unwrap_or_else(|| {
                    self.addrs.push(h);
                    self.addrs.len() - 1
                });
            }
            None => self.leader = (self.leader + 1) % self.addrs.len(),
        }
    }

    fn conn(&mut self, addr: &str) -> Result<KvClient<Channel>, ClientError> {
        if let Some(c) = self.conns.get(addr) {
            return Ok(c.clone());
        }
        let ep = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| ClientError::Rejected(e.to_string()))?
            .connect_timeout(self.attempt_timeout)
            .tcp_nodelay(true);
        let c = KvClient::new(ep.connect_lazy());
        self.conns.insert(addr.to_string(), c.clone());
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: ids were seeded from the clock alone, so clients created in the same
    /// microsecond shared a session and one's writes were "deduplicated" away.
    #[test]
    fn ids_are_unique_across_threads() {
        let ids: Vec<u64> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..8)
                .map(|_| {
                    s.spawn(|| {
                        (0..1000)
                            .map(|_| Client::new(vec![]).id)
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
        });
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len());
    }
}
