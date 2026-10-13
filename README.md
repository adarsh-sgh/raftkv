# raftkv

A Raft-replicated key-value store in Rust. Nodes talk gRPC (tonic on Tokio) to clients and to
each other, persist the Raft log to local files with fsync, and implement leader election
(with PreVote and CheckQuorum), log replication, snapshotting with InstallSnapshot, single-server
membership changes and linearizable ReadIndex reads. Correctness is checked by a Jepsen-style
harness that injects partitions, crashes and message loss, records client histories and runs a
Porcupine-style linearizability checker over them on every CI run.

```
 client ──gRPC Kv.Get/Write──▶ ┌──────────────────── node ─────────────────────┐
 (leader discovery,            │ tonic services ──mpsc──▶ driver task          │
  retries, session id)         │                          ├─ RaftNode (sans-IO)│◀─┐ Raft.Send
                               │                          ├─ KvStore + sessions│  │ (one client
 raftkv CLI ──Admin.Add/Remove─▶                          └─ FileStorage ──────┼─▶│  stream per
                               │                               log, hardstate, │  │  peer)
                               │                               snapshot (fsync)│  │
                               └───────────────────────────────────────────────┘  ▼ other nodes
```

| crate | what |
|---|---|
| `crates/raft` | Sans-IO consensus core: no threads, clock or sockets. Callers feed `tick`/`step`/`propose` and drain `Ready`. |
| `crates/kv` | State machine: put / delete / compare-and-set with per-client sessions for exactly-once writes. |
| `crates/lincheck` | Linearizability checker (Wing & Gong search, Lowe's memoisation, per-key partitioning) and a register model. |
| `crates/sim` | Deterministic cluster simulator with a fault-injecting network and nemesis; runs the real core and state machine. |
| `crates/server` (`raftkv`) | gRPC node, file storage, peer transport, client library, CLI, load generator, in-process cluster. |

## Run

```sh
cargo build --release
B=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
for i in 1 2 3; do
  target/release/raftkv-server --id $i --listen 127.0.0.1:700$i --data-dir data/$i --bootstrap $B &
done
A=--addrs=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
target/release/raftkv $A put greeting hello
target/release/raftkv $A get greeting               # hello (ReadIndex on the leader)
target/release/raftkv $A cas greeting hello bye     # swapped=true prev=hello
target/release/raftkv $A status                     # role, term, commit/applied, members per node

# membership: start an empty node, then add it (it catches up by log or InstallSnapshot)
target/release/raftkv-server --id 4 --listen 127.0.0.1:7004 --data-dir data/4 &
target/release/raftkv $A add 4 127.0.0.1:7004
target/release/raftkv $A remove 1
```

```sh
cargo nextest run --workspace --release      # all tests (SIM_SEEDS=512 for the CI depth)
cargo run --release -p sim --example replay -- 42   # replay one simulated schedule
cargo bench -p raft -p lincheck              # criterion
scripts/load.sh                              # load matrix against an in-process 3-node cluster
```

## Design decisions

- **Sans-IO core.** `RaftNode` is a pure state machine over `(tick, message, proposal)` inputs.
  The same code runs under the deterministic simulator and the Tokio server, so the simulator
  tests the real protocol, not a model of it. Storage is a trait; `ready()` syncs it before it
  returns any message or committed entry, so nothing is acknowledged before it is durable.
- **Election hygiene.** PreVote stops a partitioned node from inflating terms and deposing a
  healthy leader when it rejoins. CheckQuorum makes a leader that cannot reach a majority step
  down, and followers that heard from a leader within the election timeout ignore vote requests
  (leader stickiness), which also keeps removed nodes from disrupting the cluster.
- **Single-server membership changes** (Raft thesis §4.1) instead of joint consensus: a config
  entry takes effect when appended, at most one may be uncommitted, and a new leader must commit
  an entry of its own term before proposing one (the fix for the bug Ongaro published in 2015).
  A leader that removes itself keeps leading until the removal commits, then steps down.
- **Snapshots.** The KV store (data plus client sessions) is serialised every `snapshot_every`
  applied entries and the log prefix is dropped. A follower whose `next_index` falls behind the
  snapshot gets `InstallSnapshot`; if its log already contains the snapshot's last entry it keeps
  the suffix. Joiners start with an empty config and never campaign until they learn one.
- **Linearizable reads without the log.** ReadIndex: the leader records `max(commit, noop index)`,
  confirms leadership with one heartbeat round (a `ctx` echoed in AppendEntries responses), then
  serves the read once applied. Concurrent reads share one round. No clock-based leases.
- **Exactly-once writes.** Each write carries `(client id, seq)`; the state machine caches the
  last result per client, so a retry that lands in the log twice is applied once. Clients retry
  the same seq across leaders until they get an answer.
- **Batching and fsync.** One Tokio task owns the Raft node. Each loop drains up to 1024 queued
  requests and messages, then calls `ready()` once: a batch costs one `fdatasync` and one
  AppendEntries per follower. The leader hands AppendEntries to the transport *before* its own
  fsync and counts itself toward commit only up to its durable index (thesis §10.2.1), so
  follower writes overlap the leader's. On the M5 this took single-client puts from 10.5 ms to
  7.0 ms p50 and 256-client puts from 6.4k to 10.6k ops/s.
- **Log format.** Records are `[len][crc32][protobuf Entry]`; a torn tail after a crash is cut
  on open. Hard state and snapshot files are replaced atomically (write temp, fsync, rename,
  fsync dir). Compaction rewrites the log tail into a new file.
- **Transport.** Every node keeps one client-streaming `Raft.Send` call per peer, fed by a
  bounded queue; a full queue or broken stream drops messages (Raft retransmits) so the Raft
  task never blocks on the network. Messages carry the sender's address so a joining node can
  answer a leader it has no config for yet.

## Testing: Jepsen-style, in CI

Two harnesses record histories of concurrent client operations (get, put, delete, CAS on a few
hot keys) and check them with `lincheck`. Operations that time out are recorded as
indeterminate: they may take effect at any later point, but no later than the same client's
next completed write (sessions discard older sequence numbers), which keeps the search bounded.
The register model tracks the *set* of possible values, so "may or may not have happened" stays
a deterministic search.

**Deterministic simulation** (`crates/sim`, `crates/sim/tests/jepsen.rs`). Real `RaftNode`s and
`KvStore`s on a simulated network with 1-4 tick latency, 1% long delays (reordering), message
drop rates up to 20% and duplication. A nemesis every 30-150 ticks partitions the cluster
randomly or isolates the leader, crashes and restarts nodes (half of the crashes happen *inside*
`ready()` after the leader's early AppendEntries left but before its fsync, losing unsynced
writes), and adds/removes members. Besides linearizability it asserts election safety (one
leader per term), state machine safety (every node applies the same entry at each index),
replica convergence and recovery after faults heal. Runs are a pure function of the seed;
CI runs 512 seeds for each of 3 configurations (3 nodes, 5 nodes, membership churn): 1,536
schedules, 1.14M completed operations, ~7,300 crashes, ~15,300 partitions and ~2,000
membership changes per run, in about 10 s.

The negative control matters as much: with `unsafe_local_reads` (leader answers reads from
local state, no ReadIndex, no CheckQuorum) the checker must flag the stale reads a deposed
leader serves. It does in 477 of 512 seeds; the test fails if it ever stops catching them.

**Real cluster over gRPC** (`crates/server/tests/cluster.rs`). Three nodes, each on its own Tokio
runtime with fsynced file storage, 8 concurrent clients, and a scripted nemesis: isolate the
leader, 20% message loss, kill -9 the leader (runtime dropped, only fsynced data survives) and
restart it, add an empty fourth node after the log was compacted (it must be caught up by
InstallSnapshot), remove an original member, crash the new leader. The 2-5k-op history must be
linearizable and all members must converge. This test caught a real bug: client ids were seeded
from the clock, which ticks in microseconds on macOS, so clients created together shared a
session and one client's writes were "deduplicated" away; 8,000 ids had 170 distinct values.

## Benchmarks

Every number below comes from `scripts/load.sh` (closed-loop clients against an in-process
3-node cluster: three separate runtimes on one machine, gRPC over loopback, 128-byte values,
1,000 keys, 2 s warm-up then 10 s measured) or from `cargo bench`. Server and clients share the
machine, so these bound the system's own overhead rather than predict a real deployment.

**Apple M5 laptop (10 cores)**, macOS. Rust's `sync_data` issues `F_FULLFSYNC` on macOS, and all
three replicas share one SSD, so durable writes are flush-bound. Other workloads were running
(load average 9-12 during the run).

| workload | clients | fsync | ops/s | p50 ms | p99 ms | p99.9 ms |
|---|---:|---|---:|---:|---:|---:|
| put | 1 | on | 121 | 8.02 | 14.08 | 16.50 |
| put | 32 | on | 1,525 | 20.21 | 38.94 | 58.27 |
| put | 256 | on | 10,759 | 23.14 | 39.20 | 43.39 |
| put | 32 | off | 54,846 | 0.51 | 1.65 | 5.17 |
| put | 256 | off | 72,884 | 3.32 | 7.65 | 10.95 |
| get (ReadIndex) | 32 | on | 57,329 | 0.51 | 1.35 | 3.04 |
| get (ReadIndex) | 256 | on | 59,889 | 3.62 | 14.40 | 23.65 |
| 50/50 get/put | 64 | on | 3,249 | 19.25 | 35.87 | 55.58 |

**GitHub Actions `ubuntu-latest`, 4 vCPU AMD EPYC 9V74** (the `bench` job runs the same script
with 5 s rows on every push and puts the table in the job summary; these rows are from
[run 38485720808](https://github.com/adarsh-sgh/raftkv/actions/runs/38485720808)). Linux
`fdatasync` on the runner's disk is much cheaper than `F_FULLFSYNC`, so durable writes are
CPU-bound here. Runner hardware differs between runs: an earlier run of the same server code
measured 39.6k ops/s for the 256-client put row.

| workload | clients | fsync | ops/s | p50 ms | p99 ms | p99.9 ms |
|---|---:|---|---:|---:|---:|---:|
| put | 1 | on | 1,075 | 0.45 | 41.05 | 41.63 |
| put | 32 | on | 15,876 | 1.94 | 3.67 | 6.63 |
| put | 256 | on | 26,359 | 9.48 | 15.58 | 60.45 |
| put | 32 | off | 25,026 | 1.21 | 2.41 | 5.14 |
| put | 256 | off | 33,529 | 7.39 | 14.12 | 17.52 |
| get (ReadIndex) | 32 | on | 28,484 | 1.08 | 2.00 | 2.87 |
| get (ReadIndex) | 256 | on | 38,610 | 6.38 | 12.75 | 15.68 |
| 50/50 get/put | 64 | on | 21,414 | 2.90 | 4.99 | 17.20 |

Criterion (`cargo bench -p raft -p lincheck`, M5):

| bench | result |
|---|---|
| `raft_commit/1`: propose + commit one entry, 3 in-memory nodes, no I/O | 1.03 µs |
| `raft_commit/64` / `512`: batched proposals | 4.1M / 3.5M entries/s |
| `lincheck` 1,000 ops, 4 concurrent clients | 294 µs |
| `lincheck` 10,000 ops, 8 concurrent clients | 127 ms |
| `lincheck` 1,000 ops, 16 concurrent clients | 1.24 s |

The checker is exponential in concurrency (16 overlapping clients on one key is over 4,000x
slower than 4), which is why the harnesses spread load over a few keys and bound
indeterminate writes.

`raft_commit` also found the core's worst cost: followers re-scanned the whole log for the
latest config entry on every append. Only rescanning when a config entry is added or
truncated took one commit from 20.1 µs to 0.58 µs (quick-mode criterion at the time).

## Gaps

- **fsync is synchronous on the Raft task.** While it runs, that node processes nothing, so in
  the mixed workload reads queue behind disk flushes (3.2k ops/s on the M5 vs 57k for pure
  reads). The fix is an async log writer that reports a durable index back to the core.
- **No catch-up phase before a new member votes.** A joiner counts toward quorum as soon as it is
  added, so adding a node to a 3-node cluster with one node down stalls writes until the joiner
  catches up. The thesis' learner rounds are not implemented.
- **Snapshots are sent whole** in one message and kept in memory; fine for this store's sizes,
  not for multi-GB state (no chunking, no streaming from disk).
- **Sessions are never expired**, so client-session state grows with the number of distinct
  clients ever seen.
- **No TLS or auth** on client or peer traffic.
- **Benchmarks run all replicas on one machine**; numbers say nothing about cross-host latency.
- The simulator models crash-during-fsync for the whole unsynced batch only, not torn
  individual writes; torn log tails are covered by the storage test instead.
