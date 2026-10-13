//! Raft core cost without I/O: a 3-node in-memory cluster with synchronous message delivery.
//! Measures proposals committed per second on the leader for different batch sizes.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use raft::{Config, MemStorage, Member, RaftNode, bootstrap};

fn cluster() -> Vec<RaftNode<MemStorage>> {
    let members: Vec<Member> = (1..=3)
        .map(|id| Member {
            id,
            addr: String::new(),
        })
        .collect();
    let mut nodes: Vec<_> = (1..=3)
        .map(|id| {
            let mut s = MemStorage::default();
            bootstrap(&mut s, members.clone());
            RaftNode::new(Config::new(id), s)
        })
        .collect();
    while !nodes[0].is_leader() {
        nodes[0].tick();
        pump(&mut nodes);
    }
    nodes
}

/// Delivers messages until the cluster is quiet; returns the leader's newly committed count.
fn pump(nodes: &mut [RaftNode<MemStorage>]) -> usize {
    let mut committed = 0;
    loop {
        let mut msgs = Vec::new();
        for (i, n) in nodes.iter_mut().enumerate() {
            let rd = n.ready();
            if i == 0 {
                committed += rd.committed.len();
            }
            msgs.extend(rd.messages);
            if n.applied() > n.snapshot_index() + 50_000 {
                n.compact(n.applied(), Vec::new());
            }
        }
        if msgs.is_empty() {
            return committed;
        }
        for m in msgs {
            nodes[(m.to - 1) as usize].step(m);
        }
    }
}

fn replication(c: &mut Criterion) {
    let mut g = c.benchmark_group("raft_commit");
    let payload = vec![7u8; 128];
    for batch in [1usize, 64, 512] {
        let mut nodes = cluster();
        g.throughput(Throughput::Elements(batch as u64));
        g.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter(|| {
                for _ in 0..batch {
                    nodes[0].propose(payload.clone()).expect("leader");
                }
                let mut done = 0;
                while done < batch {
                    done += pump(&mut nodes);
                }
            })
        });
    }
    g.finish();
}

criterion_group!(benches, replication);
criterion_main!(benches);
