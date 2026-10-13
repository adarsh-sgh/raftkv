//! End-to-end Jepsen-style test over real gRPC: a 3-node cluster with fsynced file storage,
//! concurrent clients recording a history, and a nemesis that partitions the leader, drops
//! messages, crashes and restarts nodes, adds a fresh node (caught up by InstallSnapshot) and
//! removes an original one. The history must be linearizable.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lincheck::kv::{Input, Output, Register, Value};
use lincheck::{Operation, Outcome, check_partitioned};
use raftkv::pb;
use raftkv::{Client, ClusterOpts, LocalCluster};

#[derive(Default)]
struct History {
    clock: u64,
    ops: Vec<(String, Operation<Input, Output>)>,
}

impl History {
    fn call(&mut self, key: &str, input: Input) -> usize {
        self.clock += 1;
        self.ops.push((
            key.to_string(),
            Operation {
                input,
                output: Output::Unknown,
                call: self.clock,
                ret: u64::MAX,
            },
        ));
        self.ops.len() - 1
    }

    fn ret(&mut self, i: usize, output: Output) -> u64 {
        self.clock += 1;
        self.ops[i].1.output = output;
        self.ops[i].1.ret = self.clock;
        self.clock
    }
}

fn text(v: Option<Vec<u8>>) -> Value {
    v.map(|b| String::from_utf8(b).expect("utf8"))
}

async fn client_loop(id: u64, addrs: Vec<String>, h: Arc<Mutex<History>>, stop: Arc<AtomicBool>) {
    let mut c = Client::new(addrs);
    c.attempt_timeout = Duration::from_millis(300);
    c.op_timeout = Duration::from_secs(2);
    let mut rng = raft::Rng::new(id);
    let mut seen: Vec<Value> = vec![None; 3];
    // Writes that timed out stay pending until this client's next write applies.
    let mut abandoned: Vec<usize> = Vec::new();
    let mut n = 0;
    while !stop.load(Ordering::Relaxed) {
        n += 1;
        let k = rng.range(0, 3) as usize;
        let key = format!("k{k}");
        let fresh = format!("{id}-{n}");
        let roll = rng.range(0, 10);
        let (input, res) = match roll {
            0..5 => {
                let i = h.lock().unwrap().call(&key, Input::Get);
                (i, c.get(key.as_bytes()).await.map(|v| Output::Get(text(v))))
            }
            5..8 => {
                let i = h
                    .lock()
                    .unwrap()
                    .call(&key, Input::Put(Some(fresh.clone())));
                (
                    i,
                    c.put(key.as_bytes(), fresh.as_bytes())
                        .await
                        .map(|_| Output::Put),
                )
            }
            _ => {
                let expected = seen[k].clone();
                let input = Input::Cas {
                    expected: expected.clone(),
                    new: Some(fresh.clone()),
                };
                let i = h.lock().unwrap().call(&key, input);
                let r = c
                    .cas(
                        key.as_bytes(),
                        expected.as_deref().map(str::as_bytes),
                        Some(fresh.as_bytes()),
                    )
                    .await;
                (
                    i,
                    r.map(|(ok, prev)| {
                        seen[k] = if ok { Some(fresh.clone()) } else { text(prev) };
                        Output::Cas(ok)
                    }),
                )
            }
        };
        let mut h = h.lock().unwrap();
        match res {
            Ok(out) => {
                if let Output::Get(v) = &out {
                    seen[k] = v.clone();
                }
                let is_write = !matches!(out, Output::Get(_));
                let t = h.ret(input, out);
                if is_write {
                    for a in abandoned.drain(..) {
                        h.ops[a].1.ret = t;
                    }
                }
            }
            Err(_) if roll >= 5 => abandoned.push(input),
            Err(_) => {}
        }
    }
}

async fn add_member(cluster: &LocalCluster, id: u64, addr: String) {
    for _ in 0..100 {
        if let Some(l) = cluster.leader(Duration::from_secs(5)).await {
            let mut admin = cluster.admin(l).await.expect("admin");
            if admin
                .add_member(pb::Member {
                    id,
                    addr: addr.clone(),
                })
                .await
                .is_ok()
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not add member {id}");
}

async fn remove_member(cluster: &LocalCluster, id: u64) {
    for _ in 0..100 {
        if let Some(l) = cluster.leader(Duration::from_secs(5)).await {
            let mut admin = cluster.admin(l).await.expect("admin");
            if admin
                .remove_member(pb::RemoveMemberRequest { id })
                .await
                .is_ok()
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not remove member {id}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linearizable_over_grpc_under_faults_and_membership_changes() {
    let dir = tempfile::tempdir().unwrap();
    let opts = ClusterOpts {
        snapshot_every: 200,
        ..ClusterOpts::default()
    };
    let mut cluster = LocalCluster::start(3, dir.path(), opts).await.unwrap();
    let history = Arc::new(Mutex::new(History::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let clients: Vec<_> = (1..=8)
        .map(|id| {
            tokio::spawn(client_loop(
                id,
                cluster.addrs(),
                history.clone(),
                stop.clone(),
            ))
        })
        .collect();
    let pause = |ms| tokio::time::sleep(Duration::from_millis(ms));

    pause(1500).await;
    let l = cluster
        .leader(Duration::from_secs(5))
        .await
        .expect("leader");
    let rest: Vec<u64> = cluster.ids().into_iter().filter(|&i| i != l).collect();
    cluster.faults.partition(&[l], &rest);
    pause(1500).await;
    cluster.faults.heal();

    cluster.faults.set_drop_rate(0.2);
    pause(1500).await;
    cluster.faults.heal();

    let l = cluster
        .leader(Duration::from_secs(5))
        .await
        .expect("leader");
    cluster.crash(l).await;
    pause(1500).await;
    cluster.restart(l).await.unwrap();
    pause(1000).await;

    // Node 4 joins empty; by now the log is compacted, so it needs InstallSnapshot.
    let addr = cluster.spawn_joiner(4).await.unwrap();
    add_member(&cluster, 4, addr).await;
    pause(1000).await;
    let victim = cluster.ids().into_iter().find(|&i| i != 4).unwrap();
    remove_member(&cluster, victim).await;
    pause(500).await;
    cluster.crash(victim).await;

    let l = cluster
        .leader(Duration::from_secs(5))
        .await
        .expect("leader");
    cluster.crash(l).await;
    pause(1000).await;
    cluster.restart(l).await.unwrap();
    pause(1500).await;

    stop.store(true, Ordering::Relaxed);
    for c in clients {
        c.await.unwrap();
    }

    let h = std::mem::take(&mut history.lock().unwrap().ops);
    let completed = h.iter().filter(|(_, o)| o.ret != u64::MAX).count();
    let ops: Vec<_> = h
        .into_iter()
        .filter(|(_, o)| !(o.input == Input::Get && o.output == Output::Unknown))
        .collect();
    let outcome = check_partitioned(&Register, ops.clone(), 50_000_000);
    if let (Outcome::Illegal(_), Ok(p)) = (&outcome, std::env::var("DUMP")) {
        let mut out = String::new();
        for (k, o) in &ops {
            out += &format!(
                "{k}\t{}\t{}\t{:?}\t{:?}\n",
                o.call, o.ret, o.input, o.output
            );
        }
        std::fs::write(p, out).unwrap();
    }
    eprintln!("completed ops: {completed}, outcome: {outcome:?}");
    assert_eq!(outcome, Outcome::Linearizable);
    assert!(completed > 300, "too little progress: {completed}");

    // The joiner caught up through a snapshot and every live member converged.
    let mut last = None;
    for _ in 0..100 {
        let mut statuses = Vec::new();
        for id in cluster
            .ids()
            .into_iter()
            .filter(|&i| cluster.is_running(i) && i != victim)
        {
            statuses.push(cluster.status(id).await.expect("status"));
        }
        let applied: Vec<u64> = statuses.iter().map(|s| s.applied).collect();
        if applied.windows(2).all(|w| w[0] == w[1]) {
            last = Some(statuses);
            break;
        }
        pause(50).await;
    }
    let statuses = last.expect("members did not converge");
    let joiner = statuses.iter().find(|s| s.id == 4).unwrap();
    assert!(joiner.snapshots_installed >= 1, "{joiner:?}");
    assert!(statuses.iter().all(|s| s.keys == statuses[0].keys));
    assert!(!statuses[0].members.iter().any(|m| m.id == victim));
}
