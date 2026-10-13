//! Checker cost on synthetic linearizable histories: `clients` concurrent clients doing
//! get/put/cas on one register, each op linearized at a random point inside its interval.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lincheck::kv::{Input, Output, Register};
use lincheck::{Operation, check};

fn history(ops: usize, clients: u64, seed: u64) -> Vec<Operation<Input, Output>> {
    let mut x = seed;
    let mut rnd = move |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    // Each client's next op starts after its previous one returned.
    let mut free_at = vec![0u64; clients as usize];
    let mut planned = Vec::new();
    for i in 0..ops {
        let c = rnd(clients) as usize;
        let call = free_at[c] + rnd(5);
        let point = call + 1 + rnd(20);
        let ret = point + 1 + rnd(20);
        free_at[c] = ret + 1;
        let input = match rnd(3) {
            0 => Input::Get,
            1 => Input::Put(Some(i.to_string())),
            _ => Input::Cas {
                expected: Some((i / 2).to_string()),
                new: Some(i.to_string()),
            },
        };
        planned.push((point, call, ret, input));
    }
    // Apply in linearization order to compute outputs.
    planned.sort_by_key(|p| p.0);
    let mut state = None;
    planned
        .into_iter()
        .enumerate()
        .map(|(k, (_, call, ret, input))| {
            let output = match &input {
                Input::Get => Output::Get(state.clone()),
                Input::Put(v) => {
                    state = v.clone();
                    Output::Put
                }
                Input::Cas { expected, new } => {
                    let ok = &state == expected;
                    if ok {
                        state = new.clone();
                    }
                    Output::Cas(ok)
                }
            };
            // Distinct timestamps: scale and add the index.
            Operation {
                input,
                output,
                call: call * 10_000 + k as u64,
                ret: ret * 10_000 + k as u64,
            }
        })
        .collect()
}

fn checker(c: &mut Criterion) {
    let mut g = c.benchmark_group("lincheck_register");
    for (ops, clients) in [(1_000, 4), (1_000, 16), (10_000, 8)] {
        let h = history(ops, clients, 42);
        assert_eq!(check(&Register, &h, u64::MAX), Some(true));
        g.throughput(Throughput::Elements(ops as u64));
        g.bench_with_input(
            BenchmarkId::new(format!("{clients}_clients"), ops),
            &h,
            |b, h| b.iter(|| check(&Register, h, u64::MAX)),
        );
    }
    g.finish();
}

criterion_group!(benches, checker);
criterion_main!(benches);
