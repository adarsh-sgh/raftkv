//! Linearizability checker in the style of Porcupine / Knossos: Wing & Gong's search with
//! Lowe's memoisation of `(linearized set, model state)`, run independently per partition (key).

use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

pub mod kv;

/// Sequential specification of the object under test.
pub trait Model {
    type State: Clone + Eq + Hash;
    type Input;
    type Output;
    fn init(&self) -> Self::State;
    /// State after applying `input` with observed `output`, or `None` if impossible.
    fn step(
        &self,
        s: &Self::State,
        input: &Self::Input,
        output: &Self::Output,
    ) -> Option<Self::State>;
}

/// `call` and `ret` are positions on one real-time clock; an operation that never returned
/// (crash, timeout) has `ret = u64::MAX` and may take effect at any point after `call`.
#[derive(Clone, Debug)]
pub struct Operation<I, O> {
    pub input: I,
    pub output: O,
    pub call: u64,
    pub ret: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Linearizable,
    /// No valid linearization exists for this partition.
    Illegal(String),
    /// Search budget exhausted.
    Unknown(String),
}

const NIL: usize = usize::MAX;
const HEAD: usize = 0;

struct Node {
    op: usize,
    is_call: bool,
    prev: usize,
    next: usize,
    ret: usize,
}

/// Returns `None` if the budget (search steps) runs out.
pub fn check<M: Model>(
    model: &M,
    ops: &[Operation<M::Input, M::Output>],
    budget: u64,
) -> Option<bool> {
    // Event list; on equal timestamps returns sort first (the stricter reading).
    let mut events: Vec<(u64, bool, usize)> = Vec::with_capacity(ops.len() * 2);
    for (i, op) in ops.iter().enumerate() {
        events.push((op.call, true, i));
        events.push((op.ret, false, i));
    }
    events.sort_by_key(|&(t, is_call, i)| (t, is_call, i));

    let mut nodes: Vec<Node> = Vec::with_capacity(events.len() + 1);
    nodes.push(Node {
        op: NIL,
        is_call: false,
        prev: NIL,
        next: if events.is_empty() { NIL } else { 1 },
        ret: NIL,
    });
    let mut ret_node = vec![NIL; ops.len()];
    for (pos, &(_, is_call, op)) in events.iter().enumerate() {
        let id = pos + 1;
        nodes.push(Node {
            op,
            is_call,
            prev: pos,
            next: if id == events.len() { NIL } else { id + 1 },
            ret: NIL,
        });
        if !is_call {
            ret_node[op] = id;
        }
    }
    for n in nodes.iter_mut().skip(1).filter(|n| n.is_call) {
        n.ret = ret_node[n.op];
    }

    fn unlink(nodes: &mut [Node], i: usize) {
        let (p, n) = (nodes[i].prev, nodes[i].next);
        nodes[p].next = n;
        if n != NIL {
            nodes[n].prev = p;
        }
    }
    fn relink(nodes: &mut [Node], i: usize) {
        let (p, n) = (nodes[i].prev, nodes[i].next);
        nodes[p].next = i;
        if n != NIL {
            nodes[n].prev = i;
        }
    }

    let words = ops.len().div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut state = model.init();
    let mut cache: HashSet<(Vec<u64>, M::State)> = HashSet::new();
    let mut stack: Vec<(usize, M::State)> = Vec::new();
    let mut entry = nodes[HEAD].next;
    let mut steps = 0u64;

    while nodes[HEAD].next != NIL {
        steps += 1;
        if steps > budget {
            return None;
        }
        if entry != NIL && nodes[entry].is_call {
            let op = &ops[nodes[entry].op];
            if let Some(next) = model.step(&state, &op.input, &op.output) {
                let (w, b) = (nodes[entry].op / 64, nodes[entry].op % 64);
                linearized[w] |= 1 << b;
                if cache.insert((linearized.clone(), next.clone())) {
                    stack.push((entry, std::mem::replace(&mut state, next)));
                    let ret = nodes[entry].ret;
                    unlink(&mut nodes, entry);
                    unlink(&mut nodes, ret);
                    entry = nodes[HEAD].next;
                    continue;
                }
                linearized[w] &= !(1 << b);
            }
            entry = nodes[entry].next;
        } else {
            // Reached a return whose call was not linearized yet: backtrack.
            let Some((call, prev)) = stack.pop() else {
                return Some(false);
            };
            state = prev;
            let op = nodes[call].op;
            linearized[op / 64] &= !(1 << (op % 64));
            let ret = nodes[call].ret;
            relink(&mut nodes, ret);
            relink(&mut nodes, call);
            entry = nodes[call].next;
        }
    }
    Some(true)
}

/// Checks each partition (e.g. key) independently; P-compositionality makes this exact for
/// objects whose keys do not interact.
pub fn check_partitioned<K: Ord + Debug, M: Model>(
    model: &M,
    ops: impl IntoIterator<Item = (K, Operation<M::Input, M::Output>)>,
    budget: u64,
) -> Outcome {
    let mut parts: BTreeMap<K, Vec<_>> = BTreeMap::new();
    for (k, op) in ops {
        parts.entry(k).or_default().push(op);
    }
    for (k, ops) in &parts {
        match check(model, ops, budget) {
            Some(true) => {}
            Some(false) => return Outcome::Illegal(format!("{k:?}")),
            None => return Outcome::Unknown(format!("{k:?}")),
        }
    }
    Outcome::Linearizable
}
