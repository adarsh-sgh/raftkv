//! Register model for one key of a KV store: get, put (None = delete) and compare-and-set.

use crate::Model;

pub type Value = Option<String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Get,
    Put(Value),
    Cas { expected: Value, new: Value },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Get(Value),
    Put,
    Cas(bool),
    /// The client never learned the outcome.
    Unknown,
}

/// The state is the set of values the register may hold: an operation with unknown outcome
/// may or may not have taken effect, so it widens the set (subset construction keeps the
/// search deterministic).
pub struct Register;

fn with(s: &[Value], v: &Value) -> Vec<Value> {
    let mut out = s.to_vec();
    if !out.contains(v) {
        out.push(v.clone());
        out.sort();
    }
    out
}

impl Model for Register {
    type State = Vec<Value>;
    type Input = Input;
    type Output = Output;

    fn init(&self) -> Vec<Value> {
        vec![None]
    }

    fn step(&self, s: &Vec<Value>, input: &Input, output: &Output) -> Option<Vec<Value>> {
        match (input, output) {
            (Input::Get, Output::Get(v)) => s.contains(v).then(|| vec![v.clone()]),
            (Input::Get, Output::Unknown) => Some(s.clone()),
            (Input::Put(v), Output::Put) => Some(vec![v.clone()]),
            (Input::Put(v), Output::Unknown) => Some(with(s, v)),
            (Input::Cas { expected, new }, Output::Cas(true)) => {
                s.contains(expected).then(|| vec![new.clone()])
            }
            (Input::Cas { expected, .. }, Output::Cas(false)) => {
                let rest: Vec<Value> = s.iter().filter(|v| *v != expected).cloned().collect();
                (!rest.is_empty()).then_some(rest)
            }
            (Input::Cas { expected, new }, Output::Unknown) => Some(if s.contains(expected) {
                with(s, new)
            } else {
                s.clone()
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Operation, Outcome, check_partitioned};

    fn op(
        input: Input,
        output: Output,
        call: u64,
        ret: u64,
    ) -> (&'static str, Operation<Input, Output>) {
        (
            "k",
            Operation {
                input,
                output,
                call,
                ret,
            },
        )
    }
    fn v(s: &str) -> Value {
        Some(s.to_string())
    }

    #[test]
    fn accepts_concurrent_and_pending_rejects_stale_read() {
        // put(a) || get -> a, then a put that never returned shows up in a later read.
        let ok = vec![
            op(Input::Put(v("a")), Output::Put, 0, 3),
            op(Input::Get, Output::Get(v("a")), 1, 2),
            op(Input::Put(v("b")), Output::Unknown, 4, u64::MAX),
            op(Input::Get, Output::Get(v("a")), 5, 6),
            op(
                Input::Cas {
                    expected: v("a"),
                    new: v("c"),
                },
                Output::Cas(false),
                7,
                8,
            ),
            op(Input::Get, Output::Get(v("b")), 9, 10),
        ];
        assert_eq!(
            check_partitioned(&Register, ok, 1 << 20),
            Outcome::Linearizable
        );

        // A timed-out put bounded by a later op may also never have happened.
        let maybe = vec![
            op(Input::Put(v("a")), Output::Unknown, 0, 3),
            op(Input::Get, Output::Get(None), 4, 5),
        ];
        assert_eq!(
            check_partitioned(&Register, maybe, 1 << 20),
            Outcome::Linearizable
        );

        // put(b) completed before the read began, yet the read saw the old value.
        let stale = vec![
            op(Input::Put(v("a")), Output::Put, 0, 1),
            op(Input::Put(v("b")), Output::Put, 2, 3),
            op(Input::Get, Output::Get(v("a")), 4, 5),
        ];
        assert_eq!(
            check_partitioned(&Register, stale, 1 << 20),
            Outcome::Illegal("\"k\"".into())
        );
    }
}
