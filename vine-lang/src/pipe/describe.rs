//! A vine as described, before it is checked: the operators of a pipe and
//! the functions that build them.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;

use crate::shape::Shape;

/// Which of the four mapping operators an [`Operator::Mapped`] is: what
/// happens when a new item arrives while the inner pipe is still running
/// for an earlier one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flatten {
    /// `concatMap`: run them one at a time, in order.
    Concat,
    /// `mergeMap`: run them side by side. Results come out as they finish.
    Merge,
    /// `switchMap`: drop the earlier run and start the new one.
    Switch,
    /// `exhaustMap`: ignore the new item while a run is going.
    Exhaust,
}

impl Flatten {
    /// The operator's key in YAML and in paths, such as `mergeMap`.
    pub fn key(self) -> &'static str {
        match self {
            Flatten::Concat => "concatMap",
            Flatten::Merge => "mergeMap",
            Flatten::Switch => "switchMap",
            Flatten::Exhaust => "exhaustMap",
        }
    }
}

/// What an operator's result does to the item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keep {
    /// The result is the new item.
    Replace,
    /// The item is kept, with the result added under this name.
    Into(String),
    /// The item is kept unchanged and the result is dropped.
    Tap,
}

/// One operator of a pipe, as described.
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    /// A registered pure operator, written by its name, such as `set`.
    Named {
        name: String,
        with: Value,
        keep: Keep,
    },
    /// A registered operator that touches something outside, run by
    /// `call` or `tap`, such as `git.worktree_add`.
    Call {
        name: String,
        with: Value,
        keep: Keep,
        /// For a stream operator: the field its end value goes under, on
        /// one last item.
        end: Option<String>,
    },
    Split {
        from: Value,
        into: Option<String>,
    },
    Filter {
        when: Value,
    },
    Take(u64),
    /// When the stream ends, let only its final item through.
    Last,
    /// Batches of `buffer_size`, and what is left when the stream ends.
    BufferCount {
        buffer_size: u64,
        into: Option<String>,
    },
    ForkJoin(Vec<Operator>),
    /// `concatMap`, `mergeMap`, `switchMap` or `exhaustMap`, as `flatten`
    /// says: run a pipe for each item. `concurrent` caps how many run side
    /// by side, and only `mergeMap` takes it.
    Mapped {
        flatten: Flatten,
        concurrent: Option<usize>,
        pipe: Vec<Operator>,
    },
    /// Send each item down every side pipe, and on down this one.
    Fork(Vec<Vec<Operator>>),
    /// Applies to the operators before it in its pipe: if one of them
    /// fails for an item, run these instead.
    CatchError(Vec<Operator>),
    /// Applies to the operators before it: run these afterwards, however
    /// the item's work ended.
    Finalize(Vec<Operator>),
    /// Applies to the operators before it: if one fails, wait `delay` and
    /// run them again, up to `count` more times.
    Retry {
        count: u32,
        delay: Duration,
    },
    /// Applies to the operators before it: fail if they take longer than
    /// this.
    Timeout(Duration),
    /// Run another vine for each item.
    Use {
        vine: String,
        with: Option<Value>,
        keep: Keep,
        on: Vec<(String, Vec<Operator>)>,
    },
    Branch {
        on: Value,
        cases: Vec<(String, Vec<Operator>)>,
        otherwise: Option<Vec<Operator>>,
    },
    Emit {
        to: String,
        with: Option<Value>,
    },
}

impl Operator {
    /// Keep the item and add this operator's result to it under `name`.
    /// For [`split`], each new item is the old one with the element added
    /// under `name`. For [`buffer_count`], the item kept is the last one of
    /// the batch, with the batch added under `name`.
    pub fn into(mut self, name: &str) -> Self {
        match &mut self {
            Operator::Named { keep, .. }
            | Operator::Call { keep, .. }
            | Operator::Use { keep, .. } => *keep = Keep::Into(name.to_owned()),
            Operator::Split { into, .. } | Operator::BufferCount { into, .. } => {
                *into = Some(name.to_owned())
            }
            _ => {}
        }
        self
    }

    /// For [`call`] on a stream operator: after the items, give one last
    /// item with the operator's end value added under `name`.
    pub fn end(mut self, name: &str) -> Self {
        if let Operator::Call { end, .. } = &mut self {
            *end = Some(name.to_owned());
        }
        self
    }

    /// For [`use_vine`]: the operators to run for what the other vine
    /// sends to this output. Every output of the other vine needs one.
    pub fn on(mut self, output: &str, operators: impl IntoIterator<Item = Operator>) -> Self {
        if let Operator::Use { on, .. } = &mut self {
            on.push((output.to_owned(), operators.into_iter().collect()));
        }
        self
    }

    /// For [`merge_map`]: run at most this many side by side. The checker
    /// refuses it on the other mapping operators.
    pub fn concurrent(mut self, limit: usize) -> Self {
        if let Operator::Mapped { concurrent, .. } = &mut self {
            *concurrent = Some(limit);
        }
        self
    }
}

/// Run a registered pure operator, such as `set`, for each item.
pub fn op(name: &str, with: Value) -> Operator {
    Operator::Named {
        name: name.to_owned(),
        with,
        keep: Keep::Replace,
    }
}

/// Run a registered operator that touches something outside, such as
/// `git.worktree_add`, for each item. On a stream operator, each item
/// becomes one item per value it gives.
pub fn call(name: &str, with: Value) -> Operator {
    Operator::Call {
        name: name.to_owned(),
        with,
        keep: Keep::Replace,
        end: None,
    }
}

/// Run a registered operator that touches something outside for its
/// effect. The item passes through unchanged. A stream operator cannot be
/// tapped: use [`call`].
pub fn tap(name: &str, with: Value) -> Operator {
    Operator::Call {
        name: name.to_owned(),
        with,
        keep: Keep::Tap,
        end: None,
    }
}

/// One item becomes one item per element of the list at `from`.
pub fn split(from: &str) -> Operator {
    Operator::Split {
        from: Value::String(from.to_owned()),
        into: None,
    }
}

/// Drop the items the condition does not hold for.
pub fn filter(when: &str) -> Operator {
    Operator::Filter {
        when: Value::String(when.to_owned()),
    }
}

/// Let this many items through, then end the stream.
pub fn take(count: u64) -> Operator {
    Operator::Take(count)
}

/// When the stream ends, let only its final item through. An empty stream
/// gives nothing.
pub fn last() -> Operator {
    Operator::Last
}

/// Gather items into batches of `buffer_size`. A batch is
/// `{ items, count }`. When the stream ends, what is left comes as one
/// more, smaller batch. An empty batch is never given. With
/// [`into`](Operator::into), the item is the last one of the batch with
/// the batch added under that name.
pub fn buffer_count(buffer_size: u64) -> Operator {
    Operator::BufferCount {
        buffer_size,
        into: None,
    }
}

/// Run these registered operators side by side for one item, and go on
/// when all of them are done. Each one says where its result goes with
/// `into`.
pub fn fork_join(operators: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::ForkJoin(operators.into_iter().collect())
}

fn mapped(flatten: Flatten, pipe: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::Mapped {
        flatten,
        concurrent: None,
        pipe: pipe.into_iter().collect(),
    }
}

/// Run a pipe for each item, one item at a time, in order.
pub fn concat_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Concat, pipe)
}

/// Run a pipe for each item, side by side. What it gives comes out as it
/// finishes. Cap how many run at once with
/// [`concurrent`](Operator::concurrent).
pub fn merge_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Merge, pipe)
}

/// Run a pipe for each item. A new item drops the run for the one before.
pub fn switch_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Switch, pipe)
}

/// Run a pipe for each item. An item that arrives while a run is going is
/// ignored.
pub fn exhaust_map(pipe: impl IntoIterator<Item = Operator>) -> Operator {
    mapped(Flatten::Exhaust, pipe)
}

/// Send each item down every one of these side pipes, and on down this
/// pipe unchanged. See the module docs.
pub fn fork<P>(pipes: impl IntoIterator<Item = P>) -> Operator
where
    P: IntoIterator<Item = Operator>,
{
    Operator::Fork(pipes.into_iter().map(|p| p.into_iter().collect()).collect())
}

/// If an operator before this one fails for an item, run these instead.
/// They get the item as it was when it failed, with the failure added as
/// `$item.error`: its `code`, its `message`, and `from`, the name of the
/// operator that failed.
pub fn catch_error(operators: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::CatchError(operators.into_iter().collect())
}

/// Run these after the operators before this one, however the item's work
/// ended. They get the item as it was when it arrived. They are for
/// cleanup: what they give back is dropped, and they may not emit.
pub fn finalize(operators: impl IntoIterator<Item = Operator>) -> Operator {
    Operator::Finalize(operators.into_iter().collect())
}

/// If an operator before this one fails for an item, wait `delay` and run
/// them again, up to `count` more times. Every operator among them that
/// touches something outside must be safe to repeat.
pub fn retry(count: u32, delay: Duration) -> Operator {
    Operator::Retry { count, delay }
}

/// Fail an item with the code `timeout` if the operators before this one
/// take longer than `limit`.
pub fn timeout(limit: Duration) -> Operator {
    Operator::Timeout(limit)
}

/// Run another vine for each item. `with` builds its input. With no
/// `with`, the item is its input. Add an [`on`](Operator::on) for each of
/// its outputs.
pub fn use_vine(vine: &str, with: Option<Value>) -> Operator {
    Operator::Use {
        vine: vine.to_owned(),
        with,
        keep: Keep::Replace,
        on: Vec::new(),
    }
}

/// Run the pipe whose key equals the value at `on`. With no `otherwise`,
/// an item that matches no case passes through unchanged.
pub fn branch<'a, C, P>(on: &str, cases: C, otherwise: Option<Vec<Operator>>) -> Operator
where
    C: IntoIterator<Item = (&'a str, P)>,
    P: IntoIterator<Item = Operator>,
{
    Operator::Branch {
        on: Value::String(on.to_owned()),
        cases: cases
            .into_iter()
            .map(|(k, p)| (k.to_owned(), p.into_iter().collect()))
            .collect(),
        otherwise,
    }
}

/// Send the item, as it is, to one of the vine's outputs.
pub fn emit(to: &str) -> Operator {
    Operator::Emit {
        to: to.to_owned(),
        with: None,
    }
}

/// Send this value to one of the vine's outputs.
pub fn emit_with(to: &str, with: Value) -> Operator {
    Operator::Emit {
        to: to.to_owned(),
        with: Some(with),
    }
}

/// A vine as described, before it is checked.
#[derive(Debug, Clone)]
pub struct PipeDef {
    pub(crate) name: String,
    pub(crate) input: Shape,
    pub(crate) outputs: BTreeMap<String, Shape>,
    pub(crate) operators: Vec<Operator>,
}

/// Start describing a vine.
pub fn pipe(name: &str) -> PipeDef {
    PipeDef {
        name: name.to_owned(),
        input: Shape::new(),
        outputs: BTreeMap::new(),
        operators: Vec::new(),
    }
}

impl PipeDef {
    /// The shape of each item that goes in.
    pub fn input(mut self, shape: Shape) -> Self {
        self.input = shape;
        self
    }

    /// Declare one named output and the shape of what it carries.
    pub fn output(mut self, name: &str, shape: Shape) -> Self {
        self.outputs.insert(name.to_owned(), shape);
        self
    }

    /// The operators, first to last.
    pub fn operators(mut self, operators: impl IntoIterator<Item = Operator>) -> Self {
        self.operators = operators.into_iter().collect();
        self
    }
}
