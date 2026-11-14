//! What a replay can rely on about the order in which values reach an
//! operator, and what the checker refuses because of it.
//!
//! A `mergeMap` runs its values side by side. In a saved run each
//! value that enters it starts a group, and the saved calls and outputs of
//! a group are keyed by the group, so they replay in the group's own order
//! whatever order the groups finish in. Values that leave the `mergeMap`
//! keep their group, so the operators after it are keyed by group too, and
//! replay.
//!
//! What does not replay is an operator whose result depends on the order
//! in which values from different groups reach it. After a restart the groups
//! can finish in another order, so:
//!
//! - `take` could let other values through, and an output the caller was
//!   handed could be made no more. It is refused.
//! - `bufferCount` could put other values in a batch, and the caller would see
//!   one value twice and another never. It is refused, in a side pipe too.
//! - `last` gives one value, and after a restart it can be another
//!   group's, as it could be in a run that went at another pace. It is
//!   allowed. The value it gives is lifted out of its group, so an output
//!   made from it is matched the same whichever value it is. A `call` or
//!   `tap` after it is refused: its saved result was for the value picked
//!   before.
//!
//! A `catchError` or `finalize` that applies to a `mergeMap`, at any
//! depth, drops the values still running at the first failure. How far
//! they got depends on timing, and an output the caller was handed could
//! be made no more after a restart, so a vine with one cannot be resumed.
//! A `catchError` inside the `mergeMap` applies to one value's own work,
//! and replays.

use super::check::Stream;
use super::{Flatten, Operator};
use crate::registry::Registry;

/// What a pipe's operators see of the stream that reaches them.
#[derive(Debug, Clone)]
pub(super) struct View {
    /// For a run from a saved position: how much of the input stream.
    pub(super) stream: Stream,
    /// The path of the pipe whose stream this is: the vine's own, or one an
    /// operator runs for each value. A fork's side pipes share their pipe's.
    pub(super) root: String,
    /// Values from more than one group reach here, in an order a restart
    /// may not repeat: the path of the operator they came out of.
    pub(super) mixed: Option<String>,
    /// The value here is the one `last` picked out of such values: the path
    /// of that `last`.
    pub(super) picked: Option<String>,
}

impl View {
    /// What the vine's own pipe sees: the whole input stream.
    pub(super) fn whole() -> View {
        View {
            stream: Stream::Whole,
            root: "pipe".into(),
            mixed: None,
            picked: None,
        }
    }

    /// What a pipe at `path` that an operator runs for each value sees: one
    /// value's own work. A value `last` picked is still picked inside it.
    pub(super) fn per_value(&self, path: &str) -> View {
        View {
            stream: Stream::PerItem,
            root: path.into(),
            mixed: None,
            picked: self.picked.clone(),
        }
    }

    pub(super) fn with_stream(&self, stream: Stream) -> View {
        View {
            stream,
            ..self.clone()
        }
    }

    /// What the operator after `op`, at `here`, sees.
    pub(super) fn after(&mut self, reg: &Registry, op: &Operator, here: &str) {
        if matches!(op, Operator::Last) && self.mixed.is_some() {
            self.mixed = None;
            self.picked = Some(here.into());
        } else if mixes(reg, op) {
            self.mixed = Some(here.into());
        }
    }

    /// Why the operator at `here` cannot replay when it sees this, if it
    /// cannot.
    pub(super) fn refusal(&self, reg: &Registry, op: &Operator, here: &str) -> Option<String> {
        if let Some(from) = &self.mixed {
            match op {
                Operator::Take(_) => return Some(format!(
                    "{here} is a `take` after {from}, which runs items side by side: which items it lets through depends on which finish first, and after a restart others could"
                )),
                Operator::BufferCount { .. } => return Some(format!(
                    "{here} is a `bufferCount` after {from}, which runs items side by side: a batch holds the items that finished first, and after a restart it could hold others"
                )),
                _ => {}
            }
        }
        let picked = self.picked.as_ref()?;
        let called = match op {
            Operator::Call { name, .. } => Some(name.clone()),
            Operator::ForkJoin(parts) => parts.iter().find_map(|p| match p {
                Operator::Call { name, .. } => Some(name.clone()),
                _ => None,
            }),
            Operator::Use { vine, .. } => {
                reg.get_vine(vine).and_then(|v| v.calls().first().cloned())
            }
            _ => None,
        }?;
        Some(format!(
            "{here} runs `{called}` on the value the `last` at {picked} picked out of items run side by side; after a restart it could pick another, and the saved result of `{called}` would not fit it"
        ))
    }
}

/// Whether the values that come out of `op` can come from more than one
/// group, in an order a restart may not repeat. A fork's main pipe gets
/// the values that reached it, and a finalize's cleanup gives nothing, so
/// neither mixes by itself.
pub(super) fn mixes(reg: &Registry, op: &Operator) -> bool {
    match op {
        Operator::Fork(_) | Operator::Finalize(_) => false,
        _ => merges_anywhere(reg, std::slice::from_ref(op)),
    }
}

/// Whether these operators hold a `mergeMap`, at any depth, in a
/// side pipe or in a vine they run by `use`.
pub(super) fn merges_anywhere(reg: &Registry, ops: &[Operator]) -> bool {
    ops.iter().any(|s| match s {
        Operator::Mapped {
            flatten: Flatten::Merge,
            ..
        } => true,
        Operator::Mapped { pipe, .. } | Operator::CatchError(pipe) | Operator::Finalize(pipe) => {
            merges_anywhere(reg, pipe)
        }
        Operator::Fork(sides) => sides.iter().any(|p| merges_anywhere(reg, p)),
        Operator::Use { vine, on, .. } => {
            reg.get_vine(vine).is_some_and(|v| v.merges)
                || on.iter().any(|(_, p)| merges_anywhere(reg, p))
        }
        Operator::Branch {
            cases, otherwise, ..
        } => {
            cases.iter().any(|(_, p)| merges_anywhere(reg, p))
                || otherwise
                    .as_deref()
                    .is_some_and(|p| merges_anywhere(reg, p))
        }
        _ => false,
    })
}
