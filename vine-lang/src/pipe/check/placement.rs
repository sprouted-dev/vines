//! Where an operator may stand in a pipe: what the operators before it
//! already emit, and which operators apply to the ones before them.

use crate::pipe::{Flatten, Operator};

/// Whether every item that enters these operators is sure to be emitted.
pub(super) fn emits(ops: &[Operator]) -> bool {
    let Some((last, before)) = ops.split_last() else {
        return false;
    };
    match last {
        // These let what was emitted before them go by.
        Operator::Take(_)
        | Operator::Finalize(_)
        | Operator::Retry { .. }
        | Operator::Timeout(_) => emits(before),
        Operator::CatchError(handler) => emits(before) && emits(handler),
        Operator::Emit { .. } => true,
        Operator::Mapped { pipe, .. } => emits(pipe),
        Operator::Use { on, .. } => on.iter().all(|(_, p)| emits(p)),
        Operator::Branch {
            cases, otherwise, ..
        } => otherwise.as_deref().is_some_and(emits) && cases.iter().all(|(_, p)| emits(p)),
        // A fork's side pipes may emit, but the item still goes on down
        // the main pipe.
        _ => false,
    }
}

/// Whether an operator may stand after operators that already emit.
pub(super) fn follows_emit(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Take(_)
            | Operator::CatchError(_)
            | Operator::Finalize(_)
            | Operator::Retry { .. }
            | Operator::Timeout(_)
    )
}

/// Whether an operator applies to the operators before it in its pipe, one
/// item at a time, as `catchError`, `finalize`, `retry` and `timeout` do.
pub(super) fn applies_to_before(op: &Operator) -> Option<&'static str> {
    match op {
        Operator::CatchError(_) => Some("catchError"),
        Operator::Finalize(_) => Some("finalize"),
        Operator::Retry { .. } => Some("retry"),
        Operator::Timeout(_) => Some("timeout"),
        _ => None,
    }
}

/// Whether an operator works across the whole stream, not one item at a
/// time, in a way `catchError`, `finalize`, `retry` and `timeout` cannot
/// hold. `last` and `bufferCount` are not here: before those four they work
/// on one item's own stream.
pub(super) fn stream_wide(op: &Operator) -> Option<&'static str> {
    match op {
        Operator::Take(_) => Some("take"),
        Operator::Mapped { flatten, .. } if *flatten != Flatten::Concat => Some(flatten.key()),
        _ => None,
    }
}

/// Whether these operators emit anywhere, at any depth.
pub(super) fn emits_anywhere(ops: &[Operator]) -> bool {
    ops.iter().any(|s| match s {
        Operator::Emit { .. } => true,
        Operator::Mapped { pipe, .. } | Operator::CatchError(pipe) | Operator::Finalize(pipe) => {
            emits_anywhere(pipe)
        }
        Operator::Fork(sides) => sides.iter().any(|p| emits_anywhere(p)),
        Operator::Use { on, .. } => on.iter().any(|(_, p)| emits_anywhere(p)),
        Operator::Branch {
            cases, otherwise, ..
        } => {
            cases.iter().any(|(_, p)| emits_anywhere(p))
                || otherwise.as_deref().is_some_and(emits_anywhere)
        }
        _ => false,
    })
}
