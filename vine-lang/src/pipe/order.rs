//! What a replay can rely on about the order in which values reach a
//! stage, and what the checker refuses because of it.
//!
//! An `each` that merges runs its values side by side. In a saved run each
//! value that enters it starts a group, and the acts and outputs of a group
//! are keyed by the group, so they replay in the group's own order whatever
//! order the groups finish in. Values that leave the `each` keep their
//! group, so the stages after it are keyed by group too, and replay.
//!
//! What does not replay is a stage whose result depends on the order in
//! which values from different groups reach it. After a restart the groups
//! can finish in another order, so:
//!
//! - `take` could let other values through, and an output the caller was
//!   handed could be made no more. It is refused.
//! - `collect` could put other values in a batch, and the caller would see
//!   one value twice and another never. It is refused, in a side pipe too.
//! - `last` gives one value, and after a restart it can be another
//!   group's, as it could be in a run that went at another pace. It is
//!   allowed. The value it gives is lifted out of its group, so an output
//!   made from it is matched the same whichever value it is. An act after
//!   it is refused: its saved result was for the value picked before.
//!
//! A `catch` or `finalize` that covers an `each` that merges, at any depth,
//! drops the values still running at the first failure. How far they got
//! depends on timing, and an output the caller was handed could be made no
//! more after a restart, so a vine with one cannot be resumed. A `catch`
//! inside the `each` covers one value's own work, and replays.

use super::check::Stream;
use super::{Overlap, Stage};
use crate::registry::Registry;

/// What a pipe's stages see of the stream that reaches them.
#[derive(Debug, Clone)]
pub(super) struct View {
    /// For a run from a saved position: how much of the input stream.
    pub(super) stream: Stream,
    /// The path of the pipe whose stream this is: the vine's own, or one a
    /// stage runs for each value. A fork's side pipes share their pipe's.
    pub(super) root: String,
    /// Values from more than one group reach here, in an order a restart
    /// may not repeat: the path of the stage they came out of.
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

    /// What a pipe at `path` that a stage runs for each value sees: one
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

    /// What the stage after `stage`, at `here`, sees.
    pub(super) fn after(&mut self, reg: &Registry, stage: &Stage, here: &str) {
        if matches!(stage, Stage::Last) && self.mixed.is_some() {
            self.mixed = None;
            self.picked = Some(here.into());
        } else if mixes(reg, stage) {
            self.mixed = Some(here.into());
        }
    }

    /// Why the stage at `here` cannot replay when it sees this, if it
    /// cannot.
    pub(super) fn refusal(&self, reg: &Registry, stage: &Stage, here: &str) -> Option<String> {
        if let Some(from) = &self.mixed {
            match stage {
                Stage::Take(_) => return Some(format!(
                    "{here} is a `take` after {from}, which runs items side by side: which items it lets through depends on which finish first, and after a restart others could"
                )),
                Stage::Collect { .. } => return Some(format!(
                    "{here} is a `collect` after {from}, which runs items side by side: a batch holds the items that finished first, and after a restart it could hold others"
                )),
                _ => {}
            }
        }
        let picked = self.picked.as_ref()?;
        let act = match stage {
            Stage::Call { act, .. } => Some(act.clone()),
            Stage::Together(parts) => parts.iter().find_map(|p| match p {
                Stage::Call { act, .. } => Some(act.clone()),
                _ => None,
            }),
            Stage::Use { vine, .. } => reg.get_vine(vine).and_then(|v| v.acts().first().cloned()),
            _ => None,
        }?;
        Some(format!(
            "{here} runs `{act}` on the value the `last` at {picked} picked out of items run side by side; after a restart it could pick another, and the act's saved result would not fit it"
        ))
    }
}

/// Whether the values that come out of `stage` can come from more than one
/// group, in an order a restart may not repeat. A fork's main pipe gets
/// the values that reached it, and a finalize's cleanup gives nothing, so
/// neither mixes by itself.
pub(super) fn mixes(reg: &Registry, stage: &Stage) -> bool {
    match stage {
        Stage::Fork(_) | Stage::Finalize(_) => false,
        _ => merges_anywhere(reg, std::slice::from_ref(stage)),
    }
}

/// Whether these stages hold an `each` that merges, at any depth, in a
/// side pipe or in a vine they run by `use`.
pub(super) fn merges_anywhere(reg: &Registry, stages: &[Stage]) -> bool {
    stages.iter().any(|s| match s {
        Stage::Each {
            overlap: Overlap::Merge,
            ..
        } => true,
        Stage::Each { pipe, .. } | Stage::Catch(pipe) | Stage::Finalize(pipe) => {
            merges_anywhere(reg, pipe)
        }
        Stage::Fork(sides) => sides.iter().any(|p| merges_anywhere(reg, p)),
        Stage::Use { vine, on, .. } => {
            reg.get_vine(vine).is_some_and(|v| v.merges)
                || on.iter().any(|(_, p)| merges_anywhere(reg, p))
        }
        Stage::Branch {
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
