//! The loop check: refuse a trellis whose moves start each other in a
//! circle that nothing stops.
//!
//! The check draws one arrow from move `a` to move `b` for each way a
//! press of `a` can cause a press of `b`:
//!
//! - an output of `a`'s vine lands an event that a reaction answers by
//!   pressing `b`;
//! - an output of `a`'s vine is abandoned, and a reaction to that presses
//!   `b`;
//! - `a`'s vine calls `move.b`, which presses `b` from inside the run.
//!
//! Every simple circle of arrows is a loop. A loop is allowed only when a
//! gate on it is sure to refuse the second time round: the gate reads a
//! fact of the move's own target, such as `job.state`, and going once
//! round the loop from that move leaves that fact set, by the events'
//! declared effects, to a value the gate refuses. That can be trusted
//! only when every step keeps to one artifact: each reaction on the loop
//! presses its move on the artifact the event landed on. A nested press,
//! or a reaction that takes its target from the event's payload, may work
//! on another artifact, so no gate is credited on such a loop.
//!
//! What this does not see is listed on [`LoopWhy`] and in `GUIDE.md`.

use std::collections::BTreeMap;

use vine_lang::pipe::Pipe;
use vine_lang::Value;

use super::install::press_operator;
use super::{InstallError, Move, Pick, Reaction, Trigger};

/// Why a loop was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopWhy {
    /// No move on the loop has a gate.
    NoGate,
    /// The loop has gates, and going round it never sets a fact a gate
    /// reads to a value that gate refuses.
    GatesHold,
    /// A step of the loop may work on another artifact, so no gate on it
    /// can be trusted to stop it.
    ArtifactMayChange,
}

struct Arrow<'a> {
    from: usize,
    to: usize,
    /// The facts the step sets on the artifact, in order.
    sets: &'a [(&'static str, Value)],
    same_artifact: bool,
}

pub(super) fn check(
    moves: &[Move],
    vines: &BTreeMap<&str, &Pipe>,
    reactions: &[Reaction],
) -> Result<(), InstallError> {
    let arrows = arrows(moves, vines, reactions);
    for start in 0..moves.len() {
        let mut path = Vec::new();
        if let Some(why) = walk(moves, &arrows, start, start, &mut path) {
            let names = path
                .iter()
                .map(|&a| moves[arrows[a].from].name.into())
                .collect();
            return Err(InstallError::Loop { moves: names, why });
        }
    }
    Ok(())
}

fn arrows<'a>(
    moves: &'a [Move],
    vines: &BTreeMap<&str, &Pipe>,
    reactions: &'a [Reaction],
) -> Vec<Arrow<'a>> {
    let index = |name: &str| moves.iter().position(|m| m.name == name);
    let keeps_artifact = |r: &Reaction, to: usize| {
        r.payload
            .iter()
            .any(|(f, pick)| *f == moves[to].target && *pick == Pick::Artifact)
    };
    let mut out = Vec::new();
    for (from, decl) in moves.iter().enumerate() {
        for r in reactions {
            let Some(to) = index(r.press) else { continue };
            match r.on {
                Trigger::Event(kind) => {
                    for lands in decl.events.iter().filter(|l| l.kind == kind) {
                        out.push(Arrow {
                            from,
                            to,
                            sets: &lands.sets,
                            same_artifact: keeps_artifact(r, to),
                        });
                    }
                }
                Trigger::Abandoned { by } if by == decl.name && decl.vine.is_some() => {
                    out.push(Arrow {
                        from,
                        to,
                        sets: &[],
                        same_artifact: keeps_artifact(r, to),
                    });
                }
                Trigger::Abandoned { .. } => {}
            }
        }
        let Some(vine) = decl.vine.and_then(|v| vines.get(v)) else {
            continue;
        };
        for (to, other) in moves.iter().enumerate() {
            if vine.calls().contains(&press_operator(other.name)) {
                out.push(Arrow {
                    from,
                    to,
                    sets: &[],
                    same_artifact: false,
                });
            }
        }
    }
    out
}

/// Look for a loop back to `start` through moves after it, so each loop
/// is found once, from its first move. Gives why the first loop found
/// cannot be allowed, with its arrows left in `path`.
fn walk(
    moves: &[Move],
    arrows: &[Arrow],
    start: usize,
    at: usize,
    path: &mut Vec<usize>,
) -> Option<LoopWhy> {
    for (n, arrow) in arrows.iter().enumerate().filter(|(_, a)| a.from == at) {
        if arrow.to != start {
            let visited = arrow.to == at || path.iter().any(|&p| arrows[p].from == arrow.to);
            if arrow.to < start || visited {
                continue;
            }
        }
        path.push(n);
        let found = if arrow.to == start {
            refuse(moves, arrows, path)
        } else {
            walk(moves, arrows, start, arrow.to, path)
        };
        if found.is_some() {
            return found;
        }
        path.pop();
    }
    None
}

/// Why this loop cannot be allowed, or nothing when a gate on it stops it.
fn refuse(moves: &[Move], arrows: &[Arrow], path: &[usize]) -> Option<LoopWhy> {
    let steps: Vec<&Arrow> = path.iter().map(|&n| &arrows[n]).collect();
    if steps.iter().all(|s| moves[s.from].gates.is_empty()) {
        return Some(LoopWhy::NoGate);
    }
    if !steps.iter().all(|s| s.same_artifact) {
        return Some(LoopWhy::ArtifactMayChange);
    }
    let stopped = (0..steps.len()).any(|i| {
        let decl = &moves[steps[i].from];
        decl.gates.iter().any(|gate| {
            let Some((target, field)) = gate.path.split_once('.') else {
                return false;
            };
            if target != decl.target || field.contains('.') {
                return false;
            }
            // Once round, from the gated move back to it.
            let last = steps
                .iter()
                .cycle()
                .skip(i)
                .take(steps.len())
                .flat_map(|s| s.sets.iter())
                .filter(|(f, _)| *f == field)
                .last();
            last.is_some_and(|(_, value)| *value != gate.equals)
        })
    });
    (!stopped).then_some(LoopWhy::GatesHold)
}
