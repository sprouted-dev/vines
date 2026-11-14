//! A small stand-in for Terra, for the tests. It is not part of the
//! library, and the library knows nothing about it.
//!
//! It does what the vines leave to whoever runs them:
//!
//! 1. Checks a move's gates against the facts. A gate that fails refuses
//!    the press by name, and the vine never runs.
//! 2. Builds the vine's input from the payload and the facts the move
//!    names. The vine reads only its input.
//! 3. Runs the vine with `run_saved`, fed one item, and keeps its journal.
//! 4. Turns each output into an event, but only if nothing the press read
//!    has changed since, other than by its own events. An output that is
//!    refused lands as an `abandoned` event that carries it.
//!
//! # Trellises
//!
//! A trellis is installed as a whole: its vines, its moves and its
//! reactions. Its vines can use only its own vines, and press only its
//! own moves, because each trellis gets its own registry.
//!
//! # Pressing a move from a vine
//!
//! For each move, Terra registers a stream operator named `move.<name>`. Its
//! settings are the move's payload plus `request`, the id of the press the
//! calling vine runs under. It presses the move exactly as an outside press
//! does, and gives one item per output of the inner vine, after that output
//! landed or was abandoned:
//!
//! ```text
//! { output, event, landed, payload }
//! ```
//!
//! It ends with `{ status: done }`, `{ status: failed, reason }` or
//! `{ status: refused, gate }`. A refusal is data the calling vine can
//! branch on, not a failure.
//!
//! # Restart
//!
//! Every press has a request id. Terra keeps, per request, what it read,
//! the vine's journal, and every output it landed or abandoned. A press
//! with an id Terra has seen attaches to that request: it gives the items
//! it gave, and then resumes the vine from its journal. The resumed run
//! can give again an output that landed before the crash, with the same
//! output id; Terra keys each landing by its request id and the output id,
//! and lands it once. Two outputs with the same name and payload are two
//! events: they have two ids. A nested press
//! gets its id from its parent's id, its move and its payload, so after a
//! restart the same press gets the same id. [`Terra::crash_at`] stops
//! everything at a chosen point, as a crash of the process would.
#![allow(dead_code)]

mod install;
mod loops;
pub mod moves;
mod press;
mod store;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use vine_lang::pipe::{Pipe, PipeDef};
use vine_lang::{LoadError, Registry, Ty, Value};

pub use loops::LoopWhy;
use store::Store;

/// A gate: the fact at `path` must equal `equals`, or the press is refused
/// with `gate`. Paths start at the payload field the move names, such as
/// `job.state` or `job.plan.state`.
pub struct Gate {
    pub path: &'static str,
    pub equals: Value,
    pub gate: &'static str,
}

/// Where a value of the vine's input comes from.
pub enum Read {
    /// A fact, read along a path such as `job.plan.branch`.
    Fact(&'static str),
    /// The id of this press, for a vine that presses other moves.
    Request,
}

/// A value the vine needs that the caller does not send.
pub struct Need {
    pub name: &'static str,
    pub ty: Ty,
    pub from: Read,
}

impl Need {
    pub fn fact(name: &'static str, ty: Ty, path: &'static str) -> Need {
        Need {
            name,
            ty,
            from: Read::Fact(path),
        }
    }

    /// The press's own request id, under the name `request`.
    pub fn request() -> Need {
        Need {
            name: "request",
            ty: Ty::Text,
            from: Read::Request,
        }
    }
}

/// What one output of the move's vine becomes: an event of `kind` on the
/// move's target, which sets these facts of the target when it lands.
pub struct Lands {
    pub output: &'static str,
    pub kind: &'static str,
    pub sets: Vec<(&'static str, Value)>,
}

impl Lands {
    pub fn new(output: &'static str, kind: &'static str) -> Lands {
        Lands {
            output,
            kind,
            sets: Vec::new(),
        }
    }

    pub fn sets(mut self, field: &'static str, value: Value) -> Lands {
        self.sets.push((field, value));
        self
    }
}

/// A move as declared. `target` is the payload field that names the
/// artifact the move works on.
pub struct Move {
    pub name: &'static str,
    pub target: &'static str,
    pub payload: Vec<(&'static str, Ty)>,
    pub gates: Vec<Gate>,
    pub needs: Vec<Need>,
    /// The vine the move starts. A move with no vine lands its `done`
    /// event once its gates pass.
    pub vine: Option<&'static str>,
    pub events: Vec<Lands>,
}

/// What starts a reaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// An event of this kind landed.
    Event(&'static str),
    /// An output of a press of this move was abandoned.
    Abandoned { by: &'static str },
}

/// Where one payload field of a reaction's press comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pick {
    /// The artifact the event landed on.
    Artifact,
    /// A field of the event's payload, along a dotted path.
    Field(&'static str),
}

/// When `on` happens, press `press` with a payload made from the event.
pub struct Reaction {
    pub on: Trigger,
    pub press: &'static str,
    pub payload: Vec<(&'static str, Pick)>,
}

/// One vine of a trellis, as Rust or as YAML.
pub enum VineSource {
    Def(PipeDef),
    #[cfg(feature = "yaml")]
    Yaml(String),
}

/// A package of vines, the moves that start some of them, and the
/// reactions that press moves when events land. Vines are built in the
/// order listed, so a vine can use only a vine listed before it.
pub struct Trellis {
    pub vines: Vec<VineSource>,
    pub moves: Vec<Move>,
    pub reactions: Vec<Reaction>,
}

/// An event that landed on an artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub artifact: String,
    pub kind: String,
    pub payload: Value,
}

/// The event kind of an output Terra refused because a fact changed.
pub const ABANDONED: &str = "abandoned";

/// Why a press was refused before its vine ran.
#[derive(Debug, Clone, PartialEq)]
pub enum Refusal {
    UnknownMove,
    /// The payload does not fit the move.
    Payload(String),
    /// This gate did not hold.
    Gate(String),
    /// A fact the vine needs, at this path, is missing.
    Missing(String),
    /// The request id names a press of another move or payload.
    IdTaken,
}

impl Refusal {
    /// The name a calling vine sees under `gate`.
    pub fn gate(&self) -> String {
        match self {
            Refusal::UnknownMove => "unknown_move".into(),
            Refusal::Payload(_) => "payload".into(),
            Refusal::Gate(gate) => gate.clone(),
            Refusal::Missing(path) => format!("missing:{path}"),
            Refusal::IdTaken => "id_taken".into(),
        }
    }
}

/// Whether an output became its event, or was abandoned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fate {
    Landed,
    Abandoned,
}

/// One output of a press's vine, and what Terra did with it.
#[derive(Debug, Clone, PartialEq)]
pub struct Landing {
    pub output: String,
    pub payload: Value,
    pub fate: Fate,
    /// The event that landed: the output's own, or an `abandoned` one.
    pub event: Event,
}

/// How a press's vine ended.
#[derive(Debug, Clone, PartialEq)]
pub enum End {
    Done,
    /// The vine failed and nothing in it caught the failure.
    Failed(String),
}

/// The answer to a press.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    Refused(Refusal),
    Ran {
        request: String,
        landings: Vec<Landing>,
        end: End,
    },
}

impl Answer {
    /// The events this press landed for its own outputs, in order.
    pub fn landed(&self) -> Vec<Event> {
        self.with_fate(Fate::Landed)
    }

    /// The `abandoned` events this press landed, in order.
    pub fn abandoned(&self) -> Vec<Event> {
        self.with_fate(Fate::Abandoned)
    }

    fn with_fate(&self, fate: Fate) -> Vec<Event> {
        match self {
            Answer::Refused(_) => Vec::new(),
            Answer::Ran { landings, .. } => landings
                .iter()
                .filter(|l| l.fate == fate)
                .map(|l| l.event.clone())
                .collect(),
        }
    }
}

/// Why a trellis was not installed.
#[derive(Debug, Clone, PartialEq)]
pub enum InstallError {
    /// The vine at this place in the trellis's list did not build.
    Load {
        at: usize,
        error: LoadError,
    },
    DuplicateVine(String),
    DuplicateMove(String),
    /// The move's payload has a field Terra keeps for itself.
    ReservedField {
        move_name: String,
        field: String,
    },
    /// The move starts a vine the trellis does not have.
    UnknownVine {
        move_name: String,
        vine: String,
    },
    /// The payload plus the needs do not fit the vine's input.
    InputMisfit {
        move_name: String,
        vine: String,
        problem: String,
    },
    /// The vine can end with an output the move has no event for.
    NoEvent {
        move_name: String,
        output: String,
    },
    /// The move has an event for an output its vine does not have.
    NoSuchOutput {
        move_name: String,
        output: String,
    },
    NoVineNoDone(String),
    /// Terra runs every vine with `run_saved`, and this one cannot be.
    NotResumable {
        vine: String,
    },
    /// `vine` runs `uses` with `use`, and `started_by` starts `uses`.
    UsesStartedVine {
        vine: String,
        uses: String,
        started_by: String,
    },
    /// A reaction presses a move the trellis does not have.
    UnknownReactionMove(String),
    /// A reaction's payload does not give the move the fields it takes.
    ReactionPayload {
        move_name: String,
        problem: String,
    },
    /// These moves start each other in a loop nothing stops.
    Loop {
        moves: Vec<String>,
        why: LoopWhy,
    },
}

struct Installed {
    decl: Move,
    vine: Option<Pipe>,
}

/// Where the stand-in stops, as a crash would.
#[derive(Default)]
struct Fault {
    state: Mutex<FaultState>,
    crashed: Notify,
}

#[derive(Default)]
struct FaultState {
    at: Option<u64>,
    ticks: u64,
    crashed: bool,
    point: Option<String>,
}

impl Fault {
    /// Count one point where a crash can fall. Says whether Terra has
    /// crashed, at this point or before.
    fn tick(&self, point: impl FnOnce() -> String) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.crashed {
            return true;
        }
        s.ticks += 1;
        if s.at == Some(s.ticks) {
            s.crashed = true;
            s.point = Some(point());
            self.crashed.notify_one();
            return true;
        }
        false
    }
}

pub(crate) struct Inner {
    base: Registry,
    store: Mutex<Store>,
    moves: Mutex<BTreeMap<String, Arc<Installed>>>,
    reactions: Mutex<Vec<Arc<Reaction>>>,
    live: press::Live,
    fault: Fault,
}

impl Inner {
    fn installed(&self, name: &str) -> Option<Arc<Installed>> {
        self.moves.lock().unwrap().get(name).cloned()
    }
}

pub struct Terra {
    inner: Arc<Inner>,
}

impl Terra {
    /// A Terra whose vines can run the registered operators of `base`.
    pub fn new(base: Registry) -> Terra {
        Terra {
            inner: Arc::new(Inner {
                base,
                store: Mutex::new(Store::default()),
                moves: Mutex::new(BTreeMap::new()),
                reactions: Mutex::new(Vec::new()),
                live: press::Live::default(),
                fault: Fault::default(),
            }),
        }
    }

    /// Put an artifact's facts in place.
    pub fn put(&self, id: &str, facts: Value) {
        self.inner.store.lock().unwrap().put(id, facts);
    }

    /// Change one fact of an artifact. Its version moves.
    pub fn set(&self, id: &str, field: &str, value: Value) {
        self.inner.store.lock().unwrap().set(id, field, value);
    }

    /// One fact of an artifact, or null.
    pub fn fact(&self, id: &str, field: &str) -> Value {
        self.inner.store.lock().unwrap().fact(id, field)
    }

    /// Every event that landed, in the order it landed.
    pub fn events(&self) -> Vec<Event> {
        self.inner.store.lock().unwrap().events.clone()
    }

    /// The ids of every request Terra holds for `move_name`, sorted.
    pub fn requests_of(&self, move_name: &str) -> Vec<String> {
        self.inner.store.lock().unwrap().requests_of(move_name)
    }

    /// Install a trellis, or say why not. Nothing waits until a move is
    /// pressed to fail. See [`InstallError`] for what is checked.
    pub fn install(&self, trellis: Trellis) -> Result<(), InstallError> {
        install::install(&self.inner, trellis)
    }

    /// Press a move as an outside caller, under a new request id.
    pub async fn press(&self, name: &str, payload: Value) -> Answer {
        let id = self.inner.store.lock().unwrap().new_request_id();
        self.press_as(&id, name, payload)
            .await
            .expect("no crash is set for a plain press")
    }

    /// Press a move under `id`. A press with an id Terra holds attaches
    /// to that request. Gives nothing when Terra crashed partway.
    pub async fn press_as(&self, id: &str, name: &str, payload: Value) -> Option<Answer> {
        if self.inner.installed(name).is_none() {
            return Some(Answer::Refused(Refusal::UnknownMove));
        }
        if self.inner.store.lock().unwrap().clashes(id, name, &payload) {
            return Some(Answer::Refused(Refusal::IdTaken));
        }
        let parts = press::parts(self.inner.clone(), id.into(), name.into(), payload);
        if !press::drain(parts, &self.inner.fault.crashed).await {
            return None;
        }
        Some(self.inner.store.lock().unwrap().answer(id))
    }

    /// Press the moves whose reactions match the events that landed since
    /// the last call, and the events those land, until none is left. Each
    /// reaction's request id is made from the event's place in the log, so
    /// a reaction pressed again attaches to the first.
    pub async fn react(&self) -> Vec<Answer> {
        let mut answers = Vec::new();
        loop {
            let next = {
                let mut store = self.inner.store.lock().unwrap();
                let at = store.reacted;
                if at == store.events.len() {
                    return answers;
                }
                assert!(
                    at < 1000,
                    "reactions ran 1000 events: a loop got past install"
                );
                store.reacted += 1;
                at
            };
            answers.extend(self.react_to(next).await);
        }
    }

    /// Press the reactions to the event at `at` in the log again, as a
    /// queue that delivers twice would.
    pub async fn redeliver(&self, at: usize) -> Vec<Answer> {
        self.react_to(at).await
    }

    async fn react_to(&self, at: usize) -> Vec<Answer> {
        let event = self.inner.store.lock().unwrap().events[at].clone();
        let reactions = self.inner.reactions.lock().unwrap().clone();
        let mut answers = Vec::new();
        for (n, reaction) in reactions.iter().enumerate() {
            let Some(payload) = install::reaction_payload(reaction, &event) else {
                continue;
            };
            let id = format!("react/{at}/{n}");
            let answer = self.press_as(&id, reaction.press, payload).await;
            answers.push(answer.expect("no crash is set while reacting"));
        }
        answers
    }

    /// Crash at the `n`th point from now where a crash can fall: before
    /// and after Terra keeps each thing a vine's run gives.
    pub fn crash_at(&self, n: u64) {
        let mut s = self.inner.fault.state.lock().unwrap();
        *s = FaultState {
            at: Some(n),
            ..FaultState::default()
        };
    }

    /// Start again after a crash, keeping everything stored.
    pub fn restart(&self) {
        *self.inner.fault.state.lock().unwrap() = FaultState::default();
    }

    /// How many points where a crash can fall have passed since the last
    /// [`Terra::crash_at`] or [`Terra::restart`].
    pub fn ticks(&self) -> u64 {
        self.inner.fault.state.lock().unwrap().ticks
    }

    /// Where the last crash fell, such as `before r-1 keeps {..}`.
    pub fn crash_point(&self) -> Option<String> {
        self.inner.fault.state.lock().unwrap().point.clone()
    }
}
