//! The operator registry.
//!
//! Every step of a pipe is an operator. The built-in ones, such as `filter`
//! and `mergeMap`, ship with the library and are written by their keys.
//! The rest are registered here by name: an async Rust function from
//! resolved settings to a value. Each one declares its name, its settings
//! shape, the shape of what it returns (or nothing, meaning any value),
//! and what it touches.
//!
//! What an operator touches decides how a pipe treats it:
//!
//! - One that touches nothing is pure, such as the core `set` and
//!   `select`. A pipe runs it by its name, as `set: { ... }`, and saves
//!   nothing about it: a replay runs it again.
//! - One that touches something reaches outside, such as
//!   `git.worktree_add`. The library ships none; the caller supplies the
//!   Rust behind each name, so the library never learns what `git` is. A
//!   pipe runs it with `call` or `tap`, saves its result so a replay does
//!   not run it again, and counts what it touches toward the vine's
//!   reach. It must be safe to run twice: a result can repeat after a
//!   crash, and one whose result was not saved runs again.
//!
//! A stream operator, registered with [`Registry::register_stream`] or
//! [`Registry::register_replayable_stream`], gives many items and then an
//! end. It always reaches outside.
//!
//! Every registered operator shares one namespace.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use serde_json::Value;

use crate::error::{LoadError, RegistryError};
use crate::shape::{Shape, Ty};

/// The error an operator returns. A `catchError` gets its `code` and
/// `message` as `$item.error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpError {
    pub code: String,
    pub message: String,
}

impl OpError {
    /// The code an operator returns when it stopped because it was
    /// cancelled. No `catchError` handles it: the run ends with
    /// [`RunError::Cancelled`](crate::RunError::Cancelled), and the failure
    /// is not saved, so a resumed run asks the operator again.
    pub const CANCELLED: &'static str = "cancelled";

    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        OpError {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(Self::CANCELLED, message)
    }
}

/// What an operator gets besides its settings.
#[derive(Debug, Clone, Default)]
pub struct OpContext {
    resume_from: u64,
}

impl OpContext {
    pub(crate) fn new() -> Self {
        OpContext::default()
    }

    pub(crate) fn resuming(mut self, from: u64) -> Self {
        self.resume_from = from;
        self
    }

    /// For a stream operator: give the items of this same call from the
    /// one after this many. It is 0 on a fresh start. For one registered
    /// with [`Registry::register_stream`] it is the number of items the
    /// call gave in an earlier run that was saved. For one registered with
    /// [`Registry::register_replayable_stream`] it can be fewer: the items
    /// through it are fully done, and the operator gives the rest again.
    pub fn resume_from(&self) -> u64 {
        self.resume_from
    }
}

/// What an operator declares about itself.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorSpec {
    pub name: String,
    pub settings: Shape,
    pub returns: Option<Shape>,
    /// For a stream operator: the shape of the value it ends with.
    pub ends: Option<Shape>,
    /// What it reaches outside. Empty for a pure operator.
    pub touches: BTreeSet<String>,
}

impl OperatorSpec {
    /// A spec with no settings, any return value, touching nothing.
    pub fn new(name: &str) -> Self {
        OperatorSpec {
            name: name.to_owned(),
            settings: Shape::new(),
            returns: None,
            ends: None,
            touches: BTreeSet::new(),
        }
    }

    pub fn settings(mut self, shape: Shape) -> Self {
        self.settings = shape;
        self
    }

    /// Declare the shape of a map the operator returns. Lets the checker
    /// refuse `$item.field` for a field the operator never returns.
    pub fn returns(mut self, shape: Shape) -> Self {
        self.returns = Some(shape);
        self
    }

    /// For a stream operator: declare the shape of the value it ends with.
    /// One that declares it must end with [`Part::End`].
    pub fn ends(mut self, shape: Shape) -> Self {
        self.ends = Some(shape);
        self
    }

    /// Declare what the operator reaches outside, such as `git` or
    /// `facts.read`. An operator that declares nothing is pure.
    pub fn touches<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.touches.extend(names.into_iter().map(Into::into));
        self
    }
}

/// One thing a stream operator gives.
#[derive(Debug, Clone, PartialEq)]
pub enum Part {
    /// One more item. It is checked against the operator's `returns`
    /// shape.
    Item(Value),
    /// The operator is done, and this is how it ended, such as
    /// `{ status: finished }`. It is checked against the operator's `ends`
    /// shape. Nothing after it is read.
    End(Value),
}

/// What a stream operator gives back: parts, then the end of the stream.
/// An `Err` is a failure, and nothing after it is read.
pub type OperatorStream = BoxStream<'static, Result<Part, OpError>>;

pub(crate) type StreamHandler = Arc<dyn Fn(Value, OpContext) -> OperatorStream + Send + Sync>;

pub(crate) type Handler =
    Arc<dyn Fn(Value, OpContext) -> BoxFuture<'static, Result<Value, OpError>> + Send + Sync>;

/// What a stream operator can do when a run that was saved asks it for its
/// items again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gives {
    /// It gives only the items after those it gave, so the run saves every
    /// item it gives.
    OnlyNew,
    /// It can give its items again from any item it gave, so the run saves
    /// only how far it got.
    Again,
}

/// How a registered operator runs.
#[derive(Clone)]
pub(crate) enum Run {
    /// One result for each item.
    Once(Handler),
    /// Many items and then an end.
    Stream {
        handler: StreamHandler,
        gives: Gives,
    },
}

#[derive(Clone)]
pub(crate) struct Registered {
    pub spec: Arc<OperatorSpec>,
    pub run: Run,
}

impl Registered {
    /// Whether it touches something outside. One that does is run with
    /// `call` or `tap`, and its result is saved.
    pub(crate) fn reaches_outside(&self) -> bool {
        !self.spec.touches.is_empty()
    }
}

/// The key that runs a registered operator that reaches outside:
/// `call: git.worktree_add`.
pub(crate) const CALL: &str = "call";

/// The lower-case words of the pipe model. No pure operator may take one,
/// since a pure operator is written by its name where these keys stand.
///
/// The pipe's camelCase keys, such as `mergeMap` and `catchError`, need no
/// entry: a pure operator's name is lower snake_case, so it can never be
/// one. `each`, `collect`, `together` and `catch` are the old names of the
/// mapping operators, `bufferCount`, `forkJoin` and `catchError`. The
/// pipe's YAML refuses them as keys, so an operator by any of them could
/// never run in a pipe, and they stay reserved.
pub(crate) const PIPE_WORDS: &[&str] = &[
    "tap", "split", "filter", "take", "each", "collect", "together", "catch", "finalize", "retry",
    "timeout", "branch", "emit", "with", "into", "on", "fork", "last", "end",
];

/// Words that read as pulling another vine or a piece of one into this
/// one. A pipe does that only with `use`, so no pure operator may take
/// one.
pub(crate) const COMPOSITION_WORDS: &[&str] = &[
    "include",
    "import",
    "imports",
    "extends",
    "use",
    "uses",
    "vine",
    "vines",
    "subvine",
    "sub_vine",
    "call_vine",
    "run_vine",
];

/// The operators and vines a vine may name.
#[derive(Clone)]
pub struct Registry {
    ops: BTreeMap<String, Registered>,
    vines: BTreeMap<String, crate::pipe::Pipe>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    /// A registry holding the core operators, `set` and `select`. Both are
    /// pure.
    pub fn new() -> Self {
        let mut r = Registry {
            ops: BTreeMap::new(),
            vines: BTreeMap::new(),
        };
        // `set`: shape data. Gives its settings, resolved, as a map.
        r.insert_once(
            OperatorSpec::new("set").settings(Shape::open()),
            |settings, _cx| async move { Ok(settings) },
        );
        // `select`: keep the items of a list whose fields equal every field
        // of `where`. Pure data, like a filter over a list.
        r.insert_once(
            OperatorSpec::new("select")
                .settings(
                    Shape::new()
                        .required("from", Ty::List)
                        .required("where", Ty::Object),
                )
                .returns(
                    Shape::new()
                        .required("items", Ty::List)
                        .required("count", Ty::Int),
                ),
            |settings, _cx| async move {
                let want = settings["where"].as_object().cloned().unwrap_or_default();
                let items: Vec<Value> = settings["from"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|item| want.iter().all(|(k, v)| item.get(k) == Some(v)))
                    .cloned()
                    .collect();
                Ok(serde_json::json!({ "count": items.len(), "items": items }))
            },
        );
        r
    }

    /// Register an operator that gives one result for each item.
    ///
    /// What `spec` says it touches decides what it is (see the module
    /// docs). A pure one, touching nothing, has a lower snake_case name
    /// that is not a word of the pipe, and a vine runs it by its name. One
    /// that touches something reaches outside: its name is lower
    /// snake_case words that may be joined by dots, such as
    /// `git.worktree_add`, and a vine runs it with `call` or `tap`.
    ///
    /// Refused: a name that breaks those rules, a dotted name on an
    /// operator that touches nothing ([`RegistryError::TouchesNothing`]),
    /// a name already registered, and a touch that is not dot-separated
    /// lower snake_case words.
    pub fn register<F, Fut>(&mut self, spec: OperatorSpec, f: F) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, OpError>> + Send + 'static,
    {
        self.check(&spec, Kind::Once)?;
        self.insert_once(spec, f);
        Ok(())
    }

    /// Register a stream operator: one item in becomes one item per
    /// [`Part::Item`] it gives, and the stream may end with a
    /// [`Part::End`] that says how it ended. An `Err` is an ordinary
    /// failure, which a `catchError` can handle.
    ///
    /// A stream operator reaches outside, so it must declare what it
    /// touches, and its name follows the rules for an operator that does
    /// in [`Registry::register`].
    ///
    /// A run that is resumed calls it again for the same input. It reads
    /// [`OpContext::resume_from`] and gives only the items after those it
    /// already gave. So one that starts long work should start it the
    /// first time and attach to it after that.
    pub fn register_stream<F, S>(&mut self, spec: OperatorSpec, f: F) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> S + Send + Sync + 'static,
        S: Stream<Item = Result<Part, OpError>> + Send + 'static,
    {
        self.insert_stream(spec, Gives::OnlyNew, f)
    }

    /// Register a stream operator that can give its items again from any
    /// item it gave, such as an agent host that stores every line a
    /// session wrote. A saved run then keeps how far it got instead of
    /// every item, so what the caller keeps does not grow with the stream.
    ///
    /// The operator makes a promise: called again for the same input with
    /// [`OpContext::resume_from`] set to `n`, it gives the same items it
    /// gave the first time, from item `n + 1` on, and then goes on as it
    /// would have. `n` can be any number up to the items it gave. A run
    /// catches one that gives fewer items again than it gave before it
    /// stopped. It cannot see one that gives other items.
    ///
    /// Where the operators after it could not be rebuilt from a later
    /// item, the run saves every item it gives instead.
    /// [`Pipe::saves_every_item`](crate::Pipe::saves_every_item) says
    /// where and why.
    pub fn register_replayable_stream<F, S>(
        &mut self,
        spec: OperatorSpec,
        f: F,
    ) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> S + Send + Sync + 'static,
        S: Stream<Item = Result<Part, OpError>> + Send + 'static,
    {
        self.insert_stream(spec, Gives::Again, f)
    }

    fn insert_stream<F, S>(
        &mut self,
        spec: OperatorSpec,
        gives: Gives,
        f: F,
    ) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> S + Send + Sync + 'static,
        S: Stream<Item = Result<Part, OpError>> + Send + 'static,
    {
        self.check(&spec, Kind::Stream)?;
        let handler: StreamHandler = Arc::new(move |v, cx| f(v, cx).boxed());
        self.ops.insert(
            spec.name.clone(),
            Registered {
                spec: Arc::new(spec),
                run: Run::Stream { handler, gives },
            },
        );
        Ok(())
    }

    fn check(&self, spec: &OperatorSpec, kind: Kind) -> Result<(), RegistryError> {
        let name = &spec.name;
        let reaches = !spec.touches.is_empty() || kind == Kind::Stream;
        if reaches {
            if !is_touch(name) {
                return Err(RegistryError::BadName(name.clone()));
            }
        } else if !crate::expr::is_name(name) {
            return Err(if is_touch(name) {
                RegistryError::TouchesNothing(name.clone())
            } else {
                RegistryError::BadName(name.clone())
            });
        } else if is_reserved(name) {
            return Err(RegistryError::Reserved(name.clone()));
        }
        if self.ops.contains_key(name) {
            return Err(RegistryError::Duplicate(name.clone()));
        }
        if spec.touches.is_empty() && kind == Kind::Stream {
            return Err(RegistryError::TouchesNothing(name.clone()));
        }
        if let Some(bad) = spec.touches.iter().find(|t| !is_touch(t)) {
            return Err(RegistryError::BadTouch {
                operator: name.clone(),
                touch: bad.clone(),
            });
        }
        Ok(())
    }

    fn insert_once<F, Fut>(&mut self, spec: OperatorSpec, f: F)
    where
        F: Fn(Value, OpContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, OpError>> + Send + 'static,
    {
        let handler: Handler = Arc::new(move |v, cx| f(v, cx).boxed());
        self.ops.insert(
            spec.name.clone(),
            Registered {
                spec: Arc::new(spec),
                run: Run::Once(handler),
            },
        );
    }

    pub(crate) fn get(&self, name: &str) -> Option<&Registered> {
        self.ops.get(name)
    }

    /// For a stream operator: whether it gives its items again.
    pub(crate) fn stream_gives(&self, name: &str) -> Option<Gives> {
        match self.ops.get(name)?.run {
            Run::Stream { gives, .. } => Some(gives),
            Run::Once(_) => None,
        }
    }

    /// The specs of every registered operator, the core ones and the
    /// stream operators included, sorted by name.
    pub fn specs(&self) -> Vec<OperatorSpec> {
        self.ops.values().map(|r| (*r.spec).clone()).collect()
    }

    /// The specs of every registered stream operator, sorted by name.
    pub fn stream_specs(&self) -> Vec<OperatorSpec> {
        self.ops
            .values()
            .filter(|r| matches!(r.run, Run::Stream { .. }))
            .map(|r| (*r.spec).clone())
            .collect()
    }

    /// Make a checked vine available to other vines, which run it with a
    /// `use` operator. A vine can only use a vine that was added before it
    /// was built, so two vines can never use each other.
    pub fn add_vine(&mut self, vine: crate::pipe::Pipe) -> Result<(), RegistryError> {
        if self.vines.contains_key(vine.name()) {
            return Err(RegistryError::Duplicate(vine.name().to_owned()));
        }
        self.vines.insert(vine.name().to_owned(), vine);
        Ok(())
    }

    pub(crate) fn get_vine(&self, name: &str) -> Option<&crate::pipe::Pipe> {
        self.vines.get(name)
    }

    /// Check a vine built as a pipe in Rust and return it ready to run.
    pub fn build_pipe(&self, def: crate::pipe::PipeDef) -> Result<crate::pipe::Pipe, LoadError> {
        crate::pipe::assemble(self, def)
    }

    /// Check a vine written as a pipe in YAML and return it ready to run.
    #[cfg(feature = "yaml")]
    pub fn load_pipe(&self, yaml: &str) -> Result<crate::pipe::Pipe, LoadError> {
        crate::pipe::load(self, yaml)
    }
}

/// Which registration a spec came through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Once,
    Stream,
}

/// A word of the pipe, which a pure operator written by its name could be
/// mistaken for.
fn is_reserved(name: &str) -> bool {
    name == CALL || PIPE_WORDS.contains(&name) || COMPOSITION_WORDS.contains(&name)
}

/// `facts.read`, `git`, `agent`: dot-separated lower snake_case words.
fn is_touch(t: &str) -> bool {
    !t.is_empty() && t.split('.').all(crate::expr::is_name)
}
