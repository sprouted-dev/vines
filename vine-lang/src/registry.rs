//! The operator registry.
//!
//! An operator is Rust: an async function from resolved settings to a
//! value. Each one declares its name, its settings shape, the shape of what
//! it returns (or nothing, meaning any value), and what it touches.
//!
//! The library registers its core operators in [`Registry::new`]. Anyone
//! else registers more with [`Registry::register`].

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

    /// For a stream act: give the items of this same call from the one
    /// after this many. It is 0 on a fresh start. For an act registered
    /// with [`Registry::register_stream_act`] it is the number of items the
    /// call gave in an earlier run that was saved. For one registered with
    /// [`Registry::register_replayable_stream_act`] it can be fewer: the
    /// items through it are fully done, and the act gives the rest again.
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
    /// For a stream act: the shape of the value it ends with.
    pub ends: Option<Shape>,
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

    /// Declare the shape of a map the operator returns. Lets the loader
    /// refuse `$x.field` for a field the operator never returns.
    pub fn returns(mut self, shape: Shape) -> Self {
        self.returns = Some(shape);
        self
    }

    /// For a stream act: declare the shape of the value it ends with. A
    /// stream act that declares one must end with [`Part::End`].
    pub fn ends(mut self, shape: Shape) -> Self {
        self.ends = Some(shape);
        self
    }

    pub fn touches<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.touches.extend(names.into_iter().map(Into::into));
        self
    }
}

/// One thing a stream act gives.
#[derive(Debug, Clone, PartialEq)]
pub enum Part {
    /// One more item. It is checked against the act's `returns` shape.
    Item(Value),
    /// The act is done, and this is how it ended, such as
    /// `{ status: finished }`. It is checked against the act's `ends`
    /// shape. Nothing after it is read.
    End(Value),
}

/// What a stream act gives back: parts, then the end of the stream. An
/// `Err` is a failure, and nothing after it is read.
pub type ActStream = BoxStream<'static, Result<Part, OpError>>;

pub(crate) type StreamHandler = Arc<dyn Fn(Value, OpContext) -> ActStream + Send + Sync>;

/// What a stream act can do when a run that was saved asks it for its
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

#[derive(Clone)]
pub(crate) struct RegisteredStreamAct {
    pub spec: Arc<OperatorSpec>,
    pub handler: StreamHandler,
    pub gives: Gives,
}

pub(crate) type Handler =
    Arc<dyn Fn(Value, OpContext) -> BoxFuture<'static, Result<Value, OpError>> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct Registered {
    pub spec: Arc<OperatorSpec>,
    pub handler: Handler,
}

/// The step that runs a named act: `call: git.worktree_add`.
pub(crate) const CALL: &str = "call";

/// The lower-case words of the pipe model. No operator may take one.
///
/// The pipe's camelCase keys, such as `mergeMap` and `catchError`, need no
/// entry: an operator name is lower snake_case, so it can never be one.
/// `each`, `collect`, `together` and `catch` are the old names of the
/// mapping stages, `bufferCount`, `forkJoin` and `catchError`. The pipe's
/// YAML refuses them as stage keys, so an operator by any of them could
/// never run in a pipe, and they stay reserved.
pub(crate) const PIPE_WORDS: &[&str] = &[
    "tap", "split", "filter", "take", "each", "collect", "together", "catch", "finalize", "retry",
    "timeout", "branch", "emit", "with", "into", "on", "fork", "last", "end",
];

/// Words that read as pulling another vine or a piece of one into this
/// one. A pipe does that only with `use`, so no operator may take one.
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

/// Operators a vine may name.
#[derive(Clone)]
pub struct Registry {
    ops: BTreeMap<String, Registered>,
    acts: BTreeMap<String, Registered>,
    stream_acts: BTreeMap<String, RegisteredStreamAct>,
    vines: BTreeMap<String, crate::pipe::Pipe>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    /// A registry holding the core operators.
    pub fn new() -> Self {
        let mut r = Registry {
            ops: BTreeMap::new(),
            acts: BTreeMap::new(),
            stream_acts: BTreeMap::new(),
            vines: BTreeMap::new(),
        };
        // `set`: shape data. Binds its settings, resolved, as a map.
        r.insert(
            OperatorSpec::new("set").settings(Shape::open()),
            |settings, _cx| async move { Ok(settings) },
        );
        // `select`: keep the items of a list whose fields equal every field
        // of `where`. Pure data, like a filter over a list.
        r.insert(
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

    /// Register a provided operator.
    pub fn register<F, Fut>(&mut self, spec: OperatorSpec, f: F) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, OpError>> + Send + 'static,
    {
        self.check_new(&spec.name, &spec.touches)?;
        self.insert(spec, f);
        Ok(())
    }

    /// Register an act: something that reaches outside, run by a `call`
    /// step. The library ships none. Whoever uses the library supplies the
    /// Rust behind each name, so the library never learns what `git` is.
    ///
    /// The name is dot-separated, such as `git.worktree_add`. An act must
    /// declare what it touches: a step that touches nothing is an
    /// operator, not an act.
    pub fn register_act<F, Fut>(&mut self, spec: OperatorSpec, f: F) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, OpError>> + Send + 'static,
    {
        self.check_act(&spec)?;
        let handler: Handler = Arc::new(move |v, cx| f(v, cx).boxed());
        self.acts.insert(
            spec.name.clone(),
            Registered {
                spec: Arc::new(spec),
                handler,
            },
        );
        Ok(())
    }

    /// Register a stream act: an act whose result is a stream. One item in
    /// becomes one item per [`Part::Item`] it gives, and the stream may end
    /// with a [`Part::End`] that says how it ended. An `Err` is an ordinary
    /// failure, which a `catchError` can handle.
    ///
    /// A run that is resumed calls the act again for the same input. The
    /// act reads [`OpContext::resume_from`] and gives only the items after
    /// those it already gave. So an act that starts long work should start
    /// it the first time and attach to it after that.
    ///
    /// Names follow the rules of [`Registry::register_act`], and acts and
    /// stream acts share one namespace.
    pub fn register_stream_act<F, S>(
        &mut self,
        spec: OperatorSpec,
        f: F,
    ) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> S + Send + Sync + 'static,
        S: Stream<Item = Result<Part, OpError>> + Send + 'static,
    {
        self.insert_stream_act(spec, Gives::OnlyNew, f)
    }

    /// Register a stream act that can give its items again from any item
    /// it gave, such as an agent host that stores every line a session
    /// wrote. A saved run then keeps how far it got instead of every item,
    /// so what the caller keeps does not grow with the stream.
    ///
    /// The act makes a promise: called again for the same input with
    /// [`OpContext::resume_from`] set to `n`, it gives the same items it
    /// gave the first time, from item `n + 1` on, and then goes on as it
    /// would have. `n` can be any number up to the items it gave. A run
    /// catches an act that gives fewer items again than it gave before it
    /// stopped. It cannot see an act that gives other items.
    ///
    /// Where a vine's stages could not be rebuilt from a later item, the
    /// run saves every item of the act instead.
    /// [`Pipe::saves_every_item`](crate::Pipe::saves_every_item) says
    /// where and why.
    pub fn register_replayable_stream_act<F, S>(
        &mut self,
        spec: OperatorSpec,
        f: F,
    ) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> S + Send + Sync + 'static,
        S: Stream<Item = Result<Part, OpError>> + Send + 'static,
    {
        self.insert_stream_act(spec, Gives::Again, f)
    }

    fn insert_stream_act<F, S>(
        &mut self,
        spec: OperatorSpec,
        gives: Gives,
        f: F,
    ) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> S + Send + Sync + 'static,
        S: Stream<Item = Result<Part, OpError>> + Send + 'static,
    {
        self.check_act(&spec)?;
        let handler: StreamHandler = Arc::new(move |v, cx| f(v, cx).boxed());
        self.stream_acts.insert(
            spec.name.clone(),
            RegisteredStreamAct {
                spec: Arc::new(spec),
                handler,
                gives,
            },
        );
        Ok(())
    }

    fn check_act(&self, spec: &OperatorSpec) -> Result<(), RegistryError> {
        if !is_touch(&spec.name) {
            return Err(RegistryError::BadActName(spec.name.clone()));
        }
        if self.acts.contains_key(&spec.name) || self.stream_acts.contains_key(&spec.name) {
            return Err(RegistryError::Duplicate(spec.name.clone()));
        }
        if spec.touches.is_empty() {
            return Err(RegistryError::ActTouchesNothing(spec.name.clone()));
        }
        if let Some(bad) = spec.touches.iter().find(|t| !is_touch(t)) {
            return Err(RegistryError::BadTouch {
                operator: spec.name.clone(),
                touch: bad.clone(),
            });
        }
        Ok(())
    }

    pub(crate) fn get_stream_act(&self, name: &str) -> Option<&RegisteredStreamAct> {
        self.stream_acts.get(name)
    }

    /// The specs of every registered stream act.
    pub fn stream_act_specs(&self) -> Vec<OperatorSpec> {
        self.stream_acts
            .values()
            .map(|r| (*r.spec).clone())
            .collect()
    }

    pub(crate) fn get_act(&self, name: &str) -> Option<&Registered> {
        self.acts.get(name)
    }

    /// The specs of every registered act.
    pub fn act_specs(&self) -> Vec<OperatorSpec> {
        self.acts.values().map(|r| (*r.spec).clone()).collect()
    }

    fn check_new(&self, name: &str, touches: &BTreeSet<String>) -> Result<(), RegistryError> {
        if !crate::expr::is_name(name) {
            return Err(RegistryError::BadName(name.into()));
        }
        if name == CALL || PIPE_WORDS.contains(&name) || COMPOSITION_WORDS.contains(&name) {
            return Err(RegistryError::Reserved(name.into()));
        }
        if self.ops.contains_key(name) {
            return Err(RegistryError::Duplicate(name.into()));
        }
        if let Some(bad) = touches.iter().find(|t| !is_touch(t)) {
            return Err(RegistryError::BadTouch {
                operator: name.into(),
                touch: bad.clone(),
            });
        }
        Ok(())
    }

    fn insert<F, Fut>(&mut self, spec: OperatorSpec, f: F)
    where
        F: Fn(Value, OpContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, OpError>> + Send + 'static,
    {
        let handler: Handler = Arc::new(move |v, cx| f(v, cx).boxed());
        self.ops.insert(
            spec.name.clone(),
            Registered {
                spec: Arc::new(spec),
                handler,
            },
        );
    }

    pub(crate) fn get(&self, name: &str) -> Option<&Registered> {
        self.ops.get(name)
    }

    /// The specs of every registered operator, the core ones included.
    pub fn specs(&self) -> Vec<OperatorSpec> {
        self.ops.values().map(|r| (*r.spec).clone()).collect()
    }

    /// Make a checked vine available to other vines, which run it with a
    /// `use` stage. A vine can only use a vine that was added before it was
    /// built, so two vines can never use each other.
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

/// `facts.read`, `git`, `agent`: dot-separated lower snake_case words.
fn is_touch(t: &str) -> bool {
    !t.is_empty() && t.split('.').all(crate::expr::is_name)
}
