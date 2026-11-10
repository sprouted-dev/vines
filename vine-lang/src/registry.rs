//! The operator registry.
//!
//! An operator is Rust: an async function from resolved settings to a
//! value. Each one declares its name, its settings shape, the shape of what
//! it returns (or nothing, meaning any value), and what it touches.
//!
//! The library registers its core operators in [`Registry::new`]. Anyone
//! else registers more with [`Registry::register`]. The control-flow core
//! operators (`guard`, `switch`, `try`, `out`) hold blocks of steps, so the
//! loader and runner handle them directly; they are listed here, touch
//! nothing, and their names are reserved.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::{LoadError, RegistryError};
use crate::shape::{Shape, Ty};
use crate::vine::Vine;

/// The error an operator returns. `code` is what a `catch` block matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpError {
    pub code: String,
    pub message: String,
}

impl OpError {
    /// The code an operator returns when it stopped because the run was
    /// cancelled. A vine cannot catch it; `finally` still runs.
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
    cancel: CancellationToken,
}

impl OpContext {
    pub(crate) fn new(cancel: CancellationToken) -> Self {
        OpContext { cancel }
    }

    /// The run's cancellation token. A long operator should watch it and
    /// return [`OpError::cancelled`] when it fires.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
}

/// What an operator declares about itself.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorSpec {
    pub name: String,
    pub settings: Shape,
    pub returns: Option<Shape>,
    pub touches: BTreeSet<String>,
}

impl OperatorSpec {
    /// A spec with no settings, any return value, touching nothing.
    pub fn new(name: &str) -> Self {
        OperatorSpec {
            name: name.to_owned(),
            settings: Shape::new(),
            returns: None,
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

    pub fn touches<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.touches.extend(names.into_iter().map(Into::into));
        self
    }
}

pub(crate) type Handler =
    Arc<dyn Fn(Value, OpContext) -> BoxFuture<'static, Result<Value, OpError>> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct Registered {
    pub spec: Arc<OperatorSpec>,
    pub handler: Handler,
}

/// The control-flow core operators. Handled by the loader and runner.
pub(crate) const CONSTRUCTS: &[&str] = &["guard", "switch", "try", "out"];

/// Keys that would pull another vine or a piece of one into this one.
/// Refused at the top level and as step keys, and never registrable.
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
        };
        // `set`: shape data. Binds its settings, resolved, as a map.
        r.insert(
            OperatorSpec::new("set").settings(Shape::open()),
            |settings, _cx| async move { Ok(settings) },
        );
        r
    }

    /// Register a provided operator.
    pub fn register<F, Fut>(&mut self, spec: OperatorSpec, f: F) -> Result<(), RegistryError>
    where
        F: Fn(Value, OpContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, OpError>> + Send + 'static,
    {
        let name = spec.name.clone();
        if !crate::expr::is_name(&name) {
            return Err(RegistryError::BadName(name));
        }
        if CONSTRUCTS.contains(&name.as_str()) || COMPOSITION_WORDS.contains(&name.as_str()) {
            return Err(RegistryError::Reserved(name));
        }
        if self.ops.contains_key(&name) {
            return Err(RegistryError::Duplicate(name));
        }
        if let Some(bad) = spec.touches.iter().find(|t| !is_touch(t)) {
            return Err(RegistryError::BadTouch {
                operator: name,
                touch: bad.clone(),
            });
        }
        self.insert(spec, f);
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

    /// Whether a vine may name this operator.
    pub fn contains(&self, name: &str) -> bool {
        self.ops.contains_key(name) || CONSTRUCTS.contains(&name)
    }

    /// The specs of every registered operator, plus the control-flow core
    /// operators (which touch nothing and take block settings).
    pub fn specs(&self) -> Vec<OperatorSpec> {
        let mut out: Vec<OperatorSpec> = self.ops.values().map(|r| (*r.spec).clone()).collect();
        out.extend(construct_specs());
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Check a vine document and return it ready to run.
    pub fn load(&self, yaml: &str) -> Result<Vine, LoadError> {
        crate::vine::load(self, yaml)
    }
}

/// `facts.read`, `git`, `agent`: dot-separated lower snake_case words.
fn is_touch(t: &str) -> bool {
    !t.is_empty() && t.split('.').all(crate::expr::is_name)
}

pub(crate) fn construct_specs() -> Vec<OperatorSpec> {
    vec![
        OperatorSpec::new("guard").settings(
            Shape::new()
                .required("when", Ty::Any)
                .required("out", Ty::Text)
                .optional("with", Ty::Any),
        ),
        OperatorSpec::new("out").settings(
            Shape::new()
                .required("name", Ty::Text)
                .optional("with", Ty::Any),
        ),
        OperatorSpec::new("switch").settings(
            Shape::new()
                .required("on", Ty::Any)
                .required("cases", Ty::Object)
                .optional("else", Ty::List),
        ),
        OperatorSpec::new("try").settings(
            Shape::new()
                .required("do", Ty::List)
                .optional("catch", Ty::Object)
                .optional("finally", Ty::List),
        ),
    ]
}
