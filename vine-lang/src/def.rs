//! Build a vine in Rust.
//!
//! This is the core. A vine is a pipe of steps with an input shape and
//! named outputs, and these functions are how one is put together. The
//! YAML layer writes the same thing down as text: each YAML construct maps
//! to one function here, and both go through the one checker in
//! [`Registry::build`](crate::Registry::build). A vine built here and the
//! same vine loaded from YAML are the same vine.
//!
//! ```
//! use vine_lang::def::{guard, once, out};
//! use vine_lang::{json, Registry, Shape, Ty};
//!
//! let claim = once("claim")
//!     .input(Shape::new().required("item", Ty::Id).required("state", Ty::Text))
//!     .output("claimed", Shape::new().required("item", Ty::Id))
//!     .output("refused", Shape::new().required("gate", Ty::Text))
//!     .steps([
//!         guard(r#"$in.state != "open""#, "refused", json!({ "gate": "state" })),
//!         out("claimed", json!({ "item": "$in.item" })),
//!     ]);
//! let vine = Registry::new().build(claim).unwrap();
//! assert_eq!(vine.name(), "claim");
//! ```
//!
//! Values written with `json!` may refer to what is in scope: `$in.field`
//! in a once vine, `$item.field` and `$span.first` in a pipe, and the name
//! a step was bound to with [`Step::bind`].

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use crate::error::LoadError;
use crate::registry::Registry;
use crate::shape::Shape;
use crate::vine::{assemble, Mode, Vine};

/// A vine as described, before it is checked.
#[derive(Debug, Clone)]
pub struct VineDef {
    name: String,
    mode: Mode,
    input: Shape,
    outputs: BTreeMap<String, Shape>,
    steps: Vec<Step>,
}

/// A vine that runs one time for one input.
pub fn once(name: &str) -> VineDef {
    VineDef::new(name, Mode::Once)
}

/// A vine that runs over a stream of inputs. Its steps are a pipe.
pub fn stream(name: &str) -> VineDef {
    VineDef::new(name, Mode::Stream)
}

impl VineDef {
    fn new(name: &str, mode: Mode) -> Self {
        VineDef {
            name: name.to_owned(),
            mode,
            input: Shape::new(),
            outputs: BTreeMap::new(),
            steps: Vec::new(),
        }
    }

    /// The shape of the input, or of each item for a stream vine.
    pub fn input(mut self, shape: Shape) -> Self {
        self.input = shape;
        self
    }

    /// Declare one named output and its shape.
    pub fn output(mut self, name: &str, shape: Shape) -> Self {
        self.outputs.insert(name.to_owned(), shape);
        self
    }

    /// The steps, in order. For a stream vine they are the pipe.
    pub fn steps(mut self, steps: impl IntoIterator<Item = Step>) -> Self {
        self.steps = steps.into_iter().collect();
        self
    }

    pub(crate) fn assemble(self, reg: &Registry) -> Result<Vine, LoadError> {
        let steps = block(self.steps);
        assemble(reg, self.name, self.mode, self.input, self.outputs, &steps)
    }
}

/// One step of a vine.
#[derive(Debug, Clone, PartialEq)]
pub struct Step(Map<String, Value>);

impl Step {
    fn of(key: &str, value: Value) -> Self {
        let mut map = Map::new();
        map.insert(key.to_owned(), value);
        Step(map)
    }

    /// Bind this step's result to a name later steps can refer to.
    pub fn bind(mut self, name: &str) -> Self {
        self.0
            .insert("as".to_owned(), Value::String(name.to_owned()));
        self
    }
}

fn block(steps: impl IntoIterator<Item = Step>) -> Value {
    Value::Array(steps.into_iter().map(|s| Value::Object(s.0)).collect())
}

/// Run a registered operator with these settings.
pub fn op(name: &str, settings: Value) -> Step {
    Step::of(name, settings)
}

/// Run a registered act, such as `git.worktree_add`, with this input.
pub fn call(act: &str, with: Value) -> Step {
    let mut step = Step::of("call", Value::String(act.to_owned()));
    step.0.insert("with".to_owned(), with);
    step
}

/// End the run with output `out` when the condition holds.
pub fn guard(when: &str, out: &str, with: Value) -> Step {
    Step::of("guard", json!({ "when": when, "out": out, "with": with }))
}

/// End the run with this output.
pub fn out(name: &str, with: Value) -> Step {
    Step::of("out", json!({ "name": name, "with": with }))
}

/// Run the block whose key equals the value of `on`.
pub fn switch<'a, C, B>(on: &str, cases: C) -> Switch
where
    C: IntoIterator<Item = (&'a str, B)>,
    B: IntoIterator<Item = Step>,
{
    Switch {
        on: on.to_owned(),
        cases: cases
            .into_iter()
            .map(|(k, b)| (k.to_owned(), block(b)))
            .collect(),
        otherwise: None,
    }
}

/// A `switch` being built. Turn it into a step with [`Switch::end`].
#[derive(Debug, Clone)]
pub struct Switch {
    on: String,
    cases: Map<String, Value>,
    otherwise: Option<Value>,
}

impl Switch {
    /// The block to run when no case matches.
    pub fn otherwise(mut self, steps: impl IntoIterator<Item = Step>) -> Self {
        self.otherwise = Some(block(steps));
        self
    }

    pub fn end(self) -> Step {
        let mut settings = Map::new();
        settings.insert("on".to_owned(), Value::String(self.on));
        settings.insert("cases".to_owned(), Value::Object(self.cases));
        if let Some(b) = self.otherwise {
            settings.insert("else".to_owned(), b);
        }
        Step::of("switch", Value::Object(settings))
    }
}

/// Run a block and decide what happens if a step in it fails.
pub fn attempt(steps: impl IntoIterator<Item = Step>) -> Attempt {
    Attempt {
        body: block(steps),
        catch: Map::new(),
        on_error: None,
        finally: None,
    }
}

/// A `try` being built. Turn it into a step with [`Attempt::end`].
#[derive(Debug, Clone)]
pub struct Attempt {
    body: Value,
    catch: Map<String, Value>,
    on_error: Option<Value>,
    finally: Option<Value>,
}

impl Attempt {
    /// The block to run for any failure. It sees the failure as data:
    /// `$error.code`, `$error.message` and `$error.from`.
    pub fn on_error(mut self, steps: impl IntoIterator<Item = Step>) -> Self {
        self.on_error = Some(block(steps));
        self
    }

    /// The block to run for a failure with this code.
    pub fn catch(mut self, code: &str, steps: impl IntoIterator<Item = Step>) -> Self {
        self.catch.insert(code.to_owned(), block(steps));
        self
    }

    /// The block that always runs last, however the body ended.
    pub fn finally(mut self, steps: impl IntoIterator<Item = Step>) -> Self {
        self.finally = Some(block(steps));
        self
    }

    pub fn end(self) -> Step {
        let mut settings = Map::new();
        settings.insert("do".to_owned(), self.body);
        if !self.catch.is_empty() {
            settings.insert("catch".to_owned(), Value::Object(self.catch));
        }
        if let Some(b) = self.on_error {
            settings.insert("on_error".to_owned(), b);
        }
        if let Some(b) = self.finally {
            settings.insert("finally".to_owned(), b);
        }
        Step::of("try", Value::Object(settings))
    }
}

/// The last step of a pipe: run these once-style steps for each item.
pub fn each(steps: impl IntoIterator<Item = Step>) -> Step {
    Step::of("each", block(steps))
}
