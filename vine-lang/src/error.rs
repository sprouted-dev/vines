//! Errors. Every refusal names where it is and what is wrong.

use thiserror::Error;

/// Registering an operator failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegistryError {
    #[error("operator name `{0}` must be lower snake_case (a-z, 0-9, _)")]
    BadName(String),
    #[error("operator name `{0}` is reserved by the vine language")]
    Reserved(String),
    #[error("an operator named `{0}` is already registered")]
    Duplicate(String),
    #[error("operator `{operator}` declares it touches `{touch}`: touches are dot-separated lower snake_case words such as `facts.read`")]
    BadTouch { operator: String, touch: String },
    #[error(
        "act name `{0}` must be dot-separated lower snake_case words such as `git.worktree_add`"
    )]
    BadActName(String),
    #[error("act `{0}` declares it touches nothing; an act reaches outside and must say what it touches")]
    ActTouchesNothing(String),
}

/// A vine document was refused before it ran.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LoadError {
    #[error("not valid YAML: {0}")]
    Yaml(String),
    #[error("{path}: {problem}")]
    Malformed { path: String, problem: String },
    #[error("{path}: unknown key `{key}`")]
    UnknownKey { path: String, key: String },
    #[error("{path}: `{key}` would pull in another vine or a piece of one; vines do not compose, reuse is by operators only")]
    Composition { path: String, key: String },
    #[error("{path}: unknown operator `{name}` (not registered)")]
    UnknownOperator { path: String, name: String },
    #[error("{path}: unknown act `{name}` (not registered)")]
    UnknownAct { path: String, name: String },
    #[error("{path}: settings do not fit operator `{operator}`: {problem}")]
    BadSettings {
        path: String,
        operator: String,
        problem: String,
    },
    #[error("{path}: `{reference}` refers to `{name}`, which no earlier step produced")]
    UnboundReference {
        path: String,
        reference: String,
        name: String,
    },
    #[error("{path}: `{reference}` reads field `{field}`, which `{name}` never has")]
    UnknownField {
        path: String,
        reference: String,
        name: String,
        field: String,
    },
    #[error("{path}: output `{name}` is not declared in `outputs`")]
    UndeclaredOutput { path: String, name: String },
    #[error("{path}: payload does not fit output `{output}`: {problem}")]
    OutputMisfit {
        path: String,
        output: String,
        problem: String,
    },
    #[error("{path}: `as: {name}` is already bound")]
    DuplicateBinding { path: String, name: String },
    #[error("{path}: this step can never run; the step before it always ends the run")]
    Unreachable { path: String },
    #[error("{path}: these steps can finish without ending in an output; end every path with `out` or a `switch` with `else`")]
    MissingOutput { path: String },
    #[error("{path}: `finally` must not end the run (no `out` or `guard` inside it)")]
    EndsInFinally { path: String },
    #[error("{path}: `cancelled` cannot be caught; use `finally` for cleanup")]
    CatchCancelled { path: String },
    #[error("{path}: `{name}` is per-item grammar and cannot be a step of a stream pipe; put it inside `each`")]
    OnceOnly { path: String, name: String },
    #[error("{path}: `{name}` is a stream step; it may only stand in a stream vine's pipe (not in a once vine, nor inside `each`)")]
    PipeOnly { path: String, name: String },
    #[error("{path}: `each` must be the last step of the pipe; what its `out` steps name are the vine's outputs")]
    EachNotLast { path: String },
    #[error("{path}: the pipe's results cannot be matched to a declared output: {problem}")]
    UnmatchedOutput { path: String, problem: String },
}

/// A run ended without a declared output.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RunError {
    #[error("vine `{vine}` runs in {mode} mode; call the matching run method")]
    WrongMode { vine: String, mode: &'static str },
    #[error("input does not fit the declared shape: {0}")]
    InputMisfit(String),
    #[error("{path}: operator `{operator}` failed with `{code}`: {message} (no catch handles it)")]
    Unhandled {
        path: String,
        operator: String,
        code: String,
        message: String,
    },
    #[error("{path}: run cancelled")]
    Cancelled { path: String },
    #[error("{path}: resolved settings do not fit operator `{operator}`: {problem}")]
    SettingsMisfit {
        path: String,
        operator: String,
        problem: String,
    },
    #[error("{path}: operator `{operator}` returned a value that does not fit its declared shape: {problem}")]
    ResultMisfit {
        path: String,
        operator: String,
        problem: String,
    },
    #[error("{path}: payload does not fit output `{output}`: {problem}")]
    OutputMisfit {
        path: String,
        output: String,
        problem: String,
    },
    #[error("{path}: cleanup operator `{operator}` failed with `{code}`: {message}; the run would otherwise have ended with {masked}")]
    CleanupFailed {
        path: String,
        operator: String,
        code: String,
        message: String,
        masked: String,
    },
    #[error("{path}: {problem}")]
    ItemMisfit { path: String, problem: String },
    #[error("cannot resume from this checkpoint: {0}")]
    ResumeMisfit(String),
    #[error("{path}: stream operator `{operator}` was woken but its deadline is still due; `wake` must clear or move it")]
    DeadlineNotMoved { path: String, operator: String },
}
