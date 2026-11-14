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
    /// A key from before the pipe took its RxJS names.
    #[error("{path}: `{old}` is not a key any more; use {new}")]
    Renamed {
        path: String,
        old: String,
        new: String,
    },
    #[error("{path}: unknown operator `{name}` (not registered)")]
    UnknownOperator { path: String, name: String },
    #[error("{path}: unknown act `{name}` (not registered)")]
    UnknownAct { path: String, name: String },
    #[error("{path}: unknown vine `{name}` (not added to the registry)")]
    UnknownVine { path: String, name: String },
    #[error("{path}: settings do not fit operator `{operator}`: {problem}")]
    BadSettings {
        path: String,
        operator: String,
        problem: String,
    },
    #[error("{path}: `{reference}` refers to `{name}`, and `$item` is the only name in a pipe")]
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
    #[error("{path}: `{name}` already holds another result here")]
    DuplicateBinding { path: String, name: String },
    #[error("{path}: this can never run; everything before it already emits")]
    Unreachable { path: String },
    #[error("{path}: an item can get through without being emitted; end every path with `emit`, and give a `branch` an `else`")]
    MissingOutput { path: String },
}

/// A run failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RunError {
    #[error("input does not fit the declared shape: {0}")]
    InputMisfit(String),
    #[error("{path}: operator `{operator}` failed with `{code}`: {message} (no `catchError` handles it)")]
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
    #[error("{path}: {problem}")]
    ItemMisfit { path: String, problem: String },
    #[error("cannot resume from this journal: {problem}")]
    ReplayMisfit { problem: String },
    #[error("vine `{vine}` cannot be resumed: {why}")]
    NotResumable { vine: String, why: String },
}
