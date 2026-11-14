//! Errors. Every refusal names where it is and what is wrong.
//!
//! A `path` names a place in a vine, such as `pipe[1].with`. A `problem`
//! is the text of the finer refusal underneath, such as why a value does
//! not fit a shape.

use thiserror::Error;

/// Registering an operator failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegistryError {
    /// The name is neither lower `snake_case` nor dotted words of it.
    #[error("operator name `{0}` must be lower snake_case (a-z, 0-9, _); an operator that touches something outside may join such words with dots, such as `git.worktree_add`")]
    BadName(String),
    /// The name is a key the vine language gives a meaning to.
    #[error("operator name `{0}` is reserved by the vine language")]
    Reserved(String),
    /// Another operator holds the name.
    #[error("an operator named `{0}` is already registered")]
    Duplicate(String),
    /// A declared touch is not dotted lower `snake_case` words.
    #[error("operator `{operator}` declares it touches `{touch}`: touches are dot-separated lower snake_case words such as `facts.read`")]
    BadTouch {
        /// The operator being registered.
        operator: String,
        /// The touch as declared.
        touch: String,
    },
    /// An operator that reaches outside declares no touch.
    #[error("operator `{0}` declares it touches nothing; a stream operator, or one with a dotted name such as `git.worktree_add`, reaches outside and must say what it touches")]
    TouchesNothing(String),
}

/// A vine document was refused before it ran.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LoadError {
    /// The document is not YAML; the parser's message.
    #[error("not valid YAML: {0}")]
    Yaml(String),
    /// Something at `path` is not written the way the language allows.
    #[error("{path}: {problem}")]
    Malformed {
        /// Where.
        path: String,
        /// What is wrong.
        problem: String,
    },
    /// A key the place it stands in does not take.
    #[error("{path}: unknown key `{key}`")]
    UnknownKey {
        /// Where.
        path: String,
        /// The key.
        key: String,
    },
    /// A key from before the pipe took its `RxJS` names.
    #[error("{path}: `{old}` is not a key any more; use {new}")]
    Renamed {
        /// Where.
        path: String,
        /// The key as written.
        old: String,
        /// What to write instead.
        new: String,
    },
    /// No operator is registered under the name.
    #[error("{path}: unknown operator `{name}` (not registered)")]
    UnknownOperator {
        /// Where.
        path: String,
        /// The operator's name.
        name: String,
    },
    /// No vine was added to the registry under the name.
    #[error("{path}: unknown vine `{name}` (not added to the registry)")]
    UnknownVine {
        /// Where.
        path: String,
        /// The vine's name.
        name: String,
    },
    /// An operator's settings do not fit the shape it declares for them.
    #[error("{path}: settings do not fit operator `{operator}`: {problem}")]
    BadSettings {
        /// Where.
        path: String,
        /// The operator or vine the settings are for.
        operator: String,
        /// How they do not fit.
        problem: String,
    },
    /// A reference reads a name other than `$item`.
    #[error("{path}: `{reference}` refers to `{name}`, and `$item` is the only name in a pipe")]
    UnboundReference {
        /// Where.
        path: String,
        /// The reference as written, such as `$job.id`.
        reference: String,
        /// The name it reads.
        name: String,
    },
    /// A reference reads a field the item is known never to have.
    #[error("{path}: `{reference}` reads field `{field}`, which `{name}` never has")]
    UnknownField {
        /// Where.
        path: String,
        /// The reference as written.
        reference: String,
        /// The name it reads.
        name: String,
        /// The field.
        field: String,
    },
    /// An `emit` or `on` names an output the vine does not declare.
    #[error("{path}: output `{name}` is not declared in `outputs`")]
    UndeclaredOutput {
        /// Where.
        path: String,
        /// The output's name.
        name: String,
    },
    /// What an `emit` sends cannot fit the output's shape.
    #[error("{path}: payload does not fit output `{output}`: {problem}")]
    OutputMisfit {
        /// Where.
        path: String,
        /// The output.
        output: String,
        /// How it does not fit.
        problem: String,
    },
    /// A name that already holds a result here is given another.
    #[error("{path}: `{name}` already holds another result here")]
    DuplicateBinding {
        /// Where.
        path: String,
        /// The name.
        name: String,
    },
    /// An operator after operators that already emit every item.
    #[error("{path}: this can never run; everything before it already emits")]
    Unreachable {
        /// Where.
        path: String,
    },
    /// Some path through the pipe ends without an `emit`.
    #[error("{path}: an item can get through without being emitted; end every path with `emit`, and give a `branch` an `else`")]
    MissingOutput {
        /// The pipe.
        path: String,
    },
}

/// A run failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RunError {
    /// An input item does not fit the vine's input shape; how.
    #[error("input does not fit the declared shape: {0}")]
    InputMisfit(String),
    /// An operator failed and no `catchError` applies to it.
    #[error("{path}: operator `{operator}` failed with `{code}`: {message} (no `catchError` handles it)")]
    Unhandled {
        /// Where.
        path: String,
        /// The operator that failed.
        operator: String,
        /// Its failure code.
        code: String,
        /// Its failure message.
        message: String,
    },
    /// The run was cancelled while an operator ran.
    #[error("{path}: run cancelled")]
    Cancelled {
        /// Where.
        path: String,
    },
    /// An operator's settings, once resolved, do not fit its shape.
    #[error("{path}: resolved settings do not fit operator `{operator}`: {problem}")]
    SettingsMisfit {
        /// Where.
        path: String,
        /// The operator.
        operator: String,
        /// How they do not fit.
        problem: String,
    },
    /// An operator gave a value that does not fit the shape it declares.
    #[error("{path}: operator `{operator}` returned a value that does not fit its declared shape: {problem}")]
    ResultMisfit {
        /// Where.
        path: String,
        /// The operator.
        operator: String,
        /// How it does not fit.
        problem: String,
    },
    /// A payload, once resolved, does not fit its output's shape.
    #[error("{path}: payload does not fit output `{output}`: {problem}")]
    OutputMisfit {
        /// Where.
        path: String,
        /// The output.
        output: String,
        /// How it does not fit.
        problem: String,
    },
    /// An item cannot be used the way an operator needs.
    #[error("{path}: {problem}")]
    ItemMisfit {
        /// Where.
        path: String,
        /// What is wrong.
        problem: String,
    },
    /// What the caller saved does not fit what the run does again.
    #[error("cannot resume from this journal: {problem}")]
    ReplayMisfit {
        /// How it does not fit.
        problem: String,
    },
    /// The vine cannot run the resumable way asked for.
    #[error("vine `{vine}` cannot be resumed: {why}")]
    NotResumable {
        /// The vine.
        vine: String,
        /// Why not.
        why: String,
    },
}
