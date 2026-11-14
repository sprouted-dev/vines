//! What a pipe hands its caller, and the id that names each output.
//!
//! An output's id says where in the run the output comes from: the input
//! item it belongs to, the group of work it was made in, and how many
//! outputs that group made before it. It is built from the same things a
//! replay uses to match an output with one the caller was handed, so an
//! output handed out again after a restart has the id it had before. It is
//! never built from the payload, a clock or a random source.
//!
//! The id is unique within one run, and within one saved run across its
//! restarts. It says nothing about which run: a caller that keeps the
//! outputs of many runs in one place joins the id with its own key for the
//! run, such as a request id.

use std::fmt;

use serde_json::Value;

/// The stable id of one output of a run. See the module docs.
///
/// Compare it, store it with [`OutputId::as_str`], and drop an output
/// whose id you already hold: it is one you were handed before a restart.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutputId(String);

impl OutputId {
    /// The id as text, such as `#2`, `pipe[1].each@3/#0` or
    /// `7:pipe[1]#0@2/#1`. Keep it as it is: its parts are not an API.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The `n`th output, counted from 0, made at `place` for input item
    /// `item`. Item 0 is the whole run, in a journal or a run that saves
    /// nothing.
    pub(crate) fn at(item: u64, place: &str, n: u64) -> OutputId {
        if item == 0 {
            OutputId(format!("{place}#{n}"))
        } else {
            OutputId(format!("{item}:{place}#{n}"))
        }
    }
}

impl fmt::Display for OutputId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One output a pipe hands its caller.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeOutput {
    /// Where in the run it comes from. The same output handed out again
    /// after a restart has the same id; two outputs of one run never do.
    pub id: OutputId,
    /// The declared output it was sent to.
    pub name: String,
    pub payload: Value,
}
