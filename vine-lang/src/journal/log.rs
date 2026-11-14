//! The journal of a run that ends: every entry its caller saved, and what
//! the run hands out.

use serde_json::Value;
use thiserror::Error;

use super::entry::{Entry, EntryError};
use crate::pipe::PipeOutput;

/// Saved JSON that is not a [`Journal`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JournalError {
    /// The journal is not a list.
    #[error("a journal is a list of entries")]
    NotAList,
    /// One entry of the list cannot be read.
    #[error("entry {index}: {source}")]
    Entry {
        /// The entry's place in the list, from 0.
        index: usize,
        /// Why it cannot be read.
        source: EntryError,
    },
}

/// What a resumable run gives its caller.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The vine emitted this.
    Output(PipeOutput),
    /// Append this to the journal you keep for this run.
    Save(Entry),
}

/// Everything a caller saved for one run, oldest first.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Journal {
    entries: Vec<Entry>,
}

impl Journal {
    /// An empty journal: a fresh start.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an entry as it is, dropping nothing.
    pub fn push(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    /// Keep what the run handed out: append an entry, and drop the
    /// entries it makes useless. An [`Entry::Through`] replaces the one
    /// before it for the same stream operator and drops the entries of the
    /// items of that stream it passed. An output changes nothing.
    pub fn record(&mut self, event: &Event) {
        if let Event::Save(entry) = event {
            if matches!(entry, Entry::Through { .. }) {
                self.entries.retain(|kept| !kept.passed_by(entry));
            }
            self.entries.push(entry.clone());
        }
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The journal as a JSON list of entries.
    pub fn to_json(&self) -> Value {
        Value::Array(self.entries.iter().map(Entry::to_json).collect())
    }

    /// Read a JSON list of entries.
    pub fn from_json(v: &Value) -> Result<Journal, JournalError> {
        let Some(list) = v.as_array() else {
            return Err(JournalError::NotAList);
        };
        let entries = list
            .iter()
            .enumerate()
            .map(|(index, e)| {
                Entry::from_json(e).map_err(|source| JournalError::Entry { index, source })
            })
            .collect::<Result<_, _>>()?;
        Ok(Journal { entries })
    }
}

impl From<Vec<Entry>> for Journal {
    fn from(entries: Vec<Entry>) -> Self {
        Journal { entries }
    }
}

impl FromIterator<Entry> for Journal {
    fn from_iter<I: IntoIterator<Item = Entry>>(iter: I) -> Self {
        Journal {
            entries: iter.into_iter().collect(),
        }
    }
}
