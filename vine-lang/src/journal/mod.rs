//! What a resumable run asks its caller to save, and gets back after a
//! restart.
//!
//! There are two ways to survive a restart, and they share one save format.
//!
//! - A run that ends, such as one job, uses
//!   [`Pipe::run_saved`](crate::Pipe::run_saved). It gives a stream of
//!   [`Event`]s. An [`Event::Output`] is something the vine emitted. An
//!   [`Event::Save`] is one [`Entry`] the caller appends to its store. To
//!   resume, the caller builds a [`Journal`] from what it stored and runs
//!   the vine again over the same input items, from the first.
//! - A run over an endless stream uses
//!   [`Pipe::run_from`](crate::Pipe::run_from). It gives [`Progress`]: the
//!   same outputs, the same entries as [`Line`]s that name the input item
//!   they belong to, and [`Progress::Through`], which says every input item
//!   through a number is fully done. The caller keeps a [`Bookmark`]: the
//!   position and the lines past it. To resume, it feeds input from the
//!   item after the position, with the bookmark.
//!
//! The library stores nothing. Both are plain data: [`Entry::to_json`],
//! [`Journal::from_json`] and [`Bookmark::from_json`] turn them into JSON
//! and back.
//!
//! # What an entry means
//!
//! Each call a run makes has a key: the path of its operator and a count of how
//! many times that operator ran before, such as `pipe[1]#0`. In a journal the
//! count runs over the whole run. In a bookmark it runs over the work of
//! one input item, so the keys of item 7 are the same whether the run
//! started at item 1 or at item 5. A replay runs the operators in the same
//! order, so the keys line up.
//!
//! - [`Entry::Done`] and [`Entry::Failed`]: a call gave this result or
//!   failed this way. On replay the call is not made again. Its saved result is
//!   used.
//! - [`Entry::Started`], [`Entry::Item`] and [`Entry::Ended`]: a stream operator
//!   was called, gave one more item, and ended. On replay the saved items
//!   flow again. If it had not ended, the operator is called again with
//!   [`OpContext::resume_from`](crate::OpContext::resume_from) set to the
//!   number of items saved, and the run carries on from there.
//! - [`Entry::Emitted`]: the caller was handed this output. On replay the
//!   same output is made again, with the same [`OutputId`](crate::OutputId), and not handed
//!   out twice. The outputs of one group of work are matched in order, so
//!   an output made in a group of a `mergeMap` names its group,
//!   and the groups need not finish in the same order again.
//! - [`Entry::Through`] and [`Entry::Stopped`]: for a stream operator that gives
//!   its items again, how far a call got, how many outputs made from the
//!   items after that were handed out, and how it stopped. They take the
//!   place of `Item`, `Ended` and the `Emitted` lines of those outputs. On
//!   replay the operator is called with `resume_from` set to the position, and
//!   the run hands out again, silently, as many outputs as were handed.
//!   If the call had stopped, the run reads the operator only up to the items it
//!   gave before and then gives the saved end.
//! - [`Entry::Timed`]: what a `timeout` decided for one run of the
//!   operators it applies to. Each run gets a scope of its own, numbered in the order
//!   values reach the `timeout`, such as `pipe[2].timeout@0`, and the keys
//!   of the calls it applies to start with it, such as
//!   `pipe[2].timeout@0/pipe[0]#0`. On replay, [`Timing::TimedOut`] fails
//!   the item at once without running the operators it applies to, and
//!   [`Timing::InTime`] runs them with no limit. With no decision saved,
//!   the wait starts again from nothing.
//!
//! # Output ids
//!
//! Every output a pipe hands out carries an [`OutputId`](crate::OutputId): the input item
//! it belongs to (none in a journal, which covers the whole run), the
//! group of work it was made in, and how many outputs that group made
//! before it. A replay makes the same outputs in the same order within
//! each group, so it makes each id again. No line saves an id: the lines
//! that say where outputs stand are all a replay needs, so a journal or
//! bookmark is the same size it was, and one saved before outputs had ids
//! reads and resumes as it always did, giving the ids a run of today
//! gives. An output handed out in the window between being handed out
//! and its line being saved comes again after a restart, with its id, so a
//! caller drops a repeat by id.
//!
//! # Keeping it small
//!
//! [`Journal::record`] and [`Bookmark::record`] keep what a run hands out.
//! For most entries they append. An [`Entry::Through`] replaces the one
//! before it for the same call, and drops every entry whose key belongs to
//! an item of that call it passed: the keys of item 7 of `pipe[1]#0` start
//! `pipe[1]#0@7/`. It keeps the `Emitted` line of an output `last` lifted
//! out of groups from such an item: that output is never made again, but
//! it took a place in the order of its group's outputs, and the outputs
//! after it keep their ids by it. There is one such line for each time a
//! `last` gives a value. [`Journal::push`] appends and drops nothing.
//!
//! # Groups of work
//!
//! A `mergeMap` runs its values side by side, so their calls and
//! outputs can interleave in another order after a restart. In a saved
//! run each value that enters it starts a group, numbered in the order the
//! values enter, and the keys of the calls made for it start with the
//! group's scope, such as `pipe[1].mergeMap@3/pipe[1].mergeMap.do[0]#0`. Its
//! outputs are saved with `within` set to the scope, such as
//! `pipe[1].mergeMap@3/`. A replay matches each group's own calls and outputs in
//! that group's own order. A vine with no `mergeMap` saves exactly the
//! lines it did before groups existed.
//!
//! # Which input item a line belongs to
//!
//! Input items are numbered from 1, counted from the first item ever. A
//! line belongs to the input item the call's value came from. A value made
//! from several items, such as a batch from `bufferCount`, belongs to the last
//! of them. A batch ties its items together: the position never stops
//! between them, so a restart feeds the whole batch again.
//!
//! # The cut line
//!
//! [`Progress::Through`] with `n` means nothing in the run still holds
//! anything derived from items 1 to `n`: every call they caused has
//! finished, every output they caused was handed out, and no operator holds
//! them. Their lines will never be read again. [`Bookmark::record`] drops
//! them. A bookmark that still has some, because the caller saved the
//! position and crashed before it dropped them, is fine: a run ignores
//! lines at or before the position.
//!
//! Items can finish out of order under a `mergeMap`: item 5 can
//! be done while item 3 is still running. [`Progress::Done`] with 5 says
//! so. The position stays at 2, and [`Bookmark::record`] keeps 5 as done
//! and drops its lines. After a restart the caller feeds items from 3 on,
//! as always, and the run skips item 5 without running it again. So what
//! a bookmark keeps is the lines of the items still in flight, plus the
//! numbers of the items done past the position, kept as runs of numbers.

mod entry;
mod log;
mod position;

pub(crate) use entry::item_of_stream;
pub use entry::{Ending, Entry, EntryError, Timing};
pub use log::{Event, Journal, JournalError};
pub use position::{Bookmark, BookmarkError, Line, LineError, Progress};
