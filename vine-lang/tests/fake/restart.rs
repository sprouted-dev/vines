//! A caller that runs a vine from a saved position, or from a journal,
//! kills it at chosen points, and restarts it from what it saved, through
//! JSON each time.

use futures::{stream, StreamExt};
use vine_lang::{Bookmark, Entry, Event, Journal, Pipe, Progress, RunError, Value};

/// Everything a caller saw and kept over one or more runs.
pub(crate) struct Caller {
    /// The bookmark, as a store would hold it.
    pub saved: Value,
    pub outs: Vec<(String, Value)>,
    /// Every position the runs reported, in order.
    pub throughs: Vec<u64>,
    /// Every item the runs reported done past the position, in order.
    pub dones: Vec<u64>,
    /// The most lines the bookmark held at once.
    pub most: usize,
    /// The largest the saved bookmark was, in bytes of JSON.
    pub largest: usize,
}

impl Caller {
    pub(crate) fn new() -> Caller {
        Caller::from(Bookmark::new())
    }

    pub(crate) fn bookmark(&self) -> Bookmark {
        Bookmark::from_json(&self.saved).unwrap()
    }

    pub(crate) fn position(&self) -> u64 {
        self.bookmark().position()
    }
}

impl From<Bookmark> for Caller {
    fn from(bookmark: Bookmark) -> Caller {
        Caller {
            saved: bookmark.to_json(),
            outs: Vec::new(),
            throughs: Vec::new(),
            dones: Vec::new(),
            most: 0,
            largest: 0,
        }
    }
}

/// How one run ended.
#[derive(Debug)]
pub(crate) enum End {
    /// The input ran out and the run ended by itself.
    Ended,
    /// The caller stopped reading. `after_output` says the last thing it
    /// read was an output, so the line that records it was never saved.
    Killed {
        after_output: bool,
    },
    Failed(RunError),
}

/// Run `vine` once, as a caller would: read the bookmark back from JSON,
/// feed the input after its position, and after every event record it
/// and save the bookmark. Stop after `kill` events.
pub(crate) async fn run(
    vine: &Pipe,
    input: &[Value],
    caller: &mut Caller,
    kill: Option<usize>,
) -> End {
    let mut bookmark = caller.bookmark();
    let from = usize::try_from(bookmark.position()).unwrap();
    let feed = input[from.min(input.len())..].to_vec();
    let mut run = vine.run_from(stream::iter(feed), bookmark.clone());
    let mut seen = 0;
    while let Some(progress) = run.next().await {
        let progress = match progress {
            Ok(p) => p,
            Err(e) => return End::Failed(e),
        };
        match &progress {
            Progress::Output(o) => caller.outs.push((o.name.clone(), o.payload.clone())),
            Progress::Through(n) => caller.throughs.push(*n),
            Progress::Done(n) => caller.dones.push(*n),
            Progress::Save(_) => {}
        }
        bookmark.record(&progress);
        caller.saved = bookmark.to_json();
        caller.most = caller.most.max(bookmark.len());
        caller.largest = caller.largest.max(caller.saved.to_string().len());
        seen += 1;
        if Some(seen) == kill {
            return End::Killed {
                after_output: matches!(progress, Progress::Output(_)),
            };
        }
    }
    End::Ended
}

/// Where an output handed out right before a kill comes again after the
/// restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Repeat {
    /// First: the run makes its outputs in one order.
    First,
    /// Somewhere after the restart: under a `mergeMap`, other
    /// items can hand out new outputs before the one whose line was lost
    /// is made again.
    Later,
}

/// Take the repeat of `outs[at]` out of what came after `mark`, where
/// `repeat` says it comes.
fn take_repeat(outs: &mut Vec<(String, Value)>, mark: usize, at: usize, repeat: Repeat) {
    let again = match repeat {
        Repeat::First => Some(mark).filter(|&m| outs.get(m) == outs.get(at)),
        Repeat::Later => (mark..outs.len()).find(|&i| outs[i] == outs[at]),
    };
    let again = again.unwrap_or_else(|| {
        panic!(
            "the output {:?} handed out before the kill comes again {repeat:?}: {:?}",
            outs[at],
            &outs[mark..]
        )
    });
    outs.remove(again);
}

/// Run with kills after each count of events in `kills`, then to the end.
/// An output handed out right before a kill comes again first in the next
/// run, since its line was never saved; that one repeat is checked and
/// taken out of `outs`. Gives how many repeats there were.
pub(crate) async fn run_with_kills(
    vine: impl Fn() -> Pipe,
    input: &[Value],
    caller: &mut Caller,
    kills: &[usize],
) -> usize {
    run_with_kills_where(vine, input, caller, kills, Repeat::First).await
}

/// [`run_with_kills`], where the repeat comes where `repeat` says.
pub(crate) async fn run_with_kills_where(
    vine: impl Fn() -> Pipe,
    input: &[Value],
    caller: &mut Caller,
    kills: &[usize],
    repeat: Repeat,
) -> usize {
    let mut repeats = 0;
    let mut repeat_of: Option<usize> = None;
    let plan = kills.iter().map(|&k| Some(k)).chain([None]);
    for kill in plan {
        let mark = caller.outs.len();
        let end = run(&vine(), input, caller, kill).await;
        if let Some(at) = repeat_of.take() {
            take_repeat(&mut caller.outs, mark, at, repeat);
            repeats += 1;
        }
        match end {
            End::Killed { after_output } => {
                if after_output {
                    repeat_of = Some(caller.outs.len() - 1);
                }
            }
            End::Ended => return repeats,
            End::Failed(e) => panic!("kills {kills:?}: {e}"),
        }
    }
    repeats
}

/// How many events an uninterrupted run gives.
pub(crate) async fn events_in(vine: &Pipe, input: &[Value]) -> usize {
    let run = vine.run_from(stream::iter(input.to_vec()), Bookmark::new());
    run.count().await
}

/// The world's log without the lines of a stream operator called again after
/// a restart, such as `feed.pages 3 again from 2`: what is left is what
/// started.
pub(crate) fn started(log: Vec<String>) -> Vec<String> {
    log.into_iter().filter(|l| !l.contains(" again ")).collect()
}

/// Positions only ever move forward.
pub(crate) fn assert_forward(throughs: &[u64]) {
    assert!(
        throughs.windows(2).all(|w| w[0] < w[1]),
        "the position went back or stood still: {throughs:?}"
    );
}

/// Everything a caller of `run_saved` saw and kept over one or more runs.
#[derive(Default)]
pub(crate) struct Keeper {
    /// The journal, as a store would hold it.
    pub saved: Value,
    pub outs: Vec<(String, Value)>,
    /// How many operator results and failures the runs handed out and the
    /// keeper saved, counted before the journal drops any.
    pub results: usize,
    /// The most entries the journal held at once.
    pub most: usize,
    /// The largest the saved journal was, in bytes of JSON.
    pub largest: usize,
}

impl Keeper {
    pub(crate) fn new() -> Keeper {
        Keeper {
            saved: Journal::new().to_json(),
            ..Keeper::default()
        }
    }

    pub(crate) fn journal(&self) -> Journal {
        Journal::from_json(&self.saved).unwrap()
    }
}

/// Run `vine` once with `run_saved`, as a caller would: read the journal
/// back from JSON, feed the whole input, and after every event record it
/// and save the journal. Stop after `kill` events.
pub(crate) async fn run_saved(
    vine: &Pipe,
    input: &[Value],
    keeper: &mut Keeper,
    kill: Option<usize>,
) -> End {
    let mut journal = keeper.journal();
    let mut run = vine.run_saved(stream::iter(input.to_vec()), journal.clone());
    let mut seen = 0;
    while let Some(event) = run.next().await {
        let event = match event {
            Ok(e) => e,
            Err(e) => return End::Failed(e),
        };
        match &event {
            Event::Output(o) => keeper.outs.push((o.name.clone(), o.payload.clone())),
            Event::Save(Entry::Done { .. } | Entry::Failed { .. }) => keeper.results += 1,
            Event::Save(_) => {}
        }
        journal.record(&event);
        keeper.saved = journal.to_json();
        keeper.most = keeper.most.max(journal.len());
        keeper.largest = keeper.largest.max(keeper.saved.to_string().len());
        seen += 1;
        if Some(seen) == kill {
            return End::Killed {
                after_output: matches!(event, Event::Output(_)),
            };
        }
    }
    End::Ended
}

/// [`run_saved`] with kills after each count of events in `kills`, then
/// to the end, as [`run_with_kills`] does for a bookmark. Gives how many
/// outputs came twice because the kill fell between an output and the
/// entry that records it.
pub(crate) async fn run_saved_with_kills(
    vine: impl Fn() -> Pipe,
    input: &[Value],
    keeper: &mut Keeper,
    kills: &[usize],
) -> usize {
    run_saved_with_kills_where(vine, input, keeper, kills, Repeat::First).await
}

/// [`run_saved_with_kills`], where the repeat comes where `repeat` says.
pub(crate) async fn run_saved_with_kills_where(
    vine: impl Fn() -> Pipe,
    input: &[Value],
    keeper: &mut Keeper,
    kills: &[usize],
    repeat: Repeat,
) -> usize {
    let mut repeats = 0;
    let mut repeat_of: Option<usize> = None;
    let plan = kills.iter().map(|&k| Some(k)).chain([None]);
    for kill in plan {
        let mark = keeper.outs.len();
        let end = run_saved(&vine(), input, keeper, kill).await;
        if let Some(at) = repeat_of.take() {
            take_repeat(&mut keeper.outs, mark, at, repeat);
            repeats += 1;
        }
        match end {
            End::Killed { after_output } => {
                if after_output {
                    repeat_of = Some(keeper.outs.len() - 1);
                }
            }
            End::Ended => return repeats,
            End::Failed(e) => panic!("kills {kills:?}: {e}"),
        }
    }
    repeats
}

/// How many events an uninterrupted `run_saved` gives.
pub(crate) async fn events_saved(vine: &Pipe, input: &[Value]) -> usize {
    let run = vine.run_saved(stream::iter(input.to_vec()), Journal::new());
    run.count().await
}
