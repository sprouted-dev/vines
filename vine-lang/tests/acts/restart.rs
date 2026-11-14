//! A caller that runs a vine from a saved position, kills it at chosen
//! points, and restarts it from what it saved, through JSON each time.

use futures::{stream, StreamExt};
use vine_lang::{Bookmark, Pipe, Progress, RunError, Value};

/// Everything a caller saw and kept over one or more runs.
pub struct Caller {
    /// The bookmark, as a store would hold it.
    pub saved: Value,
    pub outs: Vec<(String, Value)>,
    /// Every position the runs reported, in order.
    pub throughs: Vec<u64>,
    /// The most lines the bookmark held at once.
    pub most: usize,
}

impl Caller {
    pub fn new() -> Caller {
        Caller::from(Bookmark::new())
    }

    pub fn bookmark(&self) -> Bookmark {
        Bookmark::from_json(&self.saved).unwrap()
    }

    pub fn position(&self) -> u64 {
        self.bookmark().position()
    }
}

impl From<Bookmark> for Caller {
    fn from(bookmark: Bookmark) -> Caller {
        Caller {
            saved: bookmark.to_json(),
            outs: Vec::new(),
            throughs: Vec::new(),
            most: 0,
        }
    }
}

/// How one run ended.
#[derive(Debug)]
pub enum End {
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
pub async fn run(vine: &Pipe, input: &[Value], caller: &mut Caller, kill: Option<usize>) -> End {
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
            Progress::Save(_) => {}
        }
        bookmark.record(&progress);
        caller.saved = bookmark.to_json();
        caller.most = caller.most.max(bookmark.len());
        seen += 1;
        if Some(seen) == kill {
            return End::Killed {
                after_output: matches!(progress, Progress::Output(_)),
            };
        }
    }
    End::Ended
}

/// Run with kills after each count of events in `kills`, then to the end.
/// An output handed out right before a kill comes again first in the next
/// run, since its line was never saved; that one repeat is checked and
/// taken out of `outs`. Gives how many repeats there were.
pub async fn run_with_kills(
    vine: impl Fn() -> Pipe,
    input: &[Value],
    caller: &mut Caller,
    kills: &[usize],
) -> usize {
    let mut repeats = 0;
    let mut repeat_of: Option<usize> = None;
    let plan = kills.iter().map(|&k| Some(k)).chain([None]);
    for kill in plan {
        let mark = caller.outs.len();
        let end = run(&vine(), input, caller, kill).await;
        if let Some(at) = repeat_of.take() {
            assert_eq!(
                caller.outs.get(mark),
                caller.outs.get(at),
                "the output handed out before the kill comes again first"
            );
            caller.outs.remove(mark);
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
pub async fn events_in(vine: &Pipe, input: &[Value]) -> usize {
    let run = vine.run_from(stream::iter(input.to_vec()), Bookmark::new());
    run.count().await
}

/// The world's log without the lines of a stream act called again after
/// a restart, such as `feed.pages 3 again from 2`: what is left is what
/// started.
pub fn started(log: Vec<String>) -> Vec<String> {
    log.into_iter().filter(|l| !l.contains(" again ")).collect()
}

/// Positions only ever move forward.
pub fn assert_forward(throughs: &[u64]) {
    assert!(
        throughs.windows(2).all(|w| w[0] < w[1]),
        "the position went back or stood still: {throughs:?}"
    );
}
