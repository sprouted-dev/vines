//! session_summary: collect 50 lines or 30 seconds, then a provided stream
//! operator that keeps "summary so far" as must-keep memory.
mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use support::{channel, items, lines, register_summarise, vine_source, Saver};
use vine_lang::{
    json, CancellationToken, Checkpoint, Registry, RunError, Stop, StreamReport, Value, Vine,
};

fn setup() -> Arc<Vine> {
    let mut reg = Registry::new();
    register_summarise(&mut reg);
    Arc::new(reg.load(&vine_source("session_summary")).expect("loads"))
}

async fn idle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

async fn run_to_end(vine: &Vine, from: Checkpoint, values: Vec<Value>) -> (StreamReport, Saver) {
    let mut saver = Saver::default();
    let report = vine
        .run_stream(items(values), from, &mut saver, CancellationToken::new())
        .await;
    (report, saver)
}

fn spawn(
    vine: &Arc<Vine>,
    from: Checkpoint,
    token: CancellationToken,
) -> (
    tokio::sync::mpsc::UnboundedSender<Value>,
    Saver,
    tokio::task::JoinHandle<StreamReport>,
) {
    let (tx, source) = channel();
    let saver = Saver::default();
    let (v, mut s) = (vine.clone(), saver.clone());
    let handle = tokio::spawn(async move { v.run_stream(source, from, &mut s, token).await });
    (tx, saver, handle)
}

fn counts(saver: &Saver) -> Vec<(i64, bool)> {
    saver
        .payloads()
        .iter()
        .map(|p| (p["lines"].as_i64().unwrap(), p["final"].as_bool().unwrap()))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn an_updated_summary_comes_every_fifty_lines() {
    let vine = setup();
    let (tx, saver, handle) = spawn(&vine, Checkpoint::start(), CancellationToken::new());
    for l in lines("s", 1, 49) {
        tx.send(l).unwrap();
    }
    idle().await;
    assert!(saver.outputs().is_empty());
    tx.send(lines("s", 50, 50).remove(0)).unwrap();
    idle().await;
    assert_eq!(counts(&saver), vec![(50, false)]);
    assert_eq!(saver.outputs()[0].name, "summary");
    // The checkpoint carries the summary so far, at the matching position.
    let cp = saver.last();
    assert_eq!(cp.position, 50);
    assert_eq!(cp.memory["steps[1]"]["lines"], json!(50));
    assert_eq!(cp.memory["steps[1]"]["last"], json!("s line 50"));

    for l in lines("s", 51, 100) {
        tx.send(l).unwrap();
    }
    idle().await;
    assert_eq!(counts(&saver), vec![(50, false), (100, false)]);
    drop(tx);
    handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_updated_summary_comes_thirty_seconds_after_a_batch_starts() {
    let vine = setup();
    let (tx, saver, handle) = spawn(&vine, Checkpoint::start(), CancellationToken::new());
    for l in lines("s", 1, 10) {
        tx.send(l).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(29)).await;
    assert!(saver.outputs().is_empty());
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(counts(&saver), vec![(10, false)]);
    assert_eq!(saver.last().position, 10);
    drop(tx);
    let report = handle.await.unwrap();
    assert_eq!(report.stop, Stop::Ended);
    // Nothing was buffered at the end: only the final summary follows.
    assert_eq!(counts(&saver), vec![(10, false), (10, true)]);
}

#[tokio::test(start_paused = true)]
async fn the_end_of_the_stream_flushes_the_partial_batch_and_emits_a_final_summary() {
    let vine = setup();
    let (report, saver) = run_to_end(&vine, Checkpoint::start(), lines("s", 1, 120)).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(
        counts(&saver),
        vec![(50, false), (100, false), (120, false), (120, true)]
    );
    let last = saver.payloads().pop().unwrap();
    assert!(last["text"]
        .as_str()
        .unwrap()
        .starts_with("120 lines, last \"s line 120\""));
}

#[tokio::test(start_paused = true)]
async fn the_end_drops_memory() {
    let vine = setup();
    let (report, saver) = run_to_end(&vine, Checkpoint::start(), lines("s", 1, 60)).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(
        report.checkpoint,
        Checkpoint {
            position: 60,
            memory: BTreeMap::new()
        }
    );
    assert_eq!(saver.last(), report.checkpoint);
    // Memory is gone, so the ended stream cannot be picked up again.
    let (again, _) = run_to_end(&vine, report.checkpoint, lines("s", 61, 70)).await;
    assert!(
        matches!(again.stop, Stop::Failed(RunError::ResumeMisfit(ref m)) if m.contains("must be kept"))
    );
}

#[tokio::test(start_paused = true)]
async fn pause_and_resume_keeps_the_summary_so_far_and_matches_an_uninterrupted_run() {
    let vine = setup();
    let (_, whole) = run_to_end(&vine, Checkpoint::start(), lines("s", 1, 120)).await;

    let token = CancellationToken::new();
    let (tx, first, handle) = spawn(&vine, Checkpoint::start(), token.clone());
    for l in lines("s", 1, 75) {
        tx.send(l).unwrap();
    }
    idle().await;
    token.cancel();
    let paused = handle.await.unwrap();
    assert_eq!(paused.stop, Stop::Paused);
    // 25 lines were in collect's buffer: not consumed, not summarised.
    assert_eq!(paused.checkpoint.position, 50);
    assert_eq!(paused.checkpoint.memory["steps[1]"]["lines"], json!(50));

    let from = first.last();
    assert_eq!(from, paused.checkpoint);
    let (resumed, second) = run_to_end(&vine, from, lines("s", 51, 120)).await;
    assert_eq!(resumed.stop, Stop::Ended);
    let mut joined = first.outputs();
    joined.extend(second.outputs());
    assert_eq!(joined, whole.outputs());
}

#[tokio::test(start_paused = true)]
async fn a_crash_and_resume_from_saved_memory_matches_an_uninterrupted_run() {
    let vine = setup();
    let (_, whole) = run_to_end(&vine, Checkpoint::start(), lines("s", 1, 130)).await;

    let (tx, first, handle) = spawn(&vine, Checkpoint::start(), CancellationToken::new());
    for l in lines("s", 1, 110) {
        tx.send(l).unwrap();
    }
    idle().await;
    handle.abort();
    let _ = handle.await;
    let from = first.last();
    assert_eq!(from.position, 100);

    let replay = lines("s", from.position + 1, 130);
    let (_, second) = run_to_end(&vine, from, replay).await;
    let mut joined = first.outputs();
    joined.extend(second.outputs());
    assert_eq!(joined, whole.outputs());
}

#[tokio::test(start_paused = true)]
async fn resuming_without_the_kept_memory_is_refused() {
    let vine = setup();
    let (report, saver) = run_to_end(
        &vine,
        Checkpoint {
            position: 50,
            memory: BTreeMap::new(),
        },
        lines("s", 51, 60),
    )
    .await;
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::ResumeMisfit(
            "no saved memory for `steps[1]` (`summarise`), which must be kept; resuming at position 50 without it would lose what it remembered".into()
        ))
    );
    assert!(saver.outputs().is_empty());
}

#[tokio::test(start_paused = true)]
async fn resuming_with_memory_that_does_not_fit_is_refused() {
    let vine = setup();
    let bad = Checkpoint {
        position: 50,
        memory: BTreeMap::from([("steps[1]".to_string(), json!({ "lines": "many" }))]),
    };
    let (report, _) = run_to_end(&vine, bad, vec![]).await;
    assert!(matches!(
        report.stop,
        Stop::Failed(RunError::ResumeMisfit(ref m)) if m.starts_with("memory for `steps[1]` (`summarise`) does not fit")
    ));

    let stray = Checkpoint {
        position: 0,
        memory: BTreeMap::from([("steps[0]".to_string(), json!([]))]),
    };
    let (report, _) = run_to_end(&vine, stray, vec![]).await;
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::ResumeMisfit(
            "saved memory names `steps[0]` (`collect`), which keeps no memory".into()
        ))
    );

    let unknown = Checkpoint {
        position: 0,
        memory: BTreeMap::from([("steps[7]".to_string(), json!({}))]),
    };
    let (report, _) = run_to_end(&vine, unknown, vec![]).await;
    assert!(matches!(
        report.stop,
        Stop::Failed(RunError::ResumeMisfit(ref m)) if m.contains("`steps[7]`")
    ));
}

#[tokio::test]
async fn reach_is_the_model() {
    let vine = setup();
    let reach: Vec<&str> = vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["model"]);
}

// ---------------------------------------------------------------- one run per stream

/// A mixed feed: lines tagged by session, and the end of a session.
enum Feed {
    Line(&'static str, Value),
    End(&'static str),
}

struct PerSession {
    runs: BTreeMap<&'static str, (StreamReport, Saver)>,
    /// For each session, in the order they ended: which sessions were
    /// still open when its run had emitted its final summary.
    ended: Vec<(&'static str, Vec<&'static str>)>,
}

/// One independent run of `vine` per session key, each with its own
/// memory, each ending when its own stream ends. Lives in the tests: who
/// routes a feed to runs is the host's business, not the vine's.
async fn run_per_session(vine: &Vine, feed: Vec<Feed>) -> PerSession {
    let mut keys: Vec<&'static str> = Vec::new();
    for f in &feed {
        let (Feed::Line(k, _) | Feed::End(k)) = f;
        if !keys.contains(k) {
            keys.push(k);
        }
    }
    let mut senders = BTreeMap::new();
    let mut savers = BTreeMap::new();
    let mut runs = Vec::new();
    for k in &keys {
        let (tx, source) = channel();
        let saver = Saver::default();
        senders.insert(*k, tx);
        savers.insert(*k, saver.clone());
        let mut s = saver.clone();
        let k = *k;
        runs.push(async move {
            let report = vine
                .run_stream(
                    source,
                    Checkpoint::start(),
                    &mut s,
                    CancellationToken::new(),
                )
                .await;
            (k, report)
        });
    }
    let mut ended = Vec::new();
    let feeder = {
        let savers = savers.clone();
        let ended = &mut ended;
        async move {
            for f in feed {
                match f {
                    Feed::Line(k, v) => senders[k].send(v).unwrap(),
                    Feed::End(k) => {
                        senders.remove(k);
                        // This session finishes on its own while others go on.
                        while !savers[k]
                            .payloads()
                            .iter()
                            .any(|p| p["final"] == json!(true))
                        {
                            tokio::task::yield_now().await;
                        }
                        ended.push((k, senders.keys().copied().collect()));
                    }
                }
                tokio::task::yield_now().await;
            }
        }
    };
    let ((), reports) = futures::join!(feeder, join_all(runs));
    PerSession {
        runs: reports
            .into_iter()
            .map(|(k, r)| (k, (r, savers[k].clone())))
            .collect(),
        ended,
    }
}

#[tokio::test(start_paused = true)]
async fn one_run_per_session_and_sessions_do_not_share_memory() {
    let vine = setup();
    let a = lines("a", 1, 60);
    let b = lines("b", 1, 55);
    let mut feed = Vec::new();
    // Interleave a and b line by line for the first 30 of each.
    for i in 0..30 {
        feed.push(Feed::Line("a", a[i].clone()));
        feed.push(Feed::Line("b", b[i].clone()));
    }
    for l in &a[30..] {
        feed.push(Feed::Line("a", l.clone()));
    }
    feed.push(Feed::End("a"));
    for l in &b[30..] {
        feed.push(Feed::Line("b", l.clone()));
    }
    feed.push(Feed::End("b"));

    let PerSession { runs, ended } = run_per_session(&vine, feed).await;
    // a's run finished while b's stream was still open.
    assert_eq!(ended, vec![("a", vec!["b"]), ("b", vec![])]);

    // Each session's summaries are what it gives run alone.
    let (_, a_alone) = run_to_end(&vine, Checkpoint::start(), a).await;
    let (_, b_alone) = run_to_end(&vine, Checkpoint::start(), b).await;
    let (a_report, a_saver) = &runs["a"];
    let (b_report, b_saver) = &runs["b"];
    assert_eq!(a_report.stop, Stop::Ended);
    assert_eq!(b_report.stop, Stop::Ended);
    assert_eq!(a_saver.outputs(), a_alone.outputs());
    assert_eq!(b_saver.outputs(), b_alone.outputs());
    assert_eq!(counts(a_saver), vec![(50, false), (60, false), (60, true)]);
    assert_eq!(counts(b_saver), vec![(50, false), (55, false), (55, true)]);
    // Neither saw the other's lines.
    for p in a_saver.payloads() {
        assert!(p["text"].as_str().unwrap().contains("\"a line"));
    }
    for p in b_saver.payloads() {
        assert!(p["text"].as_str().unwrap().contains("\"b line"));
    }
    // Each run's own positions and memory.
    assert_eq!(a_report.checkpoint.position, 60);
    assert_eq!(b_report.checkpoint.position, 55);
    let a_mid = &a_saver.checkpoints()[0];
    assert_eq!(
        (a_mid.position, &a_mid.memory["steps[1]"]["lines"]),
        (50, &json!(50))
    );
}
