#![cfg(feature = "yaml")]

//! backup: collect batches, encrypt each, store it at an outside provider.
//! Time is tokio's paused clock: it moves only when every task is idle.
mod support;

use std::sync::Arc;
use std::time::Duration;

use support::{channel, items, records, register_backup, vine_source, KeyStore, Provider, Saver};
use vine_lang::{
    json, CancellationToken, Checkpoint, Registry, RunError, Stop, StreamReport, Vine,
};

fn setup_with(keys: &[&str]) -> (Arc<Provider>, Arc<Vine>) {
    let provider = Provider::new();
    let mut reg = Registry::new();
    register_backup(&mut reg, &KeyStore::new(keys), &provider);
    let vine = reg.load(&vine_source("backup")).expect("backup loads");
    (provider, Arc::new(vine))
}

fn setup() -> (Arc<Provider>, Arc<Vine>) {
    setup_with(&["backup"])
}

fn name(first: u64, last: u64) -> String {
    format!("backup_provider/{first:06}-{last:06}")
}

/// Let every task run until it waits; with paused time this moves the
/// clock by one millisecond only.
async fn idle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

async fn run_to_end(
    vine: &Vine,
    from: Checkpoint,
    values: Vec<serde_json::Value>,
) -> (StreamReport, Saver) {
    let mut saver = Saver::default();
    let report = vine
        .run_stream(items(values), from, &mut saver, CancellationToken::new())
        .await;
    (report, saver)
}

/// Start a run over a hand-fed source on its own task.
fn spawn(
    vine: &Arc<Vine>,
    from: Checkpoint,
    token: CancellationToken,
) -> (
    tokio::sync::mpsc::UnboundedSender<serde_json::Value>,
    Saver,
    tokio::task::JoinHandle<StreamReport>,
) {
    let (tx, source) = channel();
    let saver = Saver::default();
    let (v, mut s) = (vine.clone(), saver.clone());
    let handle = tokio::spawn(async move { v.run_stream(source, from, &mut s, token).await });
    (tx, saver, handle)
}

#[tokio::test(start_paused = true)]
async fn batches_by_count_and_the_position_moves_only_when_a_batch_is_stored() {
    let (provider, vine) = setup();
    let (tx, saver, handle) = spawn(&vine, Checkpoint::start(), CancellationToken::new());

    for r in records(1, 499) {
        tx.send(r).unwrap();
    }
    idle().await;
    // 499 records sit in collect's buffer: nothing stored, nothing consumed.
    assert!(provider.names().is_empty());
    assert!(saver.checkpoints().is_empty());

    tx.send(records(500, 500).remove(0)).unwrap();
    idle().await;
    assert_eq!(provider.names(), vec![name(1, 500)]);
    assert_eq!(saver.positions(), vec![500]);
    assert_eq!(
        saver.payloads(),
        vec![json!({ "name": name(1, 500), "first": 1, "last": 500, "count": 500 })]
    );

    for r in records(501, 999) {
        tx.send(r).unwrap();
    }
    idle().await;
    assert_eq!(saver.positions(), vec![500]);
    tx.send(records(1000, 1000).remove(0)).unwrap();
    idle().await;
    assert_eq!(provider.names(), vec![name(1, 500), name(501, 1000)]);
    assert_eq!(saver.positions(), vec![500, 1000]);

    drop(tx);
    let report = handle.await.unwrap();
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(report.checkpoint.position, 1000);
    assert_eq!(provider.seqs(), (1..=1000).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn batches_by_time_limit_counted_from_the_first_record_of_the_batch() {
    let (provider, vine) = setup();
    let (tx, saver, handle) = spawn(&vine, Checkpoint::start(), CancellationToken::new());

    // Nothing in the buffer: no timer, however long we wait.
    tokio::time::sleep(Duration::from_secs(600)).await;
    assert!(provider.names().is_empty());

    for r in records(1, 3) {
        tx.send(r).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(59)).await;
    assert!(provider.names().is_empty(), "not yet 60s");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(provider.names(), vec![name(1, 3)]);
    assert_eq!(saver.positions(), vec![3]);

    // The next batch's clock starts at its own first record.
    tokio::time::sleep(Duration::from_secs(45)).await;
    for r in records(4, 5) {
        tx.send(r).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(provider.names(), vec![name(1, 3)], "only 30s into batch 2");
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(provider.names(), vec![name(1, 3), name(4, 5)]);

    drop(tx);
    assert_eq!(handle.await.unwrap().stop, Stop::Ended);
    assert_eq!(saver.positions(), vec![3, 5]);
}

#[tokio::test(start_paused = true)]
async fn the_count_wins_when_it_comes_before_the_time_limit() {
    let (provider, vine) = setup();
    let (tx, saver, handle) = spawn(&vine, Checkpoint::start(), CancellationToken::new());
    for r in records(1, 500) {
        tx.send(r).unwrap();
    }
    idle().await;
    assert_eq!(provider.names(), vec![name(1, 500)]);
    // The timer went with the batch: no empty batch 60s later.
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(provider.names(), vec![name(1, 500)]);
    drop(tx);
    handle.await.unwrap();
    assert_eq!(saver.positions(), vec![500]);
}

#[tokio::test(start_paused = true)]
async fn a_partial_batch_is_flushed_when_the_stream_ends() {
    let (provider, vine) = setup();
    let (report, saver) = run_to_end(&vine, Checkpoint::start(), records(1, 750)).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(provider.names(), vec![name(1, 500), name(501, 750)]);
    assert_eq!(saver.positions(), vec![500, 750]);
    assert_eq!(report.checkpoint.position, 750);
    assert_eq!(provider.seqs(), (1..=750).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_crash_with_records_in_the_buffer_then_resume_loses_and_repeats_nothing() {
    let (provider, vine) = setup();
    let (tx, saver, handle) = spawn(&vine, Checkpoint::start(), CancellationToken::new());
    for r in records(1, 750) {
        tx.send(r).unwrap();
    }
    idle().await;
    assert_eq!(provider.names(), vec![name(1, 500)]);
    // Crash: the run is dropped mid-stream, 250 records in its buffer.
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    drop(tx);

    // Resume from what was saved; the caller's log replays from there.
    let from = saver.last();
    assert_eq!(
        from,
        Checkpoint {
            position: 500,
            memory: Default::default()
        }
    );
    let replay = records(from.position as i64 + 1, 1250);
    let (report, saver2) = run_to_end(&vine, from, replay).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(
        provider.names(),
        vec![name(1, 500), name(501, 1000), name(1001, 1250)]
    );
    // Every record stored exactly once, and no batch was stored twice.
    assert_eq!(provider.seqs(), (1..=1250).collect::<Vec<_>>());
    assert_eq!(provider.puts(), provider.names());
    assert_eq!(saver2.positions(), vec![1000, 1250]);
}

#[tokio::test(start_paused = true)]
async fn a_pause_with_records_in_the_buffer_does_not_consume_them() {
    let (provider, vine) = setup();
    let token = CancellationToken::new();
    let (tx, saver, handle) = spawn(&vine, Checkpoint::start(), token.clone());
    for r in records(1, 600) {
        tx.send(r).unwrap();
    }
    idle().await;
    token.cancel();
    let report = handle.await.unwrap();
    assert_eq!(report.stop, Stop::Paused);
    // The 100 buffered records were not flushed and are not consumed.
    assert_eq!(report.checkpoint.position, 500);
    assert_eq!(provider.names(), vec![name(1, 500)]);
    assert_eq!(saver.last(), report.checkpoint);

    let (resumed, _) = run_to_end(&vine, report.checkpoint, records(501, 600)).await;
    assert_eq!(resumed.stop, Stop::Ended);
    assert_eq!(provider.names(), vec![name(1, 500), name(501, 600)]);
    assert_eq!(provider.seqs(), (1..=600).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_store_failure_leaves_the_position_before_that_batch_and_resume_retries_exactly_it() {
    let (provider, vine) = setup();
    let (report, saver) = run_to_end(&vine, Checkpoint::start(), records(1, 500)).await;
    assert_eq!(report.checkpoint.position, 500);
    assert_eq!(saver.positions(), vec![500]);

    // The provider is down for the next batch.
    provider.fail_next(1);
    let (report, saver) = run_to_end(&vine, report.checkpoint, records(501, 1100)).await;
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::Unhandled {
            path: "steps[2]".into(),
            operator: "store".into(),
            code: "unavailable".into(),
            message: "provider is down".into(),
        })
    );
    assert_eq!(report.checkpoint.position, 500);
    assert!(saver.checkpoints().is_empty(), "the position never moved");
    assert_eq!(provider.names(), vec![name(1, 500)]);

    let (resumed, saver) = run_to_end(&vine, report.checkpoint, records(501, 1100)).await;
    assert_eq!(resumed.stop, Stop::Ended);
    // Exactly the failed batch was retried first, then the rest.
    assert_eq!(
        provider.puts(),
        vec![name(1, 500), name(501, 1000), name(1001, 1100)]
    );
    assert_eq!(saver.positions(), vec![1000, 1100]);
    assert_eq!(provider.seqs(), (1..=1100).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_store_whose_answer_was_lost_is_repeated_on_resume_without_a_duplicate() {
    let (provider, vine) = setup();
    let (report, _) = run_to_end(&vine, Checkpoint::start(), records(1, 500)).await;
    // Batch 2 reaches the provider, but the answer never comes back.
    provider.lose_ack_next(1);
    let (report, _) = run_to_end(&vine, report.checkpoint, records(501, 1000)).await;
    assert!(matches!(
        report.stop,
        Stop::Failed(RunError::Unhandled { ref code, .. }) if code == "timeout"
    ));
    assert_eq!(report.checkpoint.position, 500);

    let (resumed, _) = run_to_end(&vine, report.checkpoint, records(501, 1000)).await;
    assert_eq!(resumed.stop, Stop::Ended);
    // Stored twice under the same name: one copy at the provider.
    assert_eq!(
        provider.puts(),
        vec![name(1, 500), name(501, 1000), name(501, 1000)]
    );
    assert_eq!(provider.names(), vec![name(1, 500), name(501, 1000)]);
    assert_eq!(provider.seqs(), (1..=1000).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_failure_flushing_the_last_partial_batch_is_retried_by_replaying_from_the_position() {
    let (provider, vine) = setup();
    provider.fail_next(1);
    let (report, _) = run_to_end(&vine, Checkpoint::start(), records(1, 20)).await;
    assert!(matches!(
        report.stop,
        Stop::Failed(RunError::Unhandled { .. })
    ));
    assert_eq!(report.checkpoint.position, 0);
    // The source had closed; the caller replays from the position.
    let (resumed, _) = run_to_end(&vine, report.checkpoint, records(1, 20)).await;
    assert_eq!(resumed.stop, Stop::Ended);
    assert_eq!(provider.names(), vec![name(1, 20)]);
}

#[tokio::test(start_paused = true)]
async fn an_encrypt_failure_stops_the_run_and_stores_nothing() {
    let (provider, vine) = setup_with(&["other"]);
    let (report, _) = run_to_end(&vine, Checkpoint::start(), records(1, 500)).await;
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::Unhandled {
            path: "steps[1]".into(),
            operator: "encrypt".into(),
            code: "no_key".into(),
            message: "no key named backup".into(),
        })
    );
    assert_eq!(report.checkpoint.position, 0);
    assert!(provider.names().is_empty());
}

#[tokio::test]
async fn reach_is_the_key_store_and_the_provider() {
    let (_provider, vine) = setup();
    let reach: Vec<&str> = vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["backup.write", "keys.read"]);
}
