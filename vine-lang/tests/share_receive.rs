#![cfg(feature = "yaml")]

//! share_receive: a stream pipe of one `each`. Verify each envelope,
//! reject bad ones as `rejected`, write good ones, output `accepted`.
mod support;

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::stream::Stream;
use support::{
    envelope, items, register_facts, register_keys, vine_source, Facts, Journal, Keys, Saver,
};
use vine_lang::{
    json, CancellationToken, Checkpoint, Output, Registry, RunError, Stop, StreamReport, Value,
    Vine,
};

fn setup_with(keys: Arc<Keys>) -> (Arc<Facts>, Vine) {
    let facts = Facts::new();
    let mut reg = Registry::new();
    register_facts(&mut reg, &facts);
    register_keys(&mut reg, &keys);
    let vine = reg.load(&vine_source("share_receive")).expect("loads");
    (facts, vine)
}

fn setup() -> (Arc<Facts>, Vine) {
    setup_with(Keys::new(&["peer-a"]))
}

fn item(env: Value) -> Value {
    json!({ "envelope": env })
}

fn good(ids: &[&str]) -> Vec<Value> {
    ids.iter()
        .map(|id| item(envelope("peer-a", id, "an-1")))
        .collect()
}

fn accepted(id: &str) -> Output {
    Output {
        name: "accepted".into(),
        payload: json!({ "event": id }),
    }
}

fn rejected(reason: &str) -> Output {
    Output {
        name: "rejected".into(),
        payload: json!({ "reason": reason }),
    }
}

async fn run<S: Stream<Item = Value> + Unpin>(
    vine: &Vine,
    source: S,
    from: Checkpoint,
    saver: &mut Saver,
    token: CancellationToken,
) -> StreamReport {
    vine.run_stream(source, from, saver, token).await
}

async fn run_all(vine: &Vine, values: Vec<Value>) -> (StreamReport, Saver) {
    let mut saver = Saver::default();
    let report = run(
        vine,
        items(values),
        Checkpoint::start(),
        &mut saver,
        CancellationToken::new(),
    )
    .await;
    (report, saver)
}

#[tokio::test]
async fn accepts_and_writes_every_good_envelope_and_the_position_follows_each_item() {
    let (facts, vine) = setup();
    let (report, saver) = run_all(&vine, good(&["e1", "e2", "e3"])).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(
        report.checkpoint,
        Checkpoint {
            position: 3,
            memory: Default::default()
        }
    );
    assert_eq!(
        saver.outputs(),
        vec![accepted("e1"), accepted("e2"), accepted("e3")]
    );
    // One checkpoint per item, each after that item's output; no memory.
    assert_eq!(saver.positions(), vec![1, 2, 3]);
    assert!(saver.checkpoints().iter().all(|c| c.memory.is_empty()));
    let ids: Vec<Value> = facts
        .records("an-1")
        .iter()
        .map(|r| r["id"].clone())
        .collect();
    assert_eq!(ids, vec![json!("e1"), json!("e2"), json!("e3")]);
}

#[tokio::test]
async fn rejects_each_bad_envelope_with_a_reason_and_writes_only_good_ones() {
    let (facts, vine) = setup();
    let mut forged = envelope("peer-a", "e3", "an-1");
    forged["signature"] = json!("peer-a/other");
    let values = vec![
        item(envelope("peer-a", "e1", "an-1")),
        item(envelope("stranger", "e2", "an-1")),
        item(forged),
        item(json!({ "signer": "peer-a", "event": { "id": "e4" } })),
        item(envelope("peer-a", "e5", "an-1")),
    ];
    let (report, saver) = run_all(&vine, values).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(
        saver.outputs(),
        vec![
            accepted("e1"),
            rejected("unknown_signer"),
            rejected("bad_signature"),
            rejected("malformed"),
            accepted("e5"),
        ]
    );
    // A rejection is a result: the position moves past it.
    assert_eq!(saver.positions(), vec![1, 2, 3, 4, 5]);
    assert_eq!(facts.records("an-1").len(), 2);
}

#[tokio::test]
async fn items_go_through_one_at_a_time_in_order() {
    let journal: Journal = Arc::default();
    let (facts, vine) = setup_with(Keys::with_journal(&["peer-a"], &journal));
    facts.set_journal(&journal);
    let (report, _saver) = run_all(&vine, good(&["e1", "e2", "e3"])).await;
    assert_eq!(report.stop, Stop::Ended);
    // Each item finishes its `each` steps before the next one starts.
    assert_eq!(
        *journal.lock().unwrap(),
        [
            "verify \"e1\"",
            "write \"e1\"",
            "verify \"e2\"",
            "write \"e2\"",
            "verify \"e3\"",
            "write \"e3\""
        ]
    );
}

#[tokio::test]
async fn no_two_items_are_ever_in_flight_at_once() {
    let journal: Journal = Arc::default();
    let keys = Keys::with_journal(&["peer-a"], &journal);
    let (_facts, vine) = setup_with(keys.clone());
    let (report, _saver) = run_all(&vine, good(&["e1", "e2", "e3", "e4"])).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(keys.most_at_once(), 1);
}

#[tokio::test]
async fn a_store_error_stops_the_stream_with_the_position_at_the_failed_item() {
    let (facts, vine) = setup();
    facts.fail_writes_after(1);
    let all = good(&["e1", "e2", "e3"]);
    let (report, saver) = run_all(&vine, all.clone()).await;
    assert_eq!(saver.outputs(), vec![accepted("e1")]);
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::Unhandled {
            path: "steps[0].each[2]".into(),
            operator: "write".into(),
            code: "unavailable".into(),
            message: "fact store is down".into(),
        })
    );
    // Consumed through e1; e2 failed, so it is not consumed.
    assert_eq!(report.checkpoint.position, 1);
    assert_eq!(saver.last(), report.checkpoint);

    // Resume from what was saved: e2 is retried, nothing lost or repeated.
    facts.heal();
    let from = saver.last();
    let rest = all[from.position as usize..].to_vec();
    let mut saver2 = Saver::default();
    let resumed = run(
        &vine,
        items(rest),
        from,
        &mut saver2,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(resumed.stop, Stop::Ended);
    assert_eq!(saver2.outputs(), vec![accepted("e2"), accepted("e3")]);
    assert_eq!(saver2.positions(), vec![2, 3]);
    let ids: Vec<Value> = facts
        .records("an-1")
        .iter()
        .map(|r| r["id"].clone())
        .collect();
    assert_eq!(ids, vec![json!("e1"), json!("e2"), json!("e3")]);
}

#[tokio::test]
async fn an_item_that_does_not_fit_the_input_shape_stops_the_stream_before_it() {
    let (facts, vine) = setup();
    let values = vec![
        item(envelope("peer-a", "e1", "an-1")),
        json!({ "envelope": "not a map" }),
        item(envelope("peer-a", "e3", "an-1")),
    ];
    let (report, saver) = run_all(&vine, values).await;
    assert_eq!(report.checkpoint.position, 1);
    assert_eq!(
        report.stop,
        Stop::Failed(RunError::InputMisfit(
            "field `envelope` should be object, found text \"not a map\"".into()
        ))
    );
    assert_eq!(saver.outputs(), vec![accepted("e1")]);
    assert_eq!(facts.records("an-1").len(), 1);
}

/// Yields items and cancels the token as it hands out item `cancel_at`.
struct CancelAt {
    items: Vec<Value>,
    next: usize,
    cancel_at: usize,
    token: CancellationToken,
}

impl Stream for CancelAt {
    type Item = Value;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Value>> {
        let i = self.next;
        if i == self.cancel_at {
            self.token.cancel();
        }
        self.next += 1;
        Poll::Ready(self.items.get(i).cloned())
    }
}

#[tokio::test]
async fn a_pause_mid_stream_does_not_consume_the_item_in_flight() {
    let (facts, vine) = setup();
    let token = CancellationToken::new();
    let source = CancelAt {
        items: good(&["e1", "e2", "e3"]),
        next: 0,
        cancel_at: 1,
        token: token.clone(),
    };
    let mut saver = Saver::default();
    let report = run(&vine, source, Checkpoint::start(), &mut saver, token).await;
    assert_eq!(report.stop, Stop::Paused);
    assert_eq!(report.checkpoint.position, 1);
    assert_eq!(saver.outputs(), vec![accepted("e1")]);
    // e2 was pulled, but its `each` saw the pause before its first step.
    assert_eq!(facts.records("an-1").len(), 1);
}

#[tokio::test]
async fn a_pause_before_the_stream_starts_pulls_nothing() {
    let (_facts, vine) = setup();
    let token = CancellationToken::new();
    token.cancel();
    let pulled = Arc::new(Mutex::new(0));
    let p = pulled.clone();
    let source = futures::stream::poll_fn(move |_| {
        *p.lock().unwrap() += 1;
        Poll::Ready(Some(item(envelope("peer-a", "e1", "an-1"))))
    });
    let mut saver = Saver::default();
    let report = run(&vine, source, Checkpoint::start(), &mut saver, token).await;
    assert_eq!(report.stop, Stop::Paused);
    assert_eq!(report.checkpoint.position, 0);
    assert_eq!(*pulled.lock().unwrap(), 0);
}

#[tokio::test]
async fn an_empty_stream_ends_having_consumed_nothing() {
    let (_facts, vine) = setup();
    let (report, saver) = run_all(&vine, vec![]).await;
    assert_eq!(report.stop, Stop::Ended);
    assert_eq!(report.checkpoint.position, 0);
    assert!(saver.checkpoints().is_empty());
}

#[tokio::test]
async fn a_stream_vine_refuses_a_once_run() {
    let (_facts, vine) = setup();
    assert_eq!(
        vine.run(item(envelope("peer-a", "e1", "an-1")))
            .await
            .unwrap_err(),
        RunError::WrongMode {
            vine: "share_receive".into(),
            mode: "stream"
        }
    );
}

#[tokio::test]
async fn redelivery_writes_a_duplicate_because_nothing_here_remembers_ids() {
    // Finding, pinned as behaviour: dedupe would need an operator with
    // kept memory of seen ids (or an idempotent write). The vine has none.
    let (facts, vine) = setup();
    let e = item(envelope("peer-a", "e1", "an-1"));
    let (report, _saver) = run_all(&vine, vec![e.clone(), e]).await;
    assert_eq!(report.checkpoint.position, 2);
    assert_eq!(facts.records("an-1").len(), 2);
}

#[tokio::test]
async fn reach_counts_the_operators_inside_each() {
    let (_facts, vine) = setup();
    let reach: Vec<&str> = vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["facts.write", "keys.read"]);
}
