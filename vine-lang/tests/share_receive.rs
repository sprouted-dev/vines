mod support;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{self, Stream};
use support::{envelope, register_facts, register_keys, vine_source, Facts, Keys};
use vine_lang::{json, CancellationToken, Output, Registry, RunError, Value, Vine};

fn setup() -> (Arc<Facts>, Vine) {
    let facts = Facts::new();
    let keys = Keys::new(&["peer-a"]);
    let mut reg = Registry::new();
    register_facts(&mut reg, &facts);
    register_keys(&mut reg, &keys);
    let vine = reg.load(&vine_source("share_receive")).expect("loads");
    (facts, vine)
}

fn item(env: Value) -> Value {
    json!({ "envelope": env })
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

#[tokio::test]
async fn accepts_and_writes_every_good_envelope() {
    let (facts, vine) = setup();
    let items: Vec<Value> = ["e1", "e2", "e3"]
        .iter()
        .map(|id| item(envelope("peer-a", id, "an-1")))
        .collect();
    let report = vine
        .run_stream(stream::iter(items), CancellationToken::new())
        .await;
    assert_eq!(report.stopped, None);
    assert_eq!(report.consumed, 3);
    assert_eq!(
        report.outputs,
        vec![accepted("e1"), accepted("e2"), accepted("e3")]
    );
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
    let items = vec![
        item(envelope("peer-a", "e1", "an-1")),
        item(envelope("stranger", "e2", "an-1")),
        item(forged),
        item(json!({ "signer": "peer-a", "event": { "id": "e4" } })),
        item(envelope("peer-a", "e5", "an-1")),
    ];
    let report = vine
        .run_stream(stream::iter(items), CancellationToken::new())
        .await;
    assert_eq!(report.stopped, None);
    assert_eq!(report.consumed, 5);
    assert_eq!(
        report.outputs,
        vec![
            accepted("e1"),
            rejected("unknown_signer"),
            rejected("bad_signature"),
            rejected("malformed"),
            accepted("e5"),
        ]
    );
    assert_eq!(facts.records("an-1").len(), 2);
}

#[tokio::test]
async fn a_store_error_stops_the_stream_and_the_position_points_at_the_failed_item() {
    let (facts, vine) = setup();
    facts.fail_writes_after(1);
    let items: Vec<Value> = ["e1", "e2", "e3"]
        .iter()
        .map(|id| item(envelope("peer-a", id, "an-1")))
        .collect();
    let report = vine
        .run_stream(stream::iter(items.clone()), CancellationToken::new())
        .await;
    assert_eq!(report.consumed, 1);
    assert_eq!(report.outputs, vec![accepted("e1")]);
    assert_eq!(
        report.stopped,
        Some(RunError::Unhandled {
            path: "steps[2]".into(),
            operator: "write".into(),
            code: "unavailable".into(),
            message: "fact store is down".into(),
        })
    );

    // The caller saved the position; resuming from it loses nothing and
    // repeats nothing.
    facts.heal();
    let rest = items[report.consumed..].to_vec();
    let resumed = vine
        .run_stream(stream::iter(rest), CancellationToken::new())
        .await;
    assert_eq!(resumed.stopped, None);
    assert_eq!(resumed.outputs, vec![accepted("e2"), accepted("e3")]);
    assert_eq!(facts.records("an-1").len(), 3);
}

#[tokio::test]
async fn an_item_that_does_not_fit_the_input_shape_stops_the_stream() {
    let (facts, vine) = setup();
    let items = vec![
        item(envelope("peer-a", "e1", "an-1")),
        json!({ "envelope": "not a map" }),
        item(envelope("peer-a", "e3", "an-1")),
    ];
    let report = vine
        .run_stream(stream::iter(items), CancellationToken::new())
        .await;
    assert_eq!(report.consumed, 1);
    assert_eq!(
        report.stopped,
        Some(RunError::InputMisfit(
            "field `envelope` should be object, found text \"not a map\"".into()
        ))
    );
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
async fn cancellation_mid_stream_does_not_consume_the_item_in_flight() {
    let (facts, vine) = setup();
    let token = CancellationToken::new();
    let items = CancelAt {
        items: ["e1", "e2", "e3"]
            .iter()
            .map(|id| item(envelope("peer-a", id, "an-1")))
            .collect(),
        next: 0,
        cancel_at: 1,
        token: token.clone(),
    };
    let report = vine.run_stream(items, token).await;
    assert_eq!(report.consumed, 1);
    assert_eq!(report.outputs, vec![accepted("e1")]);
    assert_eq!(
        report.stopped,
        Some(RunError::Cancelled {
            path: "steps[0]".into()
        })
    );
    assert_eq!(facts.records("an-1").len(), 1);
}

#[tokio::test]
async fn cancellation_before_the_stream_starts_pulls_nothing() {
    let (_facts, vine) = setup();
    let token = CancellationToken::new();
    token.cancel();
    let report = vine
        .run_stream(
            stream::iter(vec![item(envelope("peer-a", "e1", "an-1"))]),
            token,
        )
        .await;
    assert_eq!(report.consumed, 0);
    assert_eq!(
        report.stopped,
        Some(RunError::Cancelled {
            path: "stream".into()
        })
    );
}

#[tokio::test]
async fn an_empty_stream_consumes_nothing() {
    let (_facts, vine) = setup();
    let report = vine
        .run_stream(stream::iter(Vec::<Value>::new()), CancellationToken::new())
        .await;
    assert_eq!((report.consumed, report.stopped), (0, None));
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
async fn redelivery_writes_a_duplicate_because_the_vine_holds_no_state() {
    // Finding, pinned as behaviour: dedupe needs memory the vine may not have.
    let (facts, vine) = setup();
    let e = item(envelope("peer-a", "e1", "an-1"));
    let report = vine
        .run_stream(stream::iter(vec![e.clone(), e]), CancellationToken::new())
        .await;
    assert_eq!(report.consumed, 2);
    assert_eq!(facts.records("an-1").len(), 2);
}

#[tokio::test]
async fn reach_is_facts_write_and_keys_read() {
    let (_facts, vine) = setup();
    let reach: Vec<&str> = vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["facts.write", "keys.read"]);
}
