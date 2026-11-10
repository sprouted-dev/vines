mod support;

use std::sync::Arc;

use support::{register_facts, vine_source, Facts};
use vine_lang::{json, CancellationToken, Registry, RunError, Value, Vine};

fn setup() -> (Arc<Facts>, Vine) {
    let facts = Facts::new();
    let mut reg = Registry::new();
    register_facts(&mut reg, &facts);
    let vine = reg.load(&vine_source("claim")).expect("claim loads");
    (facts, vine)
}

fn input(item: &str, version: i64) -> Value {
    json!({ "item": item, "claimant": "ana", "version": version })
}

#[tokio::test]
async fn claims_an_open_unheld_item() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open" }));
    let out = vine.run(input("an-1", 1)).await.unwrap();
    assert_eq!(out.name, "claimed");
    assert_eq!(out.payload, json!({ "item": "an-1", "claimant": "ana" }));
    assert_eq!(facts.view("an-1").unwrap()["holder"], "ana");
    assert_eq!(facts.view("an-1").unwrap()["version"], 2);
    assert_eq!(
        facts.records("an-1"),
        vec![json!({ "type": "claimed", "claimant": "ana" })]
    );
}

async fn refused(vine: &Vine, inp: Value, gate: &str) {
    let out = vine.run(inp).await.unwrap();
    assert_eq!(out.name, "refused");
    assert_eq!(out.payload, json!({ "gate": gate }));
}

#[tokio::test]
async fn refuses_a_missing_item() {
    let (facts, vine) = setup();
    refused(&vine, input("an-404", 0), "missing").await;
    assert!(facts.records("an-404").is_empty());
}

#[tokio::test]
async fn refuses_an_item_that_is_not_open() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "closed" }));
    refused(&vine, input("an-1", 1), "state").await;
    assert!(facts.records("an-1").is_empty());
}

#[tokio::test]
async fn refuses_an_item_already_held() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open", "holder": "bo" }));
    refused(&vine, input("an-1", 1), "held").await;
    assert!(facts.records("an-1").is_empty());
    assert_eq!(facts.view("an-1").unwrap()["holder"], "bo");
}

#[tokio::test]
async fn refuses_on_a_version_conflict_and_writes_nothing() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open" }));
    refused(&vine, input("an-1", 7), "version").await;
    assert!(facts.records("an-1").is_empty());
    assert!(facts.view("an-1").unwrap().get("holder").is_none());
}

#[tokio::test]
async fn a_second_claim_is_refused_as_held() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open" }));
    assert_eq!(vine.run(input("an-1", 1)).await.unwrap().name, "claimed");
    let again = json!({ "item": "an-1", "claimant": "bo", "version": 2 });
    let out = vine.run(again).await.unwrap();
    assert_eq!(
        (out.name.as_str(), out.payload),
        ("refused", json!({ "gate": "held" }))
    );
}

#[tokio::test]
async fn a_read_error_is_a_run_error_not_an_output() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open" }));
    facts.fail_reads();
    let err = vine.run(input("an-1", 1)).await.unwrap_err();
    assert_eq!(
        err,
        RunError::Unhandled {
            path: "steps[0]".into(),
            operator: "read".into(),
            code: "unavailable".into(),
            message: "fact store is down".into(),
        }
    );
}

#[tokio::test]
async fn a_write_error_other_than_conflict_is_not_caught() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open" }));
    facts.fail_writes_after(0);
    let err = vine.run(input("an-1", 1)).await.unwrap_err();
    match err {
        RunError::Unhandled {
            path,
            operator,
            code,
            ..
        } => {
            assert_eq!(path, "steps[4].try.do[0]");
            assert_eq!(operator, "write");
            assert_eq!(code, "unavailable");
        }
        other => panic!("expected an unhandled error, got {other:?}"),
    }
}

#[tokio::test]
async fn refuses_input_that_does_not_fit() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open" }));
    let cases = [
        (
            json!({ "item": "an-1", "claimant": "ana", "version": "1" }),
            "field `version` should be int, found text \"1\"",
        ),
        (
            json!({ "item": "an-1", "version": 1 }),
            "missing required field `claimant` (text)",
        ),
        (
            json!({ "item": "", "claimant": "ana", "version": 1 }),
            "field `item` should be id, found empty text",
        ),
        (
            json!({ "item": "an-1", "claimant": "ana", "version": 1, "force": true }),
            "unexpected field `force` (not in the shape)",
        ),
        (json!(["an-1"]), "expected a map, found a list"),
    ];
    for (inp, problem) in cases {
        assert_eq!(
            vine.run(inp).await.unwrap_err(),
            RunError::InputMisfit(problem.into())
        );
    }
    assert!(facts.records("an-1").is_empty());
}

#[tokio::test]
async fn cancelled_before_it_starts_writes_nothing() {
    let (facts, vine) = setup();
    facts.put("an-1", json!({ "state": "open" }));
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = vine.run_with(input("an-1", 1), cancel).await.unwrap_err();
    assert_eq!(
        err,
        RunError::Cancelled {
            path: "steps[0]".into()
        }
    );
    assert!(facts.records("an-1").is_empty());
}

#[tokio::test]
async fn reach_is_exactly_the_fact_store() {
    let (_facts, vine) = setup();
    let reach: Vec<&str> = vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["facts.read", "facts.write"]);
}

#[tokio::test]
async fn a_once_vine_refuses_a_stream_run() {
    let (_facts, vine) = setup();
    let report = vine
        .run_stream(futures::stream::iter(vec![]), CancellationToken::new())
        .await;
    assert_eq!(report.consumed, 0);
    assert!(matches!(report.stopped, Some(RunError::WrongMode { .. })));
}
