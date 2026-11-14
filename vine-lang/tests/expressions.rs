//! The expression form inside a pipe: what a template resolves to, what a
//! `filter` condition holds for, which `branch` case a value matches, and
//! which expressions are refused before a vine runs. Nothing here uses
//! YAML, and this file passes with the feature off:
//!
//!     cargo test --no-default-features --test expressions

use vine_lang::pipe::{branch, emit, emit_with, filter, op, pipe, PipeDef, Stage};
use vine_lang::{json, LoadError, Registry, Shape, Ty, Value};

/// A vine over items with optional fields, with one open output, `out`.
fn over(stages: impl IntoIterator<Item = Stage>) -> PipeDef {
    pipe("v")
        .input(
            Shape::new()
                .required("kind", Ty::Text)
                .optional("n", Ty::Int)
                .optional("flag", Ty::Bool)
                .optional("tag", Ty::Text)
                .optional("nested", Ty::Object),
        )
        .output("out", Shape::open())
        .stages(stages)
}

/// What the vine emits for one item, as values.
async fn emitted(def: PipeDef, item: Value) -> Vec<Value> {
    let vine = Registry::new().build_pipe(def).unwrap();
    let out = vine.run_one(item).await.unwrap();
    out.into_iter().map(|o| o.payload).collect()
}

#[tokio::test]
async fn set_shapes_values_and_a_missing_field_reads_as_null() {
    let def = over([
        op(
            "set",
            json!({
                "label": "$item.kind",
                "inner": ["$item.n", "lit"],
                "note": "$item.nested.missing.deeper",
            }),
        ),
        emit("out"),
    ]);
    assert_eq!(
        emitted(def, json!({ "kind": "a", "n": 2, "nested": {} })).await,
        [json!({ "label": "a", "inner": [2, "lit"], "note": null })]
    );
}

#[tokio::test]
async fn text_that_starts_with_two_dollars_is_literal_with_one_removed() {
    let def = over([emit_with("out", json!({ "raw": "$$item.kind" }))]);
    assert_eq!(
        emitted(def, json!({ "kind": "a" })).await,
        [json!({ "raw": "$item.kind" })]
    );
}

#[tokio::test]
async fn filter_compares_quoted_text() {
    let def = || over([filter("$item.tag == 'stop'"), emit("out")]);
    assert_eq!(
        emitted(def(), json!({ "kind": "a", "tag": "stop" }))
            .await
            .len(),
        1
    );
    assert!(emitted(def(), json!({ "kind": "a", "tag": "go" }))
        .await
        .is_empty());
}

#[tokio::test]
async fn one_value_alone_holds_unless_it_is_false_or_null() {
    let def = || over([filter("$item.flag"), emit("out")]);
    assert_eq!(
        emitted(def(), json!({ "kind": "a", "flag": true }))
            .await
            .len(),
        1
    );
    assert!(emitted(def(), json!({ "kind": "a", "flag": false }))
        .await
        .is_empty());
    // Absent reads as null.
    assert!(emitted(def(), json!({ "kind": "a" })).await.is_empty());
}

#[tokio::test]
async fn a_branch_case_matches_an_integer_as_written_and_an_absent_value_matches_none() {
    let def = || {
        over([
            branch(
                "$item.n",
                [("1", [emit_with("out", json!({ "seen": "one" }))])],
                None,
            ),
            emit_with("out", json!({ "seen": "other" })),
        ])
    };
    assert_eq!(
        emitted(def(), json!({ "kind": "a", "n": 1 })).await,
        [json!({ "seen": "one" })]
    );
    assert_eq!(
        emitted(def(), json!({ "kind": "a" })).await,
        [json!({ "seen": "other" })]
    );
}

#[tokio::test]
async fn a_branch_case_matches_a_bool_as_written_and_else_takes_the_rest() {
    let def = || {
        over([branch(
            "$item.flag",
            [("true", [emit_with("out", json!({ "seen": "yes" }))])],
            Some(vec![emit_with("out", json!({ "seen": "else" }))]),
        )])
    };
    assert_eq!(
        emitted(def(), json!({ "kind": "a", "flag": true })).await,
        [json!({ "seen": "yes" })]
    );
    assert_eq!(
        emitted(def(), json!({ "kind": "a", "flag": false })).await,
        [json!({ "seen": "else" })]
    );
}

fn refused(def: PipeDef) -> LoadError {
    Registry::new().build_pipe(def).unwrap_err()
}

#[test]
fn a_bare_word_in_a_condition_is_refused_and_the_error_says_how_to_write_it() {
    let err = refused(over([filter("$item.tag != open"), emit("out")]));
    assert_eq!(
        err,
        LoadError::Malformed {
            path: "pipe[0].filter".into(),
            problem: "bare word `open`: quote text (\"open\"), or write a reference ($item.field), an integer, true, false or null".into()
        }
    );
}

#[test]
fn a_condition_is_one_value_or_two_joined_by_equals_or_not_equals() {
    for when in [
        "$item.kind == \"a\" == \"b\"",
        "$item.n > 3",
        "$item.flag && $item.tag",
        "$item.tag == 'open",
    ] {
        let err = refused(over([filter(when), emit("out")]));
        assert!(
            matches!(err, LoadError::Malformed { ref path, .. } if path == "pipe[0].filter"),
            "for {when}: {err:?}"
        );
    }
}

#[test]
fn a_reference_that_is_not_lower_snake_case_is_refused() {
    let err = refused(over([emit_with("out", json!({ "x": "$Item.kind" }))]));
    assert!(
        matches!(err, LoadError::Malformed { ref path, ref problem }
            if path == "pipe[0].emit.with" && problem.contains("not a reference")),
        "{err:?}"
    );
}
