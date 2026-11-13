#![cfg(feature = "yaml")]

//! The registry from the outside, and the core operators.
use vine_lang::{
    json, LoadError, OpError, OperatorSpec, Registry, RegistryError, Shape, Ty, Value,
};

/// A provided operator written here, outside the library, with only the
/// public API.
fn register_shout(reg: &mut Registry) {
    reg.register(
        OperatorSpec::new("shout")
            .settings(Shape::new().required("text", Ty::Text))
            .returns(Shape::new().required("text", Ty::Text))
            .touches(["speaker"]),
        |s, _cx| async move {
            let text = s["text"].as_str().unwrap();
            if text.is_empty() {
                return Err(OpError::new("empty", "nothing to shout"));
            }
            Ok(json!({ "text": text.to_uppercase() }))
        },
    )
    .unwrap();
}

const SHOUT: &str = "
vine: shout_it
input: { text: text }
outputs:
  said: { text: text }
  quiet: { why: text }
steps:
  - try:
      do:
        - shout: { text: $in.text }
          as: loud
        - out: { name: said, with: { text: $loud.text } }
      catch:
        empty:
          - out: { name: quiet, with: { why: $error.message } }
";

#[tokio::test]
async fn a_provided_operator_registered_outside_the_library_runs() {
    let mut reg = Registry::new();
    register_shout(&mut reg);
    let vine = reg.load(SHOUT).unwrap();
    let out = vine.run(json!({ "text": "hi" })).await.unwrap();
    assert_eq!(
        (out.name.as_str(), out.payload),
        ("said", json!({ "text": "HI" }))
    );
    let out = vine.run(json!({ "text": "" })).await.unwrap();
    assert_eq!(
        (out.name.as_str(), out.payload),
        ("quiet", json!({ "why": "nothing to shout" }))
    );
    assert_eq!(vine.reach().iter().collect::<Vec<_>>(), vec!["speaker"]);
}

#[test]
fn the_same_vine_without_the_operator_registered_is_refused() {
    assert_eq!(
        Registry::new().load(SHOUT).unwrap_err(),
        LoadError::UnknownOperator {
            path: "steps[0].try.do[0]".into(),
            name: "shout".into()
        }
    );
}

#[test]
fn registration_refuses_bad_names_reserved_names_duplicates_and_bad_touches() {
    let mut reg = Registry::new();
    let noop = |_s: Value, _cx| async { Ok(Value::Null) };
    assert_eq!(
        reg.register(OperatorSpec::new("Read"), noop),
        Err(RegistryError::BadName("Read".into()))
    );
    for reserved in ["guard", "switch", "try", "out", "include", "import", "vine"] {
        assert_eq!(
            reg.register(OperatorSpec::new(reserved), noop),
            Err(RegistryError::Reserved(reserved.into()))
        );
    }
    assert_eq!(
        reg.register(OperatorSpec::new("set"), noop),
        Err(RegistryError::Duplicate("set".into()))
    );
    reg.register(OperatorSpec::new("ping"), noop).unwrap();
    assert_eq!(
        reg.register(OperatorSpec::new("ping"), noop),
        Err(RegistryError::Duplicate("ping".into()))
    );
    assert_eq!(
        reg.register(OperatorSpec::new("pong").touches(["Facts.Read"]), noop),
        Err(RegistryError::BadTouch {
            operator: "pong".into(),
            touch: "Facts.Read".into()
        })
    );
}

#[test]
fn core_operators_are_listed_and_touch_nothing() {
    let specs = Registry::new().specs();
    let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["guard", "out", "select", "set", "switch", "try"]);
    assert!(specs.iter().all(|s| s.touches.is_empty()));
}

#[tokio::test]
async fn a_vine_of_only_core_operators_has_empty_reach() {
    let vine = Registry::new()
        .load("vine: echo\ninput: { x: int }\noutputs: { done: { x: int } }\nsteps:\n  - out: { name: done, with: { x: $in.x } }\n")
        .unwrap();
    assert!(vine.reach().is_empty());
    assert_eq!(
        vine.run(json!({ "x": 4 })).await.unwrap().payload,
        json!({ "x": 4 })
    );
}

const CORE: &str = r#"
vine: core
input: { kind: text, flag: bool?, n: int?, tag: text? }
outputs:
  result: { label: text, note: any, raw: text, nested: object }
  other: { seen: any }
steps:
  - set:
      label: $in.kind
      nested: { inner: [$in.n, "lit"] }
    as: shaped
  - guard: { when: "$in.tag == 'stop'", out: other, with: { seen: $in.tag } }
  - guard: { when: $in.flag, out: other, with: { seen: flag } }
  - switch:
      on: $in.n
      cases:
        "1":
          - out: { name: other, with: { seen: one } }
        "true":
          - out: { name: other, with: { seen: never } }
  - switch:
      on: $in.kind
      cases:
        a:
          - out:
              name: result
              with: { label: $shaped.label, note: $shaped.missing.deeper, raw: "$$in.kind", nested: $shaped.nested }
      else:
        - out: { name: other, with: { seen: $in.kind } }
"#;

async fn core(input: Value) -> (String, Value) {
    let out = Registry::new()
        .load(CORE)
        .unwrap()
        .run(input)
        .await
        .unwrap();
    (out.name, out.payload)
}

#[tokio::test]
async fn set_shapes_values_and_missing_fields_read_as_null() {
    assert_eq!(
        core(json!({ "kind": "a", "n": 2 })).await,
        (
            "result".into(),
            json!({
                "label": "a",
                "note": null,
                "raw": "$in.kind",
                "nested": { "inner": [2, "lit"] }
            })
        )
    );
}

#[tokio::test]
async fn guards_compare_quoted_text_and_test_truthiness() {
    assert_eq!(
        core(json!({ "kind": "a", "tag": "stop" })).await,
        ("other".into(), json!({ "seen": "stop" }))
    );
    assert_eq!(
        core(json!({ "kind": "a", "flag": true })).await,
        ("other".into(), json!({ "seen": "flag" }))
    );
    // false and null (absent) are not truthy
    assert_eq!(
        core(json!({ "kind": "a", "flag": false })).await.0,
        "result"
    );
}

#[tokio::test]
async fn switch_matches_integers_as_written_falls_through_without_else_and_uses_else() {
    assert_eq!(
        core(json!({ "kind": "a", "n": 1 })).await,
        ("other".into(), json!({ "seen": "one" }))
    );
    // n absent: no case matches, no else, so the run goes on.
    assert_eq!(core(json!({ "kind": "a" })).await.0, "result");
    assert_eq!(
        core(json!({ "kind": "zzz" })).await,
        ("other".into(), json!({ "seen": "zzz" }))
    );
}

#[tokio::test]
async fn nested_try_rethrows_uncaught_codes_to_the_outer_catch_and_runs_both_finallys() {
    use std::sync::{Arc, Mutex};
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut reg = Registry::new();
    let l = log.clone();
    reg.register(
        OperatorSpec::new("note")
            .settings(Shape::new().required("m", Ty::Text))
            .touches(["log"]),
        move |s, _| {
            let l = l.clone();
            async move {
                let m = s["m"].as_str().unwrap().to_owned();
                l.lock().unwrap().push(m.clone());
                if m == "boom" {
                    return Err(OpError::new("boom", "it blew"));
                }
                Ok(Value::Null)
            }
        },
    )
    .unwrap();
    let vine = reg
        .load(
            "
vine: nest
input: {}
outputs: { caught: { by: text } }
steps:
  - try:
      do:
        - try:
            do:
              - note: { m: boom }
            catch:
              other:
                - out: { name: caught, with: { by: inner } }
            finally:
              - note: { m: inner_finally }
        - out: { name: caught, with: { by: nobody } }
      catch:
        boom:
          - out: { name: caught, with: { by: $error.code } }
      finally:
        - note: { m: outer_finally }
",
        )
        .unwrap();
    let out = vine.run(json!({})).await.unwrap();
    assert_eq!(out.payload, json!({ "by": "boom" }));
    assert_eq!(
        *log.lock().unwrap(),
        ["boom", "inner_finally", "outer_finally"]
    );
}
