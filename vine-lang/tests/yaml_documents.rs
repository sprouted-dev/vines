//! What the YAML layer refuses about a document as a whole, before it
//! reads any operator, and what it allows.
#![cfg(test)]
#![cfg(feature = "yaml")]

use vine_lang::{json, LoadError, Registry};

fn refused(yaml: &str) -> LoadError {
    match Registry::new().load_pipe(yaml) {
        Ok(_) => panic!("expected a refusal for:\n{yaml}"),
        Err(e) => e,
    }
}

#[test]
fn text_that_is_not_yaml_is_refused() {
    assert!(matches!(refused("vine: [unclosed"), LoadError::Yaml(_)));
}

#[test]
fn a_document_that_is_not_a_map_or_misses_a_part_is_refused_where_it_is_wrong() {
    let cases = [
        ("- a list", "vine"),
        (
            "input: {}\noutputs: { done: {} }\npipe: [ { emit: done } ]\n",
            "vine",
        ),
        (
            "vine: T\ninput: {}\noutputs: { done: {} }\npipe: [ { emit: done } ]\n",
            "vine",
        ),
        (
            "vine: t\noutputs: { done: {} }\npipe: [ { emit: done } ]\n",
            "vine",
        ),
        (
            "vine: t\ninput: { job: uuid }\noutputs: { done: {} }\npipe: [ { emit: done } ]\n",
            "input",
        ),
        (
            "vine: t\ninput: {}\noutputs: {}\npipe: [ { emit: done } ]\n",
            "outputs",
        ),
        ("vine: t\ninput: {}\noutputs: { done: {} }\n", "vine"),
        (
            "vine: t\ninput: {}\noutputs: { done: {} }\npipe: []\n",
            "pipe",
        ),
    ];
    for (yaml, path) in cases {
        match refused(yaml) {
            LoadError::Malformed { path: p, .. } => assert_eq!(p, path, "for {yaml}"),
            other => panic!("expected Malformed for {yaml}, got {other:?}"),
        }
    }
}

#[test]
fn a_duplicate_key_is_refused_instead_of_keeping_the_last() {
    let top = "vine: t\nvine: u\ninput: {}\noutputs: { done: {} }\npipe: [ { emit: done } ]\n";
    assert!(matches!(refused(top), LoadError::Yaml(ref m) if m.contains("duplicate")));
    let operator =
        "vine: t\ninput: {}\noutputs: { done: {} }\npipe:\n  - emit: done\n    emit: done\n";
    assert!(matches!(refused(operator), LoadError::Yaml(ref m) if m.contains("duplicate")));
}

#[tokio::test]
async fn an_anchor_inside_one_document_is_allowed() {
    // Nothing outside this document is pulled in.
    let yaml = "vine: t\ninput: {}\noutputs: { done: { gate: text } }\npipe:\n  - set: &zero { gate: zero }\n  - emit: { to: done, with: *zero }\n";
    let vine = Registry::new().load_pipe(yaml).unwrap();
    let out = vine.run_one(json!({})).await.unwrap();
    assert_eq!(out[0].payload, json!({ "gate": "zero" }));
}
