#![cfg(feature = "yaml")]

//! Every refusal the loader makes before a vine runs.
mod support;

use support::{register_agent, register_facts, register_git, Agent, Facts, Git, Script};
use vine_lang::{json, LoadError, Registry, RunError};

fn registry() -> Registry {
    let mut reg = Registry::new();
    let git = Git::new();
    register_facts(&mut reg, &Facts::new());
    register_git(&mut reg, &git);
    register_agent(&mut reg, &Agent::new(Script::Complete, &git));
    reg
}

/// A vine with the given steps and a fixed header.
fn with_steps(steps: &str) -> String {
    format!(
        "vine: t\ninput: {{ item: id, version: int }}\noutputs:\n  done: {{ item: id }}\n  refused: {{ gate: text? }}\nsteps:\n{steps}"
    )
}

fn refuse(yaml: &str) -> LoadError {
    match registry().load(yaml) {
        Ok(_) => panic!("expected a refusal for:\n{yaml}"),
        Err(e) => e,
    }
}

fn refuse_steps(steps: &str) -> LoadError {
    refuse(&with_steps(steps))
}

const END: &str = "  - out: { name: done, with: { item: $in.item } }\n";

#[test]
fn the_header_and_end_used_below_load() {
    registry().load(&with_steps(END)).unwrap();
}

#[test]
fn refuses_an_unknown_operator() {
    let e = refuse_steps(&format!("  - frobnicate: {{ x: 1 }}\n{END}"));
    assert_eq!(
        e,
        LoadError::UnknownOperator {
            path: "steps[0]".into(),
            name: "frobnicate".into()
        }
    );
    assert_eq!(
        e.to_string(),
        "steps[0]: unknown operator `frobnicate` (not registered)"
    );
}

#[test]
fn refuses_an_operator_name_that_is_not_registered_in_this_registry() {
    // `read` exists only where a host registered it.
    let yaml = with_steps(&format!("  - read: {{ artifact: $in.item }}\n{END}"));
    assert!(registry().load(&yaml).is_ok());
    assert_eq!(
        Registry::new().load(&yaml).unwrap_err(),
        LoadError::UnknownOperator {
            path: "steps[0]".into(),
            name: "read".into()
        }
    );
}

#[test]
fn refuses_settings_that_do_not_fit_the_operator() {
    let cases = [
        ("  - read: {}\n", "missing required field `artifact` (id)"),
        (
            "  - read: { artifact: $in.item, artifcat: x }\n",
            "unexpected field `artifcat` (not in the shape)",
        ),
        (
            "  - write: { artifact: $in.item, expect_version: three, records: [] }\n",
            "field `expect_version` should be int, found text \"three\"",
        ),
        (
            "  - write: { artifact: $in.item, records: { a: 1 } }\n",
            "field `records` should be list, found a map",
        ),
        ("  - read: an-1\n", "expected a map, found text"),
    ];
    for (step, problem) in cases {
        let e = refuse_steps(&format!("{step}{END}"));
        let LoadError::BadSettings {
            path,
            operator,
            problem: got,
        } = e
        else {
            panic!("expected BadSettings for {step}, got {e:?}");
        };
        assert_eq!(path, "steps[0].".to_string() + &operator);
        assert_eq!(got, problem, "for {step}");
    }
}

#[test]
fn refuses_settings_for_core_operators_too() {
    let e = refuse_steps("  - guard: { when: true, out: refused, then: x }\n  - out: refused\n");
    assert_eq!(
        e,
        LoadError::BadSettings {
            path: "steps[0].guard".into(),
            operator: "guard".into(),
            problem: "unexpected field `then` (not in the shape)".into()
        }
    );
}

#[tokio::test]
async fn settings_that_only_misfit_once_resolved_are_refused_at_run_time() {
    let mut reg = registry();
    reg.register(
        vine_lang::OperatorSpec::new("count")
            .settings(vine_lang::Shape::new().required("n", vine_lang::Ty::Int)),
        |s, _| async move { Ok(s) },
    )
    .unwrap();
    let vine = reg
        .load(&with_steps(&format!("  - count: {{ n: $in.item }}\n{END}")))
        .unwrap();
    assert_eq!(
        vine.run(json!({ "item": "an-1", "version": 1 }))
            .await
            .unwrap_err(),
        RunError::SettingsMisfit {
            path: "steps[0]".into(),
            operator: "count".into(),
            problem: "field `n` should be int, found text \"an-1\"".into()
        }
    );
}

#[test]
fn refuses_a_reference_to_a_value_no_earlier_step_produced() {
    let cases = [
        // never bound
        (
            format!("  - read: {{ artifact: $itme.id }}\n{END}"),
            "steps[0].read",
            "$itme.id",
        ),
        // bound later
        (
            format!("  - guard: {{ when: $x == null, out: refused, with: {{ gate: g }} }}\n  - read: {{ artifact: $in.item }}\n    as: x\n{END}"),
            "steps[0].guard.when",
            "$x",
        ),
        // bound inside a block, used after it
        (
            "  - try:\n      do:\n        - read: { artifact: $in.item }\n          as: x\n  - out: { name: done, with: { item: $x.id } }\n".to_string(),
            "steps[1].out.with",
            "$x.id",
        ),
        // `error` exists only inside catch
        (
            "  - guard: { when: $error.code == \"x\", out: refused, with: { gate: g } }\n  - out: refused\n".to_string(),
            "steps[0].guard.when",
            "$error.code",
        ),
    ];
    for (steps, path, reference) in cases {
        match refuse_steps(&steps) {
            LoadError::UnboundReference {
                path: p,
                reference: r,
                ..
            } => {
                assert_eq!((p.as_str(), r.as_str()), (path, reference));
            }
            other => panic!("expected UnboundReference for\n{steps}\ngot {other:?}"),
        }
    }
}

#[test]
fn refuses_a_field_the_value_never_has_when_its_shape_is_known() {
    let e = refuse_steps("  - out: { name: done, with: { item: $in.itme } }\n");
    assert_eq!(
        e.to_string(),
        "steps[0].out.with: `$in.itme` reads field `itme`, which `in` never has"
    );
    let e = refuse_steps(&format!(
        "  - worktree: {{ branch: b }}\n    as: tree\n  - worktree_release: {{ path: $tree.pth }}\n{END}"
    ));
    assert!(matches!(e, LoadError::UnknownField { ref field, .. } if field == "pth"));
}

#[test]
fn refuses_an_out_naming_an_undeclared_output() {
    for steps in [
        "  - out: nope\n",
        "  - out: { name: nope }\n",
        "  - guard: { when: true, out: nope }\n  - out: refused\n",
    ] {
        assert!(
            matches!(refuse_steps(steps), LoadError::UndeclaredOutput { ref name, .. } if name == "nope"),
            "for {steps}"
        );
    }
}

#[test]
fn refuses_an_output_payload_that_cannot_fit() {
    let cases = [
        ("  - out: done\n", "missing required field `item` (id)"),
        (
            "  - out: { name: done, with: { item: $in.item, extra: 1 } }\n",
            "unexpected field `extra` (not in the shape)",
        ),
        (
            "  - guard: { when: true, out: refused, with: { gate: 5 } }\n  - out: refused\n",
            "field `gate` should be text, found number 5",
        ),
    ];
    for (steps, problem) in cases {
        match refuse_steps(steps) {
            LoadError::OutputMisfit { problem: p, .. } => assert_eq!(p, problem),
            other => panic!("expected OutputMisfit for {steps}, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn an_output_payload_that_only_misfits_once_resolved_is_refused_at_run_time() {
    let vine = registry()
        .load(&with_steps(
            "  - out: { name: refused, with: { gate: $in.version } }\n",
        ))
        .unwrap();
    assert_eq!(
        vine.run(json!({ "item": "an-1", "version": 3 }))
            .await
            .unwrap_err(),
        RunError::OutputMisfit {
            path: "steps[0]".into(),
            output: "refused".into(),
            problem: "field `gate` should be text, found number 3".into()
        }
    );
}

#[test]
fn refuses_include_import_and_sub_vines_anywhere() {
    let top = [
        ("include: [common.yaml]\n", "include"),
        ("import: { common: ./common.yaml }\n", "import"),
        ("extends: base\n", "extends"),
        ("uses: [lib]\n", "uses"),
    ];
    for (extra, key) in top {
        let e = refuse(&format!("{extra}{}", with_steps(END)));
        assert_eq!(
            e,
            LoadError::Composition {
                path: "vine".into(),
                key: key.into()
            }
        );
    }
    let steps = [
        ("  - include: common.yaml\n", "include"),
        ("  - vine: other\n", "vine"),
        ("  - call_vine: { name: other, with: {} }\n", "call_vine"),
        (
            "  - read: { artifact: $in.item }\n    import: helpers\n",
            "import",
        ),
    ];
    for (step, key) in steps {
        let e = refuse_steps(&format!("{step}{END}"));
        assert_eq!(
            e,
            LoadError::Composition {
                path: "steps[0]".into(),
                key: key.into()
            }
        );
    }
    // Nested blocks too.
    let e = refuse_steps("  - try:\n      do:\n        - include: x\n");
    assert_eq!(
        e,
        LoadError::Composition {
            path: "steps[0].try.do[0]".into(),
            key: "include".into()
        }
    );
    assert!(e.to_string().contains("reuse is by operators only"));
}

#[test]
fn refuses_unknown_top_level_keys() {
    for key in ["triggers", "state", "on", "version"] {
        let e = refuse(&format!("{key}: x\n{}", with_steps(END)));
        assert_eq!(
            e,
            LoadError::UnknownKey {
                path: "vine".into(),
                key: key.into()
            }
        );
    }
}

#[test]
fn refuses_unknown_step_keys_beside_an_operator() {
    let e = refuse_steps(&format!(
        "  - write: {{ artifact: $in.item, records: [] }}\n    on_conflict: refused\n{END}"
    ));
    assert_eq!(
        e,
        LoadError::UnknownKey {
            path: "steps[0]".into(),
            key: "on_conflict".into()
        }
    );
}

#[test]
fn refuses_two_operators_in_one_step() {
    let e = refuse_steps(&format!(
        "  - read: {{ artifact: $in.item }}\n    worktree: {{ branch: b }}\n{END}"
    ));
    assert!(
        matches!(e, LoadError::Malformed { ref problem, .. } if problem.contains("one operator"))
    );
}

#[test]
fn refuses_a_duplicate_binding_and_as_on_control_flow() {
    let e = refuse_steps(&format!(
        "  - read: {{ artifact: $in.item }}\n    as: x\n  - read: {{ artifact: $in.item }}\n    as: x\n{END}"
    ));
    assert_eq!(
        e,
        LoadError::DuplicateBinding {
            path: "steps[1].as".into(),
            name: "x".into()
        }
    );
    let e = refuse_steps(&format!(
        "  - read: {{ artifact: $in.item }}\n    as: in\n{END}"
    ));
    assert!(matches!(e, LoadError::DuplicateBinding { ref name, .. } if name == "in"));
    let e = refuse_steps("  - out: done\n    as: x\n");
    assert!(
        matches!(e, LoadError::Malformed { ref problem, .. } if problem.contains("returns nothing"))
    );
}

#[test]
fn refuses_steps_after_an_ending_and_steps_that_can_end_without_one() {
    assert_eq!(
        refuse_steps(&format!("{END}{END}")),
        LoadError::Unreachable {
            path: "steps[1]".into()
        }
    );
    assert_eq!(
        refuse_steps("  - read: { artifact: $in.item }\n"),
        LoadError::MissingOutput {
            path: "steps".into()
        }
    );
    // A switch without `else` can fall through.
    assert_eq!(
        refuse_steps(
            "  - switch:\n      on: $in.item\n      cases:\n        a:\n          - out: refused\n"
        ),
        LoadError::MissingOutput {
            path: "steps".into()
        }
    );
    // A catch that does not end lets the try fall through.
    assert_eq!(
        refuse_steps("  - try:\n      do:\n        - out: refused\n      catch:\n        git:\n          - read: { artifact: $in.item }\n"),
        LoadError::MissingOutput {
            path: "steps".into()
        }
    );
}

#[test]
fn refuses_an_ending_inside_finally() {
    let e = refuse_steps(
        "  - try:\n      do:\n        - out: refused\n      finally:\n        - out: refused\n",
    );
    assert_eq!(
        e,
        LoadError::EndsInFinally {
            path: "steps[0].try.finally[0].out".into()
        }
    );
}

#[test]
fn refuses_catching_cancellation() {
    let e = refuse_steps(
        "  - try:\n      do:\n        - out: refused\n      catch:\n        cancelled:\n          - out: refused\n",
    );
    assert_eq!(
        e,
        LoadError::CatchCancelled {
            path: "steps[0].try.catch.cancelled".into()
        }
    );
}

#[test]
fn refuses_malformed_expressions() {
    let e = refuse_steps("  - guard: { when: $in.item != open, out: refused, with: { gate: g } }\n  - out: refused\n");
    assert_eq!(e.to_string(), "steps[0].guard.when: bare word `open`: quote text (\"open\"), or write a reference ($name.field), an integer, true, false or null");
    for when in [
        "$in.item == \\\"a\\\" == \\\"b\\\"",
        "$in.item > 3",
        "$in.item && $in.version",
    ] {
        let e = refuse_steps(&format!(
            "  - guard: {{ when: \"{when}\", out: refused, with: {{ gate: g }} }}\n  - out: refused\n"
        ));
        assert!(
            matches!(e, LoadError::Malformed { ref path, .. } if path == "steps[0].guard.when"),
            "for {when}: {e:?}"
        );
    }
    let e = refuse_steps(&format!("  - read: {{ artifact: $In.item }}\n{END}"));
    assert!(
        matches!(e, LoadError::Malformed { ref problem, .. } if problem.contains("not a reference"))
    );
}

#[test]
fn refuses_malformed_documents() {
    assert!(matches!(refuse("vine: [unclosed"), LoadError::Yaml(_)));
    assert!(matches!(refuse("- a list"), LoadError::Malformed { .. }));
    let cases = [
        (
            "vine: t\ninput: { item: uuid }\noutputs: { done: {} }\nsteps: [ { out: done } ]\n",
            "input",
        ),
        (
            "vine: t\nmode: forever\ninput: {}\noutputs: { done: {} }\nsteps: [ { out: done } ]\n",
            "mode",
        ),
        (
            "vine: t\ninput: {}\noutputs: {}\nsteps: [ { out: done } ]\n",
            "outputs",
        ),
        (
            "vine: T\ninput: {}\noutputs: { done: {} }\nsteps: [ { out: done } ]\n",
            "vine",
        ),
        (
            "input: {}\noutputs: { done: {} }\nsteps: [ { out: done } ]\n",
            "vine",
        ),
        (
            "vine: t\ninput: {}\noutputs: { done: {} }\nsteps: []\n",
            "steps",
        ),
    ];
    for (yaml, path) in cases {
        match refuse(yaml) {
            LoadError::Malformed { path: p, .. } => assert_eq!(p, path, "for {yaml}"),
            other => panic!("expected Malformed for {yaml}, got {other:?}"),
        }
    }
}

#[test]
fn the_three_legs_load() {
    let mut reg = registry();
    support::register_keys(&mut reg, &support::Keys::new(&[]));
    for leg in ["claim", "launch", "share_receive"] {
        reg.load(&support::vine_source(leg))
            .unwrap_or_else(|e| panic!("{leg}: {e}"));
    }
}

#[test]
fn refuses_yaml_tags_the_usual_include_mechanism() {
    let e = refuse_steps("  - !include common.yaml\n");
    assert_eq!(
        e,
        LoadError::Composition {
            path: "vine".into(),
            key: "!include".into()
        }
    );
    let e = refuse_steps("  - out: { name: done, with: !ref other.yaml#payload }\n");
    assert!(matches!(e, LoadError::Composition { ref key, .. } if key == "!ref"));
}

#[test]
fn refuses_duplicate_keys_instead_of_keeping_the_last() {
    let yaml = format!("{}steps:\n  - out: refused\n", with_steps(END));
    assert!(matches!(refuse(&yaml), LoadError::Yaml(ref m) if m.contains("duplicate")));
    let e = refuse_steps(&format!(
        "  - read: {{ artifact: $in.item }}\n    read: {{ artifact: x }}\n{END}"
    ));
    assert!(matches!(e, LoadError::Yaml(ref m) if m.contains("duplicate")));
}

#[tokio::test]
async fn anchors_inside_one_document_are_allowed() {
    // Not composition: nothing outside this document is pulled in.
    let vine = registry()
        .load(&with_steps(
            "  - guard: { when: $in.version == 0, out: refused, with: &no { gate: zero } }\n  - out: { name: refused, with: *no }\n",
        ))
        .unwrap();
    let out = vine
        .run(json!({ "item": "a", "version": 1 }))
        .await
        .unwrap();
    assert_eq!(out.payload, json!({ "gate": "zero" }));
}
