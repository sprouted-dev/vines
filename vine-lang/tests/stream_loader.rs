#![cfg(feature = "yaml")]

//! Every refusal the loader makes for stream vines, and the registry's
//! rules for stream operators.
mod support;

use support::{
    register_backup, register_facts, register_keys, register_summarise, vine_source, Facts,
    KeyStore, Keys, Provider,
};
use vine_lang::{
    LoadError, Memory, OperatorSpec, Registry, RegistryError, Shape, StreamOperatorSpec, Ty, Value,
};

fn registry() -> Registry {
    let mut reg = Registry::new();
    register_facts(&mut reg, &Facts::new());
    register_keys(&mut reg, &Keys::new(&[]));
    register_backup(&mut reg, &KeyStore::new(&["backup"]), &Provider::new());
    register_summarise(&mut reg);
    // An operator that declares no return shape.
    reg.register(
        OperatorSpec::new("ping").settings(Shape::open()),
        |_s, _| async { Ok(Value::Null) },
    )
    .unwrap();
    // An operator whose results may hold any field.
    reg.register(
        OperatorSpec::new("loose").returns(Shape::open()),
        |_s, _| async { Ok(Value::Null) },
    )
    .unwrap();
    // A stream operator that declares nothing about what it emits.
    reg.register_stream(StreamOperatorSpec::new("anything"), |_| Ok(Box::new(Pass)))
        .unwrap();
    reg
}

struct Pass;

impl vine_lang::StreamOperator for Pass {
    fn push<'a>(
        &'a mut self,
        item: Value,
        out: &'a mut vine_lang::Emit,
        _cx: &'a vine_lang::OpContext,
    ) -> vine_lang::BoxFuture<'a, Result<(), vine_lang::OpError>> {
        out.emit(item);
        Box::pin(async { Ok(()) })
    }
}

/// A stream vine reading `{ n: int, line: text }` with two outputs.
fn stream(steps: &str) -> String {
    format!(
        "vine: t\nmode: stream\ninput: {{ n: int, line: text }}\noutputs:\n  done: {{ n: int }}\n  other: {{ why: text }}\nsteps:\n{steps}"
    )
}

/// A stream vine with one output of the given shape.
fn stream_one(output: &str, steps: &str) -> String {
    format!(
        "vine: t\nmode: stream\ninput: {{ n: int, line: text }}\noutputs:\n  only: {output}\nsteps:\n{steps}"
    )
}

fn once(steps: &str) -> String {
    format!("vine: t\ninput: {{ n: int }}\noutputs:\n  done: {{ n: int }}\nsteps:\n{steps}")
}

fn refuse(yaml: &str) -> LoadError {
    match registry().load(yaml) {
        Ok(_) => panic!("expected a refusal for:\n{yaml}"),
        Err(e) => e,
    }
}

const EACH_END: &str = "  - each:\n      - out: { name: done, with: { n: $item.n } }\n";

#[test]
fn the_shapes_used_below_load() {
    registry().load(&stream(EACH_END)).unwrap();
    registry()
        .load(&stream_one(
            "{ items: list, count: int }",
            "  - collect: { size: 3 }\n",
        ))
        .unwrap();
}

#[test]
fn the_three_stream_legs_load_and_their_reach_includes_every_operator() {
    let reg = registry();
    let reach = |leg: &str| -> Vec<String> {
        let v = reg
            .load(&vine_source(leg))
            .unwrap_or_else(|e| panic!("{leg}: {e}"));
        v.reach().iter().cloned().collect()
    };
    assert_eq!(reach("backup"), ["backup.write", "keys.read"]);
    assert_eq!(reach("session_summary"), ["model"]);
    // Operators inside `each` count.
    assert_eq!(reach("share_receive"), ["facts.write", "keys.read"]);
}

#[test]
fn reach_joins_pipe_operators_stream_operators_and_operators_inside_each() {
    let vine = registry()
        .load(&stream(
            "  - collect: { size: 2 }\n  - summarise: { field: line }\n  - each:\n      - write: { artifact: a1, records: [] }\n      - out: { name: other, with: { why: $item.text } }\n",
        ))
        .unwrap();
    let reach: Vec<&str> = vine.reach().iter().map(String::as_str).collect();
    assert_eq!(reach, ["facts.write", "model"]);
}

#[test]
fn refuses_once_grammar_directly_in_a_stream_pipe() {
    let cases = [
        ("  - guard: { when: true, out: other, with: { why: x } }\n", "guard"),
        ("  - out: { name: done, with: { n: $item.n } }\n", "out"),
        ("  - out: done\n", "out"),
        (
            "  - switch:\n      on: $item.n\n      cases:\n        \"1\":\n          - out: other\n",
            "switch",
        ),
        ("  - try:\n      do:\n        - out: other\n", "try"),
    ];
    for (step, name) in cases {
        let e = refuse(&stream(&format!("{step}{EACH_END}")));
        assert_eq!(
            e,
            LoadError::OnceOnly {
                path: "steps[0]".into(),
                name: name.into()
            },
            "for {step}"
        );
    }
    // After other steps too.
    let e = refuse(&stream(
        "  - collect: { size: 2 }\n  - guard: { when: true, out: other, with: { why: x } }\n",
    ));
    assert_eq!(
        e.to_string(),
        "steps[1]: `guard` is per-item grammar and cannot be a step of a stream pipe; put it inside `each`"
    );
}

#[test]
fn refuses_each_and_stream_operators_in_a_once_vine() {
    let cases = [
        ("  - each:\n      - out: done\n", "steps[0]", "each"),
        ("  - collect: { size: 2 }\n", "steps[0]", "collect"),
        ("  - summarise: { field: line }\n", "steps[0]", "summarise"),
        (
            "  - try:\n      do:\n        - collect: { size: 2 }\n        - out: done\n",
            "steps[0].try.do[0]",
            "collect",
        ),
    ];
    for (step, path, name) in cases {
        let e = refuse(&once(&format!(
            "{step}  - out: {{ name: done, with: {{ n: $in.n }} }}\n"
        )));
        assert_eq!(
            e,
            LoadError::PipeOnly {
                path: path.into(),
                name: name.into()
            },
            "for {step}"
        );
    }
    let e = refuse(&once("  - each:\n      - out: done\n"));
    assert_eq!(
        e.to_string(),
        "steps[0]: `each` is a stream step; it may only stand in a stream vine's pipe (not in a once vine, nor inside `each`)"
    );
}

#[test]
fn refuses_stream_steps_inside_each() {
    let e = refuse(&stream(
        "  - each:\n      - collect: { size: 2 }\n      - out: { name: done, with: { n: $item.n } }\n",
    ));
    assert_eq!(
        e,
        LoadError::PipeOnly {
            path: "steps[0].each[0]".into(),
            name: "collect".into()
        }
    );
    let e = refuse(&stream(
        "  - each:\n      - each:\n          - out: other\n      - out: other\n",
    ));
    assert_eq!(
        e,
        LoadError::PipeOnly {
            path: "steps[0].each[0]".into(),
            name: "each".into()
        }
    );
}

#[test]
fn refuses_each_that_is_not_the_last_step() {
    let e = refuse(&stream(&format!("{EACH_END}  - collect: {{ size: 2 }}\n")));
    assert_eq!(
        e,
        LoadError::EachNotLast {
            path: "steps[0].each".into()
        }
    );
}

#[test]
fn refuses_each_whose_steps_can_finish_without_an_output() {
    assert_eq!(
        refuse(&stream(
            "  - each:\n      - write: { artifact: a1, records: [] }\n"
        )),
        LoadError::MissingOutput {
            path: "steps[0].each".into()
        }
    );
    assert!(matches!(
        refuse(&stream("  - each:\n      - out: nope\n")),
        LoadError::UndeclaredOutput { ref path, ref name } if path == "steps[0].each[0].out" && name == "nope"
    ));
}

#[test]
fn refuses_a_pipe_whose_results_cannot_be_matched_to_a_declared_output() {
    // Two outputs, and the pipe does not end in `each`.
    let e = refuse(&stream("  - collect: { size: 2 }\n"));
    assert_eq!(
        e.to_string(),
        "steps[0].collect: the pipe's results cannot be matched to a declared output: a pipe that does not end in `each` sends each result of its last step, `collect`, to the vine's one output; declare exactly one output (found 2: `done`, `other`)"
    );
    let cases = [
        // The last step says nothing about its results.
        (
            "{ n: int }",
            "  - ping: { x: $item.n }\n",
            "steps[0].ping",
            "`ping` does not declare the shape of its results, so they cannot be checked against output `only`; declare it, or end the pipe in `each`",
        ),
        (
            "{ n: int }",
            "  - anything: {}\n",
            "steps[0].anything",
            "`anything` does not declare the shape of its results, so they cannot be checked against output `only`; declare it, or end the pipe in `each`",
        ),
        // The declared shape does not fit.
        (
            "{ items: list }",
            "  - collect: { size: 2 }\n",
            "steps[0].collect",
            "results of `collect` do not fit output `only`: unexpected field `count` (not in the output)",
        ),
        (
            "{ items: list, count: int, extra: text }",
            "  - collect: { size: 2 }\n",
            "steps[0].collect",
            "results of `collect` do not fit output `only`: missing required field `extra` (text)",
        ),
        (
            "{ items: list, count: text }",
            "  - collect: { size: 2 }\n",
            "steps[0].collect",
            "results of `collect` do not fit output `only`: field `count` is int, the output wants text",
        ),
        (
            "{ ok: bool, reason: text, event: object }",
            "  - verify_envelope: { envelope: {} }\n",
            "steps[0].verify_envelope",
            "results of `verify_envelope` do not fit output `only`: field `event` is optional there but required in the output",
        ),
        // `set` declares no return shape.
        (
            "{ n: int }",
            "  - set: { n: $item.n }\n",
            "steps[0].set",
            "`set` does not declare the shape of its results, so they cannot be checked against output `only`; declare it, or end the pipe in `each`",
        ),
        // An open return shape may hold any field.
        (
            "{ n: int }",
            "  - loose: {}\n",
            "steps[0].loose",
            "results of `loose` do not fit output `only`: its shape is open (it may hold fields the output does not list)",
        ),
    ];
    for (output, steps, path, problem) in cases {
        assert_eq!(
            refuse(&stream_one(output, steps)),
            LoadError::UnmatchedOutput {
                path: path.into(),
                problem: problem.into()
            },
            "for {steps}"
        );
    }
    // An id fits where the output wants text.
    let mut reg = registry();
    reg.register(
        OperatorSpec::new("ided").returns(Shape::new().required("n", Ty::Id)),
        |_s, _| async { Ok(Value::Null) },
    )
    .unwrap();
    reg.load(&stream_one("{ n: text }", "  - ided: {}\n"))
        .unwrap();
    assert!(matches!(
        reg.load(&stream_one("{ n: int }", "  - ided: {}\n")).unwrap_err(),
        LoadError::UnmatchedOutput { ref problem, .. } if problem.ends_with("field `n` is id, the output wants int")
    ));
    // Optional fields in the output may be missing from the results.
    registry()
        .load(&stream_one(
            "{ items: list, count: int, note: text? }",
            "  - collect: { size: 2 }\n",
        ))
        .unwrap();
}

#[test]
fn refuses_stream_operator_settings_that_do_not_fit() {
    let cases = [
        ("collect: {}", "collect", "missing required field `size` (int)"),
        ("collect: { size: many }", "collect", "field `size` should be int, found text \"many\""),
        ("collect: { size: 0 }", "collect", "`size` must be at least 1"),
        ("collect: { size: -3 }", "collect", "`size` must be at least 1"),
        (
            "collect: { size: 5, or_every: soon }",
            "collect",
            "`or_every` is a duration such as `60s`, `500ms` or `2m`, found \"soon\"",
        ),
        (
            "collect: { size: 5, or_every: 60 }",
            "collect",
            "field `or_every` should be text, found number 60",
        ),
        ("collect: { size: 5, or_every: 0s }", "collect", "`or_every` must be longer than zero"),
        ("collect: { size: 5, every: 60s }", "collect", "unexpected field `every` (not in the shape)"),
        (
            "collect: { size: $item.n }",
            "collect",
            "`$item.n`: a stream operator's settings are fixed when the vine loads and cannot refer to items",
        ),
        ("collect: 5", "collect", "expected a map, found a number"),
        ("summarise: { field: \"\" }", "summarise", "`field` must name the text field of each line"),
        ("summarise: {}", "summarise", "missing required field `field` (text)"),
    ];
    for (step, operator, problem) in cases {
        let e = refuse(&stream(&format!("  - {step}\n{EACH_END}")));
        assert_eq!(
            e,
            LoadError::BadSettings {
                path: format!("steps[0].{operator}"),
                operator: operator.into(),
                problem: problem.into()
            },
            "for {step}"
        );
    }
}

#[test]
fn refuses_ordinary_operator_settings_that_do_not_fit_in_a_pipe() {
    let e = refuse(&stream(&format!(
        "  - write: {{ artifact: $item.n }}\n{EACH_END}"
    )));
    assert_eq!(
        e,
        LoadError::BadSettings {
            path: "steps[0].write".into(),
            operator: "write".into(),
            problem: "missing required field `records` (list)".into()
        }
    );
}

#[test]
fn refuses_as_in_a_pipe() {
    let e = refuse(&stream(&format!(
        "  - collect: {{ size: 2 }}\n    as: batch\n{EACH_END}"
    )));
    assert!(
        matches!(e, LoadError::Malformed { ref path, ref problem } if path == "steps[0].as" && problem.contains("next step's `$item`")),
        "{e:?}"
    );
}

#[test]
fn refuses_references_a_stream_step_cannot_see() {
    // No `$in` in a stream vine.
    assert!(matches!(
        refuse(&stream("  - each:\n      - out: { name: done, with: { n: $in.n } }\n")),
        LoadError::UnboundReference { ref reference, .. } if reference == "$in.n"
    ));
    // Fields the item never has, at pipe level and inside `each`.
    assert_eq!(
        refuse(&stream(&format!(
            "  - read: {{ artifact: $item.id }}\n{EACH_END}"
        )))
        .to_string(),
        "steps[0].read: `$item.id` reads field `id`, which `item` never has"
    );
    assert!(matches!(
        refuse(&stream("  - collect: { size: 2 }\n  - each:\n      - out: { name: done, with: { n: $item.n } }\n")),
        LoadError::UnknownField { ref field, ref path, .. } if field == "n" && path == "steps[1].each[0].out.with"
    ));
    assert!(matches!(
        refuse(&stream(&format!("  - read: {{ artifact: $span.start }}\n{EACH_END}"))),
        LoadError::UnknownField { ref field, .. } if field == "start"
    ));
    // `as: item` inside each would hide the item.
    assert!(matches!(
        refuse(&stream("  - each:\n      - set: { x: 1 }\n        as: item\n      - out: other\n")),
        LoadError::DuplicateBinding { ref name, .. } if name == "item"
    ));
    // After a step with no declared results, fields are unknown, not refused.
    registry()
        .load(&stream("  - ping: {}\n  - each:\n      - out: { name: other, with: { why: $item.anything } }\n"))
        .unwrap();
}

#[test]
fn refuses_unknown_operators_and_composition_in_a_pipe() {
    assert_eq!(
        refuse(&stream(&format!("  - batch: {{ size: 2 }}\n{EACH_END}"))),
        LoadError::UnknownOperator {
            path: "steps[0]".into(),
            name: "batch".into()
        }
    );
    assert_eq!(
        refuse(&stream(&format!("  - include: other.yaml\n{EACH_END}"))),
        LoadError::Composition {
            path: "steps[0]".into(),
            key: "include".into()
        }
    );
    assert_eq!(
        refuse(&stream(
            "  - each:\n      - call_vine: { name: x }\n      - out: other\n"
        )),
        LoadError::Composition {
            path: "steps[0].each[0]".into(),
            key: "call_vine".into()
        }
    );
}

#[test]
fn refuses_a_malformed_pipe() {
    for (steps, problem) in [
        ("steps: []\n", "a stream pipe needs at least one step"),
        (
            "steps: { collect: { size: 2 } }\n",
            "a stream pipe is a list of steps",
        ),
        (
            "steps:\n  - collect\n",
            "a step is a map with one operator key",
        ),
    ] {
        let yaml = format!(
            "vine: t\nmode: stream\ninput: {{ n: int }}\noutputs:\n  done: {{ n: int }}\n{steps}"
        );
        match refuse(&yaml) {
            LoadError::Malformed { problem: p, .. } => assert_eq!(p, problem),
            other => panic!("expected Malformed for {steps}, got {other:?}"),
        }
    }
}

#[test]
fn stream_operators_share_the_namespace_and_the_reserved_names() {
    let mut reg = Registry::new();
    let none =
        |_: &Value| -> Result<Box<dyn vine_lang::StreamOperator>, String> { Err("unused".into()) };
    for reserved in ["each", "guard", "out", "include", "vine"] {
        assert_eq!(
            reg.register_stream(StreamOperatorSpec::new(reserved), none),
            Err(RegistryError::Reserved(reserved.into()))
        );
    }
    assert_eq!(
        reg.register(OperatorSpec::new("each"), |_s, _| async { Ok(Value::Null) }),
        Err(RegistryError::Reserved("each".into()))
    );
    // A stream operator cannot take a plain operator's name, nor the reverse.
    assert_eq!(
        reg.register_stream(StreamOperatorSpec::new("set"), none),
        Err(RegistryError::Duplicate("set".into()))
    );
    assert_eq!(
        reg.register(OperatorSpec::new("collect"), |_s, _| async {
            Ok(Value::Null)
        }),
        Err(RegistryError::Duplicate("collect".into()))
    );
    assert_eq!(
        reg.register_stream(StreamOperatorSpec::new("Batch"), none),
        Err(RegistryError::BadName("Batch".into()))
    );
    assert_eq!(
        reg.register_stream(StreamOperatorSpec::new("batch").touches(["a..b"]), none),
        Err(RegistryError::BadTouch {
            operator: "batch".into(),
            touch: "a..b".into()
        })
    );
    reg.register_stream(StreamOperatorSpec::new("batch"), none)
        .unwrap();
    assert_eq!(
        reg.register_stream(StreamOperatorSpec::new("batch"), none),
        Err(RegistryError::Duplicate("batch".into()))
    );
}

#[test]
fn collect_is_the_core_stream_operator_and_touches_nothing() {
    let specs = Registry::new().stream_specs();
    assert_eq!(specs.len(), 1);
    let collect = &specs[0];
    assert_eq!(collect.name, "collect");
    assert_eq!(collect.memory, Memory::Rebuildable);
    assert!(collect.touches.is_empty());
    assert_eq!(
        collect.emits,
        Some(
            Shape::new()
                .required("items", Ty::List)
                .required("count", Ty::Int)
        )
    );
}
