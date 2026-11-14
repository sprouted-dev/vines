//! What the checker says, before a vine runs, about resuming a vine with
//! an `each` that merges: what replays, what does not, and why. Nothing
//! here uses YAML:
//!
//!     cargo test --no-default-features --test merge_checker

mod acts;

use std::time::Duration;

use acts::desk::{desk_registry, each_job, jobs, jobs_vine, step, Desk};
use futures::{stream, StreamExt};
use vine_lang::pipe::{
    call, catch, collect, each, emit_with, filter, finalize, fork, last, pipe, retry, take,
    timeout, together, use_vine, Overlap, Pipe, PipeDef, Stage,
};
use vine_lang::{json, Bookmark, Journal, Registry, RunError, Shape, Ty};

fn registry() -> Registry {
    let mut reg = desk_registry(&Desk::new());
    // A helper that runs one job's steps side by side with nothing.
    let fan = pipe("fan")
        .input(Shape::new().required("jobs", Ty::List))
        .output("done", Shape::new().required("id", Ty::Text))
        .stages([
            vine_lang::pipe::split("$item.jobs").into("job"),
            each_job(
                Overlap::Merge,
                Some(2),
                vec![
                    step("one", json!(0)),
                    emit_with("done", json!({ "id": "$item.job.id" })),
                ],
            ),
        ]);
    let fan = reg.build_pipe(fan).unwrap();
    reg.add_vine(fan).unwrap();
    let steps = pipe("steps")
        .input(Shape::new().required("job", Ty::Any))
        .output("done", Shape::new().required("id", Ty::Text))
        .stages([
            step("one", json!("$item.job.secs")),
            step("two", json!(1)),
            emit_with("done", json!({ "id": "$item.job.id" })),
        ]);
    let steps = reg.build_pipe(steps).unwrap();
    reg.add_vine(steps).unwrap();
    reg
}

fn build(def: PipeDef) -> Pipe {
    registry().build_pipe(def).unwrap()
}

fn merge(work: Vec<Stage>) -> Stage {
    each_job(Overlap::Merge, Some(2), work)
}

fn one() -> Stage {
    step("one", json!("$item.job.secs"))
}

fn done() -> Stage {
    emit_with("done", json!({ "id": "$item.job.id" }))
}

/// The vine is refused by both ways to resume, and the run says why.
async fn refused(def: PipeDef, says: &str) {
    let vine = build(def);
    assert!(!vine.resumable(), "{says}");
    assert!(!vine.resumable_from_position(), "{says}");
    let input = vec![jobs(&[("a", 1)])];
    let got: Vec<_> = vine
        .run_saved(stream::iter(input.clone()), Journal::new())
        .collect()
        .await;
    assert!(
        matches!(&got[..], [Err(RunError::NotResumable { why, .. })] if why.contains(says)),
        "{says}: {got:?}"
    );
    let got: Vec<_> = vine
        .run_from(stream::iter(input), Bookmark::new())
        .collect()
        .await;
    assert!(
        matches!(&got[..], [Err(RunError::NotResumable { why, .. })] if why.contains(says)),
        "{says}: {got:?}"
    );
}

#[tokio::test]
async fn a_take_after_a_merge_is_refused_for_it_lets_through_whichever_finish_first() {
    refused(
        jobs_vine([merge(vec![one()]), take(1), done()]),
        "pipe[2] is a `take` after pipe[1], which runs items side by side",
    )
    .await;
}

#[tokio::test]
async fn a_collect_after_a_merge_is_refused_in_the_pipe_or_a_side_pipe() {
    refused(
        jobs_vine([merge(vec![one()]), collect(2), emit_with("all", json!({}))]),
        "pipe[2] is a `collect` after pipe[1]",
    )
    .await;
    refused(
        jobs_vine([
            merge(vec![one()]),
            fork([vec![collect(2), emit_with("all", json!({}))]]),
            done(),
        ]),
        "pipe[2].fork[0][0] is a `collect` after pipe[1]",
    )
    .await;
}

#[tokio::test]
async fn side_by_side_work_inside_a_branch_or_a_used_vine_counts_as_a_merge() {
    refused(
        jobs_vine([
            vine_lang::pipe::branch("$item.job.id", [("a", vec![merge(vec![one()])])], None),
            take(1),
            done(),
        ]),
        "pipe[2] is a `take` after pipe[1]",
    )
    .await;
    refused(
        pipe("outer")
            .input(Shape::new().required("jobs", Ty::List))
            .output("done", Shape::open())
            .stages([
                use_vine("fan", None).on("done", [filter("$item.id")]),
                take(1),
                emit_with("done", json!({})),
            ]),
        "pipe[1] is a `take` after pipe[0]",
    )
    .await;
}

#[tokio::test]
async fn a_catch_or_finalize_over_a_merge_is_refused_for_it_drops_the_items_still_running() {
    let outer = |cover: Stage| {
        pipe("outer")
            .input(Shape::new().required("jobs", Ty::List))
            .output("done", Shape::open())
            .stages([
                use_vine("fan", None).on("done", [emit_with("done", json!({}))]),
                cover,
            ])
    };
    refused(
        outer(catch([emit_with("done", json!({}))])),
        "pipe[1] is a `catch` over an `each` that merges",
    )
    .await;
    refused(
        outer(finalize([filter("$item.jobs")])),
        "pipe[1] is a `finalize` over an `each` that merges",
    )
    .await;
}

#[tokio::test]
async fn an_act_on_the_value_last_picked_after_a_merge_is_refused() {
    refused(
        jobs_vine([merge(vec![one()]), last(), step("two", json!(1)), done()]),
        "pipe[3] runs `work.step` on the value the `last` at pipe[2] picked",
    )
    .await;
    refused(
        jobs_vine([
            merge(vec![one()]),
            last(),
            use_vine("steps", Some(json!({ "job": "$item.job" })))
                .on("done", [emit_with("done", json!({ "id": "$item.id" }))]),
        ]),
        "pipe[3] runs `work.step` on the value the `last` at pipe[2] picked",
    )
    .await;
    // Inside a later stage too.
    refused(
        jobs_vine([
            merge(vec![one()]),
            last(),
            each(Overlap::Concat, [step("two", json!(1)), done()]),
        ]),
        "pipe[3].each.do[0] runs `work.step`",
    )
    .await;
}

#[tokio::test]
async fn switch_and_exhaust_still_cannot_be_resumed() {
    for (overlap, word) in [(Overlap::Switch, "switch"), (Overlap::Exhaust, "exhaust")] {
        refused(
            jobs_vine([each(overlap, [one(), done()])]),
            &format!("overlap `{word}`"),
        )
        .await;
    }
}

#[test]
fn what_works_one_value_at_a_time_after_or_inside_a_merge_can_be_resumed() {
    let cases: Vec<(&str, PipeDef)> = vec![
        ("one merge", jobs_vine([merge(vec![one(), done()])])),
        (
            "an emit after `last`",
            jobs_vine([merge(vec![one()]), last(), emit_with("all", json!({}))]),
        ),
        (
            "acts after the merge, one value at a time",
            jobs_vine([merge(vec![one()]), step("two", json!(1)), done()]),
        ),
        (
            "a merge after a merge",
            jobs_vine([
                merge(vec![one()]),
                merge(vec![step("two", json!(1))]),
                done(),
            ]),
        ),
        (
            "a merge inside a merge",
            jobs_vine([merge(vec![merge(vec![one()]), done()])]),
        ),
        (
            "`take` inside the merge",
            jobs_vine([merge(vec![one(), take(1), done()])]),
        ),
        (
            "`catch` inside the merge",
            jobs_vine([merge(vec![
                one(),
                done(),
                catch([emit_with(
                    "failed",
                    json!({ "id": "x", "code": "$item.error.code" }),
                )]),
            ])]),
        ),
        (
            "`retry` inside the merge",
            jobs_vine([merge(vec![one(), retry(1, Duration::ZERO), done()])]),
        ),
        (
            "`timeout` inside the merge",
            jobs_vine([merge(vec![one(), timeout(Duration::from_secs(9)), done()])]),
        ),
        (
            "`use` inside the merge",
            jobs_vine([merge(vec![use_vine(
                "steps",
                Some(json!({ "job": "$item.job" })),
            )
            .on("done", [emit_with("done", json!({ "id": "$item.id" }))])])]),
        ),
        (
            "`together` inside the merge",
            jobs_vine([merge(vec![
                together([
                    call(
                        "work.step",
                        json!({ "id": "$item.job.id", "step": "x", "secs": 1 }),
                    )
                    .into("x"),
                    call(
                        "work.step",
                        json!({ "id": "$item.job.id", "step": "y", "secs": 1 }),
                    )
                    .into("y"),
                ]),
                done(),
            ])]),
        ),
    ];
    for (what, def) in cases {
        let vine = build(def);
        assert!(vine.resumable(), "{what}");
        assert!(vine.resumable_from_position(), "{what}");
    }
}
