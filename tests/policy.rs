//! Every policy combination rule, refused with its typed error.

mod common;

use common::{nz, MemorySink, GRACE};
use vines::policy::{
    Answer, Durability, Loss, ObserverDecl, ObserverPolicy, Overflow, OverflowDecl, OverflowKind,
    SourceDecl,
};
use vines::run::RunBuilder;
use vines::PolicyError;

fn unbackpressured() -> RunBuilder<u32> {
    RunBuilder::new(SourceDecl::without_backpressure(), GRACE).unwrap()
}

fn backpressured() -> RunBuilder<u32> {
    RunBuilder::new(SourceDecl::with_backpressure(), GRACE).unwrap()
}

fn decl(overflow: OverflowDecl) -> ObserverDecl {
    ObserverDecl {
        answer: Answer::None,
        durability: Durability::Ephemeral,
        loss: Loss::Allowed,
        queue: Some(8),
        overflow,
    }
}

#[test]
fn loss_none_forbids_both_drop_policies() {
    for (overflow, kind) in [
        (Overflow::DropOldest, OverflowKind::DropOldest),
        (Overflow::DropNewest, OverflowKind::DropNewest),
    ] {
        let policy = ObserverPolicy::new(nz(8), overflow).loss(Loss::None);
        let expected = PolicyError::LossNoneForbidsDrop {
            observer: "summary".into(),
            overflow: kind,
        };
        assert_eq!(policy.check("summary"), Err(expected.clone()));
        // Attaching re-checks, so a hand-built policy cannot slip through.
        assert_eq!(
            unbackpressured().observe("summary", policy).err(),
            Some(expected)
        );
    }
}

#[test]
fn loss_none_accepts_spill_and_stop_producer() {
    let spill = ObserverPolicy::new(nz(8), Overflow::Spill { quota: nz(64) }).loss(Loss::None);
    assert!(unbackpressured().observe("spill", spill).is_ok());
    let stop = ObserverPolicy::new(nz(8), Overflow::StopProducer).loss(Loss::None);
    assert!(backpressured().observe("stop", stop).is_ok());
}

#[test]
fn loss_allowed_accepts_drop_policies() {
    let mut b = unbackpressured();
    assert!(b
        .observe("a", ObserverPolicy::new(nz(1), Overflow::DropOldest))
        .is_ok());
    assert!(b
        .observe("b", ObserverPolicy::new(nz(1), Overflow::DropNewest))
        .is_ok());
}

#[test]
fn durable_requires_a_sink() {
    let policy = ObserverPolicy::new(nz(8), Overflow::Spill { quota: nz(8) })
        .durability(Durability::Durable);
    assert_eq!(
        unbackpressured().observe("summary", policy).err(),
        Some(PolicyError::DurableRequiresSink {
            observer: "summary".into()
        })
    );
    assert!(unbackpressured()
        .observe_durable("summary", policy, MemorySink::new())
        .is_ok());
}

#[test]
fn an_ephemeral_observer_refuses_a_durable_sink() {
    let policy = ObserverPolicy::new(nz(8), Overflow::DropOldest);
    assert_eq!(
        unbackpressured()
            .observe_durable("display", policy, MemorySink::new())
            .err(),
        Some(PolicyError::EphemeralGivenSink {
            observer: "display".into()
        })
    );
}

#[test]
fn stop_producer_requires_a_backpressure_source() {
    let policy = ObserverPolicy::new(nz(8), Overflow::StopProducer);
    let expected = Some(PolicyError::StopProducerRequiresBackpressure {
        observer: "recorder".into(),
    });
    assert_eq!(
        unbackpressured().observe("recorder", policy).err(),
        expected
    );
    // The durable path is checked too.
    let durable = policy.durability(Durability::Durable);
    assert_eq!(
        unbackpressured()
            .observe_durable("recorder", durable, MemorySink::new())
            .err(),
        expected
    );
    assert!(backpressured().observe("recorder", policy).is_ok());
}

#[tokio::test(start_paused = true)]
async fn late_observers_are_checked_by_the_same_rules() {
    let (hold_tx, hold_rx) = tokio::sync::oneshot::channel::<()>();
    let run = unbackpressured().spawn(&vines::cancel::CancelScope::new(), |_ctx| async move {
        let _ = hold_rx.await;
        vines::run::Exit::<(), ()>::Completed(())
    });
    let stop = ObserverPolicy::new(nz(8), Overflow::StopProducer);
    assert!(matches!(
        run.observe("late", stop),
        Err(PolicyError::StopProducerRequiresBackpressure { .. })
    ));
    let durable = ObserverPolicy::new(nz(8), Overflow::DropOldest).durability(Durability::Durable);
    assert!(matches!(
        run.observe("late", durable),
        Err(PolicyError::DurableRequiresSink { .. })
    ));
    let required_durable = durable.answer(Answer::Required);
    assert!(matches!(
        run.observe_durable("late", required_durable, MemorySink::new()),
        Err(PolicyError::LateRequiredObserver { .. })
    ));
    let ephemeral = ObserverPolicy::new(nz(8), Overflow::DropOldest);
    assert!(matches!(
        run.observe_durable("late", ephemeral, MemorySink::new()),
        Err(PolicyError::EphemeralGivenSink { .. })
    ));
    drop(hold_tx);
    run.finish().await;
}

#[test]
fn spill_requires_a_quota() {
    assert_eq!(
        decl(OverflowDecl::Spill { quota: None }).validate("summary"),
        Err(PolicyError::SpillRequiresQuota {
            observer: "summary".into()
        })
    );
}

#[test]
fn a_zero_spill_quota_is_refused() {
    assert_eq!(
        decl(OverflowDecl::Spill { quota: Some(0) }).validate("summary"),
        Err(PolicyError::ZeroLimit {
            scope: "summary".into(),
            field: "spill.quota"
        })
    );
}

#[test]
fn a_queue_must_be_declared_and_non_zero() {
    let mut missing = decl(OverflowDecl::DropOldest);
    missing.queue = None;
    assert_eq!(
        missing.validate("display"),
        Err(PolicyError::MissingLimit {
            scope: "display".into(),
            field: "queue"
        })
    );
    let mut zero = decl(OverflowDecl::DropOldest);
    zero.queue = Some(0);
    assert_eq!(
        zero.validate("display"),
        Err(PolicyError::ZeroLimit {
            scope: "display".into(),
            field: "queue"
        })
    );
}

#[test]
fn a_declaration_checks_the_loss_rule() {
    let mut lossless_drop = decl(OverflowDecl::DropNewest);
    lossless_drop.loss = Loss::None;
    assert!(matches!(
        lossless_drop.validate("summary"),
        Err(PolicyError::LossNoneForbidsDrop {
            overflow: OverflowKind::DropNewest,
            ..
        })
    ));
}

#[test]
fn a_valid_declaration_becomes_the_typed_policy() {
    let authored = ObserverDecl {
        answer: Answer::Required,
        durability: Durability::Durable,
        loss: Loss::None,
        queue: Some(256),
        overflow: OverflowDecl::Spill { quota: Some(4096) },
    };
    let policy = authored.validate("summary").unwrap();
    assert_eq!(
        policy,
        ObserverPolicy {
            answer: Answer::Required,
            durability: Durability::Durable,
            loss: Loss::None,
            queue: nz(256),
            overflow: Overflow::Spill { quota: nz(4096) },
        }
    );
    assert_eq!(policy.holding_limit(), 256 + 4096);
    for (overflow, expected) in [
        (OverflowDecl::DropOldest, Overflow::DropOldest),
        (OverflowDecl::DropNewest, Overflow::DropNewest),
        (OverflowDecl::StopProducer, Overflow::StopProducer),
    ] {
        assert_eq!(decl(overflow).validate("x").unwrap().overflow, expected);
    }
}

#[test]
fn errors_name_the_field_an_author_wrote() {
    let message = PolicyError::StopProducerRequiresBackpressure {
        observer: "recorder".into(),
    }
    .to_string();
    assert!(message.contains("recorder") && message.contains("stop_producer"));
    let message = PolicyError::LossNoneForbidsDrop {
        observer: "summary".into(),
        overflow: OverflowKind::DropOldest,
    }
    .to_string();
    assert!(message.contains("drop_oldest"));
}
