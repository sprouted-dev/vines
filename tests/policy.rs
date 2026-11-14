//! Every policy combination rule, refused with its typed error.

mod common;

use common::{backpressured, builder as unbackpressured, nz, MemorySink};
use vines::policy::{
    Answer, Durability, Loss, ObserverDecl, ObserverPolicy, Overflow, OverflowDecl, OverflowKind,
};
use vines::PolicyError;

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
    assert_eq!(unbackpressured().observe("spill", spill).err(), None);
    let stop = ObserverPolicy::new(nz(8), Overflow::StopProducer).loss(Loss::None);
    assert_eq!(backpressured().observe("stop", stop).err(), None);
}

#[test]
fn loss_allowed_accepts_drop_policies() {
    let mut b = unbackpressured();
    for (name, overflow) in [("a", Overflow::DropOldest), ("b", Overflow::DropNewest)] {
        let policy = ObserverPolicy::new(nz(1), overflow);
        assert_eq!(b.observe(name, policy).err(), None);
    }
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
    assert_eq!(
        unbackpressured()
            .observe_durable("summary", policy, MemorySink::new())
            .err(),
        None
    );
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
    assert_eq!(backpressured().observe("recorder", policy).err(), None);
}

#[tokio::test(start_paused = true)]
async fn late_observers_are_checked_by_the_same_rules() {
    let run = unbackpressured().spawn(&vines::cancel::CancelScope::new(), |ctx| async move {
        ctx.cancelled().await;
        vines::run::Exit::<(), ()>::Stopped
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
    run.cancel();
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
fn a_refusal_names_the_overflow_as_an_author_writes_it() {
    for (overflow, authored) in [
        (OverflowKind::DropOldest, "drop_oldest"),
        (OverflowKind::DropNewest, "drop_newest"),
        (OverflowKind::StopProducer, "stop_producer"),
        (OverflowKind::Spill, "spill"),
    ] {
        let error = PolicyError::LossNoneForbidsDrop {
            observer: "summary".into(),
            overflow,
        };
        assert_eq!(
            error.to_string(),
            format!(
                "observer `summary`: loss=none forbids overflow={authored}; declare stop_producer or spill"
            )
        );
    }
}
