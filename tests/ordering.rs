//! Per-stream order always; cross-branch order only where declared.

use std::time::Duration;

use futures::{stream, FutureExt, StreamExt};
use tokio::sync::mpsc;
use vines::ordering::{merge_arrival, merge_by_sequence, sequence, OrderViolation, Sequenced};

fn seq<T>(seq: u64, item: T) -> Sequenced<T> {
    Sequenced { seq, item }
}

#[tokio::test]
async fn sequence_numbers_items_from_zero() {
    let out: Vec<_> = sequence(stream::iter(["a", "b", "c"])).collect().await;
    assert_eq!(out, vec![seq(0, "a"), seq(1, "b"), seq(2, "c")]);
}

#[tokio::test]
async fn merge_by_sequence_restores_source_order_across_branches() {
    // One source split into two branches (evens and odds), rejoined.
    let evens = stream::iter([seq(0, 'a'), seq(2, 'c'), seq(4, 'e')]);
    let odds = stream::iter([seq(1, 'b'), seq(3, 'd')]);
    let out: Vec<_> = merge_by_sequence(vec![evens.boxed(), odds.boxed()])
        .map(Result::unwrap)
        .map(|s| s.item)
        .collect()
        .await;
    assert_eq!(out, vec!['a', 'b', 'c', 'd', 'e']);
}

#[tokio::test]
async fn merge_by_sequence_waits_for_a_branch_that_has_not_spoken() {
    let (fast_tx, fast_rx) = mpsc::unbounded_channel();
    let (slow_tx, slow_rx) = mpsc::unbounded_channel();
    let branch = |mut rx: mpsc::UnboundedReceiver<Sequenced<u8>>| {
        stream::poll_fn(move |cx| rx.poll_recv(cx)).boxed()
    };
    let mut merged = merge_by_sequence(vec![branch(fast_rx), branch(slow_rx)]);
    fast_tx.send(seq(1, 1)).unwrap();
    fast_tx.send(seq(2, 2)).unwrap();
    // seq 0 may still come from the slow branch: nothing is emitted yet.
    assert!(merged.next().now_or_never().is_none());
    slow_tx.send(seq(0, 0)).unwrap();
    assert_eq!(merged.next().await.unwrap().unwrap().seq, 0);
    drop(slow_tx);
    assert_eq!(merged.next().await.unwrap().unwrap().seq, 1);
    assert_eq!(merged.next().await.unwrap().unwrap().seq, 2);
    drop(fast_tx);
    assert!(merged.next().await.is_none());
}

#[tokio::test]
async fn merge_by_sequence_keeps_equal_numbers_in_branch_order() {
    // One item fanned into two branches carries the same number in both.
    let left = stream::iter([seq(0, "left"), seq(1, "left")]);
    let right = stream::iter([seq(0, "right")]);
    let out: Vec<_> = merge_by_sequence(vec![left.boxed(), right.boxed()])
        .map(Result::unwrap)
        .map(|s| (s.seq, s.item))
        .collect()
        .await;
    assert_eq!(out, vec![(0, "left"), (0, "right"), (1, "left")]);
}

#[tokio::test]
async fn a_branch_going_backwards_is_reported_and_ends_the_merge() {
    let broken = stream::iter([seq(5, ()), seq(3, ()), seq(9, ())]);
    let out: Vec<_> = merge_by_sequence(vec![broken]).collect().await;
    assert_eq!(
        out,
        vec![
            Ok(seq(5, ())),
            Err(OrderViolation {
                branch: 0,
                seq: 3,
                after: 5
            })
        ]
    );
}

#[tokio::test]
async fn merge_by_sequence_of_nothing_is_empty() {
    let out: Vec<_> =
        merge_by_sequence(Vec::<stream::Iter<std::vec::IntoIter<Sequenced<u8>>>>::new())
            .collect()
            .await;
    assert!(out.is_empty());
}

#[tokio::test(start_paused = true)]
async fn merge_arrival_declares_no_cross_branch_order_but_keeps_each_branch() {
    let delayed = |items: Vec<(u64, &'static str)>, every: u64| {
        stream::iter(items).then(move |(n, item)| async move {
            tokio::time::sleep(Duration::from_millis(every)).await;
            seq(n, item)
        })
    };
    let slow = delayed(vec![(0, "s0"), (2, "s1")], 30).boxed();
    let fast = delayed(vec![(1, "f0"), (3, "f1")], 10).boxed();
    let out: Vec<_> = merge_arrival(vec![slow, fast]).collect().await;
    // Arrival order, not source order...
    assert_eq!(
        out.iter().map(|s| s.item).collect::<Vec<_>>(),
        vec!["f0", "f1", "s0", "s1"]
    );
    // ...but each branch's own order holds.
    let slow_seqs: Vec<_> = out
        .iter()
        .filter(|s| s.item.starts_with('s'))
        .map(|s| s.seq)
        .collect();
    assert_eq!(slow_seqs, vec![0, 2]);
}
