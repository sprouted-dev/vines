//! Cancel scopes: request vs deadline, first cause wins, cascade.

use std::time::Duration;

use futures::FutureExt;
use tokio::time::Instant;
use vines::cancel::{CancelCause, CancelScope};

#[tokio::test(start_paused = true)]
async fn a_new_scope_is_not_cancelled() {
    let scope = CancelScope::new();
    assert!(!scope.is_cancelled());
    assert_eq!(scope.cause(), None);
    assert_eq!(scope.deadline(), None);
    assert!(scope.cancelled().now_or_never().is_none());
}

#[tokio::test(start_paused = true)]
async fn cancel_is_a_request() {
    let scope = CancelScope::new();
    scope.cancel();
    assert!(scope.is_cancelled());
    assert_eq!(scope.cause(), Some(CancelCause::Requested));
    assert_eq!(scope.cancelled().await, CancelCause::Requested);
    assert!(scope.token().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn the_deadline_fires_without_a_request() {
    let scope = CancelScope::with_timeout(Duration::from_secs(10));
    let started = Instant::now();
    assert_eq!(scope.deadline(), Some(started + Duration::from_secs(10)));
    assert_eq!(scope.cancelled().await, CancelCause::TimedOut);
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    // Awaiting past the deadline cancels the token for token holders.
    assert!(scope.token().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn a_passed_deadline_is_visible_before_anyone_awaits() {
    let scope = CancelScope::with_timeout(Duration::from_secs(1));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(scope.cause(), Some(CancelCause::TimedOut));
    // The raw token is cancelled only once something awaits the scope.
    assert!(!scope.token().is_cancelled());
    assert_eq!(scope.cancelled().await, CancelCause::TimedOut);
    assert!(scope.token().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn a_request_after_the_deadline_leaves_it_timed_out() {
    let scope = CancelScope::with_timeout(Duration::from_secs(1));
    tokio::time::advance(Duration::from_secs(2)).await;
    scope.cancel();
    assert_eq!(scope.cause(), Some(CancelCause::TimedOut));
    assert_eq!(scope.cancelled().await, CancelCause::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn a_request_at_the_deadline_instant_reads_as_timed_out() {
    let scope = CancelScope::with_timeout(Duration::from_secs(5));
    tokio::time::advance(Duration::from_secs(5)).await;
    scope.cancel();
    assert_eq!(scope.cause(), Some(CancelCause::TimedOut));
}

#[tokio::test(start_paused = true)]
async fn a_deadline_after_a_request_leaves_it_requested() {
    let scope = CancelScope::with_timeout(Duration::from_secs(5));
    scope.cancel();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(scope.cause(), Some(CancelCause::Requested));
    assert_eq!(scope.cancelled().await, CancelCause::Requested);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_parent_cascades_to_children() {
    let parent = CancelScope::new();
    let child = parent.child();
    let grandchild = child.child();
    parent.cancel();
    assert_eq!(child.cause(), Some(CancelCause::Requested));
    assert_eq!(grandchild.cancelled().await, CancelCause::Requested);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_child_leaves_the_parent_alone() {
    let parent = CancelScope::new();
    let child = parent.child();
    let sibling = parent.child();
    child.cancel();
    assert!(child.is_cancelled());
    assert!(!parent.is_cancelled());
    assert!(!sibling.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn a_child_inherits_its_parents_deadline() {
    let started = Instant::now();
    let parent = CancelScope::with_timeout(Duration::from_secs(3));
    let child = parent.child();
    assert_eq!(child.deadline(), Some(started + Duration::from_secs(3)));
    assert_eq!(child.cancelled().await, CancelCause::TimedOut);
    assert_eq!(started.elapsed(), Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn a_childs_earlier_deadline_does_not_touch_the_parent() {
    let parent = CancelScope::with_timeout(Duration::from_secs(10));
    let child = parent.child_with_timeout(Duration::from_secs(2));
    assert_eq!(child.cancelled().await, CancelCause::TimedOut);
    assert!(!parent.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn a_childs_later_deadline_is_capped_by_the_parent() {
    let started = Instant::now();
    let parent = CancelScope::with_timeout(Duration::from_secs(2));
    let child = parent.child_with_deadline(started + Duration::from_mins(1));
    assert_eq!(child.deadline(), Some(started + Duration::from_secs(2)));
    assert_eq!(child.cancelled().await, CancelCause::TimedOut);
    assert_eq!(started.elapsed(), Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn a_parent_request_before_the_childs_deadline_reads_as_a_request() {
    let parent = CancelScope::new();
    let child = parent.child_with_timeout(Duration::from_secs(5));
    parent.cancel();
    // Nothing awaited the child before its deadline passed; timestamps
    // still say the request came first.
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(child.cause(), Some(CancelCause::Requested));
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_raw_token_counts_as_a_request() {
    let scope = CancelScope::new();
    scope.token().cancel();
    assert_eq!(scope.cause(), Some(CancelCause::Requested));
    assert_eq!(scope.cancelled().await, CancelCause::Requested);
}
