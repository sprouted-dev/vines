//! What an observer reads off a frame and a collected record.

use vines::frame::{Coverage, End, Ending, Frame, Gap, GapReason, Record};
use vines::observer::ObserverFailure;

fn gap(first_seq: u64, last_seq: u64) -> Gap {
    Gap {
        reason: GapReason::DroppedNewest,
        first_seq,
        last_seq,
    }
}

fn end(lost: u64) -> End {
    End {
        ending: Ending::Completed,
        coverage: Coverage {
            emitted: 4,
            lost,
            gaps: 0,
            failure: None,
        },
    }
}

#[test]
fn only_an_item_frame_has_an_item_and_a_seq() {
    let item = Frame::Item { seq: 3, item: "x" };
    assert_eq!((item.item(), item.seq()), (Some(&"x"), Some(3)));
    for other in [Frame::<&str>::Gap(gap(0, 1)), Frame::End(end(0))] {
        assert_eq!((other.item(), other.seq()), (None, None));
    }
}

#[test]
fn a_record_that_never_ended_is_incomplete() {
    let record = Record::<u8> {
        items: vec![(0, 1)],
        gaps: Vec::new(),
        end: None,
    };
    assert!(!record.is_complete());
}

#[test]
fn a_record_with_a_gap_is_incomplete_whatever_its_end_says() {
    let record = Record::<u8> {
        items: Vec::new(),
        gaps: vec![gap(0, 3)],
        end: Some(end(0)),
    };
    assert!(!record.is_complete());
}

#[test]
fn a_record_whose_end_counts_loss_is_incomplete() {
    let record = Record::<u8> {
        items: Vec::new(),
        gaps: Vec::new(),
        end: Some(end(4)),
    };
    assert!(!record.is_complete());
}

#[test]
fn coverage_is_complete_only_with_no_loss_and_no_failure() {
    let coverage = |lost, failure| Coverage {
        emitted: 4,
        lost,
        gaps: 0,
        failure,
    };
    assert!(coverage(0, None).is_complete());
    assert!(!coverage(1, None).is_complete());
    assert!(!coverage(0, Some(ObserverFailure::Abandoned)).is_complete());
}
