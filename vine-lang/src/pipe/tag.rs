//! Which input items a value came from, and when an input item is done.
//!
//! In a run that saves its position, each input item gets a [`Tag`] when
//! it goes in. Every value, output and failure derived from it carries a
//! clone. A batch carries the tags of the items in it. When the last clone
//! of an item's tag is dropped, nothing in the run still holds anything
//! derived from that item, so the item is done. The [`Ledger`] counts the
//! done items and moves the position past them in order.
//!
//! An output's tag is dropped when the run hands the output out, so an item
//! is not done until its outputs have been handed out.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use super::flow::lock;

/// The input items a value came from. Empty in a run that does not save
/// its position.
#[derive(Clone, Default)]
pub(super) struct Tag(Option<Arc<Hold>>);

struct Hold {
    /// The first and last input item this value came from.
    first: u64,
    last: u64,
    ledger: Arc<Mutex<Ledger>>,
    /// For a batch: the tags of the values in it. Empty for an input item.
    parts: Vec<Tag>,
}

impl Drop for Hold {
    fn drop(&mut self) {
        if self.parts.is_empty() {
            lock(&self.ledger).done(self.first);
        }
    }
}

impl Tag {
    /// The tag of input item `n`.
    pub(super) fn item(n: u64, ledger: &Arc<Mutex<Ledger>>) -> Tag {
        Tag(Some(Arc::new(Hold {
            first: n,
            last: n,
            ledger: ledger.clone(),
            parts: Vec::new(),
        })))
    }

    /// The tag of a batch made of values with these tags. The items in it
    /// are done together: the position never stops between them.
    pub(super) fn batch(parts: Vec<Tag>) -> Tag {
        let holds: Vec<&Arc<Hold>> = parts.iter().filter_map(|t| t.0.as_ref()).collect();
        let (Some(first), Some(last), Some(any)) = (
            holds.iter().map(|h| h.first).min(),
            holds.iter().map(|h| h.last).max(),
            holds.first(),
        ) else {
            return Tag::default();
        };
        let ledger = any.ledger.clone();
        lock(&ledger).join(first, last);
        Tag(Some(Arc::new(Hold {
            first,
            last,
            ledger,
            parts,
        })))
    }

    /// Take `other` when this tag holds nothing.
    pub(super) fn or(&mut self, other: &Tag) {
        if self.0.is_none() {
            self.0.clone_from(&other.0);
        }
    }

    /// The input item a journal line for this value belongs to: the last
    /// item it came from. 0 in a run that does not save its position.
    pub(super) fn item_number(&self) -> u64 {
        self.0.as_ref().map_or(0, |h| h.last)
    }
}

/// Which input items are done, and the position: every item through it is
/// done. It keeps only what lies past the position, so it is bounded by
/// the work in flight.
#[derive(Debug, Default)]
pub(super) struct Ledger {
    position: u64,
    /// Items done past the position, waiting for an earlier one.
    done: BTreeSet<u64>,
    /// Runs of items that are done together, first to last. They never
    /// overlap and all lie past the position.
    joins: BTreeMap<u64, u64>,
}

impl Ledger {
    /// A ledger for a run that resumes after `position`.
    pub(super) fn after(position: u64) -> Ledger {
        Ledger {
            position,
            ..Ledger::default()
        }
    }

    pub(super) fn position(&self) -> u64 {
        self.position
    }

    fn done(&mut self, n: u64) {
        if n > self.position {
            self.done.insert(n);
            self.advance();
        }
    }

    /// Items `first` to `last` are done together.
    fn join(&mut self, first: u64, last: u64) {
        let (mut first, mut last) = (first.max(self.position + 1), last);
        if first >= last {
            return;
        }
        let touching: Vec<(u64, u64)> = self
            .joins
            .range(..=last)
            .filter(|(_, &l)| l >= first)
            .map(|(&f, &l)| (f, l))
            .collect();
        for (f, l) in touching {
            self.joins.remove(&f);
            first = first.min(f);
            last = last.max(l);
        }
        self.joins.insert(first, last);
    }

    fn advance(&mut self) {
        loop {
            let next = self.position + 1;
            let last = match self.joins.get(&next) {
                Some(&last) => last,
                None => next,
            };
            if !(next..=last).all(|k| self.done.contains(&k)) {
                return;
            }
            for k in next..=last {
                self.done.remove(&k);
            }
            self.joins.remove(&next);
            self.position = last;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger() -> Arc<Mutex<Ledger>> {
        Arc::new(Mutex::new(Ledger::default()))
    }

    #[test]
    fn the_position_moves_past_items_in_order_only() {
        let l = ledger();
        let (one, two, three) = (Tag::item(1, &l), Tag::item(2, &l), Tag::item(3, &l));
        drop(two);
        assert_eq!(lock(&l).position(), 0);
        drop(one);
        assert_eq!(lock(&l).position(), 2);
        let again = three.clone();
        drop(three);
        assert_eq!(lock(&l).position(), 2, "a clone still holds item 3");
        drop(again);
        assert_eq!(lock(&l).position(), 3);
    }

    #[test]
    fn items_in_one_batch_are_done_together() {
        let l = ledger();
        let tags: Vec<Tag> = (1..=4).map(|n| Tag::item(n, &l)).collect();
        // A fork keeps another copy of item 2 going down the main pipe.
        let main_copy = tags[1].clone();
        let batch = Tag::batch(vec![tags[0].clone(), tags[1].clone(), tags[2].clone()]);
        assert_eq!(batch.item_number(), 3);
        drop(tags);
        drop(batch);
        // Item 1 is done, but the batch ties it to item 2, which is not.
        assert_eq!(lock(&l).position(), 0);
        drop(main_copy);
        assert_eq!(lock(&l).position(), 4);
    }

    #[test]
    fn overlapping_batches_join_into_one_run() {
        let mut l = Ledger::default();
        l.join(2, 4);
        l.join(4, 6);
        l.join(9, 10);
        assert_eq!(l.joins, BTreeMap::from([(2, 6), (9, 10)]));
        for n in [1, 2, 3, 4, 5] {
            l.done(n);
        }
        assert_eq!(l.position(), 1);
        l.done(6);
        assert_eq!(l.position(), 6);
        assert!(l.done.is_empty() && l.joins == BTreeMap::from([(9, 10)]));
    }
}
