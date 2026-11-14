//! Which input items a value came from, when an input item is done, and
//! which group of work a value belongs to.
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
//!
//! The same idea works one level down. In a saved run, each item a stream
//! act that gives its items again gives gets a tag of its own, numbered
//! from 1 within that call and counted by a ledger of the call's own. It
//! holds the tag of the value the act was given, so that value is not done
//! while any item the act gave is in flight. When the call's ledger moves,
//! it says so on [`Moves`], and the run saves how far the call got.
//!
//! # Groups
//!
//! In a saved run, each value that enters an `each` that merges starts a
//! group: the work of that one value, numbered in the order the values
//! enter. Its tag holds the tag of the value as it entered, and names the
//! group in its scope, such as `pipe[1].each@3/`. The keys of acts run for
//! the group, and the outputs made from it, start with that scope, so they
//! line up after a restart in whatever order the groups finish. Values that
//! leave the `each` keep the group's tag, so the stages after it key their
//! acts by group too, until `last` picks one value out of many groups and
//! [lifts](Tag::lifted) it back out.
//!
//! Each run of the stages a `timeout` covers is a group too, such as
//! `pipe[3].timeout@0/`, numbered in the order values reach the `timeout`.
//! A replay of a run that timed out skips its stages, and the keys of every
//! other run stay where they were.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use super::book::Key;
use super::flow::lock;

/// The input items a value came from, and the group of work it belongs
/// to. Empty in a run that saves nothing.
#[derive(Clone, Default)]
pub(super) struct Tag(Option<Arc<Hold>>);

struct Hold {
    /// For a value a ledger counts: which of its items it came from.
    count: Option<Count>,
    /// For a batch: the tags of the values in it. Empty for one item.
    parts: Vec<Tag>,
    /// For an item a stream act call gave, or a batch of them: the call.
    call: Option<Arc<Key>>,
    /// For the work of one value inside an `each` that merges: where that
    /// `each` stands, with the scope of the vine it is in, such as
    /// `pipe[1].each`.
    group: Option<Arc<str>>,
    /// Where the keys of acts run for this value start.
    scope: Arc<str>,
    /// For a value `last` lifted out of its groups: the scope of the item
    /// of a stream act call it was made from, such as `pipe[1]#0@7/`, if
    /// any. When the call's position passes that item, the value is never
    /// made again.
    from: Option<Arc<str>>,
    /// The input item a line for this value belongs to.
    item: u64,
    /// The tag this one was made from: the value a stream act was given,
    /// the value that started a group, or the value `last` lifted out of
    /// its groups. It is held so that value is not done while this one is
    /// in flight, and read only to find a group.
    up: Tag,
}

struct Count {
    /// The first and last item this value came from, as `ledger` counts
    /// them: input items, or the items one stream act call gave.
    first: u64,
    last: u64,
    ledger: Arc<Mutex<Ledger>>,
}

impl Drop for Hold {
    fn drop(&mut self) {
        if let (Some(count), true) = (&self.count, self.parts.is_empty()) {
            lock(&count.ledger).done(count.first);
        }
    }
}

impl Tag {
    /// The tag of input item `n`.
    pub(super) fn item(n: u64, ledger: &Arc<Mutex<Ledger>>) -> Tag {
        Tag(Some(Arc::new(Hold {
            count: Some(Count {
                first: n,
                last: n,
                ledger: ledger.clone(),
            }),
            parts: Vec::new(),
            call: None,
            group: None,
            scope: "".into(),
            from: None,
            item: n,
            up: Tag::default(),
        })))
    }

    /// The tag of item `n` that the stream act call `call` gave, counted by
    /// the call's `ledger`. `up` is the tag of the value the call was
    /// given.
    pub(super) fn given(n: u64, ledger: &Arc<Mutex<Ledger>>, call: &Arc<Key>, up: &Tag) -> Tag {
        Tag(Some(Arc::new(Hold {
            count: Some(Count {
                first: n,
                last: n,
                ledger: ledger.clone(),
            }),
            parts: Vec::new(),
            call: Some(call.clone()),
            group: None,
            scope: format!("{}@{n}/", call.at).into(),
            from: None,
            item: call.item,
            up: up.clone(),
        })))
    }

    /// The tag of group `n` of the `each` at `site`, started by the value
    /// with tag `up`.
    pub(super) fn group(site: &str, n: u64, up: &Tag) -> Tag {
        Tag(Some(Arc::new(Hold {
            count: None,
            parts: Vec::new(),
            call: None,
            group: Some(site.into()),
            scope: format!("{}{site}@{n}/", up.scope()).into(),
            from: None,
            item: up.item_number(),
            up: up.clone(),
        })))
    }

    /// The tag of a batch made of values with these tags. The items in it
    /// are done together: the position never stops between them. The
    /// batch is counted by the ledger of its last value. The checker keeps
    /// the items of two stream act calls, and of two groups, out of one
    /// batch.
    pub(super) fn batch(parts: Vec<Tag>) -> Tag {
        let Some(newest) = parts.iter().rev().find_map(|t| t.0.as_ref()) else {
            return Tag::default();
        };
        let counted = newest.counted();
        let (count, call) = match counted {
            Some((hold, count)) => {
                let ledger = count.ledger.clone();
                let spans: Vec<&Count> = parts
                    .iter()
                    .filter_map(|t| t.0.as_ref()?.counted())
                    .map(|(_, c)| c)
                    .filter(|c| Arc::ptr_eq(&c.ledger, &ledger))
                    .collect();
                let first = spans.iter().map(|c| c.first).min().unwrap_or(count.first);
                let last = spans.iter().map(|c| c.last).max().unwrap_or(count.last);
                lock(&ledger).join(first, last);
                // A batch of a call's items belongs to the call, unless it
                // was made inside a group within one of them.
                let call = hold.call.clone().filter(|_| std::ptr::eq(hold, &**newest));
                (
                    Some(Count {
                        first,
                        last,
                        ledger,
                    }),
                    call,
                )
            }
            None => (None, None),
        };
        let (scope, item) = match (&call, &count) {
            (Some(call), Some(count)) => (format!("{}@{}/", call.at, count.last).into(), call.item),
            (None, Some(count)) if newest.scope.is_empty() => ("".into(), count.last),
            _ => (newest.scope.clone(), newest.item),
        };
        Tag(Some(Arc::new(Hold {
            count,
            parts,
            call,
            group: None,
            scope,
            from: None,
            item,
            up: Tag::default(),
        })))
    }

    /// This value, lifted out of every group started by an `each` whose
    /// site starts with `within`: it keeps its hold on what it came from,
    /// and its keys and outputs start where they did before the outermost
    /// of those groups.
    pub(super) fn lifted(&self, within: &str) -> Tag {
        let mut outer: Option<Arc<str>> = None;
        let mut at = self.0.as_ref();
        while let Some(hold) = at {
            if hold.group.as_deref().is_some_and(|s| s.starts_with(within)) {
                outer = Some(hold.up.scope().into());
            }
            at = hold.up.0.as_ref();
        }
        let Some(scope) = outer else {
            return self.clone();
        };
        let mut from = None;
        let mut at = self.0.as_ref();
        while let Some(hold) = at {
            if hold.call.is_some() {
                from = Some(hold.scope.clone());
                break;
            }
            at = hold.up.0.as_ref();
        }
        Tag(Some(Arc::new(Hold {
            count: None,
            parts: Vec::new(),
            call: None,
            group: None,
            scope,
            from,
            item: self.item_number(),
            up: self.clone(),
        })))
    }

    /// For a value `last` lifted out of its groups: the scope of the item
    /// of a stream act call it was made from.
    pub(super) fn stream_item(&self) -> Option<&str> {
        self.0.as_ref()?.from.as_deref()
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
        self.0.as_ref().map_or(0, |h| h.item)
    }

    /// Where the keys of acts run for this value start. Empty unless the
    /// value came from an item a stream act gives again, or from a group.
    /// For item 7 of a call it is that call's key and the number, such as
    /// `pipe[1]#0@7/`. For group 3 of an `each` it is where the `each`
    /// stands and the number, such as `pipe[1].each@3/`. Scopes nest, so
    /// the keys of a group or an item are the same whichever item a
    /// restart begins at and whatever order the groups finish in.
    pub(super) fn scope(&self) -> &str {
        self.0.as_ref().map_or("", |h| &h.scope)
    }

    /// The stream act call this value came from, and the number of its
    /// item, when the call gives its items again and no group stands
    /// between the call's item and this value.
    pub(super) fn stream(&self) -> Option<(Arc<Key>, u64)> {
        let hold = self.0.as_ref()?;
        Some((hold.call.clone()?, hold.count.as_ref()?.last))
    }
}

impl Hold {
    /// The nearest hold that a ledger counts: this one, or the one it was
    /// made from, through groups and lifts.
    fn counted(&self) -> Option<(&Hold, &Count)> {
        let mut at = self;
        loop {
            if let Some(count) = &at.count {
                return Some((at, count));
            }
            at = at.up.0.as_deref()?;
        }
    }
}

/// Where a stream act call's ledger says it moved: the call, and the
/// number of its items now fully done.
pub(super) type Moves = Arc<Mutex<VecDeque<(Arc<Key>, u64)>>>;

/// Who to tell when a stream act call's ledger moves.
#[derive(Debug)]
struct Notify {
    call: Arc<Key>,
    moves: Moves,
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
    /// For the ledger of a stream act call: where to say it moved.
    notify: Option<Notify>,
    /// For the ledger of input items: items done past the position that
    /// the run has not told its caller about yet. An item in a run of
    /// items done together is told only once the whole run is done.
    finished: VecDeque<u64>,
}

impl Ledger {
    /// A ledger for a run that resumes after `position`, where the items
    /// in `done` past it are done already.
    pub(super) fn after(position: u64, done: impl IntoIterator<Item = u64>) -> Ledger {
        let mut ledger = Ledger {
            position,
            done: done.into_iter().filter(|&n| n > position).collect(),
            ..Ledger::default()
        };
        ledger.advance();
        ledger
    }

    /// A ledger for the items the stream act call `call` gives, when the
    /// items through `position` are done. It says on `moves` each time it
    /// moves.
    pub(super) fn of_call(position: u64, call: Arc<Key>, moves: Moves) -> Ledger {
        Ledger {
            position,
            notify: Some(Notify { call, moves }),
            ..Ledger::default()
        }
    }

    pub(super) fn position(&self) -> u64 {
        self.position
    }

    /// Whether item `n` is done.
    pub(super) fn is_done(&self, n: u64) -> bool {
        n <= self.position || self.done.contains(&n)
    }

    /// The next item done past the position that the caller has not been
    /// told about.
    pub(super) fn next_finished(&mut self) -> Option<u64> {
        while let Some(n) = self.finished.pop_front() {
            if n > self.position {
                return Some(n);
            }
        }
        None
    }

    fn done(&mut self, n: u64) {
        if n <= self.position {
            return;
        }
        let before = self.position;
        self.done.insert(n);
        self.advance();
        if let Some(notify) = self.notify.as_ref().filter(|_| self.position > before) {
            lock(&notify.moves).push_back((notify.call.clone(), self.position));
        }
        if self.notify.is_none() && n > self.position {
            self.tell_finished(n);
        }
    }

    /// Item `n` is done past the position. Tell the caller, once the run
    /// of items done together that holds it is done too.
    fn tell_finished(&mut self, n: u64) {
        let run = self
            .joins
            .range(..=n)
            .next_back()
            .filter(|(_, &last)| last >= n)
            .map(|(&first, &last)| (first, last));
        match run {
            None => self.finished.push_back(n),
            Some((first, last)) => {
                if (first..=last).all(|k| self.done.contains(&k)) {
                    self.finished.extend(first..=last);
                }
            }
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

    #[test]
    fn an_item_done_past_the_position_is_told_once_its_batch_is_done() {
        let l = ledger();
        let tags: Vec<Tag> = (1..=5).map(|n| Tag::item(n, &l)).collect();
        let batch = Tag::batch(vec![tags[2].clone(), tags[3].clone()]);
        let [one, two, three, four, five] = <[Tag; 5]>::try_from(tags).ok().unwrap();
        drop(five);
        drop(two);
        drop(three);
        drop(four);
        assert_eq!(lock(&l).next_finished(), Some(5));
        assert_eq!(lock(&l).next_finished(), Some(2));
        // Items 3 and 4 are done together, and the batch still holds them.
        assert_eq!(lock(&l).next_finished(), None);
        drop(batch);
        assert_eq!(lock(&l).next_finished(), Some(3));
        assert_eq!(lock(&l).next_finished(), Some(4));
        drop(one);
        assert_eq!(lock(&l).position(), 5);
        assert_eq!(lock(&l).next_finished(), None);
    }

    #[test]
    fn a_group_holds_the_value_that_started_it_and_lifts_back_to_its_scope() {
        let l = ledger();
        let item = Tag::item(1, &l);
        let group = Tag::group("pipe[1].each", 3, &item);
        assert_eq!(group.scope(), "pipe[1].each@3/");
        assert_eq!(group.item_number(), 1);
        let inner = Tag::group("pipe[1].each.do[0].each", 0, &group);
        assert_eq!(inner.scope(), "pipe[1].each@3/pipe[1].each.do[0].each@0/");
        let lifted = inner.lifted("pipe[");
        assert_eq!(lifted.scope(), "");
        drop((item, group, inner));
        assert_eq!(lock(&l).position(), 0, "the lifted value holds item 1");
        drop(lifted);
        assert_eq!(lock(&l).position(), 1);
    }
}
