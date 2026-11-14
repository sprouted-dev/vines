//! Where a stream act that gives its items again can be saved by position,
//! and where it cannot.
//!
//! A call saved by position replays from the item after its position. The
//! stages after it rebuild what they held because the items flow again.
//! That fails for a stage that keeps something across the items of a call
//! other than the items it holds, and for a stage that drops what a call
//! gave and runs it again. So the checker follows what each call gives
//! down the pipe, as a [`Feed`], and refuses:
//!
//! - `take` anywhere the call's items or outputs reach in the same run of
//!   a pipe: it counts, and after a restart it would count from nothing.
//! - `bufferCount` where it could gather the items of more than one call, or
//!   of the call and of something else: a batch must hold one call's
//!   items to be rebuilt.
//! - `retry` over the call: it drops what a failed attempt gave and runs
//!   the attempt again, so an item it dropped looks done when it is not.
//!
//! A refused call is saved item by item instead, as an act that does not
//! give its items again is. A vine run by `use` was checked on its own, so
//! when its calls are saved by position and the outer vine refuses them,
//! the outer vine cannot be resumed at all.

use std::sync::{Arc, OnceLock};

use super::stream_call::ItemSaving;
use super::SavesEveryItem;
use crate::registry::Gives;

/// What a stream act call gives, reaching a point in a pipe.
#[derive(Clone)]
pub(super) struct Feed {
    owner: Owner,
    reaches: Reaches,
}

/// The call a feed comes from.
#[derive(Clone)]
enum Owner {
    /// A call in this vine: its index in [`CallSites`].
    Here(usize),
    /// Calls saved by position inside a vine this one runs by `use`.
    Used { path: String, vine: String },
}

#[derive(Clone, Copy)]
enum Reaches {
    /// The call's items reach here.
    Values(Batch),
    /// Only outputs made from its items reach here, out of a fork's side
    /// pipe.
    Outputs,
}

/// Whether a batch made here holds the items of one call only.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Batch {
    /// The call stands in this pipe or in a pipe it forks to, and it runs
    /// at most once for each run of this pipe.
    OneCall,
    /// Anything else.
    Mixed,
}

impl Feed {
    /// The feed as it comes out of the pipe it is in, into the pipe around
    /// it: past a stage that runs a pipe per item, such as `concatMap`, its
    /// items may mix with others.
    pub(super) fn out_of_nested(mut self) -> Feed {
        if let Reaches::Values(_) = self.reaches {
            self.reaches = Reaches::Values(Batch::Mixed);
        }
        self
    }

    /// The feed as it comes out of a fork's side pipe: only outputs leave
    /// it.
    pub(super) fn out_of_side(mut self) -> Feed {
        self.reaches = Reaches::Outputs;
        self
    }

    /// The feed past the last covering stage of a pipe that sees many
    /// items: the stages before it ran once per item, so a stage after it
    /// sees the calls of many items.
    pub(super) fn past_last_cover(mut self, single: bool) -> Feed {
        if !single {
            self = self.out_of_nested();
        }
        self
    }
}

struct Site {
    path: String,
    act: String,
    gives: Gives,
    saving: Arc<OnceLock<ItemSaving>>,
    why_not: Option<String>,
}

/// Every stream act call in one vine, how each will be saved, and why.
#[derive(Default)]
pub(super) struct CallSites {
    sites: Vec<Site>,
    /// What can be refused, in the order the checker met it: this vine's
    /// calls that give again, and vines run by `use` that have such
    /// calls.
    owners: Vec<Owner>,
    /// Why the vine cannot be resumed, when it refused a used vine's call.
    unresumable: Option<String>,
    /// Calls inside used vines that save every item, named from here.
    used_lists: Vec<SavesEveryItem>,
}

impl CallSites {
    /// A stream act call at `path`. `once` says it runs at most once for
    /// each run of its pipe. Gives the feed of its items, when it gives
    /// them again.
    pub(super) fn call(
        &mut self,
        path: &str,
        act: &str,
        gives: Gives,
        once: bool,
        saving: Arc<OnceLock<ItemSaving>>,
    ) -> Option<Feed> {
        let index = self.sites.len();
        self.sites.push(Site {
            path: path.to_owned(),
            act: act.to_owned(),
            gives,
            saving,
            why_not: None,
        });
        if gives == Gives::OnlyNew {
            return None;
        }
        self.owners.push(Owner::Here(index));
        Some(Feed {
            owner: Owner::Here(index),
            reaches: Reaches::Values(if once { Batch::OneCall } else { Batch::Mixed }),
        })
    }

    /// A vine run by `use` at `path`. Gives a feed when it has calls saved
    /// by position, since what it emits flows on from here.
    pub(super) fn used(
        &mut self,
        path: &str,
        vine: &str,
        by_position: bool,
        saves_every_item: &[SavesEveryItem],
    ) -> Option<Feed> {
        self.used_lists
            .extend(saves_every_item.iter().map(|s| SavesEveryItem {
                path: format!("{path}>{vine}:{}", s.path),
                ..s.clone()
            }));
        if !by_position {
            return None;
        }
        let owner = Owner::Used {
            path: path.to_owned(),
            vine: vine.to_owned(),
        };
        self.owners.push(owner.clone());
        Some(Feed {
            owner,
            reaches: Reaches::Values(Batch::Mixed),
        })
    }

    /// How many calls and used vines have been met so far. A pipe notes it
    /// when it starts, so a `retry` can refuse what stands before it.
    pub(super) fn mark(&self) -> usize {
        self.owners.len()
    }

    /// A `take` at `path` that these feeds reach.
    pub(super) fn take(&mut self, path: &str, feeds: &[Feed]) {
        for feed in feeds {
            let why = format!(
                "{path} is a `take` that counts what {} gives, and after a restart it would count from the item after the position",
                self.name(&feed.owner)
            );
            self.refuse(&feed.owner, why);
        }
    }

    /// A `bufferCount` at `path` that these feeds reach.
    pub(super) fn buffer_count(&mut self, path: &str, feeds: &[Feed]) {
        for feed in feeds {
            if let Reaches::Values(Batch::Mixed) = feed.reaches {
                let why = format!(
                    "{path} is a `bufferCount` that could put what {} gives in one batch with items from another call or from elsewhere, and a restart could not rebuild that batch",
                    self.name(&feed.owner)
                );
                self.refuse(&feed.owner, why);
            }
        }
    }

    /// A `retry` at `path` that covers what was met since `mark`.
    pub(super) fn retry(&mut self, path: &str, mark: usize) {
        let refusals: Vec<(Owner, String)> = self.owners[mark..]
            .iter()
            .map(|owner| {
                let why = format!(
                    "{path} is a `retry` over {}: it drops what a failed attempt gave and tries again, so an item it dropped would look done",
                    self.name(owner)
                );
                (owner.clone(), why)
            })
            .collect();
        for (owner, why) in refusals {
            self.refuse(&owner, why);
        }
    }

    fn name(&self, owner: &Owner) -> String {
        match owner {
            Owner::Here(i) => format!("`{}` at {}", self.sites[*i].act, self.sites[*i].path),
            Owner::Used { path, vine } => format!("vine `{vine}` at {path}"),
        }
    }

    fn refuse(&mut self, owner: &Owner, why: String) {
        match owner {
            Owner::Here(i) => {
                let site = &mut self.sites[*i];
                site.why_not.get_or_insert(why);
            }
            Owner::Used { path, vine } => {
                self.unresumable.get_or_insert(format!(
                    "{path} uses vine `{vine}`, which saves a stream act by position, and {why}"
                ));
            }
        }
    }

    /// Decide how each call is saved, now that every stage is checked.
    pub(super) fn settle(self) -> Settled {
        let mut every = Vec::new();
        let mut by_position = false;
        for site in self.sites {
            let why = match (site.gives, site.why_not) {
                (Gives::OnlyNew, _) => Some(format!(
                    "`{}` does not say it can give its items again",
                    site.act
                )),
                (Gives::Again, why) => why,
            };
            let saving = match why {
                None => ItemSaving::Position,
                Some(_) => ItemSaving::Every,
            };
            // The cell is new and only this checker holds it until now.
            let _ = site.saving.set(saving);
            match why {
                None => by_position = true,
                Some(why) => every.push(SavesEveryItem {
                    path: site.path,
                    act: site.act,
                    why,
                }),
            }
        }
        by_position |= self.owners.iter().any(|o| matches!(o, Owner::Used { .. }));
        every.extend(self.used_lists);
        Settled {
            saves_every_item: every,
            by_position,
            unresumable: self.unresumable,
        }
    }
}

/// What the checker settled about a vine's stream act calls.
pub(super) struct Settled {
    pub(super) saves_every_item: Vec<SavesEveryItem>,
    /// Some call, here or in a used vine, is saved by position.
    pub(super) by_position: bool,
    pub(super) unresumable: Option<String>,
}
