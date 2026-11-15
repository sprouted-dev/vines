//! A kill sweep: the kill plans a restart test runs, cut into shards so one
//! sweep runs as several tests at once. The shards of a sweep tile it: each
//! plan is in exactly one of them. Only the test files with sweeps declare
//! this module, so each uses all of it.

/// One kill plan: kill the run after each count of events in it, then run
/// it to the end.
pub(crate) type Kills = Vec<usize>;

/// Every single kill point in `singles`, then every pair of a kill point in
/// `firsts` followed by one in `seconds`.
pub(crate) fn singles_then_pairs(
    singles: impl IntoIterator<Item = usize>,
    firsts: impl IntoIterator<Item = usize>,
    seconds: impl IntoIterator<Item = usize>,
) -> Vec<Kills> {
    let seconds: Vec<usize> = seconds.into_iter().collect();
    let singles = singles.into_iter().map(|k| vec![k]);
    let pairs = firsts
        .into_iter()
        .flat_map(|k1| seconds.iter().map(move |&k2| vec![k1, k2]));
    singles.chain(pairs).collect()
}

/// The part of a sweep one test runs: every `of`-th plan, from the
/// `index`-th. Striding, rather than cutting the sweep in runs, spreads the
/// long plans (late kill points) over every shard.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Shard {
    index: usize,
    of: usize,
}

impl Shard {
    /// Shard `index` of `of`, counted from 0.
    pub(crate) const fn new(index: usize, of: usize) -> Shard {
        assert!(index < of, "a shard index is below the shard count");
        Shard { index, of }
    }

    /// This shard's plans.
    pub(crate) fn take(self, plans: Vec<Kills>) -> Vec<Kills> {
        plans
            .into_iter()
            .skip(self.index)
            .step_by(self.of)
            .collect()
    }
}

/// One test per shard of a sweep, in a module named for what the sweep
/// shows: `$name::shard_0`, `$name::shard_1`, and so on, each calling
/// `$check(shard)`, an `async fn(Shard)` beside the macro call. The shards
/// must be listed 0, 1, 2, ... in order, which a constant checks as it
/// compiles, so the tests between them run every shard of the sweep once.
macro_rules! sweep_tests {
    ($(#[$doc:meta])* $name:ident: $check:ident, $($shard:ident = $index:literal),+ $(,)?) => {
        $(#[$doc])*
        mod $name {
            use crate::sweep::Shard;

            const INDICES: &[usize] = &[$($index),+];
            const _: () = {
                let mut i = 0;
                while i < INDICES.len() {
                    assert!(INDICES[i] == i, "shards are listed 0, 1, 2, ... in order");
                    i += 1;
                }
            };

            $(
                #[tokio::test(start_paused = true)]
                async fn $shard() {
                    super::$check(Shard::new($index, INDICES.len())).await;
                }
            )+
        }
    };
}
pub(crate) use sweep_tests;
