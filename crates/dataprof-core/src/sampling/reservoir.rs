use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Seeded slot chooser for Algorithm R over a fixed-capacity reservoir.
///
/// The caller owns the reservoir and its rows; this type only decides which
/// slot a newly seen row replaces, from a seeded generator so the same input
/// always yields the same sample.
#[derive(Debug, Clone)]
pub struct ReservoirSampler {
    capacity: usize,
    rng: ChaCha8Rng,
}

impl ReservoirSampler {
    /// A sampler for `capacity` slots with the default seed.
    pub fn new(capacity: usize) -> Self {
        Self::seed(capacity, 42)
    }

    /// A sampler for `capacity` slots with a custom seed.
    pub fn seed(capacity: usize, seed: u64) -> Self {
        Self {
            capacity,
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// Which reservoir slot row number `seen` should replace, if any.
    ///
    /// Algorithm R over a full reservoir: the `seen`-th row of the stream
    /// (1-based) replaces a uniformly chosen member with probability
    /// `capacity / seen`, which leaves every row seen so far equally likely to
    /// be in the sample. Returns `None` while the reservoir is still filling
    /// and when the row is not selected.
    pub fn replacement_slot(&mut self, seen: usize) -> Option<usize> {
        if self.capacity == 0 || seen <= self.capacity {
            return None;
        }
        let draw = self.rng.random_range(0..seen);
        (draw < self.capacity).then_some(draw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_replacement_while_the_reservoir_fills() {
        let mut sampler = ReservoirSampler::new(3);
        assert!((1..=3).all(|seen| sampler.replacement_slot(seen).is_none()));
    }

    #[test]
    fn slots_stay_inside_the_reservoir_and_repeat_for_a_seed() {
        let draws = |seed| {
            let mut sampler = ReservoirSampler::seed(5, seed);
            (6..500)
                .map(|seen| sampler.replacement_slot(seen))
                .collect::<Vec<_>>()
        };
        let first = draws(7);
        assert!(first.iter().flatten().all(|&slot| slot < 5));
        assert!(first.iter().any(Option::is_some));
        assert_eq!(first, draws(7));
    }
}
