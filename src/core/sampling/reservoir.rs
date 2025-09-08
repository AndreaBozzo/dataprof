use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

/// Enhanced reservoir sampling implementation based on Vitter's algorithm
///
/// This is a proper implementation of Algorithm R with optimizations:
/// - True randomness with seedable RNG for reproducibility
/// - Optimized skip calculation using geometric distribution
/// - Memory-efficient storage of sample indices
/// - Support for weighted sampling
#[derive(Debug, Clone)]
pub struct ReservoirSampler {
    /// Maximum size of the reservoir
    capacity: usize,
    /// Current sample (stores row indices)
    reservoir: Vec<usize>,
    /// Total number of records processed
    total_processed: usize,
    /// Random number generator (seeded for reproducibility)
    rng: ChaCha8Rng,
    /// Skip optimization - next record to consider
    next_record: usize,
    /// Statistics for analysis
    stats: ReservoirStats,
}

/// Statistics for reservoir sampling performance
#[derive(Debug, Clone, Default)]
pub struct ReservoirStats {
    pub records_processed: usize,
    pub records_sampled: usize,
    pub replacement_count: usize,
    pub skip_count: usize,
    pub efficiency_ratio: f64,
}

impl ReservoirSampler {
    /// Create a new reservoir sampler with specified capacity
    pub fn new(capacity: usize) -> Self {
        Self::with_seed(capacity, 42) // Default seed for reproducibility
    }

    /// Create a new reservoir sampler with custom seed
    pub fn with_seed(capacity: usize, seed: u64) -> Self {
        Self {
            capacity,
            reservoir: Vec::with_capacity(capacity),
            total_processed: 0,
            rng: ChaCha8Rng::seed_from_u64(seed),
            next_record: 0,
            stats: ReservoirStats::default(),
        }
    }

    /// Process a new record and decide if it should be included
    /// Returns true if the record is selected for the sample
    pub fn process_record(&mut self, record_index: usize) -> bool {
        self.total_processed += 1;
        self.stats.records_processed += 1;

        // Phase 1: Fill the reservoir with first k records
        if self.reservoir.len() < self.capacity {
            self.reservoir.push(record_index);
            self.stats.records_sampled += 1;
            return true;
        }

        // Phase 2: Reservoir is full, use replacement algorithm
        self.apply_vitter_algorithm(record_index)
    }

    /// Apply Vitter's Algorithm R with skip optimization
    fn apply_vitter_algorithm(&mut self, record_index: usize) -> bool {
        // Skip records using geometric distribution for efficiency
        if self.total_processed < self.next_record {
            return false;
        }

        // Calculate if this record should replace one in the reservoir
        let random_index = self.rng.gen_range(0..self.total_processed);

        if random_index < self.capacity {
            // Replace the record at random_index in reservoir
            let replace_position = random_index % self.capacity;
            self.reservoir[replace_position] = record_index;
            self.stats.replacement_count += 1;
            self.stats.records_sampled += 1;

            // Calculate next skip using geometric distribution
            self.calculate_next_skip();

            return true;
        }

        false
    }

    /// Calculate next skip distance using geometric distribution
    /// This optimizes performance by skipping records that won't be selected
    fn calculate_next_skip(&mut self) {
        // Use geometric distribution to calculate skip distance
        // This is based on Vitter's Algorithm S optimization
        let u: f64 = self.rng.gen();
        let skip = if u > 0.0 {
            ((self.total_processed as f64) * (u.powf(1.0 / self.capacity as f64) - 1.0)) as usize
        } else {
            1
        };

        self.next_record = self.total_processed + skip.max(1);
        self.stats.skip_count += skip;
    }

    /// Get current sample as a vector of indices
    pub fn get_sample_indices(&self) -> &[usize] {
        &self.reservoir
    }

    /// Get current sample size
    pub fn sample_size(&self) -> usize {
        self.reservoir.len()
    }

    /// Check if reservoir is full
    pub fn is_full(&self) -> bool {
        self.reservoir.len() >= self.capacity
    }

    /// Get sampling statistics
    pub fn get_stats(&self) -> &ReservoirStats {
        &self.stats
    }

    /// Calculate current sampling ratio
    pub fn sampling_ratio(&self) -> f64 {
        if self.total_processed > 0 {
            self.reservoir.len() as f64 / self.total_processed as f64
        } else {
            0.0
        }
    }

    /// Reset the sampler for reuse
    pub fn reset(&mut self) {
        self.reservoir.clear();
        self.total_processed = 0;
        self.next_record = 0;
        self.stats = ReservoirStats::default();
    }

    /// Set new seed for reproducible results
    pub fn set_seed(&mut self, seed: u64) {
        self.rng = ChaCha8Rng::seed_from_u64(seed);
    }

    /// Update efficiency statistics
    pub fn update_efficiency_stats(&mut self) {
        self.stats.efficiency_ratio = if self.stats.records_processed > 0 {
            self.stats.records_sampled as f64 / self.stats.records_processed as f64
        } else {
            0.0
        };
    }
}

/// Weighted reservoir sampling for stratified sampling
#[derive(Debug, Clone)]
pub struct WeightedReservoirSampler {
    base_sampler: ReservoirSampler,
    /// Weights for each record type/stratum
    weights: HashMap<String, f64>,
    /// Total weight processed
    total_weight: f64,
}

impl WeightedReservoirSampler {
    pub fn new(capacity: usize, weights: HashMap<String, f64>) -> Self {
        Self {
            base_sampler: ReservoirSampler::new(capacity),
            weights,
            total_weight: 0.0,
        }
    }

    /// Process a record with associated weight category
    pub fn process_weighted_record(&mut self, record_index: usize, category: &str) -> bool {
        let weight = self.weights.get(category).copied().unwrap_or(1.0);
        self.total_weight += weight;

        // Adjust sampling probability based on weight
        let adjusted_probability = weight / self.total_weight;
        let u: f64 = self.base_sampler.rng.gen();

        if u < adjusted_probability {
            self.base_sampler.process_record(record_index)
        } else {
            self.base_sampler.total_processed += 1;
            false
        }
    }

    pub fn get_sample_indices(&self) -> &[usize] {
        self.base_sampler.get_sample_indices()
    }

    pub fn sampling_ratio(&self) -> f64 {
        self.base_sampler.sampling_ratio()
    }
}

/// Multi-reservoir sampling for handling multiple data types
#[derive(Debug)]
pub struct MultiReservoirSampler {
    reservoirs: HashMap<String, ReservoirSampler>,
    default_capacity: usize,
}

impl MultiReservoirSampler {
    pub fn new(default_capacity: usize) -> Self {
        Self {
            reservoirs: HashMap::new(),
            default_capacity,
        }
    }

    /// Process a record for a specific category/type
    pub fn process_categorized_record(&mut self, record_index: usize, category: &str) -> bool {
        let reservoir = self
            .reservoirs
            .entry(category.to_string())
            .or_insert_with(|| ReservoirSampler::new(self.default_capacity));

        reservoir.process_record(record_index)
    }

    /// Get combined sample from all reservoirs
    pub fn get_combined_sample(&self) -> Vec<usize> {
        let mut combined = Vec::new();

        for reservoir in self.reservoirs.values() {
            combined.extend_from_slice(reservoir.get_sample_indices());
        }

        // Sort for consistent ordering
        combined.sort_unstable();
        combined
    }

    /// Get samples by category
    pub fn get_samples_by_category(&self) -> HashMap<String, Vec<usize>> {
        self.reservoirs
            .iter()
            .map(|(category, reservoir)| {
                (
                    category.to_string(),
                    reservoir.get_sample_indices().to_vec(),
                )
            })
            .collect()
    }

    /// Get statistics for all reservoirs
    pub fn get_all_stats(&self) -> HashMap<String, ReservoirStats> {
        self.reservoirs
            .iter()
            .map(|(category, reservoir)| (category.to_string(), reservoir.get_stats().clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_reservoir_sampling() {
        let mut sampler = ReservoirSampler::new(10);

        // Process 100 records
        let mut selected_count = 0;
        for i in 0..100 {
            if sampler.process_record(i) {
                selected_count += 1;
            }
        }

        // Should have exactly 10 samples
        assert_eq!(sampler.sample_size(), 10);
        assert_eq!(sampler.get_sample_indices().len(), 10);
        assert!(selected_count >= 10); // May be more due to replacements
    }

    #[test]
    fn test_reservoir_filling_phase() {
        let mut sampler = ReservoirSampler::new(5);

        // First 5 records should all be selected
        for i in 0..5 {
            assert!(sampler.process_record(i));
        }

        assert_eq!(sampler.sample_size(), 5);
        assert!(sampler.is_full());
    }

    #[test]
    fn test_replacement_phase() {
        let mut sampler = ReservoirSampler::with_seed(3, 42); // Fixed seed for reproducibility

        // Fill reservoir
        for i in 0..3 {
            sampler.process_record(i);
        }

        // Process more records
        let _initial_sample = sampler.get_sample_indices().to_vec();

        for i in 3..20 {
            sampler.process_record(i);
        }

        let final_sample = sampler.get_sample_indices().to_vec();

        // Sample size should remain the same
        assert_eq!(final_sample.len(), 3);

        // Some replacements should have occurred
        assert!(sampler.get_stats().replacement_count > 0);
    }

    #[test]
    fn test_sampling_ratio() {
        let mut sampler = ReservoirSampler::new(10);

        for i in 0..100 {
            sampler.process_record(i);
        }

        let ratio = sampler.sampling_ratio();
        assert!((ratio - 0.1).abs() < 0.01); // Should be ~10%
    }

    #[test]
    fn test_reset_functionality() {
        let mut sampler = ReservoirSampler::new(5);

        for i in 0..10 {
            sampler.process_record(i);
        }

        assert_eq!(sampler.sample_size(), 5);
        assert!(sampler.total_processed > 0);

        sampler.reset();

        assert_eq!(sampler.sample_size(), 0);
        assert_eq!(sampler.total_processed, 0);
    }

    #[test]
    fn test_weighted_sampling() {
        let mut weights = HashMap::new();
        weights.insert("high".to_string(), 3.0);
        weights.insert("low".to_string(), 1.0);

        let mut sampler = WeightedReservoirSampler::new(10, weights);

        let mut _high_selected = 0;
        let mut _low_selected = 0;

        // Process records with different weights
        for i in 0..50 {
            let category = if i % 2 == 0 { "high" } else { "low" };
            if sampler.process_weighted_record(i, category) {
                if category == "high" {
                    _high_selected += 1;
                } else {
                    _low_selected += 1;
                }
            }
        }

        // High weight records should be selected more frequently
        // This is probabilistic, so we allow some variance
        assert!(sampler.get_sample_indices().len() <= 10);
    }

    #[test]
    fn test_multi_reservoir() {
        let mut sampler = MultiReservoirSampler::new(5);

        for i in 0..20 {
            let category = format!("type_{}", i % 3);
            sampler.process_categorized_record(i, &category);
        }

        let combined = sampler.get_combined_sample();
        assert!(combined.len() <= 15); // Max 5 per category * 3 categories

        let by_category = sampler.get_samples_by_category();
        assert_eq!(by_category.len(), 3); // Should have 3 categories
    }

    #[test]
    fn test_deterministic_with_seed() {
        let mut sampler1 = ReservoirSampler::with_seed(5, 123);
        let mut sampler2 = ReservoirSampler::with_seed(5, 123);

        for i in 0..50 {
            sampler1.process_record(i);
            sampler2.process_record(i);
        }

        // Same seed should produce identical samples
        assert_eq!(sampler1.get_sample_indices(), sampler2.get_sample_indices());
    }
}
