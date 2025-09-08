use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use super::reservoir::ReservoirSampler;

#[derive(Debug, Clone)]
pub enum SamplingStrategy {
    /// No sampling - analyze all data
    None,

    /// Simple random sampling with fixed size
    Random { size: usize },

    /// Reservoir sampling for streaming data
    Reservoir { size: usize },

    /// Stratified sampling balanced by categories
    Stratified {
        key_columns: Vec<String>,
        samples_per_stratum: usize,
    },

    /// Progressive sampling - stop when confidence is reached
    Progressive {
        initial_size: usize,
        confidence_level: f64,
        max_size: usize,
    },

    /// Systematic sampling (every Nth row)
    Systematic { interval: usize },

    /// Importance sampling for anomaly detection
    Importance { weight_threshold: f64 },

    /// Multi-stage sampling (combination of strategies)
    MultiStage { stages: Vec<SamplingStrategy> },
}

/// State for advanced sampling strategies
pub struct SamplingState {
    /// Progressive sampling state
    progressive_samples: usize,
    progressive_confidence: f64,

    /// Stratified sampling state
    stratum_counts: HashMap<String, usize>,
    stratum_samples: HashMap<String, usize>,

    /// Enhanced reservoir sampler
    reservoir_sampler: Option<ReservoirSampler>,

    /// Importance sampling state
    #[allow(dead_code)] // Future use for importance sampling
    importance_scores: Vec<f64>,
}

impl SamplingState {
    pub fn new() -> Self {
        Self {
            progressive_samples: 0,
            progressive_confidence: 0.0,
            stratum_counts: HashMap::new(),
            stratum_samples: HashMap::new(),
            reservoir_sampler: None,
            importance_scores: Vec::new(),
        }
    }

    /// Initialize reservoir sampler with given capacity
    pub fn init_reservoir(&mut self, capacity: usize) {
        self.reservoir_sampler = Some(ReservoirSampler::new(capacity));
    }

    /// Get or initialize reservoir sampler
    pub fn get_or_init_reservoir(&mut self, capacity: usize) -> &mut ReservoirSampler {
        if self.reservoir_sampler.is_none() {
            self.init_reservoir(capacity);
        }
        self.reservoir_sampler
            .as_mut()
            .expect("Reservoir sampler should be initialized after init_reservoir call")
    }
}

impl Default for SamplingState {
    fn default() -> Self {
        Self::new()
    }
}

impl SamplingStrategy {
    /// Create adaptive strategy based on data characteristics
    pub fn adaptive(total_rows: Option<usize>, file_size_mb: f64) -> Self {
        match (total_rows, file_size_mb) {
            (Some(rows), size_mb) if rows <= 10_000 && size_mb < 10.0 => SamplingStrategy::None,
            (Some(rows), _) if rows <= 100_000 => SamplingStrategy::Random { size: 10_000 },
            (Some(rows), _) if rows <= 1_000_000 => SamplingStrategy::Progressive {
                initial_size: 10_000,
                confidence_level: 0.95,
                max_size: 50_000,
            },
            (_, size_mb) if size_mb > 1000.0 => SamplingStrategy::MultiStage {
                stages: vec![
                    SamplingStrategy::Systematic { interval: 100 },
                    SamplingStrategy::Progressive {
                        initial_size: 5_000,
                        confidence_level: 0.99,
                        max_size: 25_000,
                    },
                ],
            },
            _ => SamplingStrategy::Reservoir { size: 100_000 },
        }
    }

    /// Create stratified sampling strategy
    pub fn stratified(key_columns: Vec<String>, samples_per_stratum: usize) -> Self {
        Self::Stratified {
            key_columns,
            samples_per_stratum,
        }
    }

    /// Create importance sampling strategy
    pub fn importance(weight_threshold: f64) -> Self {
        Self::Importance { weight_threshold }
    }

    /// Check if row should be included in sample
    pub fn should_include(&self, row_index: usize, total_processed: usize) -> bool {
        self.should_include_with_state(row_index, total_processed, &mut SamplingState::new(), None)
    }

    /// Check if row should be included with state tracking
    pub fn should_include_with_state(
        &self,
        row_index: usize,
        total_processed: usize,
        state: &mut SamplingState,
        row_data: Option<&HashMap<String, String>>,
    ) -> bool {
        match self {
            SamplingStrategy::None => true,

            SamplingStrategy::Random { size } => {
                self.random_sample(row_index, total_processed, *size)
            }

            SamplingStrategy::Systematic { interval } => row_index % interval == 0,

            SamplingStrategy::Reservoir { size } => {
                self.reservoir_sample(row_index, total_processed, *size, state)
            }

            SamplingStrategy::Stratified {
                key_columns,
                samples_per_stratum,
            } => self.stratified_sample(row_data, key_columns, *samples_per_stratum, state),

            SamplingStrategy::Progressive {
                initial_size,
                confidence_level,
                max_size,
            } => self.progressive_sample(*initial_size, *confidence_level, *max_size, state),

            SamplingStrategy::Importance { weight_threshold } => {
                self.importance_sample(row_data, *weight_threshold)
            }

            SamplingStrategy::MultiStage { stages } => {
                // Apply all stages in sequence
                stages.iter().all(|stage| {
                    stage.should_include_with_state(row_index, total_processed, state, row_data)
                })
            }
        }
    }

    fn random_sample(&self, row_index: usize, total_processed: usize, size: usize) -> bool {
        if total_processed <= size {
            return true;
        }

        let mut hasher = DefaultHasher::new();
        row_index.hash(&mut hasher);
        let hash = hasher.finish();

        let probability = size as f64 / total_processed as f64;
        let threshold = (probability * u64::MAX as f64) as u64;

        hash < threshold
    }

    fn reservoir_sample(
        &self,
        row_index: usize,
        _total_processed: usize,
        size: usize,
        state: &mut SamplingState,
    ) -> bool {
        // Use the enhanced reservoir sampler
        let reservoir = state.get_or_init_reservoir(size);
        reservoir.process_record(row_index)
    }

    fn stratified_sample(
        &self,
        row_data: Option<&HashMap<String, String>>,
        key_columns: &[String],
        samples_per_stratum: usize,
        state: &mut SamplingState,
    ) -> bool {
        if let Some(data) = row_data {
            // Create stratum identifier from specified columns
            let stratum_id = key_columns
                .iter()
                .filter_map(|col| data.get(col))
                .cloned()
                .collect::<Vec<_>>()
                .join("|");

            // Count total rows in this stratum
            *state
                .stratum_counts
                .entry(stratum_id.to_string())
                .or_insert(0) += 1;

            // Check if we need more samples from this stratum
            let current_samples = *state.stratum_samples.get(&stratum_id).unwrap_or(&0);

            if current_samples < samples_per_stratum {
                *state.stratum_samples.entry(stratum_id).or_insert(0) += 1;
                true
            } else {
                false
            }
        } else {
            // No row data available, fall back to random sampling
            false
        }
    }

    fn progressive_sample(
        &self,
        initial_size: usize,
        confidence_level: f64,
        max_size: usize,
        state: &mut SamplingState,
    ) -> bool {
        if state.progressive_samples < initial_size {
            state.progressive_samples += 1;
            return true;
        }

        // Calculate confidence based on current sample size
        // This is a simplified confidence calculation
        let current_confidence = 1.0 - (1.0 / (state.progressive_samples as f64).sqrt());
        state.progressive_confidence = current_confidence;

        if current_confidence < confidence_level && state.progressive_samples < max_size {
            state.progressive_samples += 1;
            true
        } else {
            false
        }
    }

    fn importance_sample(
        &self,
        row_data: Option<&HashMap<String, String>>,
        weight_threshold: f64,
    ) -> bool {
        if let Some(data) = row_data {
            // Calculate importance weight based on data characteristics
            let weight = self.calculate_importance_weight(data);
            weight >= weight_threshold
        } else {
            false
        }
    }

    fn calculate_importance_weight(&self, data: &HashMap<String, String>) -> f64 {
        // Simple importance calculation based on:
        // 1. Number of non-empty values
        // 2. Diversity of values
        // 3. Presence of anomalous patterns

        let non_empty_count = data.values().filter(|v| !v.is_empty()).count();
        let total_values = data.len();

        if total_values == 0 {
            return 0.0;
        }

        let completeness = non_empty_count as f64 / total_values as f64;

        // Check for unusual patterns that might indicate anomalies
        let has_unusual_patterns = data.values().any(|v| {
            // Very long strings might be anomalous
            v.len() > 1000 ||
            // All digits might be IDs
            v.chars().all(|c| c.is_ascii_digit()) ||
            // Mixed case and special characters
            v.chars().any(|c| !c.is_ascii_alphanumeric() && !c.is_whitespace())
        });

        let anomaly_score = if has_unusual_patterns { 0.3 } else { 0.0 };

        // Combine scores
        completeness * 0.7 + anomaly_score
    }

    pub fn target_sample_size(&self) -> Option<usize> {
        match self {
            SamplingStrategy::None => None,
            SamplingStrategy::Random { size } => Some(*size),
            SamplingStrategy::Reservoir { size } => Some(*size),
            SamplingStrategy::Stratified {
                samples_per_stratum,
                ..
            } => Some(*samples_per_stratum),
            SamplingStrategy::Progressive { max_size, .. } => Some(*max_size),
            SamplingStrategy::Systematic { .. } => None,
            SamplingStrategy::Importance { .. } => None,
            SamplingStrategy::MultiStage { stages } => {
                // Return the minimum target size across all stages
                stages.iter().filter_map(|s| s.target_sample_size()).min()
            }
        }
    }

    /// Get description of the sampling strategy
    pub fn description(&self) -> String {
        match self {
            SamplingStrategy::None => "Full dataset analysis".to_string(),
            SamplingStrategy::Random { size } => format!("Random sampling ({} records)", size),
            SamplingStrategy::Reservoir { size } => {
                format!("Reservoir sampling ({} records)", size)
            }
            SamplingStrategy::Stratified {
                key_columns,
                samples_per_stratum,
            } => {
                format!(
                    "Stratified by {} ({} per stratum)",
                    key_columns.join(", "),
                    samples_per_stratum
                )
            }
            SamplingStrategy::Progressive {
                initial_size,
                confidence_level,
                max_size,
            } => {
                format!(
                    "Progressive sampling ({}-{} records, {}% confidence)",
                    initial_size,
                    max_size,
                    (confidence_level * 100.0) as u8
                )
            }
            SamplingStrategy::Systematic { interval } => {
                format!("Systematic (every {}th record)", interval)
            }
            SamplingStrategy::Importance { weight_threshold } => {
                format!("Importance sampling (weight > {:.2})", weight_threshold)
            }
            SamplingStrategy::MultiStage { stages } => {
                format!("Multi-stage ({} stages)", stages.len())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_sampling() {
        let strategy = SamplingStrategy::Random { size: 100 };
        let mut included_count = 0;

        for i in 0..1000 {
            if strategy.should_include(i, 1000) {
                included_count += 1;
            }
        }

        // Should be approximately 100 (within reasonable variance)
        assert!(included_count > 50 && included_count < 150);
    }

    #[test]
    fn test_systematic_sampling() {
        let strategy = SamplingStrategy::Systematic { interval: 10 };
        let mut state = SamplingState::new();

        for i in 0..100 {
            let included = strategy.should_include_with_state(i, i + 1, &mut state, None);
            if i % 10 == 0 {
                assert!(included);
            } else {
                assert!(!included);
            }
        }
    }

    #[test]
    fn test_progressive_sampling() {
        let strategy = SamplingStrategy::Progressive {
            initial_size: 10,
            confidence_level: 0.95,
            max_size: 50,
        };
        let mut state = SamplingState::new();
        let mut included_count = 0;

        for i in 0..100 {
            if strategy.should_include_with_state(i, i + 1, &mut state, None) {
                included_count += 1;
            }
        }

        // Should sample at least initial_size but not more than max_size
        assert!((10..=50).contains(&included_count));
    }

    #[test]
    fn test_adaptive_strategy() {
        // Small dataset - should use no sampling
        let small = SamplingStrategy::adaptive(Some(5_000), 1.0);
        matches!(small, SamplingStrategy::None);

        // Medium dataset - should use random sampling
        let medium = SamplingStrategy::adaptive(Some(50_000), 10.0);
        matches!(medium, SamplingStrategy::Random { .. });

        // Large file - should use multi-stage
        let large = SamplingStrategy::adaptive(Some(10_000_000), 2000.0);
        matches!(large, SamplingStrategy::MultiStage { .. });
    }
}
