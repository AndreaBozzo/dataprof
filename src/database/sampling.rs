//! Database table sampling strategies for large datasets

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Configuration for database table sampling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// Sampling strategy to use
    pub strategy: SamplingStrategy,
    /// Target sample size (number of rows)
    pub sample_size: usize,
    /// Random seed for reproducible sampling (optional)
    pub seed: Option<u64>,
    /// Whether to stratify sampling by a column (optional)
    pub stratify_column: Option<String>,
}

/// Available sampling strategies for large databases
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SamplingStrategy {
    /// Simple random sampling - fastest but may skip patterns
    Random,
    /// Systematic sampling - every nth row
    Systematic,
    /// Reservoir sampling - single pass, memory efficient
    Reservoir,
    /// Stratified sampling - maintains class distribution
    Stratified,
    /// Time-based sampling - for temporal data
    Temporal { column_name: String },
    /// Multi-stage sampling - first sample tables, then rows
    MultiStage,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            strategy: SamplingStrategy::Reservoir,
            sample_size: 10000,
            seed: None,
            stratify_column: None,
        }
    }
}

impl SamplingConfig {
    /// Create a new sampling config for quick analysis
    pub fn quick_sample(sample_size: usize) -> Self {
        Self {
            strategy: SamplingStrategy::Random,
            sample_size,
            seed: Some(42), // Fixed seed for reproducibility
            stratify_column: None,
        }
    }

    /// Create a config for representative sampling
    pub fn representative_sample(sample_size: usize, stratify_column: Option<String>) -> Self {
        Self {
            strategy: if stratify_column.is_some() {
                SamplingStrategy::Stratified
            } else {
                SamplingStrategy::Systematic
            },
            sample_size,
            seed: Some(42),
            stratify_column,
        }
    }

    /// Create a config for temporal data sampling
    pub fn temporal_sample(sample_size: usize, time_column: String) -> Self {
        Self {
            strategy: SamplingStrategy::Temporal {
                column_name: time_column,
            },
            sample_size,
            seed: Some(42),
            stratify_column: None,
        }
    }

    /// Generate the appropriate SQL sampling query
    pub fn generate_sample_query(&self, base_query: &str, total_rows: u64) -> Result<String> {
        // Determine if we need sampling
        if total_rows as usize <= self.sample_size {
            return Ok(base_query.to_string());
        }

        let sampling_ratio = self.sample_size as f64 / total_rows as f64;

        match &self.strategy {
            SamplingStrategy::Random => {
                let seed = self.seed.unwrap_or(42);
                if base_query.trim().to_uppercase().starts_with("SELECT") {
                    Ok(format!(
                        "SELECT * FROM ({}) AS sample_subquery ORDER BY RANDOM({}) LIMIT {}",
                        base_query, seed, self.sample_size
                    ))
                } else {
                    Ok(format!(
                        "SELECT * FROM {} ORDER BY RANDOM({}) LIMIT {}",
                        base_query, seed, self.sample_size
                    ))
                }
            }
            SamplingStrategy::Systematic => {
                let step = (total_rows as f64 / self.sample_size as f64).ceil() as u64;
                if base_query.trim().to_uppercase().starts_with("SELECT") {
                    Ok(format!(
                        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM ({})) AS numbered WHERE rn % {} = 1",
                        base_query, step
                    ))
                } else {
                    Ok(format!(
                        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER() as rn FROM {}) AS numbered WHERE rn % {} = 1",
                        base_query, step
                    ))
                }
            }
            SamplingStrategy::Reservoir => {
                // Reservoir sampling uses TABLESAMPLE if available, otherwise falls back to random
                self.generate_tablesample_query(base_query, sampling_ratio)
            }
            SamplingStrategy::Stratified => {
                if let Some(stratify_col) = &self.stratify_column {
                    self.generate_stratified_query(base_query, stratify_col, total_rows)
                } else {
                    // Fall back to random sampling
                    self.generate_sample_query(
                        &base_query.replace(
                            &format!("{:?}", SamplingStrategy::Stratified),
                            &format!("{:?}", SamplingStrategy::Random),
                        ),
                        total_rows,
                    )
                }
            }
            SamplingStrategy::Temporal { column_name } => {
                self.generate_temporal_query(base_query, column_name, total_rows)
            }
            SamplingStrategy::MultiStage => {
                // For multi-stage, we'll use systematic sampling as base
                let mut config = self.clone();
                config.strategy = SamplingStrategy::Systematic;
                config.generate_sample_query(base_query, total_rows)
            }
        }
    }

    /// Generate a TABLESAMPLE query (PostgreSQL/SQL Server)
    fn generate_tablesample_query(&self, base_query: &str, sampling_ratio: f64) -> Result<String> {
        let percentage = (sampling_ratio * 100.0).min(100.0);

        if base_query.trim().to_uppercase().starts_with("SELECT") {
            // Complex query - use subquery approach
            let seed = self.seed.unwrap_or(42);
            Ok(format!(
                "SELECT * FROM ({}) AS sample_subquery ORDER BY RANDOM({}) LIMIT {}",
                base_query, seed, self.sample_size
            ))
        } else {
            // Simple table - can use TABLESAMPLE
            Ok(format!(
                "SELECT * FROM {} TABLESAMPLE SYSTEM ({:.2}) LIMIT {}",
                base_query, percentage, self.sample_size
            ))
        }
    }

    /// Generate stratified sampling query
    fn generate_stratified_query(
        &self,
        base_query: &str,
        stratify_col: &str,
        _total_rows: u64,
    ) -> Result<String> {
        let sample_per_stratum = self.sample_size / 10; // Assume up to 10 strata for simplicity

        if base_query.trim().to_uppercase().starts_with("SELECT") {
            Ok(format!(
                r#"
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY {} ORDER BY RANDOM()) as stratum_rn
                    FROM ({}) AS base_query
                ) stratified
                WHERE stratum_rn <= {}
                LIMIT {}
                "#,
                stratify_col, base_query, sample_per_stratum, self.sample_size
            ))
        } else {
            Ok(format!(
                r#"
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY {} ORDER BY RANDOM()) as stratum_rn
                    FROM {}
                ) stratified
                WHERE stratum_rn <= {}
                LIMIT {}
                "#,
                stratify_col, base_query, sample_per_stratum, self.sample_size
            ))
        }
    }

    /// Generate temporal sampling query
    fn generate_temporal_query(
        &self,
        base_query: &str,
        time_col: &str,
        total_rows: u64,
    ) -> Result<String> {
        // Sample evenly across time periods
        if base_query.trim().to_uppercase().starts_with("SELECT") {
            Ok(format!(
                r#"
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(ORDER BY {}) as time_rn
                    FROM ({}) AS base_query
                ) temporal
                WHERE time_rn % {} = 1
                LIMIT {}
                "#,
                time_col,
                base_query,
                (total_rows as f64 / self.sample_size as f64).ceil() as u64,
                self.sample_size
            ))
        } else {
            Ok(format!(
                r#"
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(ORDER BY {}) as time_rn
                    FROM {}
                ) temporal
                WHERE time_rn % {} = 1
                LIMIT {}
                "#,
                time_col,
                base_query,
                (total_rows as f64 / self.sample_size as f64).ceil() as u64,
                self.sample_size
            ))
        }
    }
}

/// Information about the sampling process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleInfo {
    /// Total rows in the original table/query
    pub total_rows: u64,
    /// Number of rows in the sample
    pub sampled_rows: u64,
    /// Sampling ratio (0.0 - 1.0)
    pub sampling_ratio: f64,
    /// Sampling strategy used
    pub strategy: SamplingStrategy,
    /// Whether the sample is representative
    pub is_representative: bool,
    /// Estimated confidence interval for statistics
    pub confidence_margin: f64,
}

impl SampleInfo {
    /// Create new sample info
    pub fn new(total_rows: u64, sampled_rows: u64, strategy: SamplingStrategy) -> Self {
        let sampling_ratio = if total_rows > 0 {
            sampled_rows as f64 / total_rows as f64
        } else {
            1.0
        };

        // Estimate representativeness based on sample size and strategy
        let is_representative = match strategy {
            SamplingStrategy::Systematic | SamplingStrategy::Stratified => sampled_rows >= 1000,
            SamplingStrategy::Random | SamplingStrategy::Reservoir => sampled_rows >= 500,
            SamplingStrategy::Temporal { .. } => sampled_rows >= 2000, // Temporal needs more samples
            SamplingStrategy::MultiStage => sampled_rows >= 1500,
        };

        // Calculate confidence margin (simplified)
        let confidence_margin = if sampled_rows > 0 {
            1.96 / (sampled_rows as f64).sqrt() // 95% confidence interval
        } else {
            1.0
        };

        Self {
            total_rows,
            sampled_rows,
            sampling_ratio,
            strategy,
            is_representative,
            confidence_margin,
        }
    }

    /// Get a warning message if the sample might not be representative
    pub fn get_warning(&self) -> Option<String> {
        if !self.is_representative {
            Some(format!(
                "Sample size ({}) may be too small for reliable analysis. \
                Consider increasing sample size for better representation.",
                self.sampled_rows
            ))
        } else if self.confidence_margin > 0.1 {
            Some(format!(
                "Large confidence margin ({:.2}). \
                Statistics may have high uncertainty.",
                self.confidence_margin
            ))
        } else {
            None
        }
    }

    /// Get recommended actions for improving sample quality
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.sampled_rows < 1000 {
            recommendations.push(
                "Increase sample size to at least 1000 rows for better reliability".to_string(),
            );
        }

        if self.sampling_ratio < 0.01 && self.total_rows > 100000 {
            recommendations.push(
                "Consider stratified sampling for large datasets to ensure representativeness"
                    .to_string(),
            );
        }

        match &self.strategy {
            SamplingStrategy::Random if self.total_rows > 1000000 => {
                recommendations.push(
                    "For very large datasets, consider systematic or reservoir sampling"
                        .to_string(),
                );
            }
            SamplingStrategy::Temporal { .. } if self.sampled_rows < 2000 => {
                recommendations.push(
                    "Temporal sampling requires larger samples to capture time patterns"
                        .to_string(),
                );
            }
            _ => {}
        }

        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_random_sample_query() {
        let config = SamplingConfig::quick_sample(1000);
        let query = config
            .generate_sample_query("users", 10000)
            .expect("Failed to generate sample query");

        assert!(query.contains("RANDOM"));
        assert!(query.contains("LIMIT 1000"));
    }

    #[test]
    fn test_generate_systematic_sample_query() {
        let config = SamplingConfig {
            strategy: SamplingStrategy::Systematic,
            sample_size: 1000,
            seed: Some(42),
            stratify_column: None,
        };

        let query = config
            .generate_sample_query("orders", 10000)
            .expect("Failed to generate sample query");

        assert!(query.contains("ROW_NUMBER()"));
        assert!(query.contains("% 10 = 1")); // Every 10th row
    }

    #[test]
    fn test_sample_info_calculations() {
        let info = SampleInfo::new(10000, 1000, SamplingStrategy::Random);

        assert_eq!(info.sampling_ratio, 0.1);
        assert!(info.is_representative);
        assert!(info.confidence_margin < 0.1);
    }

    #[test]
    fn test_small_sample_warning() {
        let info = SampleInfo::new(10000, 100, SamplingStrategy::Random);

        assert!(!info.is_representative);
        assert!(info.get_warning().is_some());
        assert!(!info.get_recommendations().is_empty());
    }
}
