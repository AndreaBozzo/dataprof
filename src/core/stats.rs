// Statistics computation module with enhanced statistical rigor
use std::time::Duration;

// Re-export existing stats logic for backward compatibility
pub use crate::{calculate_numeric_stats, calculate_text_stats, ColumnStats};

/// Statistical configuration for rigorous benchmarking
#[derive(Debug, Clone)]
pub struct StatisticalConfig {
    /// Minimum number of samples required for statistical significance
    pub min_samples: usize,
    /// Statistical confidence level (e.g., 0.95 for 95% confidence)
    pub confidence_level: f64,
    /// Enable IQR-based outlier detection and removal
    pub outlier_detection: bool,
    /// Number of warmup iterations before actual measurement
    pub warmup_iterations: usize,
    /// Minimum measurement time per benchmark
    pub measurement_time: Duration,
}

impl StatisticalConfig {
    /// Configuration for CI/CD environments (fast but less rigorous)
    pub fn ci_config() -> Self {
        Self {
            min_samples: 10,
            confidence_level: 0.95,
            outlier_detection: true,
            warmup_iterations: 3,
            measurement_time: Duration::from_secs(10),
        }
    }

    /// Configuration for local development (more rigorous)
    pub fn local_config() -> Self {
        Self {
            min_samples: 30,
            confidence_level: 0.95,
            outlier_detection: true,
            warmup_iterations: 5,
            measurement_time: Duration::from_secs(30),
        }
    }

    /// Configuration for regression detection (highest rigor)
    pub fn regression_config() -> Self {
        Self {
            min_samples: 50,
            confidence_level: 0.99,
            outlier_detection: true,
            warmup_iterations: 10,
            measurement_time: Duration::from_secs(60),
        }
    }

    /// Custom configuration
    pub fn custom(
        min_samples: usize,
        confidence_level: f64,
        outlier_detection: bool,
        warmup_iterations: usize,
        measurement_time: Duration,
    ) -> Self {
        Self {
            min_samples,
            confidence_level,
            outlier_detection,
            warmup_iterations,
            measurement_time,
        }
    }
}

impl Default for StatisticalConfig {
    fn default() -> Self {
        // Detect if running in CI environment
        if std::env::var("CI").is_ok() {
            Self::ci_config()
        } else {
            Self::local_config()
        }
    }
}

/// Statistical sample collection and analysis
#[derive(Debug, Clone)]
pub struct StatisticalSample {
    pub values: Vec<f64>,
    pub config: StatisticalConfig,
}

impl StatisticalSample {
    pub fn new(config: StatisticalConfig) -> Self {
        Self {
            values: Vec::with_capacity(config.min_samples),
            config,
        }
    }

    /// Add a measurement value
    pub fn add_measurement(&mut self, value: f64) {
        self.values.push(value);
    }

    /// Check if we have enough samples for statistical analysis
    pub fn has_sufficient_samples(&self) -> bool {
        self.values.len() >= self.config.min_samples
    }

    /// Remove outliers using IQR method
    pub fn remove_outliers(&mut self) {
        if !self.config.outlier_detection || self.values.len() < 4 {
            return;
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let q1_idx = sorted.len() / 4;
        let q3_idx = 3 * sorted.len() / 4;
        let q1 = sorted[q1_idx];
        let q3 = sorted[q3_idx];
        let iqr = q3 - q1;
        let lower_bound = q1 - 1.5 * iqr;
        let upper_bound = q3 + 1.5 * iqr;

        self.values
            .retain(|&x| x >= lower_bound && x <= upper_bound);
    }

    /// Calculate mean of samples
    pub fn mean(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        self.values.iter().sum::<f64>() / self.values.len() as f64
    }

    /// Calculate standard deviation
    pub fn std_dev(&self) -> f64 {
        if self.values.len() < 2 {
            return 0.0;
        }
        let mean = self.mean();
        let variance = self.values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
            / (self.values.len() - 1) as f64;
        variance.sqrt()
    }

    /// Calculate confidence interval
    pub fn confidence_interval(&self) -> (f64, f64) {
        if self.values.len() < 2 {
            let mean = self.mean();
            return (mean, mean);
        }

        let mean = self.mean();
        let std_dev = self.std_dev();
        let n = self.values.len() as f64;

        // Use t-distribution for small samples
        let t_value = if n <= 30.0 {
            self.t_critical_value()
        } else {
            // Use normal distribution for large samples
            self.z_critical_value()
        };

        let margin_error = t_value * (std_dev / n.sqrt());
        (mean - margin_error, mean + margin_error)
    }

    /// Get t-critical value for t-distribution (approximation)
    fn t_critical_value(&self) -> f64 {
        let df = (self.values.len() - 1) as f64;

        // Approximation of t-critical values for common confidence levels
        match self.config.confidence_level {
            c if c >= 0.99 => {
                if df >= 30.0 {
                    2.576
                } else if df >= 15.0 {
                    2.750
                } else {
                    3.106
                }
            }
            c if c >= 0.95 => {
                if df >= 30.0 {
                    1.960
                } else if df >= 15.0 {
                    2.131
                } else {
                    2.447
                }
            }
            c if c >= 0.90 => {
                if df >= 30.0 {
                    1.645
                } else if df >= 15.0 {
                    1.753
                } else {
                    1.943
                }
            }
            _ => 1.960, // Default to 95% confidence
        }
    }

    /// Get z-critical value for normal distribution
    fn z_critical_value(&self) -> f64 {
        match self.config.confidence_level {
            c if c >= 0.99 => 2.576,
            c if c >= 0.95 => 1.960,
            c if c >= 0.90 => 1.645,
            _ => 1.960, // Default to 95% confidence
        }
    }

    /// Calculate coefficient of variation (CV)
    pub fn coefficient_of_variation(&self) -> f64 {
        let mean = self.mean();
        if mean == 0.0 {
            return 0.0;
        }
        self.std_dev() / mean * 100.0
    }

    /// Check if variance is acceptable (CV < 5%)
    pub fn is_variance_acceptable(&self) -> bool {
        self.coefficient_of_variation() < 5.0
    }

    /// Generate statistical summary
    pub fn statistical_summary(&self) -> StatisticalSummary {
        let (ci_lower, ci_upper) = self.confidence_interval();

        StatisticalSummary {
            sample_count: self.values.len(),
            mean: self.mean(),
            std_dev: self.std_dev(),
            confidence_interval: (ci_lower, ci_upper),
            confidence_level: self.config.confidence_level,
            coefficient_of_variation: self.coefficient_of_variation(),
            variance_acceptable: self.is_variance_acceptable(),
            outliers_removed: self.config.outlier_detection,
        }
    }
}

/// Statistical summary for benchmark results
#[derive(Debug, Clone)]
pub struct StatisticalSummary {
    pub sample_count: usize,
    pub mean: f64,
    pub std_dev: f64,
    pub confidence_interval: (f64, f64),
    pub confidence_level: f64,
    pub coefficient_of_variation: f64,
    pub variance_acceptable: bool,
    pub outliers_removed: bool,
}

impl StatisticalSummary {
    /// Check if this measurement is statistically significant
    pub fn is_statistically_significant(&self) -> bool {
        self.variance_acceptable && self.sample_count >= 10
    }

    /// Compare with another summary for regression detection
    pub fn is_regression(&self, baseline: &StatisticalSummary, threshold: f64) -> bool {
        if !self.is_statistically_significant() || !baseline.is_statistically_significant() {
            return false;
        }

        // Use confidence intervals for robust comparison
        let current_lower = self.confidence_interval.0;
        let baseline_upper = baseline.confidence_interval.1;

        // Regression if current lower bound is significantly higher than baseline upper bound
        current_lower > baseline_upper * (1.0 + threshold)
    }

    /// Format for display
    pub fn format_summary(&self) -> String {
        format!(
            "Mean: {:.3}s ± {:.3}s (CI: [{:.3}, {:.3}], CV: {:.1}%, n={})",
            self.mean,
            self.std_dev,
            self.confidence_interval.0,
            self.confidence_interval.1,
            self.coefficient_of_variation,
            self.sample_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistical_config_creation() {
        let ci_config = StatisticalConfig::ci_config();
        assert_eq!(ci_config.min_samples, 10);
        assert_eq!(ci_config.confidence_level, 0.95);
        assert!(ci_config.outlier_detection);

        let local_config = StatisticalConfig::local_config();
        assert_eq!(local_config.min_samples, 30);
        assert_eq!(local_config.confidence_level, 0.95);

        let regression_config = StatisticalConfig::regression_config();
        assert_eq!(regression_config.min_samples, 50);
        assert_eq!(regression_config.confidence_level, 0.99);
    }

    #[test]
    fn test_statistical_sample_basic() {
        let config = StatisticalConfig::local_config();
        let mut sample = StatisticalSample::new(config);

        assert!(!sample.has_sufficient_samples());

        // Add some measurements
        for i in 1..=30 {
            sample.add_measurement(i as f64);
        }

        assert!(sample.has_sufficient_samples());
        assert_eq!(sample.mean(), 15.5);
        assert!(sample.std_dev() > 0.0);
    }

    #[test]
    fn test_outlier_detection() {
        let config = StatisticalConfig::local_config();
        let mut sample = StatisticalSample::new(config);

        // Add normal values
        for i in 1..=20 {
            sample.add_measurement(i as f64);
        }

        // Add outliers
        sample.add_measurement(1000.0);
        sample.add_measurement(-1000.0);

        let initial_count = sample.values.len();
        sample.remove_outliers();
        let final_count = sample.values.len();

        // Should have removed the outliers
        assert!(final_count < initial_count);
        assert!(sample.values.iter().all(|&x| x >= 1.0 && x <= 20.0));
    }

    #[test]
    fn test_confidence_interval() {
        let config = StatisticalConfig::local_config();
        let mut sample = StatisticalSample::new(config);

        // Add known values
        for i in 1..=10 {
            sample.add_measurement(i as f64);
        }

        let (ci_lower, ci_upper) = sample.confidence_interval();
        let mean = sample.mean();

        // Confidence interval should contain the mean
        assert!(ci_lower <= mean);
        assert!(mean <= ci_upper);
        assert!(ci_lower < ci_upper);
    }

    #[test]
    fn test_coefficient_of_variation() {
        let config = StatisticalConfig::local_config();
        let mut sample = StatisticalSample::new(config);

        // Add values with low variation
        for _ in 0..20 {
            sample.add_measurement(10.0);
        }

        assert!(sample.coefficient_of_variation() < 1.0);
        assert!(sample.is_variance_acceptable());
    }

    #[test]
    fn test_statistical_summary() {
        let config = StatisticalConfig::local_config();
        let mut sample = StatisticalSample::new(config);

        for i in 1..=30 {
            sample.add_measurement(i as f64);
        }

        let summary = sample.statistical_summary();
        assert_eq!(summary.sample_count, 30);
        assert!(summary.mean > 0.0);
        assert!(summary.std_dev > 0.0);
        assert!(summary.confidence_level == 0.95);
    }

    #[test]
    fn test_regression_detection() {
        let config = StatisticalConfig::local_config();

        // Create baseline
        let mut baseline_sample = StatisticalSample::new(config.clone());
        for i in 1..=30 {
            baseline_sample.add_measurement(i as f64); // Mean ~15.5
        }
        let baseline_summary = baseline_sample.statistical_summary();

        // Create current (worse performance)
        let mut current_sample = StatisticalSample::new(config);
        for i in 1..=30 {
            current_sample.add_measurement((i + 10) as f64); // Mean ~25.5
        }
        let current_summary = current_sample.statistical_summary();

        // Should detect regression with 10% threshold
        assert!(current_summary.is_regression(&baseline_summary, 0.1));
        // Should not detect regression with 100% threshold
        assert!(!current_summary.is_regression(&baseline_summary, 1.0));
    }

    #[test]
    fn test_format_summary() {
        let config = StatisticalConfig::local_config();
        let mut sample = StatisticalSample::new(config);

        for i in 1..=10 {
            sample.add_measurement(i as f64);
        }

        let summary = sample.statistical_summary();
        let formatted = summary.format_summary();

        assert!(formatted.contains("Mean:"));
        assert!(formatted.contains("CI:"));
        assert!(formatted.contains("CV:"));
        assert!(formatted.contains("n=10"));
    }
}
