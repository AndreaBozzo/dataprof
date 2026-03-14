//! Centralized report assembly for all profiling engines.
//!
//! `ReportAssembler` is the single entry point for constructing a [`ProfileReport`].
//! It replaces the scattered `QualityReport::new()` calls across parsers, engines,
//! and database connectors, centralizing quality metric calculation and confidence
//! tracking in one place.
//!
//! ## Bifurcated metrics (streaming)
//!
//! When the assembler detects a streaming context (sample size < rows processed,
//! or sampling was applied), it automatically bifurcates quality metric computation:
//!
//! - **Phase A (exact)**: Completeness and key uniqueness from global `ColumnProfile`
//!   counters (exact even for infinite streams).
//! - **Phase B (sampled)**: Consistency, accuracy, timeliness, and duplicate rows
//!   from the bounded reservoir sample.
//!
//! The resulting [`QualityAssessment`] uses [`MetricConfidence::Mixed`] to record
//! which dimensions are exact vs sampled.

use std::collections::HashMap;

use crate::analysis::MetricsCalculator;
use crate::analysis::metrics::BifurcatedResult;
use crate::types::{
    ColumnProfile, DataSource, ExecutionMetadata, MetricConfidence, ProfileReport,
    QualityAssessment, QualityMetrics,
};

/// Builder for constructing a [`ProfileReport`].
///
/// # Example
/// ```ignore
/// let report = ReportAssembler::new(data_source, execution)
///     .columns(column_profiles)
///     .with_quality_data(sample_columns)
///     .build();
/// ```
pub struct ReportAssembler {
    source: DataSource,
    execution: ExecutionMetadata,
    columns: Vec<ColumnProfile>,
    quality_data: Option<HashMap<String, Vec<String>>>,
    confidence: Option<MetricConfidence>,
    skip_quality: bool,
}

impl ReportAssembler {
    /// Create a new assembler with required source and execution metadata.
    pub fn new(source: DataSource, execution: ExecutionMetadata) -> Self {
        Self {
            source,
            execution,
            columns: Vec::new(),
            quality_data: None,
            confidence: None,
            skip_quality: false,
        }
    }

    /// Set the column profiles for this report.
    pub fn columns(mut self, columns: Vec<ColumnProfile>) -> Self {
        self.columns = columns;
        self
    }

    /// Provide sample data for quality metric calculation.
    ///
    /// If provided, `build()` will compute ISO 8000/25012 quality metrics.
    /// When the assembler detects a streaming context, it automatically uses
    /// bifurcated computation (Phase A exact + Phase B sampled).
    pub fn with_quality_data(mut self, data: HashMap<String, Vec<String>>) -> Self {
        self.quality_data = Some(data);
        self
    }

    /// Override the default metric confidence level.
    ///
    /// By default, confidence is determined automatically:
    /// - `Mixed` for streaming contexts (sample < total rows)
    /// - `Exact` when sample covers the full dataset
    pub fn with_confidence(mut self, confidence: MetricConfidence) -> Self {
        self.confidence = Some(confidence);
        self
    }

    /// Explicitly skip quality metric calculation.
    ///
    /// Use this for partial analysis APIs (schema inference, row counting)
    /// where quality metrics are not meaningful.
    pub fn skip_quality(mut self) -> Self {
        self.skip_quality = true;
        self
    }

    /// Build the final [`ProfileReport`].
    ///
    /// Quality metrics are computed if `quality_data` was provided and
    /// `skip_quality` was not called. Streaming contexts automatically
    /// use bifurcated metrics with [`MetricConfidence::Mixed`].
    pub fn build(self) -> ProfileReport {
        let quality = if self.skip_quality {
            None
        } else if let Some(data) = &self.quality_data {
            self.compute_quality(data)
        } else {
            None
        };

        ProfileReport::new(self.source, self.columns, self.execution, quality)
    }

    /// Compute quality assessment, choosing between bifurcated and uniform paths.
    fn compute_quality(&self, data: &HashMap<String, Vec<String>>) -> Option<QualityAssessment> {
        let sample_size = data.values().map(|v| v.len()).max().unwrap_or(0);
        let is_streaming = self.is_streaming_context(sample_size);

        if is_streaming {
            self.compute_bifurcated_quality(data, sample_size)
        } else {
            self.compute_uniform_quality(data)
        }
    }

    /// Detect whether this is a streaming context where bifurcation improves accuracy.
    fn is_streaming_context(&self, sample_size: usize) -> bool {
        self.execution.sampling_applied
            || (sample_size > 0 && sample_size < self.execution.rows_processed)
    }

    /// Bifurcated path: Phase A (exact from profiles) + Phase B (sampled).
    fn compute_bifurcated_quality(
        &self,
        data: &HashMap<String, Vec<String>>,
        _sample_size: usize,
    ) -> Option<QualityAssessment> {
        let calculator = MetricsCalculator::new();
        match calculator.calculate_bifurcated_metrics(data, &self.columns) {
            Ok(result) => {
                let confidence = self
                    .confidence
                    .clone()
                    .unwrap_or_else(|| self.mixed_confidence(&result));
                Some(QualityAssessment {
                    metrics: result.metrics,
                    confidence,
                })
            }
            Err(e) => {
                log::warn!("Bifurcated quality metrics calculation failed: {e}");
                None
            }
        }
    }

    /// Uniform path: all metrics from the same data source (batch engines).
    fn compute_uniform_quality(
        &self,
        data: &HashMap<String, Vec<String>>,
    ) -> Option<QualityAssessment> {
        match QualityMetrics::calculate_from_data(data, &self.columns) {
            Ok(metrics) => {
                let confidence = self.confidence.clone().unwrap_or(MetricConfidence::Exact);
                Some(QualityAssessment {
                    metrics,
                    confidence,
                })
            }
            Err(e) => {
                log::warn!("Quality metrics calculation failed: {e}");
                None
            }
        }
    }

    /// Build [`MetricConfidence::Mixed`] from bifurcated result provenance.
    fn mixed_confidence(&self, result: &BifurcatedResult) -> MetricConfidence {
        MetricConfidence::Mixed {
            exact_dimensions: result.exact_dimensions.clone(),
            sampled_dimensions: result.sampled_dimensions.clone(),
            sample_size: result.sample_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileFormat;

    fn test_source() -> DataSource {
        DataSource::File {
            path: "test.csv".to_string(),
            format: FileFormat::Csv,
            size_bytes: 1024,
            modified_at: None,
            parquet_metadata: None,
        }
    }

    #[test]
    fn test_basic_report_assembly() {
        let report =
            ReportAssembler::new(test_source(), ExecutionMetadata::new(100, 3, 50)).build();

        assert_eq!(report.execution.rows_processed, 100);
        assert!(report.quality.is_none()); // No quality data provided
        assert!(report.column_profiles.is_empty());
    }

    #[test]
    fn test_skip_quality() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);

        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(2, 1, 10))
            .with_quality_data(data)
            .skip_quality()
            .build();

        assert!(report.quality.is_none());
    }

    #[test]
    fn test_batch_produces_exact_confidence() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);

        // sample_size (2) == rows_processed (2) → batch/exact path
        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(2, 1, 10))
            .with_quality_data(data)
            .build();

        assert!(report.quality.is_some());
        let quality = report.quality.unwrap();
        assert!(matches!(quality.confidence, MetricConfidence::Exact));
    }

    #[test]
    fn test_streaming_produces_mixed_confidence() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);

        // sample_size (2) < rows_processed (1000) → streaming/bifurcated path
        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(1000, 1, 50))
            .with_quality_data(data)
            .build();

        assert!(report.quality.is_some());
        let quality = report.quality.unwrap();
        match &quality.confidence {
            MetricConfidence::Mixed {
                exact_dimensions,
                sampled_dimensions,
                sample_size,
            } => {
                assert!(exact_dimensions.contains(&"completeness".to_string()));
                assert!(exact_dimensions.contains(&"key_uniqueness".to_string()));
                assert!(sampled_dimensions.contains(&"consistency".to_string()));
                assert!(sampled_dimensions.contains(&"accuracy".to_string()));
                assert!(sampled_dimensions.contains(&"timeliness".to_string()));
                assert!(sampled_dimensions.contains(&"duplicate_rows".to_string()));
                assert_eq!(*sample_size, 2);
            }
            other => panic!("Expected Mixed confidence, got {:?}", other),
        }
    }

    #[test]
    fn test_sampling_applied_triggers_bifurcation() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);

        let execution = ExecutionMetadata::new(2, 1, 10).with_sampling(0.1);

        let report = ReportAssembler::new(test_source(), execution)
            .with_quality_data(data)
            .build();

        assert!(report.quality.is_some());
        let quality = report.quality.unwrap();
        assert!(matches!(quality.confidence, MetricConfidence::Mixed { .. }));
    }
}
