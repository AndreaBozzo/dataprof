//! Centralized report assembly for all profiling engines.
//!
//! `ReportAssembler` is the single entry point for constructing a [`ProfileReport`].
//! It replaces the scattered report construction calls across parsers, engines,
//! and database connectors, centralizing quality metric calculation and confidence
//! tracking in one place.

use std::collections::HashMap;

use dataprof_core::{
    ColumnProfile, DataSource, DataType, ExecutionMetadata, QualityDimension, SemanticHintBinding,
    SemanticHintKind, SemanticHints,
};
use dataprof_metrics::{
    MetricConfidence, MetricsCalculator, QualityAssessment, RowDuplicateSummary,
    analysis::metrics::BifurcatedResult, compute_value_hint_bindings,
};

use crate::ProfileReport;

/// Builder for constructing a [`ProfileReport`].
pub struct ReportAssembler {
    source: DataSource,
    execution: ExecutionMetadata,
    columns: Vec<ColumnProfile>,
    quality_data: Option<HashMap<String, Vec<String>>>,
    confidence: Option<MetricConfidence>,
    skip_quality: bool,
    requested_dimensions: Option<Vec<QualityDimension>>,
    semantic_hints: SemanticHints,
    row_duplicates: Option<RowDuplicateSummary>,
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
            requested_dimensions: None,
            semantic_hints: SemanticHints::default(),
            row_duplicates: None,
        }
    }

    /// Set the column profiles for this report.
    pub fn columns(mut self, columns: Vec<ColumnProfile>) -> Self {
        self.columns = columns;
        self
    }

    /// Provide sample data for quality metric calculation.
    pub fn with_quality_data(mut self, data: HashMap<String, Vec<String>>) -> Self {
        self.quality_data = Some(data);
        self
    }

    /// Override the default metric confidence level.
    pub fn with_confidence(mut self, confidence: MetricConfidence) -> Self {
        self.confidence = Some(confidence);
        self
    }

    /// Explicitly skip quality metric calculation.
    pub fn skip_quality(mut self) -> Self {
        self.skip_quality = true;
        self
    }

    /// Set the quality dimensions to compute.
    pub fn with_requested_dimensions(mut self, dims: Vec<QualityDimension>) -> Self {
        self.requested_dimensions = Some(dims);
        self
    }

    /// Set semantic hints used by quality metrics.
    pub fn with_semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.semantic_hints = hints;
        self
    }

    /// Provide full-stream row-duplicate counts from an engine's row
    /// tracker; they supersede the sample-based duplicate scan.
    pub fn with_row_duplicates(mut self, summary: Option<RowDuplicateSummary>) -> Self {
        self.row_duplicates = summary;
        self
    }

    /// Build the final [`ProfileReport`].
    pub fn build(self) -> ProfileReport {
        let quality = if self.skip_quality {
            None
        } else if let Some(data) = &self.quality_data {
            self.compute_quality(data)
        } else {
            None
        };
        let bindings = self.compute_hint_bindings();

        ProfileReport::new(self.source, self.columns, self.execution, quality)
            .with_semantic_hint_bindings(bindings)
    }

    /// Measure how each semantic hint bound to the data.
    ///
    /// Identifier binding is structural — the hint coerces the column's type, so
    /// it is read off the column profiles and is always exact. Positive and
    /// temporal hints are value-driven; they are measured over the same data the
    /// quality metrics assessed, tagged `exact` only when that data covers every
    /// row (see [`Self::is_streaming_context`]).
    fn compute_hint_bindings(&self) -> Vec<SemanticHintBinding> {
        if self.semantic_hints.is_empty() {
            return Vec::new();
        }

        let mut bindings = Vec::new();
        for column in &self.semantic_hints.identifier_columns {
            if let Some(profile) = self.columns.iter().find(|c| &c.name == column) {
                let checked = profile.total_count.saturating_sub(profile.null_count);
                let matched = if profile.data_type == DataType::Identifier {
                    checked
                } else {
                    0
                };
                bindings.push(SemanticHintBinding {
                    column: column.clone(),
                    kind: SemanticHintKind::Identifier,
                    checked_values: checked,
                    matched_values: matched,
                    exact: true,
                });
            }
        }

        if let Some(data) = &self.quality_data {
            let sample_size = data.values().map(|v| v.len()).max().unwrap_or(0);
            let exact = !self.is_streaming_context(sample_size);
            bindings.extend(compute_value_hint_bindings(
                data,
                &self.semantic_hints,
                exact,
            ));
        }

        bindings
    }

    fn compute_quality(&self, data: &HashMap<String, Vec<String>>) -> Option<QualityAssessment> {
        let sample_size = data.values().map(|v| v.len()).max().unwrap_or(0);
        let is_streaming = self.is_streaming_context(sample_size);

        if is_streaming {
            self.compute_bifurcated_quality(data)
        } else {
            self.compute_uniform_quality(data)
        }
    }

    fn is_streaming_context(&self, sample_size: usize) -> bool {
        self.execution.sampling_applied
            || (sample_size > 0 && sample_size < self.execution.rows_processed)
    }

    fn compute_bifurcated_quality(
        &self,
        data: &HashMap<String, Vec<String>>,
    ) -> Option<QualityAssessment> {
        let calculator = MetricsCalculator::new();
        match calculator.calculate_bifurcated_metrics_with_all_semantic_hints(
            data,
            &self.columns,
            self.requested_dimensions.as_deref(),
            &self.semantic_hints,
            self.row_duplicates,
        ) {
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
            Err(error) => {
                log::warn!("Bifurcated quality metrics calculation failed: {error}");
                None
            }
        }
    }

    fn compute_uniform_quality(
        &self,
        data: &HashMap<String, Vec<String>>,
    ) -> Option<QualityAssessment> {
        let calculator = MetricsCalculator::new();
        match calculator.calculate_comprehensive_metrics_with_all_semantic_hints(
            data,
            &self.columns,
            self.requested_dimensions.as_deref(),
            &self.semantic_hints,
            self.row_duplicates,
        ) {
            Ok(metrics) => {
                let confidence = self.confidence.clone().unwrap_or(MetricConfidence::Exact);
                Some(QualityAssessment {
                    metrics,
                    confidence,
                })
            }
            Err(error) => {
                log::warn!("Quality metrics calculation failed: {error}");
                None
            }
        }
    }

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
    use dataprof_core::FileFormat;

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
        assert!(report.quality.is_none());
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
                // No key column exists in this fixture, so key_uniqueness
                // carries no signal and must not be claimed as exact.
                assert!(!exact_dimensions.contains(&"key_uniqueness".to_string()));
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
    fn test_streaming_exact_row_duplicates_have_exact_provenance() {
        let data = HashMap::from([("col".to_string(), vec!["a".to_string(), "b".to_string()])]);

        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(1000, 1, 50))
            .with_quality_data(data)
            .with_row_duplicates(Some(RowDuplicateSummary {
                duplicate_rows: 25,
                rows_checked: 1000,
                approximate: false,
            }))
            .build();

        let quality = report.quality.expect("quality assessment");
        let uniqueness = quality.metrics.uniqueness.expect("uniqueness metrics");
        assert_eq!(uniqueness.duplicate_rows, 25);
        assert_eq!(uniqueness.rows_checked, 1000);
        assert!(!uniqueness.duplicate_rows_approximate);
        match quality.confidence {
            MetricConfidence::Mixed {
                exact_dimensions,
                sampled_dimensions,
                ..
            } => {
                assert!(exact_dimensions.contains(&"duplicate_rows".to_string()));
                assert!(!sampled_dimensions.contains(&"duplicate_rows".to_string()));
            }
            other => panic!("Expected Mixed confidence, got {other:?}"),
        }
    }

    #[test]
    fn test_streaming_approximate_row_duplicates_have_sampled_provenance() {
        let data = HashMap::from([("col".to_string(), vec!["a".to_string(), "b".to_string()])]);

        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(20_000, 1, 50))
            .with_quality_data(data)
            .with_row_duplicates(Some(RowDuplicateSummary {
                duplicate_rows: 500,
                rows_checked: 20_000,
                approximate: true,
            }))
            .build();

        let quality = report.quality.expect("quality assessment");
        let uniqueness = quality.metrics.uniqueness.expect("uniqueness metrics");
        assert!(uniqueness.duplicate_rows_approximate);
        match quality.confidence {
            MetricConfidence::Mixed {
                exact_dimensions,
                sampled_dimensions,
                ..
            } => {
                assert!(!exact_dimensions.contains(&"duplicate_rows".to_string()));
                assert!(sampled_dimensions.contains(&"duplicate_rows".to_string()));
            }
            other => panic!("Expected Mixed confidence, got {other:?}"),
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
