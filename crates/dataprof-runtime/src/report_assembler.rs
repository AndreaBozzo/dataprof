//! Centralized report assembly for all profiling engines.
//!
//! `ReportAssembler` is the single entry point for constructing a [`ProfileReport`].
//! It replaces the scattered report construction calls across parsers, engines,
//! and database connectors, centralizing quality metric calculation and confidence
//! tracking in one place.

use std::collections::HashMap;

use dataprof_core::{
    AnalysisOptions, ColumnProfile, DataProfilerError, DataSource, DataType, ExecutionMetadata,
    QualityDimension, SemanticHintBinding, SemanticHintKind, SemanticHints,
};
use dataprof_metrics::{
    MetricConfidence, MetricsCalculator, QualityAssessment, RowCompletenessSummary,
    RowDuplicateSummary, analysis::metrics::BifurcatedResult, compute_value_hint_bindings,
};

use crate::{ProfileReport, QualityAnalysisStatus};

/// Builder for constructing a [`ProfileReport`].
pub struct ReportAssembler {
    source: DataSource,
    execution: ExecutionMetadata,
    columns: Vec<ColumnProfile>,
    quality_data: Option<HashMap<String, Vec<String>>>,
    confidence: Option<MetricConfidence>,
    /// Why quality will not be computed, when it will not be. `None` means the
    /// caller asked for it.
    skip: Option<QualityAnalysisStatus>,
    requested_dimensions: Option<Vec<QualityDimension>>,
    semantic_hints: SemanticHints,
    exact_value_hint_bindings: Option<Vec<SemanticHintBinding>>,
    row_duplicates: Option<RowDuplicateSummary>,
    row_completeness: Option<RowCompletenessSummary>,
    /// Test seam for the failure branch. No input path can currently make
    /// `MetricsCalculator` return `Err`: both of its error constructions sit
    /// behind an emptiness check that has already returned by then. The branch
    /// still has to be handled, because the calculator is a public API whose
    /// signature says it can fail, so this is how the handling is exercised.
    #[cfg(test)]
    forced_quality_failure: Option<String>,
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
            skip: None,
            requested_dimensions: None,
            semantic_hints: SemanticHints::default(),
            exact_value_hint_bindings: None,
            row_duplicates: None,
            row_completeness: None,
            #[cfg(test)]
            forced_quality_failure: None,
        }
    }

    /// Make the quality computation fail with `message`. See
    /// [`forced_quality_failure`](Self::forced_quality_failure).
    #[cfg(test)]
    fn force_quality_failure(mut self, message: &str) -> Self {
        self.forced_quality_failure = Some(message.to_string());
        self
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

    /// Explicitly skip quality metric calculation because the caller did not
    /// ask for it.
    ///
    /// Use [`skip_quality_no_data`](Self::skip_quality_no_data) when quality
    /// was wanted but the source held nothing to measure. The report states
    /// which of the two happened, so they must not be conflated here.
    pub fn skip_quality(mut self) -> Self {
        self.skip = Some(QualityAnalysisStatus::NotRequested);
        self
    }

    /// Skip quality metric calculation because there was nothing to compute
    /// from: an empty source, or a path that retained no sample.
    ///
    /// A caller that already deselected quality keeps that answer. Not asking
    /// is the more specific reason, and it does not stop being true when the
    /// source also turns out to be empty.
    pub fn skip_quality_no_data(mut self) -> Self {
        self.skip.get_or_insert(QualityAnalysisStatus::NoData);
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

    /// Apply the caller's analysis selection: requested dimensions, semantic
    /// hints, column projection, and whether quality is computed at all.
    ///
    /// Callers still pass their quality sample with
    /// [`with_quality_data`](Self::with_quality_data); this decides whether it is
    /// used. Deselecting the quality pack leaves the report with no quality
    /// object rather than an assessment with every dimension absent — "not
    /// analyzed" and "analyzed, nothing found" are different answers.
    pub fn with_analysis_options(mut self, options: &AnalysisOptions) -> Self {
        self.skip = (!options.include_quality()).then_some(QualityAnalysisStatus::NotRequested);
        self.semantic_hints = options.semantic_hints().clone();
        self.requested_dimensions = options.quality_dimensions().map(<[_]>::to_vec);
        if options.has_column_projection() && self.skip.is_none() {
            // Completeness and uniqueness both contain row-level measurements.
            // Those measurements have a different meaning after projecting a
            // row, and the current report schema cannot label only the row-level
            // fields as projected. Withhold the two dimensions instead of
            // publishing plausible numbers under full-row names.
            let mut dimensions = self
                .requested_dimensions
                .take()
                .unwrap_or_else(QualityDimension::all);
            dimensions.retain(|dimension| {
                !matches!(
                    dimension,
                    QualityDimension::Completeness | QualityDimension::Uniqueness
                )
            });
            if dimensions.is_empty() {
                self.skip = Some(QualityAnalysisStatus::WithheldByProjection);
            }
            self.requested_dimensions = Some(dimensions);
        }
        self
    }

    /// Provide full-stream evidence for value-driven semantic hints.
    ///
    /// Streaming engines should pass their bounded-memory accumulator output;
    /// it supersedes evidence recomputed from the retained quality sample.
    pub fn with_exact_value_hint_bindings(mut self, bindings: Vec<SemanticHintBinding>) -> Self {
        self.exact_value_hint_bindings = Some(bindings);
        self
    }

    /// Provide full-stream row-duplicate counts from an engine's row
    /// tracker; they supersede the sample-based duplicate scan.
    pub fn with_row_duplicates(mut self, summary: Option<RowDuplicateSummary>) -> Self {
        self.row_duplicates = summary;
        self
    }

    /// Provide full-stream complete-record counts from an engine's row
    /// tracker. Without them `complete_records_ratio` can only be bounded
    /// from below, since per-column null totals cannot tell whether two
    /// nulls fell in the same record.
    pub fn with_row_completeness(mut self, summary: Option<RowCompletenessSummary>) -> Self {
        self.row_completeness = summary;
        self
    }

    /// Build the final [`ProfileReport`].
    ///
    /// This does not fail. A quality computation that returns an error leaves
    /// the report without an assessment, exactly as a run that never asked for
    /// one does, so the report records which of the two happened in
    /// [`ProfileReport::quality_status`] rather than discarding the column
    /// profiles that did compute.
    pub fn build(self) -> ProfileReport {
        let (quality, status) = match &self.skip {
            Some(reason) => (None, reason.clone()),
            None => match &self.quality_data {
                Some(data) => self.compute_quality(data),
                None => (None, QualityAnalysisStatus::NoData),
            },
        };
        let bindings = self.compute_hint_bindings();

        ProfileReport::new(self.source, self.columns, self.execution, quality)
            .with_quality_status(status)
            .with_semantic_hint_bindings(bindings)
    }

    /// Measure how each semantic hint bound to the data.
    ///
    /// Identifier binding is structural — the hint coerces the column's type, so
    /// it is read off the column profiles and is always exact. Positive and
    /// temporal hints are value-driven. Streaming engines provide exact
    /// full-stream counts; callers without those accumulators fall back to the
    /// quality data and tag the result exact only when it covers every row.
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

        if let Some(exact) = &self.exact_value_hint_bindings {
            let full_coverage = self.execution.source_exhausted && !self.execution.sampling_applied;
            bindings.extend(exact.iter().cloned().map(|mut binding| {
                // Engine accumulators cover every value they processed. That
                // is every source row for ordinary reservoir-backed streaming,
                // but not when row-level sampling skipped records or an early
                // stop left part of the source unread.
                binding.exact &= full_coverage;
                binding
            }));
        } else if let Some(data) = &self.quality_data {
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

    fn compute_quality(
        &self,
        data: &HashMap<String, Vec<String>>,
    ) -> (Option<QualityAssessment>, QualityAnalysisStatus) {
        #[cfg(test)]
        if let Some(message) = &self.forced_quality_failure {
            return Self::failed(DataProfilerError::MetricsCalculationError {
                message: message.clone(),
            });
        }

        let sample_size = data.values().map(|v| v.len()).max().unwrap_or(0);
        let is_streaming = self.is_streaming_context(sample_size);

        if is_streaming {
            self.compute_bifurcated_quality(data)
        } else {
            self.compute_uniform_quality(data)
        }
    }

    /// Record a quality computation that was requested, attempted, and failed.
    fn failed(error: DataProfilerError) -> (Option<QualityAssessment>, QualityAnalysisStatus) {
        // Still logged, so an operator watching a run sees it happen. The
        // report carries it too: a log line is not part of the output a
        // consumer reads back, and absence alone reads as a clean skip.
        log::warn!("Quality metrics calculation failed: {error}");
        (
            None,
            QualityAnalysisStatus::Failed {
                error: error.to_string(),
            },
        )
    }

    fn is_streaming_context(&self, sample_size: usize) -> bool {
        self.execution.sampling_applied
            || (sample_size > 0 && sample_size < self.execution.rows_processed)
    }

    fn compute_bifurcated_quality(
        &self,
        data: &HashMap<String, Vec<String>>,
    ) -> (Option<QualityAssessment>, QualityAnalysisStatus) {
        let calculator = MetricsCalculator::new().with_row_completeness(self.row_completeness);
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
                (
                    Some(QualityAssessment::new(result.metrics, confidence)),
                    QualityAnalysisStatus::Computed,
                )
            }
            Err(error) => Self::failed(error),
        }
    }

    fn compute_uniform_quality(
        &self,
        data: &HashMap<String, Vec<String>>,
    ) -> (Option<QualityAssessment>, QualityAnalysisStatus) {
        let calculator = MetricsCalculator::new().with_row_completeness(self.row_completeness);
        match calculator.calculate_comprehensive_metrics_with_all_semantic_hints(
            data,
            &self.columns,
            self.requested_dimensions.as_deref(),
            &self.semantic_hints,
            self.row_duplicates,
        ) {
            Ok(metrics) => {
                let confidence = self.confidence.clone().unwrap_or(MetricConfidence::Exact);
                (
                    Some(QualityAssessment::new(metrics, confidence)),
                    QualityAnalysisStatus::Computed,
                )
            }
            Err(error) => Self::failed(error),
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
    use dataprof_core::{FileFormat, MetricPack, TruncationReason};

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

    /// The defect: a quality computation that was requested and failed used to
    /// produce a report byte-identical to one that never asked for quality.
    #[test]
    fn failed_quality_computation_is_not_a_skip() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);

        let failed = ReportAssembler::new(test_source(), ExecutionMetadata::new(2, 1, 10))
            .with_quality_data(data.clone())
            .force_quality_failure("uniqueness accumulator disagreed with the row count")
            .build();
        let skipped = ReportAssembler::new(test_source(), ExecutionMetadata::new(2, 1, 10))
            .with_quality_data(data)
            .skip_quality()
            .build();

        assert!(failed.quality.is_none());
        assert!(skipped.quality.is_none());
        assert_eq!(
            failed.quality_status,
            QualityAnalysisStatus::Failed {
                error: "Metrics calculation failed: uniqueness accumulator disagreed with the \
                        row count"
                    .to_string(),
            }
        );
        assert_eq!(skipped.quality_status, QualityAnalysisStatus::NotRequested);
        assert_ne!(failed.quality_status, skipped.quality_status);
    }

    #[test]
    fn computed_quality_reports_computed() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);

        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(2, 1, 10))
            .with_quality_data(data)
            .build();

        assert!(report.quality.is_some());
        assert_eq!(report.quality_status, QualityAnalysisStatus::Computed);
    }

    #[test]
    fn absent_quality_data_reports_no_data() {
        let explicit = ReportAssembler::new(test_source(), ExecutionMetadata::new(0, 0, 1))
            .skip_quality_no_data()
            .build();
        let implicit = ReportAssembler::new(test_source(), ExecutionMetadata::new(0, 0, 1)).build();

        assert_eq!(explicit.quality_status, QualityAnalysisStatus::NoData);
        assert_eq!(implicit.quality_status, QualityAnalysisStatus::NoData);
    }

    /// Not asking is the more specific reason, and an empty source does not
    /// make it untrue. Builder order must not change the answer either way.
    #[test]
    fn deselected_quality_survives_an_empty_source() {
        let deselected =
            AnalysisOptions::default().with_metric_packs(Some(vec![MetricPack::Schema]));
        let options_first = ReportAssembler::new(test_source(), ExecutionMetadata::new(0, 0, 1))
            .with_analysis_options(&deselected)
            .skip_quality_no_data()
            .build();
        let skip_first = ReportAssembler::new(test_source(), ExecutionMetadata::new(0, 0, 1))
            .skip_quality_no_data()
            .with_analysis_options(&deselected)
            .build();

        assert_eq!(
            options_first.quality_status,
            QualityAnalysisStatus::NotRequested
        );
        assert_eq!(
            skip_first.quality_status,
            QualityAnalysisStatus::NotRequested
        );
    }

    /// Completeness and uniqueness measure whole rows; the projection path
    /// withholds them rather than publishing projected numbers under full-row
    /// names. A consumer must not read that as "you did not ask".
    #[test]
    fn projection_withholding_every_dimension_is_not_a_skip() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);
        let options = AnalysisOptions::default()
            .with_quality_dimensions(Some(vec![
                QualityDimension::Completeness,
                QualityDimension::Uniqueness,
            ]))
            .with_columns(Some(vec!["col".to_string()]));

        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(2, 1, 10))
            .with_quality_data(data)
            .with_analysis_options(&options)
            .build();

        assert!(report.quality.is_none());
        assert_eq!(
            report.quality_status,
            QualityAnalysisStatus::WithheldByProjection
        );
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

    fn positive_binding() -> SemanticHintBinding {
        SemanticHintBinding {
            column: "col".to_string(),
            kind: SemanticHintKind::Positive,
            checked_values: 2,
            matched_values: 0,
            exact: true,
        }
    }

    #[test]
    fn exact_hint_binding_stays_exact_for_exhaustive_stream() {
        let report = ReportAssembler::new(test_source(), ExecutionMetadata::new(2, 1, 10))
            .with_semantic_hints(SemanticHints::new(vec!["col".to_string()], vec![]))
            .with_exact_value_hint_bindings(vec![positive_binding()])
            .build();

        assert!(report.semantic_hint_bindings[0].exact);
    }

    #[test]
    fn exact_hint_binding_is_downgraded_for_sampled_or_truncated_execution() {
        let executions = [
            ExecutionMetadata::new(2, 1, 10).with_sampling(0.5),
            ExecutionMetadata::new(2, 1, 10).with_truncation(TruncationReason::MaxRows(2)),
        ];

        for execution in executions {
            let report = ReportAssembler::new(test_source(), execution)
                .with_semantic_hints(SemanticHints::new(vec!["col".to_string()], vec![]))
                .with_exact_value_hint_bindings(vec![positive_binding()])
                .build();

            assert!(!report.semantic_hint_bindings[0].exact);
            assert!(!report.semantic_hint_bindings[0].is_proven_inert());
        }
    }
}
