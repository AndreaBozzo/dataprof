//! Centralized report assembly for all profiling engines.
//!
//! `ReportAssembler` is the single entry point for constructing a [`ProfileReport`].
//! It replaces the scattered `QualityReport::new()` calls across parsers, engines,
//! and database connectors, centralizing quality metric calculation and confidence
//! tracking in one place.

use std::collections::HashMap;

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
    /// If provided, `build()` will call `QualityMetrics::calculate_from_data()`
    /// to compute ISO 8000/25012 quality metrics.
    pub fn with_quality_data(mut self, data: HashMap<String, Vec<String>>) -> Self {
        self.quality_data = Some(data);
        self
    }

    /// Override the default metric confidence level.
    ///
    /// By default, confidence is `Exact` unless `execution.sampling_applied` is true,
    /// in which case it defaults to `Approximate`.
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
    /// `skip_quality` was not called. If calculation fails, quality is `None`.
    pub fn build(self) -> ProfileReport {
        let quality = if self.skip_quality {
            None
        } else if let Some(data) = &self.quality_data {
            match QualityMetrics::calculate_from_data(data, &self.columns) {
                Ok(metrics) => {
                    let confidence = self
                        .confidence
                        .clone()
                        .unwrap_or_else(|| self.default_confidence(data));
                    Some(QualityAssessment {
                        metrics,
                        confidence,
                    })
                }
                Err(_) => None,
            }
        } else {
            None
        };

        ProfileReport::new(self.source, self.columns, self.execution, quality)
    }

    /// Determine default confidence from execution metadata and data size.
    fn default_confidence(&self, data: &HashMap<String, Vec<String>>) -> MetricConfidence {
        if self.execution.sampling_applied {
            let sample_size = data.values().map(|v| v.len()).max().unwrap_or(0);
            let population_size = if self.execution.source_exhausted {
                Some(self.execution.rows_processed)
            } else {
                None
            };
            MetricConfidence::Approximate {
                sample_size,
                population_size,
            }
        } else {
            MetricConfidence::Exact
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
    fn test_with_quality_data() {
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
    fn test_sampling_sets_approximate_confidence() {
        let mut data = HashMap::new();
        data.insert("col".to_string(), vec!["a".to_string(), "b".to_string()]);

        let execution = ExecutionMetadata::new(1000, 1, 50).with_sampling(0.1);

        let report = ReportAssembler::new(test_source(), execution)
            .with_quality_data(data)
            .build();

        assert!(report.quality.is_some());
        let quality = report.quality.unwrap();
        assert!(matches!(
            quality.confidence,
            MetricConfidence::Approximate { .. }
        ));
    }
}
