use dataprof_core::{ColumnProfile, DataSource, ExecutionMetadata};
use dataprof_metrics::{QualityAssessment, QualityMetrics};

/// Complete profiling report for a data source.
///
/// Contains column-level statistics, execution metadata, and an optional
/// ISO 8000/25012 quality assessment. This is the primary output of all
/// profiling operations (`Profiler::analyze_file`, `Profiler::analyze_source`,
/// `Profiler::profile_stream`, etc.).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileReport {
    /// Unique identifier for this report (UUID v4)
    pub id: String,
    /// Timestamp when the report was generated (ISO 8601 / RFC 3339)
    pub timestamp: String,
    /// Data source metadata (file, query, etc.)
    pub data_source: DataSource,
    /// Column-level profiling results
    pub column_profiles: Vec<ColumnProfile>,
    /// Execution metadata (timing, rows processed, truncation info, etc.)
    #[serde(alias = "scan_info")]
    pub execution: ExecutionMetadata,
    /// Data quality assessment (optional — partial analysis may skip quality)
    #[serde(
        alias = "data_quality_metrics",
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_quality_compat"
    )]
    pub quality: Option<QualityAssessment>,
}

impl ProfileReport {
    /// Create a new ProfileReport with auto-generated id and timestamp
    pub fn new(
        data_source: DataSource,
        column_profiles: Vec<ColumnProfile>,
        execution: ExecutionMetadata,
        quality: Option<QualityAssessment>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            data_source,
            column_profiles,
            execution,
            quality,
        }
    }

    /// Override the auto-generated ID (useful for deterministic caching/testing)
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Override the auto-generated timestamp
    pub fn with_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.timestamp = timestamp.into();
        self
    }

    /// Calculate overall quality score using ISO 8000/25012 metrics.
    /// Returns `None` if quality metrics were not computed.
    pub fn quality_score(&self) -> Option<f64> {
        self.quality.as_ref().map(|q| q.score())
    }

    /// Get the data source identifier (for backwards compatibility)
    pub fn source_identifier(&self) -> String {
        self.data_source.identifier()
    }
}

/// Custom deserializer that handles both legacy `DataQualityMetrics` (flat)
/// and new `QualityAssessment` (wrapped with confidence) JSON formats.
fn deserialize_quality_compat<'de, D>(
    deserializer: D,
) -> Result<Option<QualityAssessment>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(v) => {
            if v.get("metrics").is_some() && v.get("confidence").is_some() {
                let assessment: QualityAssessment =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                Ok(Some(assessment))
            } else {
                let metrics: QualityMetrics =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                Ok(Some(QualityAssessment::exact(metrics)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dataprof_core::FileFormat;
    use dataprof_metrics::MetricConfidence;
    use serde_json::json;

    #[test]
    fn test_profile_report_json_roundtrip() {
        let report = ProfileReport::new(
            DataSource::File {
                path: "test.csv".to_string(),
                format: FileFormat::Csv,
                size_bytes: 1024,
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ExecutionMetadata::new(100, 5, 50),
            Some(QualityAssessment::exact(QualityMetrics::empty())),
        );

        let json = serde_json::to_string(&report).unwrap();
        let deserialized: ProfileReport = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, report.id);
        assert_eq!(deserialized.timestamp, report.timestamp);
        assert_eq!(deserialized.source_identifier(), "test.csv");
        assert_eq!(deserialized.execution.rows_processed, 100);
        assert!(deserialized.quality.is_some());
    }

    #[test]
    fn test_profile_report_without_quality() {
        let report = ProfileReport::new(
            DataSource::File {
                path: "test.csv".to_string(),
                format: FileFormat::Csv,
                size_bytes: 1024,
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ExecutionMetadata::new(100, 5, 50),
            None,
        );

        let json = serde_json::to_string(&report).unwrap();
        let deserialized: ProfileReport = serde_json::from_str(&json).unwrap();

        assert!(deserialized.quality.is_none());
        assert_eq!(deserialized.execution.rows_processed, 100);
    }

    #[test]
    fn test_profile_report_deserializes_legacy_quality_metrics() {
        let json = json!({
            "id": "legacy-report",
            "timestamp": "2026-05-22T10:00:00Z",
            "data_source": {
                "type": "file",
                "path": "test.csv",
                "format": "csv",
                "size_bytes": 42
            },
            "column_profiles": [],
            "scan_info": {
                "rows_processed": 10,
                "columns_detected": 2,
                "scan_time_ms": 5,
                "error_count": 0,
                "source_exhausted": true,
                "sampling_applied": false
            },
            "data_quality_metrics": {
                "completeness": {
                    "missing_values_ratio": 0.0,
                    "complete_records_ratio": 100.0,
                    "null_columns": []
                }
            }
        });

        let report: ProfileReport = serde_json::from_value(json).unwrap();

        assert_eq!(report.id, "legacy-report");
        assert_eq!(report.execution.rows_processed, 10);
        let quality = report
            .quality
            .expect("expected legacy quality to deserialize");
        assert!(matches!(quality.confidence, MetricConfidence::Exact));
        assert!((quality.score() - 100.0).abs() < 0.01);
    }
}
