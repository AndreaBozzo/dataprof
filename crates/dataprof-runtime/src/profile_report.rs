use dataprof_core::{ColumnProfile, DataSource, ExecutionMetadata};
use dataprof_metrics::{QualityAssessment, QualityMetrics};

/// Version of the serialized `ProfileReport` schema written by this build.
///
/// This is intentionally independent of the package version: the document
/// format only changes when the schema itself changes, not on every release.
///
/// Compatibility policy for readers:
/// - Documents without a `schema_version` field are legacy pre-0.10 reports
///   and deserialize with `schema_version == 0`.
/// - Unknown *additive* fields written by a newer dataprof are ignored, so a
///   reader accepts any document whose `schema_version` is at most this
///   constant.
/// - A document with a `schema_version` greater than this constant fails to
///   deserialize with an explicit error instead of being partially decoded
///   into a plausible-but-wrong report.
pub const REPORT_SCHEMA_VERSION: u32 = 1;

/// Complete profiling report for a data source.
///
/// Contains column-level statistics, execution metadata, and an optional
/// Quality assessment informed by ISO 8000/25012 concepts. This is the primary output of all
/// profiling operations (`Profiler::analyze_file`, `Profiler::analyze_source`,
/// `Profiler::profile_stream`, etc.).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProfileReport {
    /// Version of the serialized report schema (see [`REPORT_SCHEMA_VERSION`]).
    ///
    /// `0` means the document predates schema versioning (a 0.9-era report).
    /// Deserialization rejects versions newer than [`REPORT_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Unique identifier for this report (UUID v4)
    pub id: String,
    /// Timestamp when the report was generated (ISO 8601 / RFC 3339)
    pub timestamp: String,
    /// Data source metadata (file, query, etc.)
    pub data_source: DataSource,
    /// Column-level profiling results
    pub column_profiles: Vec<ColumnProfile>,
    /// Execution metadata (timing, rows processed, truncation info, etc.)
    pub execution: ExecutionMetadata,
    /// Data quality assessment (optional — partial analysis may skip quality)
    #[serde(skip_serializing_if = "Option::is_none")]
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
            schema_version: REPORT_SCHEMA_VERSION,
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

    /// Calculate the overall quality score (weighted across the assessed
    /// dimensions). Returns `None` if quality metrics were not computed, or
    /// if no dimension had anything to assess (e.g. an empty dataset) —
    /// absence of evidence is not a perfect score.
    pub fn quality_score(&self) -> Option<f64> {
        self.quality
            .as_ref()
            .filter(|q| !q.metrics.assessed_dimensions().is_empty())
            .map(|q| q.score())
    }

    /// Get the data source identifier (for backwards compatibility)
    pub fn source_identifier(&self) -> String {
        self.data_source.identifier()
    }
}

/// Mirror of [`ProfileReport`] carrying the field-level deserialization
/// rules (legacy aliases, quality compat). Kept private: the public entry
/// point is the manual [`serde::Deserialize`] impl below, which gates on
/// `schema_version` before any of these fields decode.
#[derive(serde::Deserialize)]
struct ProfileReportFields {
    #[serde(default)]
    schema_version: u32,
    id: String,
    timestamp: String,
    data_source: DataSource,
    column_profiles: Vec<ColumnProfile>,
    #[serde(alias = "scan_info")]
    execution: ExecutionMetadata,
    #[serde(
        alias = "data_quality_metrics",
        default,
        deserialize_with = "deserialize_quality_compat"
    )]
    quality: Option<QualityAssessment>,
}

impl From<ProfileReportFields> for ProfileReport {
    fn from(fields: ProfileReportFields) -> Self {
        Self {
            schema_version: fields.schema_version,
            id: fields.id,
            timestamp: fields.timestamp,
            data_source: fields.data_source,
            column_profiles: fields.column_profiles,
            execution: fields.execution,
            quality: fields.quality,
        }
    }
}

/// Manual deserialization so the `schema_version` gate runs before any other
/// field decodes. A derived deserializer visits fields in document order, so
/// an unsupported future document that also changed structure could fail with
/// a confusing structural error instead of the actionable version error; by
/// buffering the document first, the version check always wins.
impl<'de> serde::Deserialize<'de> for ProfileReport {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let value = serde_json::Value::deserialize(deserializer)?;
        match value.get("schema_version") {
            // Absent field: legacy pre-0.10 document, defaults to version 0.
            None => {}
            Some(serde_json::Value::Number(n)) if n.as_u64().is_some() => {
                let version = n.as_u64().unwrap_or_default();
                if version > u64::from(REPORT_SCHEMA_VERSION) {
                    return Err(D::Error::custom(format!(
                        "report schema version {version} is newer than the latest supported \
                         version {REPORT_SCHEMA_VERSION}; upgrade dataprof to read this report"
                    )));
                }
            }
            // An explicit null or non-integer is malformed, not legacy.
            Some(other) => {
                return Err(D::Error::custom(format!(
                    "report schema_version must be a non-negative integer, got {other}"
                )));
            }
        }
        ProfileReportFields::deserialize(value)
            .map(ProfileReport::from)
            .map_err(D::Error::custom)
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
        assert_eq!(deserialized.schema_version, REPORT_SCHEMA_VERSION);
    }

    #[test]
    fn test_serialized_report_carries_schema_version() {
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

        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(
            value.get("schema_version").and_then(|v| v.as_u64()),
            Some(u64::from(REPORT_SCHEMA_VERSION))
        );
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
        // A document without a schema_version field is a legacy pre-0.10
        // report; it must load and be identifiable as such.
        assert_eq!(report.schema_version, 0);
        assert_eq!(report.execution.rows_processed, 10);
        // Legacy metrics predate the assessability denominators: the facts
        // stay readable, but no score is fabricated from them.
        assert!(report.quality_score().is_none());
        let quality = report
            .quality
            .expect("expected legacy quality to deserialize");
        assert!(matches!(quality.confidence, MetricConfidence::Exact));
        let completeness = quality
            .metrics
            .completeness
            .as_ref()
            .expect("legacy completeness facts should deserialize");
        assert!((completeness.complete_records_ratio - 100.0).abs() < 0.01);
        assert!(quality.metrics.assessed_dimensions().is_empty());
    }

    fn current_document() -> serde_json::Value {
        json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "id": "current-report",
            "timestamp": "2026-07-16T10:00:00Z",
            "data_source": {
                "type": "file",
                "path": "test.csv",
                "format": "csv",
                "size_bytes": 42
            },
            "column_profiles": [],
            "execution": {
                "rows_processed": 10,
                "columns_detected": 2,
                "scan_time_ms": 5,
                "error_count": 0,
                "source_exhausted": true,
                "sampling_applied": false
            }
        })
    }

    #[test]
    fn test_additive_fields_from_newer_writer_are_ignored() {
        // A newer dataprof may add fields within the same schema version;
        // a compatible reader must not break on them.
        let mut json = current_document();
        json["a_future_additive_field"] = json!({"anything": true});
        json["column_profiles"] = json!([]);

        let report: ProfileReport = serde_json::from_value(json).unwrap();
        assert_eq!(report.schema_version, REPORT_SCHEMA_VERSION);
        assert_eq!(report.id, "current-report");
    }

    #[test]
    fn test_unsupported_future_schema_version_fails_explicitly() {
        let mut json = current_document();
        json["schema_version"] = json!(REPORT_SCHEMA_VERSION + 1);

        let err = serde_json::from_value::<ProfileReport>(json).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("schema version") && msg.contains("upgrade dataprof"),
            "expected an actionable schema-version error, got: {msg}"
        );
    }

    #[test]
    fn test_version_error_wins_over_structural_errors() {
        // A future document that also broke structure, with schema_version
        // appearing after the broken field, must still fail with the version
        // error — not a confusing type error from the field encountered first.
        let json_text = format!(
            r#"{{"column_profiles": "not-an-array", "schema_version": {}}}"#,
            REPORT_SCHEMA_VERSION + 1
        );
        let err = serde_json::from_str::<ProfileReport>(&json_text).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("upgrade dataprof"),
            "expected the schema-version error to win, got: {msg}"
        );
    }

    #[test]
    fn test_null_schema_version_is_malformed_not_legacy() {
        let mut json = current_document();
        json["schema_version"] = json!(null);

        let err = serde_json::from_value::<ProfileReport>(json).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("schema_version must be a non-negative integer"),
            "expected explicit rejection of null schema_version, got: {msg}"
        );
    }

    #[test]
    fn test_non_integer_schema_version_is_rejected() {
        for bad in [json!("1"), json!(1.5), json!(-1), json!(true)] {
            let mut json = current_document();
            json["schema_version"] = bad.clone();
            let err = serde_json::from_value::<ProfileReport>(json).unwrap_err();
            assert!(
                err.to_string().contains("non-negative integer"),
                "expected rejection of {bad}, got: {err}"
            );
        }
    }
}
