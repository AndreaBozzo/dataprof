pub use dataprof_core::classification::{DataType, PatternCategory};
pub use dataprof_core::execution::{ExecutionMetadata, TruncationReason};
pub use dataprof_core::output::OutputFormat;
pub use dataprof_core::partial::{ColumnSchema, CountMethod, RowCountEstimate, SchemaResult};
pub use dataprof_core::pattern::Pattern;
pub use dataprof_core::profile::{
    BooleanStats, ColumnProfile, ColumnStats, DateTimeStats, FrequencyItem, NumericStats,
    Quartiles, TextStats,
};
pub use dataprof_core::quality::{MetricPack, QualityDimension};
pub use dataprof_core::source::{
    DataFrameLibrary, DataSource, FileFormat, ParquetMetadata, QueryEngine, StreamSourceSystem,
};
pub use dataprof_metrics::{
    AccuracyMetrics, CompletenessMetrics, ConsistencyMetrics, MetricConfidence, QualityAssessment,
    QualityMetrics, TimelinessMetrics, UniquenessMetrics,
};

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

    // Try to deserialize as the new QualityAssessment first,
    // then fall back to bare QualityMetrics (legacy format)
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(v) => {
            // Try new format first (has "metrics" and "confidence" fields)
            if v.get("metrics").is_some() && v.get("confidence").is_some() {
                let assessment: QualityAssessment =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                Ok(Some(assessment))
            } else {
                // Legacy format: bare QualityMetrics object
                let metrics: QualityMetrics =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                Ok(Some(QualityAssessment::exact(metrics)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Partial analysis types (#226)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Quality score calculation --

    #[test]
    fn test_empty_metrics_perfect_score() {
        let metrics = QualityMetrics::empty();
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_weights_sum_to_100() {
        // With all dimensions at 100%, score should be 100
        let metrics = QualityMetrics::empty();
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_completeness_weight() {
        // Zero completeness, everything else perfect
        let mut metrics = QualityMetrics::empty();
        if let Some(ref mut c) = metrics.completeness {
            c.complete_records_ratio = 0.0;
        }
        // Score drops by 30% (completeness weight)
        assert!((metrics.overall_score() - 70.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_all_bad() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 0.0,
                ..CompletenessMetrics::default()
            }),
            consistency: Some(ConsistencyMetrics {
                data_type_consistency: 0.0,
                ..ConsistencyMetrics::default()
            }),
            uniqueness: Some(UniquenessMetrics {
                key_uniqueness: 0.0,
                ..UniquenessMetrics::default()
            }),
            accuracy: Some(AccuracyMetrics {
                outlier_ratio: 100.0,
                ..AccuracyMetrics::default()
            }),
            timeliness: Some(TimelinessMetrics {
                stale_data_ratio: 100.0,
                ..TimelinessMetrics::default()
            }),
        };
        assert!((metrics.overall_score() - 0.0).abs() < 0.01);
    }

    // -- JSON serialization roundtrip --

    #[test]
    fn test_column_profile_json_roundtrip() {
        let profile = ColumnProfile {
            name: "test_col".to_string(),
            data_type: DataType::Integer,
            null_count: 2,
            total_count: 10,
            unique_count: Some(8),
            stats: ColumnStats::Numeric(NumericStats {
                min: 1.0,
                max: 100.0,
                mean: 50.5,
                std_dev: 28.87,
                variance: 833.25,
                median: Some(50.0),
                quartiles: Some(Quartiles {
                    q1: 25.0,
                    q2: 50.0,
                    q3: 75.0,
                    iqr: 50.0,
                }),
                mode: Some(42.0),
                coefficient_of_variation: Some(57.17),
                skewness: Some(0.0),
                kurtosis: Some(-1.2),
                is_approximate: Some(false),
            }),
            patterns: vec![],
        };

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ColumnProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "test_col");
        assert_eq!(deserialized.data_type, DataType::Integer);
        assert_eq!(deserialized.total_count, 10);
        assert_eq!(deserialized.null_count, 2);

        if let ColumnStats::Numeric(n) = &deserialized.stats {
            assert!((n.min - 1.0).abs() < 0.01);
            assert!((n.max - 100.0).abs() < 0.01);
            assert!((n.mean - 50.5).abs() < 0.01);
            assert!(n.median.is_some());
            assert!(n.quartiles.is_some());
        } else {
            panic!("Expected Numeric stats after roundtrip");
        }
    }

    #[test]
    fn test_text_stats_json_roundtrip() {
        let profile = ColumnProfile {
            name: "name".to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: 3,
            unique_count: Some(3),
            stats: ColumnStats::Text(TextStats {
                min_length: 3,
                max_length: 7,
                avg_length: 5.0,
                most_frequent: None,
                least_frequent: None,
            }),
            patterns: vec![],
        };

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ColumnProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.data_type, DataType::String);
        if let ColumnStats::Text(t) = &deserialized.stats {
            assert_eq!(t.min_length, 3);
            assert_eq!(t.max_length, 7);
        } else {
            panic!("Expected Text stats after roundtrip");
        }
    }

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

        assert_eq!(deserialized.execution.rows_processed, 100);
        assert_eq!(deserialized.execution.columns_detected, 5);
        assert!((deserialized.quality_score().unwrap() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_profile_report_without_quality() {
        let report = ProfileReport::new(
            DataSource::File {
                path: "test.csv".to_string(),
                format: FileFormat::Csv,
                size_bytes: 0,
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ExecutionMetadata::new(0, 0, 0),
            None,
        );

        assert!(report.quality_score().is_none());
        assert!(report.quality.is_none());

        // Roundtrip: None quality should survive
        let json = serde_json::to_string(&report).unwrap();
        let deserialized: ProfileReport = serde_json::from_str(&json).unwrap();
        assert!(deserialized.quality.is_none());
    }

    // -- ExecutionMetadata --

    #[test]
    fn test_execution_metadata_throughput_calculation() {
        let meta = ExecutionMetadata::new(1000, 5, 500); // 500ms
        // 1000 rows / 0.5s = 2000 rows/sec
        assert!(meta.throughput_rows_sec.is_some());
        assert!((meta.throughput_rows_sec.unwrap() - 2000.0).abs() < 1.0);
        assert!(meta.source_exhausted);
        assert!(!meta.sampling_applied);
        assert!(meta.sampling_ratio.is_none());
    }

    #[test]
    fn test_execution_metadata_zero_time_no_throughput() {
        let meta = ExecutionMetadata::new(100, 3, 0);
        assert!(meta.throughput_rows_sec.is_none());
    }

    #[test]
    fn test_execution_metadata_with_sampling() {
        let meta = ExecutionMetadata::new(500, 3, 100).with_sampling(0.5);
        assert!(meta.sampling_applied);
        assert_eq!(meta.sampling_ratio, Some(0.5));
    }

    #[test]
    fn test_execution_metadata_with_truncation() {
        let meta =
            ExecutionMetadata::new(1000, 5, 200).with_truncation(TruncationReason::MaxRows(1000));
        assert!(!meta.source_exhausted);
        assert!(meta.truncation_reason.is_some());
    }

    #[test]
    fn test_truncation_reason_serde_roundtrip() {
        let reasons = vec![
            TruncationReason::MaxRows(5000),
            TruncationReason::MaxBytes(1_000_000),
            TruncationReason::MemoryPressure,
            TruncationReason::StopCondition("accuracy > 0.95".to_string()),
            TruncationReason::StreamClosed,
            TruncationReason::Timeout,
        ];
        for reason in reasons {
            let json = serde_json::to_string(&reason).unwrap();
            let deserialized: TruncationReason = serde_json::from_str(&json).unwrap();
            let json2 = serde_json::to_string(&deserialized).unwrap();
            assert_eq!(json, json2);
        }
    }

    // -- DataSource --

    #[test]
    fn test_data_source_file_identifier() {
        let ds = DataSource::File {
            path: "/path/to/data.csv".to_string(),
            format: FileFormat::Csv,
            size_bytes: 0,
            modified_at: None,
            parquet_metadata: None,
        };
        assert_eq!(ds.identifier(), "/path/to/data.csv");
        assert!(ds.is_file());
        assert!(!ds.is_query());
        assert!(!ds.is_dataframe());
        assert!(!ds.is_stream());
    }

    #[test]
    fn test_data_source_stream_identifier_and_helpers() {
        let ds = DataSource::Stream {
            topic: "events".to_string(),
            batch_id: "b1".to_string(),
            partition: Some(0),
            consumer_group: None,
            source_system: StreamSourceSystem::Kafka,
            session_id: None,
            first_record_at: None,
            last_record_at: None,
        };

        assert_eq!(ds.identifier(), "kafka[events]-batch:b1");
        assert!(ds.is_stream());
        assert_eq!(ds.stream_topic(), Some("events"));
        assert_eq!(ds.batch_id(), Some("b1"));
        assert!(!ds.is_file());
        assert!(!ds.is_query());
        assert!(ds.size_mb().is_none());
    }

    #[test]
    fn test_stream_json_serialization() {
        let ds = DataSource::Stream {
            topic: "sensor-data".to_string(),
            batch_id: "batch-789".to_string(),
            partition: Some(2),
            consumer_group: Some("processing-group".to_string()),
            source_system: StreamSourceSystem::Kinesis,
            session_id: Some("session-1".to_string()),
            first_record_at: Some("2023-01-01T10:00:00Z".to_string()),
            last_record_at: Some("2023-01-01T10:05:00Z".to_string()),
        };

        let json = serde_json::to_string(&ds).unwrap();
        assert!(json.contains(r#""type":"stream""#));
        assert!(json.contains(r#""source_system":"kinesis""#));
        assert!(json.contains(r#""topic":"sensor-data""#));

        let deserialized: DataSource = serde_json::from_str(&json).unwrap();
        assert!(deserialized.is_stream());
        assert_eq!(deserialized.stream_topic(), Some("sensor-data"));
    }

    #[test]
    fn test_stream_source_system_serialization_names() {
        let object_store = serde_json::to_string(&StreamSourceSystem::ObjectStore).unwrap();
        let message_queue = serde_json::to_string(&StreamSourceSystem::MessageQueue).unwrap();
        let database = serde_json::to_string(&StreamSourceSystem::Database).unwrap();

        assert_eq!(object_store, r#""object_store""#);
        assert_eq!(message_queue, r#""message_queue""#);
        assert_eq!(database, r#""database""#);

        let object_store: StreamSourceSystem = serde_json::from_str(r#""object_store""#).unwrap();
        let message_queue: StreamSourceSystem = serde_json::from_str(r#""message_queue""#).unwrap();
        let database: StreamSourceSystem = serde_json::from_str(r#""database""#).unwrap();

        assert_eq!(object_store, StreamSourceSystem::ObjectStore);
        assert_eq!(message_queue, StreamSourceSystem::MessageQueue);
        assert_eq!(database, StreamSourceSystem::Database);
    }

    // -- Partial dimension requests --

    #[test]
    fn test_partial_dimensions_only_completeness() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 100.0,
                missing_values_ratio: 0.0,
                null_columns: vec![],
            }),
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
        };
        assert!(metrics.completeness.is_some());
        assert!(metrics.consistency.is_none());
        assert!(metrics.uniqueness.is_none());
        assert!(metrics.accuracy.is_none());
        assert!(metrics.timeliness.is_none());
        // Score should re-normalize: 100% completeness alone → 100
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_partial_dimensions_two_dimensions() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 50.0,
                ..CompletenessMetrics::default()
            }),
            consistency: None,
            uniqueness: Some(UniquenessMetrics {
                key_uniqueness: 80.0,
                ..UniquenessMetrics::default()
            }),
            accuracy: None,
            timeliness: None,
        };
        // Weights: completeness=30, uniqueness=20 → total=50
        // Score: (50*0.30 + 80*0.20) / 0.50 = (15+16)/0.50 = 62.0
        assert!((metrics.overall_score() - 62.0).abs() < 0.01);
    }

    #[test]
    fn test_all_dimensions_none_score_zero() {
        let metrics = QualityMetrics {
            completeness: None,
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
        };
        assert!((metrics.overall_score() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_partial_dimensions_json_skips_none() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics::default()),
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
        };
        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("completeness"));
        assert!(!json.contains("consistency"));
        assert!(!json.contains("uniqueness"));
        assert!(!json.contains("accuracy"));
        assert!(!json.contains("timeliness"));
    }

    #[test]
    fn test_partial_dimensions_flat_accessors_return_defaults() {
        let metrics = QualityMetrics {
            completeness: None,
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
        };
        // Flat accessors return "perfect" defaults when dimension is None
        assert!((metrics.complete_records_ratio() - 100.0).abs() < 0.01);
        assert!((metrics.data_type_consistency() - 100.0).abs() < 0.01);
        assert!((metrics.key_uniqueness() - 100.0).abs() < 0.01);
        assert!((metrics.missing_values_ratio() - 0.0).abs() < 0.01);
        assert_eq!(metrics.duplicate_rows(), 0);
        assert!(!metrics.high_cardinality_warning());
    }

    // -- MetricPack helpers --

    #[test]
    fn test_metric_pack_include_helpers_none_means_all() {
        assert!(MetricPack::include_statistics(None));
        assert!(MetricPack::include_patterns(None));
        assert!(MetricPack::include_quality(None));
    }

    #[test]
    fn test_metric_pack_include_helpers_selective() {
        let packs = vec![MetricPack::Schema, MetricPack::Quality];
        assert!(!MetricPack::include_statistics(Some(&packs)));
        assert!(!MetricPack::include_patterns(Some(&packs)));
        assert!(MetricPack::include_quality(Some(&packs)));
    }

    #[test]
    fn test_metric_pack_from_str() {
        assert_eq!(
            "statistics".parse::<MetricPack>().unwrap(),
            MetricPack::Statistics
        );
        assert_eq!(
            "QUALITY".parse::<MetricPack>().unwrap(),
            MetricPack::Quality
        );
        assert!("invalid".parse::<MetricPack>().is_err());
    }
}
