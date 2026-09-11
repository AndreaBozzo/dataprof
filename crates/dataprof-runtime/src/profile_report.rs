use dataprof_core::{ColumnProfile, DataSource, ExecutionMetadata, SemanticHintBinding};
use dataprof_metrics::{
    AccuracyMetrics, CompletenessMetrics, ConsistencyMetrics, PrecisionMetrics, QualityAssessment,
    QualityMetrics, TimelinessMetrics, ValidityMetrics,
};

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

/// Why a report does or does not carry a quality assessment.
///
/// `quality` alone cannot answer that. A run that never asked for quality
/// metrics and a run whose quality computation failed both leave it `None`,
/// so absence itself became the plausible value that hid the failure. This
/// names the difference: a consumer deciding on a report can tell "you did
/// not ask for this" from "this broke" from "there was nothing to measure".
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum QualityAnalysisStatus {
    /// Computed: `quality` carries the assessment.
    Computed,
    /// Not requested for this run — the quality pack was deselected.
    NotRequested,
    /// Requested, but the run had nothing to compute from: an empty source,
    /// or an input path that retained no sample.
    NoData,
    /// Requested, but every requested dimension measures whole rows and the
    /// run profiled a subset of columns. Completeness and uniqueness mean
    /// something else after projection, and the report cannot label only
    /// their row-level fields as projected, so they are withheld rather than
    /// published under full-row names.
    WithheldByProjection,
    /// Requested and attempted; the computation failed. `quality` is absent
    /// because the computation broke, not because nothing was asked for.
    Failed {
        /// The error the metrics calculator reported.
        error: String,
    },
    /// Written by a release that did not record this. Only reachable by
    /// deserializing a document from before the field existed that carries no
    /// quality assessment; a stored assessment is read back as `Computed`.
    Unrecorded,
}

/// Complete profiling report for a data source.
///
/// Contains column-level statistics, execution metadata, and an optional
/// Quality assessment informed by ISO 8000/25012 concepts. This is the primary output of all
/// profiling operations (`Profiler::analyze_file`, `Profiler::analyze_source`,
/// `Profiler::profile_stream`, etc.).
#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
pub struct ProfileReport {
    /// Version of the serialized report schema (see [`REPORT_SCHEMA_VERSION`]).
    ///
    /// `0` means the document predates schema versioning (a 0.9-era report).
    /// Deserialization rejects versions newer than [`REPORT_SCHEMA_VERSION`].
    #[schemars(schema_with = "schema_version_schema")]
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
    /// What happened to the quality computation. Always present: it is the
    /// reason `quality` is or is not there, and a report must never leave a
    /// failed computation to a log line. Additive field — documents written
    /// before it deserialize as [`QualityAnalysisStatus::Unrecorded`], or as
    /// `Computed` when they carry an assessment.
    pub quality_status: QualityAnalysisStatus,
    /// Per-column evidence of how each semantic hint bound to the data.
    ///
    /// Empty when no hints were supplied. Recorded for provenance: a hint proven
    /// inert over the full data is rejected before a report is returned, so a
    /// successful report only carries bindings that matched something or whose
    /// evidence was sampled. Additive field — older readers ignore it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub semantic_hint_bindings: Vec<SemanticHintBinding>,
}

fn schema_version_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "integer",
        "const": REPORT_SCHEMA_VERSION,
        "minimum": 0
    })
}

/// Both v1 serialization dialects accepted by dataprof.
///
/// Rust serializes the complete runtime model. The high-level Python wrapper
/// predates that shape and exposes a deliberately flatter document. They share
/// one schema version and compatibility policy, so the published artifact
/// describes their union rather than pretending one dialect does not exist.
#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
#[serde(untagged)]
#[schemars(title = "ProfileReport")]
enum SerializedProfileReport {
    Rust(Box<ProfileReport>),
    Python(Box<PythonProfileReportDocument>),
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
struct PythonProfileReportDocument {
    #[schemars(schema_with = "schema_version_schema")]
    schema_version: u32,
    source: String,
    source_type: PythonSourceType,
    execution: PythonExecutionDocument,
    columns: Vec<PythonColumnDocument>,
    quality: Option<PythonQualityDocument>,
    quality_status: QualityAnalysisStatus,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    semantic_hint_bindings: Vec<SemanticHintBinding>,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
enum PythonSourceType {
    File,
    Query,
    Dataframe,
    Stream,
    Bytes,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
struct PythonExecutionDocument {
    engine: Option<String>,
    rows_processed: usize,
    columns_detected: usize,
    scan_time_ms: u128,
    source_exhausted: bool,
    truncation_reason: Option<String>,
    bytes_consumed: Option<u64>,
    throughput_rows_sec: Option<f64>,
    memory_peak_mb: Option<f64>,
    error_count: usize,
    ragged_row_count: usize,
    sampling_applied: bool,
    sampling_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sampled_row_ranges: Option<Vec<[u64; 2]>>,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
struct PythonColumnDocument {
    name: String,
    data_type: PythonDataType,
    total_count: usize,
    null_count: usize,
    null_percentage: Option<f64>,
    unique_count: Option<usize>,
    unique_count_is_approximate: Option<bool>,
    uniqueness_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    invalid_count: Option<usize>,
    /// Counts per lexical class. The Python dialect writes them as a plain
    /// mapping, but it is the same four-key object the Rust dialect writes, so
    /// it is described by the same definition: a reader that accepted a partial
    /// mapping here would validate a document the Python loader then discards
    /// as incomplete rather than inventing the missing counts.
    #[serde(skip_serializing_if = "Option::is_none")]
    type_homogeneity: Option<dataprof_core::TypeHomogeneity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<PythonColumnStatsDocument>,
    #[serde(skip_serializing_if = "Option::is_none")]
    patterns: Option<Vec<PythonPatternDocument>>,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
enum PythonDataType {
    String,
    Identifier,
    Integer,
    Float,
    Date,
    Boolean,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
struct PythonColumnStatsDocument {
    #[serde(skip_serializing_if = "Option::is_none")]
    min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mean: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    std_dev: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    variance: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    median: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skewness: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kurtosis: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    coefficient_of_variation: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quartiles: Option<dataprof_core::Quartiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_approximate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outlier_count: Option<usize>,
    /// Shortest value, in Unicode scalar values. Not UTF-8 bytes and not
    /// grapheme clusters: ASCII text is unaffected by that distinction, while a
    /// combining sequence counts each scalar, so the decomposed and precomposed
    /// spellings of the same word report different lengths.
    #[serde(skip_serializing_if = "Option::is_none")]
    min_length: Option<usize>,
    /// Longest value, in Unicode scalar values. See `min_length`.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_length: Option<usize>,
    /// Mean length, in Unicode scalar values. See `min_length`.
    #[serde(skip_serializing_if = "Option::is_none")]
    avg_length: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    true_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    false_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    true_ratio: Option<f64>,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
struct PythonPatternDocument {
    name: String,
    regex: String,
    match_count: usize,
    match_percentage: f64,
    category: dataprof_core::PatternCategory,
    confidence: f64,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
struct PythonQualityDocument {
    /// Null when no dimension was assessable; `assessed_dimensions` is then
    /// empty and every entry in `dimension_scores` is null.
    overall_score: Option<f64>,
    assessed_dimensions: Vec<PythonQualityDimension>,
    dimension_scores: std::collections::BTreeMap<PythonQualityDimension, Option<f64>>,
    low_sample_warning: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    completeness: Option<CompletenessMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consistency: Option<ConsistencyMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uniqueness: Option<PythonUniquenessDocument>,
    #[serde(skip_serializing_if = "Option::is_none")]
    accuracy: Option<AccuracyMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeliness: Option<TimelinessMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    validity: Option<ValidityMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    precision: Option<PrecisionMetrics>,
}

#[allow(dead_code)]
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
enum PythonQualityDimension {
    Completeness,
    Consistency,
    Uniqueness,
    Accuracy,
    Timeliness,
    Validity,
    Precision,
}

#[allow(dead_code)]
#[derive(serde::Serialize, schemars::JsonSchema)]
struct PythonUniquenessDocument {
    duplicate_rows: usize,
    key_uniqueness: f64,
    high_cardinality_warning: bool,
    rows_checked: usize,
    key_column: Option<String>,
    duplicate_rows_approximate: bool,
}

/// Generate the JSON Schema 2020-12 document for the current serialized report.
///
/// This is primarily used by the repository's schema generator and drift tests.
/// The committed, versioned document under `docs/schema/` is the public
/// interoperability contract.
#[doc(hidden)]
pub fn profile_report_schema_document() -> serde_json::Value {
    let settings = schemars::generate::SchemaSettings::draft2020_12().for_serialize();
    let schema = settings
        .into_generator()
        .into_root_schema_for::<SerializedProfileReport>();
    let mut document =
        serde_json::to_value(schema).expect("a Schemars schema must serialize to a JSON document");
    let object = document
        .as_object_mut()
        .expect("a root Schemars schema must be a JSON object");
    object.insert(
        "$id".to_string(),
        serde_json::Value::String(format!(
            "https://andreabozzo.github.io/dataprof/schema/profile-report.v{REPORT_SCHEMA_VERSION}.schema.json"
        )),
    );
    make_compatibility_defaults_optional(&mut document);
    allow_additive_properties(&mut document);
    canonicalize_key_order(&mut document);
    document
}

/// Sort every object's keys so the committed schema has one canonical byte
/// layout.
///
/// Key order carries no meaning in JSON Schema, but the committed document is a
/// reviewed artifact: it must not churn because `serde_json` switched its map
/// backing (`preserve_order`) or because Schemars changed the order in which it
/// builds a schema. Arrays such as `required` and `oneOf` keep their order,
/// which *is* meaningful.
fn canonicalize_key_order(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for child in object.values_mut() {
                canonicalize_key_order(child);
            }
            let mut sorted: Vec<(String, serde_json::Value)> =
                std::mem::take(object).into_iter().collect();
            sorted.sort_by(|(left, _), (right, _)| left.cmp(right));
            object.extend(sorted);
        }
        serde_json::Value::Array(items) => {
            for item in items {
                canonicalize_key_order(item);
            }
        }
        _ => {}
    }
}

/// Drop fields that this build always writes but a reader supplies a default
/// for, from the schema's `required` lists.
///
/// A document written under an earlier v1 build does not carry them. Declaring
/// them required would fail validation for documents the readers accept, which
/// is the opposite of what an additive widening means.
fn make_compatibility_defaults_optional(document: &mut serde_json::Value) {
    for (pointer, field) in [
        ("/$defs/ExecutionMetadata/required", "ragged_row_count"),
        ("/$defs/ProfileReport/required", "quality_status"),
        (
            "/$defs/PythonProfileReportDocument/required",
            "quality_status",
        ),
    ] {
        if let Some(required) = document
            .pointer_mut(pointer)
            .and_then(serde_json::Value::as_array_mut)
        {
            required.retain(|declared| declared.as_str() != Some(field));
        }
    }
}

fn allow_additive_properties(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if object.get("additionalProperties") == Some(&serde_json::Value::Bool(false)) {
                object.remove("additionalProperties");
            }
            for child in object.values_mut() {
                allow_additive_properties(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                allow_additive_properties(item);
            }
        }
        _ => {}
    }
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
            quality_status: match quality {
                Some(_) => QualityAnalysisStatus::Computed,
                // A caller constructing a report directly did not compute
                // quality; `ReportAssembler` overrides this with the reason it
                // actually observed.
                None => QualityAnalysisStatus::NotRequested,
            },
            quality,
            semantic_hint_bindings: Vec::new(),
        }
    }

    /// Record what happened to the quality computation.
    pub fn with_quality_status(mut self, status: QualityAnalysisStatus) -> Self {
        self.quality_status = status;
        self
    }

    /// Attach per-column semantic-hint binding evidence.
    pub fn with_semantic_hint_bindings(mut self, bindings: Vec<SemanticHintBinding>) -> Self {
        self.semantic_hint_bindings = bindings;
        self
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
    /// absence of evidence is neither a perfect score nor a zero.
    pub fn quality_score(&self) -> Option<f64> {
        self.quality.as_ref().and_then(|q| q.score())
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
    #[serde(default)]
    quality_status: Option<QualityAnalysisStatus>,
    #[serde(default)]
    semantic_hint_bindings: Vec<SemanticHintBinding>,
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
            // A document written before the field: a stored assessment proves
            // the computation ran, and nothing else about it is knowable.
            quality_status: fields.quality_status.unwrap_or({
                if fields.quality.is_some() {
                    QualityAnalysisStatus::Computed
                } else {
                    QualityAnalysisStatus::Unrecorded
                }
            }),
            quality: fields.quality,
            semantic_hint_bindings: fields.semantic_hint_bindings,
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

    fn report_without_quality() -> ProfileReport {
        ProfileReport::new(
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
        )
    }

    /// The `state` strings are the wire vocabulary: consumers branch on them,
    /// the Python binding repeats them by hand, and the committed schema
    /// enumerates them. Pin them here so a variant rename has to be deliberate.
    #[test]
    fn every_status_has_a_stable_wire_name() {
        let cases = [
            (QualityAnalysisStatus::Computed, "computed"),
            (QualityAnalysisStatus::NotRequested, "not_requested"),
            (QualityAnalysisStatus::NoData, "no_data"),
            (
                QualityAnalysisStatus::WithheldByProjection,
                "withheld_by_projection",
            ),
            (
                QualityAnalysisStatus::Failed {
                    error: "boom".to_string(),
                },
                "failed",
            ),
            (QualityAnalysisStatus::Unrecorded, "unrecorded"),
        ];

        for (status, name) in cases {
            let value = serde_json::to_value(&status).unwrap();
            assert_eq!(value.get("state"), Some(&json!(name)), "{status:?}");
            let restored: QualityAnalysisStatus = serde_json::from_value(value).unwrap();
            assert_eq!(restored, status);
        }
    }

    #[test]
    fn quality_status_survives_a_json_roundtrip() {
        let report = report_without_quality().with_quality_status(QualityAnalysisStatus::Failed {
            error: "Metrics calculation failed: no data columns found".to_string(),
        });

        let json = serde_json::to_string(&report).unwrap();
        let restored: ProfileReport = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.quality_status, report.quality_status);
        assert!(restored.quality.is_none());
    }

    /// The serialized document is the contract, so the two states have to be
    /// distinguishable there and not only in the Rust value.
    #[test]
    fn serialized_failure_and_skip_differ() {
        let failed = serde_json::to_value(report_without_quality().with_quality_status(
            QualityAnalysisStatus::Failed {
                error: "boom".to_string(),
            },
        ))
        .unwrap();
        let skipped = serde_json::to_value(
            report_without_quality().with_quality_status(QualityAnalysisStatus::NotRequested),
        )
        .unwrap();

        assert_eq!(
            failed.get("quality_status"),
            Some(&json!({"state": "failed", "error": "boom"}))
        );
        assert_eq!(
            skipped.get("quality_status"),
            Some(&json!({"state": "not_requested"}))
        );
        assert!(failed.get("quality").is_none());
        assert!(skipped.get("quality").is_none());
    }

    /// A document written before the field existed: an assessment proves the
    /// computation ran, and its absence proves nothing.
    #[test]
    fn documents_without_the_field_read_back_honestly() {
        let mut document = serde_json::to_value(report_without_quality()).unwrap();
        document.as_object_mut().unwrap().remove("quality_status");

        let restored: ProfileReport = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(restored.quality_status, QualityAnalysisStatus::Unrecorded);

        document.as_object_mut().unwrap().insert(
            "quality".to_string(),
            serde_json::to_value(QualityAssessment::exact(QualityMetrics::empty())).unwrap(),
        );
        let restored: ProfileReport = serde_json::from_value(document).unwrap();
        assert_eq!(restored.quality_status, QualityAnalysisStatus::Computed);
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
        // Nothing was assessable, so the assessment says so rather than
        // claiming exactness about a score it never produced (#571).
        assert!(matches!(quality.confidence, MetricConfidence::NotAssessed));
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
