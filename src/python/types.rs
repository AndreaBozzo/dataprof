use pyo3::prelude::*;

use crate::types::{
    ColumnProfile, ColumnStats, DataSource, DataType, ProfileReport, QualityMetrics,
    TruncationReason,
};

/// Python wrapper for ColumnProfile — column-level statistics.
#[pyclass(name = "ColumnProfile")]
#[derive(Clone)]
pub struct PyColumnProfile {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: String,
    #[pyo3(get)]
    pub total_count: usize,
    #[pyo3(get)]
    pub null_count: usize,
    #[pyo3(get)]
    pub unique_count: Option<usize>,
    #[pyo3(get)]
    pub null_percentage: f64,
    #[pyo3(get)]
    pub uniqueness_ratio: f64,
    // Numeric statistics (None for non-numeric columns)
    #[pyo3(get)]
    pub min: Option<f64>,
    #[pyo3(get)]
    pub max: Option<f64>,
    #[pyo3(get)]
    pub mean: Option<f64>,
    #[pyo3(get)]
    pub std_dev: Option<f64>,
    #[pyo3(get)]
    pub variance: Option<f64>,
    #[pyo3(get)]
    pub median: Option<f64>,
    #[pyo3(get)]
    pub mode: Option<f64>,
    #[pyo3(get)]
    pub skewness: Option<f64>,
    #[pyo3(get)]
    pub kurtosis: Option<f64>,
    #[pyo3(get)]
    pub coefficient_of_variation: Option<f64>,
    #[pyo3(get)]
    pub quartiles: Option<std::collections::HashMap<String, f64>>,
    #[pyo3(get)]
    pub is_approximate: Option<bool>,
}

impl From<&ColumnProfile> for PyColumnProfile {
    fn from(profile: &ColumnProfile) -> Self {
        let null_percentage = if profile.total_count > 0 {
            (profile.null_count as f64 / profile.total_count as f64) * 100.0
        } else {
            0.0
        };

        let uniqueness_ratio = if let Some(unique) = profile.unique_count {
            if profile.total_count > 0 {
                unique as f64 / profile.total_count as f64
            } else {
                0.0
            }
        } else {
            0.0
        };

        let (
            min,
            max,
            mean,
            std_dev,
            variance,
            median,
            mode,
            skewness,
            kurtosis,
            coefficient_of_variation,
            quartiles,
            is_approximate,
        ) = match &profile.stats {
            ColumnStats::Numeric(n) => {
                let q_map = n.quartiles.as_ref().map(|q| {
                    let mut m = std::collections::HashMap::new();
                    m.insert("q1".to_string(), q.q1);
                    m.insert("q2".to_string(), q.q2);
                    m.insert("q3".to_string(), q.q3);
                    m.insert("iqr".to_string(), q.iqr);
                    m
                });
                (
                    Some(n.min),
                    Some(n.max),
                    Some(n.mean),
                    Some(n.std_dev),
                    Some(n.variance),
                    n.median,
                    n.mode,
                    n.skewness,
                    n.kurtosis,
                    n.coefficient_of_variation,
                    q_map,
                    n.is_approximate,
                )
            }
            _ => (
                None, None, None, None, None, None, None, None, None, None, None, None,
            ),
        };

        Self {
            name: profile.name.clone(),
            data_type: match profile.data_type {
                DataType::Integer => "integer".to_string(),
                DataType::Float => "float".to_string(),
                DataType::String => "string".to_string(),
                DataType::Date => "date".to_string(),
            },
            total_count: profile.total_count,
            null_count: profile.null_count,
            unique_count: profile.unique_count,
            null_percentage,
            uniqueness_ratio,
            min,
            max,
            mean,
            std_dev,
            variance,
            median,
            mode,
            skewness,
            kurtosis,
            coefficient_of_variation,
            quartiles,
            is_approximate,
        }
    }
}

/// Python wrapper for QualityMetrics — ISO 8000/25012 quality dimensions.
#[pyclass(name = "DataQualityMetrics")]
#[derive(Clone)]
pub struct PyDataQualityMetrics {
    #[pyo3(get)]
    pub missing_values_ratio: f64,
    #[pyo3(get)]
    pub complete_records_ratio: f64,
    #[pyo3(get)]
    pub null_columns: Vec<String>,
    #[pyo3(get)]
    pub data_type_consistency: f64,
    #[pyo3(get)]
    pub format_violations: usize,
    #[pyo3(get)]
    pub encoding_issues: usize,
    #[pyo3(get)]
    pub duplicate_rows: usize,
    #[pyo3(get)]
    pub key_uniqueness: f64,
    #[pyo3(get)]
    pub high_cardinality_warning: bool,
    #[pyo3(get)]
    pub outlier_ratio: f64,
    #[pyo3(get)]
    pub range_violations: usize,
    #[pyo3(get)]
    pub negative_values_in_positive: usize,
    #[pyo3(get)]
    pub future_dates_count: usize,
    #[pyo3(get)]
    pub stale_data_ratio: f64,
    #[pyo3(get)]
    pub temporal_violations: usize,
}

impl From<&QualityMetrics> for PyDataQualityMetrics {
    fn from(m: &QualityMetrics) -> Self {
        Self {
            missing_values_ratio: m.missing_values_ratio,
            complete_records_ratio: m.complete_records_ratio,
            null_columns: m.null_columns.clone(),
            data_type_consistency: m.data_type_consistency,
            format_violations: m.format_violations,
            encoding_issues: m.encoding_issues,
            duplicate_rows: m.duplicate_rows,
            key_uniqueness: m.key_uniqueness,
            high_cardinality_warning: m.high_cardinality_warning,
            outlier_ratio: m.outlier_ratio,
            range_violations: m.range_violations,
            negative_values_in_positive: m.negative_values_in_positive,
            future_dates_count: m.future_dates_count,
            stale_data_ratio: m.stale_data_ratio,
            temporal_violations: m.temporal_violations,
        }
    }
}

#[pymethods]
impl PyDataQualityMetrics {
    /// Overall quality score (0-100) using ISO 8000/25012 weighted formula.
    fn overall_quality_score(&self) -> f64 {
        let completeness = self.complete_records_ratio * 0.3;
        let consistency = self.data_type_consistency * 0.25;
        let uniqueness = self.key_uniqueness * 0.2;
        let accuracy = (100.0 - self.outlier_ratio) * 0.15;
        let timeliness = (100.0 - self.stale_data_ratio) * 0.1;
        completeness + consistency + uniqueness + accuracy + timeliness
    }

    fn __str__(&self) -> String {
        format!(
            "DataQualityMetrics(score={:.1}%, completeness={:.1}%, consistency={:.1}%, uniqueness={:.1}%)",
            self.overall_quality_score(),
            self.complete_records_ratio,
            self.data_type_consistency,
            self.key_uniqueness,
        )
    }
}

/// Python wrapper for the full ProfileReport.
///
/// Wraps the Rust `ProfileReport` directly, exposing all `ExecutionMetadata`
/// fields and lazy-converting column profiles on access.
#[pyclass(name = "ProfileReport")]
pub struct PyProfileReport {
    pub(crate) inner: ProfileReport,
}

impl PyProfileReport {
    pub fn new(report: ProfileReport) -> Self {
        Self { inner: report }
    }
}

#[pymethods]
impl PyProfileReport {
    /// Data source identifier (file path, dataframe name, etc.)
    #[getter]
    fn source(&self) -> String {
        self.inner.data_source.identifier()
    }

    /// Source type: "file", "dataframe", "stream", "query"
    #[getter]
    fn source_type(&self) -> &str {
        match &self.inner.data_source {
            DataSource::File { .. } => "file",
            DataSource::DataFrame { .. } => "dataframe",
            DataSource::Stream { .. } => "stream",
            DataSource::Query { .. } => "query",
        }
    }

    /// Source library for DataFrame sources (e.g. "pandas", "polars")
    #[getter]
    fn source_library(&self) -> Option<String> {
        match &self.inner.data_source {
            DataSource::DataFrame { source_library, .. } => Some(source_library.to_string()),
            _ => None,
        }
    }

    /// Memory usage in bytes for DataFrame sources
    #[getter]
    fn memory_bytes(&self) -> Option<u64> {
        match &self.inner.data_source {
            DataSource::DataFrame { memory_bytes, .. } => *memory_bytes,
            _ => None,
        }
    }

    // -- ExecutionMetadata fields --

    /// Number of rows processed
    #[getter]
    fn rows_processed(&self) -> usize {
        self.inner.execution.rows_processed
    }

    /// Number of columns detected
    #[getter]
    fn columns_detected(&self) -> usize {
        self.inner.execution.columns_detected
    }

    /// Execution time in milliseconds
    #[getter]
    fn scan_time_ms(&self) -> u128 {
        self.inner.execution.scan_time_ms
    }

    /// Whether the entire source was consumed
    #[getter]
    fn source_exhausted(&self) -> bool {
        self.inner.execution.source_exhausted
    }

    /// Why processing stopped early (None if source was fully consumed)
    #[getter]
    fn truncation_reason(&self) -> Option<String> {
        self.inner
            .execution
            .truncation_reason
            .as_ref()
            .map(|r| match r {
                TruncationReason::MaxRows(n) => format!("max_rows({})", n),
                TruncationReason::MaxBytes(n) => format!("max_bytes({})", n),
                TruncationReason::MemoryPressure => "memory_pressure".to_string(),
                TruncationReason::StopCondition(s) => format!("stop_condition({})", s),
                TruncationReason::StreamClosed => "stream_closed".to_string(),
                TruncationReason::Timeout => "timeout".to_string(),
            })
    }

    /// Bytes consumed from source (if known)
    #[getter]
    fn bytes_consumed(&self) -> Option<u64> {
        self.inner.execution.bytes_consumed
    }

    /// Throughput in rows per second
    #[getter]
    fn throughput_rows_sec(&self) -> Option<f64> {
        self.inner.execution.throughput_rows_sec
    }

    /// Peak memory usage in MB
    #[getter]
    fn memory_peak_mb(&self) -> Option<f64> {
        self.inner.execution.memory_peak_mb
    }

    /// Number of errors encountered during profiling
    #[getter]
    fn error_count(&self) -> usize {
        self.inner.execution.error_count
    }

    /// Whether sampling was applied
    #[getter]
    fn sampling_applied(&self) -> bool {
        self.inner.execution.sampling_applied
    }

    /// Sampling ratio (1.0 = all rows analyzed)
    #[getter]
    fn sampling_ratio(&self) -> Option<f64> {
        self.inner.execution.sampling_ratio
    }

    // -- Profile data --

    /// Column-level statistics
    #[getter]
    fn column_profiles(&self) -> Vec<PyColumnProfile> {
        self.inner
            .column_profiles
            .iter()
            .map(PyColumnProfile::from)
            .collect()
    }

    /// Data quality metrics (None if quality assessment was skipped)
    #[getter]
    fn quality(&self) -> Option<PyDataQualityMetrics> {
        self.inner
            .quality
            .as_ref()
            .map(|q| PyDataQualityMetrics::from(&q.metrics))
    }

    /// Overall quality score (None if quality assessment was skipped)
    #[getter]
    fn quality_score(&self) -> Option<f64> {
        self.inner.quality_score()
    }

    /// Export as JSON string
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string_pretty(&self.inner).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("JSON serialization failed: {}", e))
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "ProfileReport(source='{}', rows={}, columns={}, time={}ms, quality={:?})",
            self.inner.data_source.identifier(),
            self.inner.execution.rows_processed,
            self.inner.execution.columns_detected,
            self.inner.execution.scan_time_ms,
            self.inner.quality_score(),
        )
    }
}
