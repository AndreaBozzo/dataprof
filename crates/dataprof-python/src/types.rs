use std::ffi::CString;

use pyo3::PyErr;
use pyo3::exceptions::PyDeprecationWarning;
use pyo3::prelude::*;

use dataprof::{
    ColumnProfile, ColumnStats, DataSource, DataType, Pattern, ProfileReport, QualityMetrics,
    TruncationReason,
};

/// Python wrapper for Pattern metrics
#[pyclass(name = "Pattern", from_py_object)]
#[derive(Clone)]
pub struct PyPattern {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub regex: String,
    #[pyo3(get)]
    pub match_count: usize,
    #[pyo3(get)]
    pub match_percentage: f64,
    #[pyo3(get)]
    pub category: String,
    #[pyo3(get)]
    pub confidence: f64,
}

impl From<&Pattern> for PyPattern {
    fn from(p: &Pattern) -> Self {
        Self {
            name: p.name.clone(),
            regex: p.regex.clone(),
            match_count: p.match_count,
            match_percentage: p.match_percentage,
            category: p.category.to_string(),
            confidence: p.confidence,
        }
    }
}

/// Python wrapper for ColumnProfile — column-level statistics.
#[pyclass(name = "ColumnProfile", from_py_object)]
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
    /// `None` when `unique_count` is `None`; `Some(False)` for an exact count;
    /// `Some(True)` when it is a HyperLogLog estimate (~1% relative error).
    #[pyo3(get)]
    pub unique_count_is_approximate: Option<bool>,
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
    #[pyo3(get)]
    pub outlier_count: Option<usize>,
    // Text statistics (None for non-text columns)
    #[pyo3(get)]
    pub min_length: Option<usize>,
    #[pyo3(get)]
    pub max_length: Option<usize>,
    #[pyo3(get)]
    pub avg_length: Option<f64>,
    // Boolean statistics (None for non-boolean columns)
    #[pyo3(get)]
    pub true_count: Option<usize>,
    #[pyo3(get)]
    pub false_count: Option<usize>,
    #[pyo3(get)]
    pub true_ratio: Option<f64>,
    // Patterns. `None` = detection did not run; `Some([])` = ran, nothing matched.
    #[pyo3(get)]
    pub patterns: Option<Vec<PyPattern>>,
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
            ColumnStats::Boolean(_) => (
                None, None, None, None, None, None, None, None, None, None, None, None,
            ),
            _ => (
                None, None, None, None, None, None, None, None, None, None, None, None,
            ),
        };

        let outlier_count = match &profile.stats {
            ColumnStats::Numeric(n) => n.outlier_count,
            _ => None,
        };

        let (min_length, max_length, avg_length) = match &profile.stats {
            ColumnStats::Text(t) => (Some(t.min_length), Some(t.max_length), Some(t.avg_length)),
            _ => (None, None, None),
        };

        let (true_count, false_count, true_ratio) = match &profile.stats {
            ColumnStats::Boolean(b) => {
                (Some(b.true_count), Some(b.false_count), Some(b.true_ratio))
            }
            _ => (None, None, None),
        };

        // Preserve the None/Some([]) distinction: `None` means detection never
        // ran, so redaction gates downstream cannot infer the column is safe.
        let patterns = profile
            .patterns
            .as_ref()
            .map(|ps| ps.iter().map(PyPattern::from).collect());

        Self {
            name: profile.name.clone(),
            data_type: match profile.data_type {
                DataType::Integer => "integer".to_string(),
                DataType::Float => "float".to_string(),
                DataType::String => "string".to_string(),
                DataType::Identifier => "identifier".to_string(),
                DataType::Date => "date".to_string(),
                DataType::Boolean => "boolean".to_string(),
            },
            total_count: profile.total_count,
            null_count: profile.null_count,
            unique_count: profile.unique_count,
            unique_count_is_approximate: profile.unique_count_is_approximate,
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
            outlier_count,
            min_length,
            max_length,
            avg_length,
            true_count,
            false_count,
            true_ratio,
            patterns,
        }
    }
}

#[pymethods]
impl PyColumnProfile {
    fn __repr__(&self) -> String {
        let mut parts = vec![
            format!("name='{}'", self.name),
            format!("type='{}'", self.data_type),
            format!("nulls={:.1}%", self.null_percentage),
        ];

        if let Some(unique) = self.unique_count {
            parts.push(format!("unique={}", unique));
        }
        if let Some(mean) = self.mean {
            parts.push(format!("mean={:.4}", mean));
        } else if let Some(avg_length) = self.avg_length {
            parts.push(format!("avg_len={:.4}", avg_length));
        } else if let Some(true_count) = self.true_count {
            let true_pct = self.true_ratio.unwrap_or(0.0) * 100.0;
            parts.push(format!("true={}({:.0}%)", true_count, true_pct));
        }

        format!("ColumnProfile({})", parts.join(", "))
    }
}

/// Python wrapper for quality dimensions informed by ISO 8000/25012 concepts.
///
/// Exposes both flat backward-compatible properties and new nested dimension
/// accessors (completeness, consistency, uniqueness, accuracy, timeliness,
/// validity, precision).
/// Un-computed dimensions are `None` at the Python level.
#[pyclass(name = "DataQualityMetrics", from_py_object)]
#[derive(Clone)]
pub struct PyDataQualityMetrics {
    inner: QualityMetrics,
}

impl From<&QualityMetrics> for PyDataQualityMetrics {
    fn from(m: &QualityMetrics) -> Self {
        Self { inner: m.clone() }
    }
}

fn warn_flat_quality_accessor(py: Python<'_>, name: &str) -> PyResult<()> {
    let message = CString::new(format!(
        "DataQualityMetrics.{name} is deprecated; use the nested dimension properties \
         such as completeness, consistency, uniqueness, accuracy, timeliness, validity, or precision instead"
    ))
    .expect("warning message contains no nul bytes");
    let category = py.get_type::<PyDeprecationWarning>();
    PyErr::warn(py, &category, message.as_c_str(), 2)
}

#[pymethods]
impl PyDataQualityMetrics {
    // -- Deprecated flat properties --

    #[getter]
    fn missing_values_ratio(&self, py: Python<'_>) -> PyResult<f64> {
        warn_flat_quality_accessor(py, "missing_values_ratio")?;
        Ok(self.inner.missing_values_ratio())
    }
    #[getter]
    fn complete_records_ratio(&self, py: Python<'_>) -> PyResult<f64> {
        warn_flat_quality_accessor(py, "complete_records_ratio")?;
        Ok(self.inner.complete_records_ratio())
    }
    #[getter]
    fn null_columns(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        warn_flat_quality_accessor(py, "null_columns")?;
        Ok(self.inner.null_columns().to_vec())
    }
    #[getter]
    fn data_type_consistency(&self, py: Python<'_>) -> PyResult<f64> {
        warn_flat_quality_accessor(py, "data_type_consistency")?;
        Ok(self.inner.data_type_consistency())
    }
    #[getter]
    fn format_violations(&self, py: Python<'_>) -> PyResult<usize> {
        warn_flat_quality_accessor(py, "format_violations")?;
        Ok(self.inner.format_violations())
    }
    #[getter]
    fn encoding_issues(&self, py: Python<'_>) -> PyResult<usize> {
        warn_flat_quality_accessor(py, "encoding_issues")?;
        Ok(self.inner.encoding_issues())
    }
    #[getter]
    fn duplicate_rows(&self, py: Python<'_>) -> PyResult<usize> {
        warn_flat_quality_accessor(py, "duplicate_rows")?;
        Ok(self.inner.duplicate_rows())
    }
    #[getter]
    fn key_uniqueness(&self, py: Python<'_>) -> PyResult<f64> {
        warn_flat_quality_accessor(py, "key_uniqueness")?;
        Ok(self.inner.key_uniqueness())
    }
    #[getter]
    fn high_cardinality_warning(&self, py: Python<'_>) -> PyResult<bool> {
        warn_flat_quality_accessor(py, "high_cardinality_warning")?;
        Ok(self.inner.high_cardinality_warning())
    }
    #[getter]
    fn outlier_ratio(&self, py: Python<'_>) -> PyResult<f64> {
        warn_flat_quality_accessor(py, "outlier_ratio")?;
        Ok(self.inner.outlier_ratio())
    }
    #[getter]
    fn range_violations(&self, py: Python<'_>) -> PyResult<usize> {
        warn_flat_quality_accessor(py, "range_violations")?;
        Ok(self.inner.range_violations())
    }
    #[getter]
    fn negative_values_in_positive(&self, py: Python<'_>) -> PyResult<usize> {
        warn_flat_quality_accessor(py, "negative_values_in_positive")?;
        Ok(self.inner.negative_values_in_positive())
    }
    #[getter]
    fn future_dates_count(&self, py: Python<'_>) -> PyResult<usize> {
        warn_flat_quality_accessor(py, "future_dates_count")?;
        Ok(self.inner.future_dates_count())
    }
    #[getter]
    fn stale_data_ratio(&self, py: Python<'_>) -> PyResult<f64> {
        warn_flat_quality_accessor(py, "stale_data_ratio")?;
        Ok(self.inner.stale_data_ratio())
    }
    #[getter]
    fn temporal_violations(&self, py: Python<'_>) -> PyResult<usize> {
        warn_flat_quality_accessor(py, "temporal_violations")?;
        Ok(self.inner.temporal_violations())
    }

    /// True when the underlying sample was below the recommended minimum
    /// (10 rows). Treat quality scores as directional rather than reliable
    /// when this is set.
    #[getter]
    fn low_sample_warning(&self) -> bool {
        self.inner.low_sample_warning
    }

    /// Relative weights used by the aggregate quality score.
    #[getter]
    fn score_weights(&self) -> std::collections::HashMap<String, f64> {
        let weights = self.inner.score_weights;
        std::collections::HashMap::from([
            ("completeness".to_string(), weights.completeness),
            ("consistency".to_string(), weights.consistency),
            ("uniqueness".to_string(), weights.uniqueness),
            ("accuracy".to_string(), weights.accuracy),
            ("timeliness".to_string(), weights.timeliness),
            ("validity".to_string(), weights.validity),
            ("precision".to_string(), weights.precision),
        ])
    }

    // -- Nested dimension accessors (composable API) --

    /// Completeness dimension dict, or None if not computed.
    #[getter]
    fn completeness(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<std::collections::HashMap<String, Py<PyAny>>>> {
        self.inner
            .completeness
            .as_ref()
            .map(|c| -> PyResult<_> {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "missing_values_ratio".into(),
                    c.missing_values_ratio
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "complete_records_ratio".into(),
                    c.complete_records_ratio
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "null_columns".into(),
                    c.null_columns
                        .as_slice()
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "total_cells".into(),
                    c.total_cells.into_pyobject(py)?.unbind().into_any(),
                );
                Ok(m)
            })
            .transpose()
    }

    /// Consistency dimension dict, or None if not computed.
    #[getter]
    fn consistency(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<std::collections::HashMap<String, Py<PyAny>>>> {
        self.inner
            .consistency
            .as_ref()
            .map(|c| -> PyResult<_> {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "data_type_consistency".into(),
                    c.data_type_consistency
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "format_violations".into(),
                    c.format_violations.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "encoding_issues".into(),
                    c.encoding_issues.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "values_checked".into(),
                    c.values_checked.into_pyobject(py)?.unbind().into_any(),
                );
                Ok(m)
            })
            .transpose()
    }

    /// Uniqueness dimension dict, or None if not computed.
    #[getter]
    fn uniqueness(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<std::collections::HashMap<String, Py<PyAny>>>> {
        self.inner
            .uniqueness
            .as_ref()
            .map(|u| -> PyResult<_> {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "duplicate_rows".into(),
                    u.duplicate_rows.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "key_uniqueness".into(),
                    u.key_uniqueness.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "high_cardinality_warning".into(),
                    u.high_cardinality_warning
                        .into_pyobject(py)?
                        .to_owned()
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "rows_checked".into(),
                    u.rows_checked.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "key_column".into(),
                    u.key_column
                        .as_deref()
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "duplicate_rows_approximate".into(),
                    u.duplicate_rows_approximate
                        .into_pyobject(py)?
                        .to_owned()
                        .unbind()
                        .into_any(),
                );
                Ok(m)
            })
            .transpose()
    }

    /// Accuracy dimension dict, or None if not computed.
    #[getter]
    fn accuracy(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<std::collections::HashMap<String, Py<PyAny>>>> {
        self.inner
            .accuracy
            .as_ref()
            .map(|a| -> PyResult<_> {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "outlier_ratio".into(),
                    a.outlier_ratio.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "range_violations".into(),
                    a.range_violations.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "negative_values_in_positive".into(),
                    a.negative_values_in_positive
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "numeric_values_checked".into(),
                    a.numeric_values_checked
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                Ok(m)
            })
            .transpose()
    }

    /// Timeliness dimension dict, or None if not computed.
    #[getter]
    fn timeliness(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<std::collections::HashMap<String, Py<PyAny>>>> {
        self.inner
            .timeliness
            .as_ref()
            .map(|t| -> PyResult<_> {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "future_dates_count".into(),
                    t.future_dates_count.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "stale_data_ratio".into(),
                    t.stale_data_ratio.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "temporal_violations".into(),
                    t.temporal_violations.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "date_values_checked".into(),
                    t.date_values_checked.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "temporal_pairs_checked".into(),
                    t.temporal_pairs_checked
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                Ok(m)
            })
            .transpose()
    }

    /// Validity dimension dict, or None if not computed.
    #[getter]
    fn validity(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<std::collections::HashMap<String, Py<PyAny>>>> {
        self.inner
            .validity
            .as_ref()
            .map(|v| -> PyResult<_> {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "valid_values_ratio".into(),
                    v.valid_values_ratio.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "invalid_values".into(),
                    v.invalid_values.into_pyobject(py)?.unbind().into_any(),
                );
                m.insert(
                    "values_checked".into(),
                    v.values_checked.into_pyobject(py)?.unbind().into_any(),
                );
                Ok(m)
            })
            .transpose()
    }

    /// Precision dimension dict, or None if not computed.
    #[getter]
    fn precision(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<std::collections::HashMap<String, Py<PyAny>>>> {
        self.inner
            .precision
            .as_ref()
            .map(|p| -> PyResult<_> {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "decimal_places_consistency".into(),
                    p.decimal_places_consistency
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "inconsistent_precision_values".into(),
                    p.inconsistent_precision_values
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                m.insert(
                    "numeric_values_checked".into(),
                    p.numeric_values_checked
                        .into_pyobject(py)?
                        .unbind()
                        .into_any(),
                );
                Ok(m)
            })
            .transpose()
    }

    /// Overall quality score (0-100): weighted average of the assessed
    /// dimension scores, renormalized over the assessed dimensions.
    /// Returns 0.0 when no dimension was assessable — check
    /// `assessed_dimensions()` to distinguish that case.
    fn overall_quality_score(&self) -> f64 {
        self.inner.overall_score()
    }

    /// Names of the dimensions that had data to assess and contribute to
    /// the overall score.
    fn assessed_dimensions(&self) -> Vec<String> {
        self.inner
            .assessed_dimensions()
            .iter()
            .map(|d| d.to_string())
            .collect()
    }

    /// Per-dimension scores (0-100). A dimension maps to None when it was
    /// not computed or had nothing to assess.
    fn dimension_scores(&self) -> std::collections::HashMap<String, Option<f64>> {
        std::collections::HashMap::from([
            ("completeness".to_string(), self.inner.completeness_score()),
            ("consistency".to_string(), self.inner.consistency_score()),
            ("uniqueness".to_string(), self.inner.uniqueness_score()),
            ("accuracy".to_string(), self.inner.accuracy_score()),
            ("timeliness".to_string(), self.inner.timeliness_score()),
            ("validity".to_string(), self.inner.validity_score()),
            ("precision".to_string(), self.inner.precision_score()),
        ])
    }

    fn __str__(&self) -> String {
        format!(
            "DataQualityMetrics(score={:.1}%, completeness={:.1}%, consistency={:.1}%, uniqueness={:.1}%)",
            self.inner.overall_score(),
            self.inner.complete_records_ratio(),
            self.inner.data_type_consistency(),
            self.inner.key_uniqueness(),
        )
    }

    fn __repr__(&self) -> String {
        let mut dimensions = Vec::new();
        if self.inner.completeness.is_some() {
            dimensions.push("completeness");
        }
        if self.inner.consistency.is_some() {
            dimensions.push("consistency");
        }
        if self.inner.uniqueness.is_some() {
            dimensions.push("uniqueness");
        }
        if self.inner.accuracy.is_some() {
            dimensions.push("accuracy");
        }
        if self.inner.timeliness.is_some() {
            dimensions.push("timeliness");
        }
        if self.inner.validity.is_some() {
            dimensions.push("validity");
        }
        if self.inner.precision.is_some() {
            dimensions.push("precision");
        }

        format!(
            "DataQualityMetrics(score={:.1}%, dimensions=[{}], low_sample_warning={})",
            self.inner.overall_score(),
            dimensions.join(", "),
            self.inner.low_sample_warning,
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

    /// Column-level statistics list (preserves original order)
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
