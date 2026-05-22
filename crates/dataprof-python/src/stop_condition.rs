use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use dataprof::StopCondition;

/// Python-friendly wrapper for composable stop conditions.
///
/// Use static constructors to create a condition, then combine with
/// `|` (any) or `&` (all) operators:
///
/// ```text
/// stop = StopCondition.max_rows(10000) | StopCondition.max_bytes(50_000_000)
/// ```
#[pyclass(name = "StopCondition")]
#[derive(Clone)]
pub struct PyStopCondition {
    pub(crate) inner: StopCondition,
}

impl PyStopCondition {
    pub(crate) fn into_inner(self) -> StopCondition {
        self.inner
    }
}

#[pymethods]
impl PyStopCondition {
    /// Stop after processing this many rows.
    #[staticmethod]
    fn max_rows(n: u64) -> PyResult<Self> {
        if n == 0 {
            return Err(PyValueError::new_err("n must be > 0"));
        }
        Ok(Self {
            inner: StopCondition::MaxRows(n),
        })
    }

    /// Stop after consuming this many bytes.
    #[staticmethod]
    fn max_bytes(n: u64) -> PyResult<Self> {
        if n == 0 {
            return Err(PyValueError::new_err("n must be > 0"));
        }
        Ok(Self {
            inner: StopCondition::MaxBytes(n),
        })
    }

    /// Stop when column types have been stable for N consecutive rows.
    #[staticmethod]
    fn schema_stable(consecutive_stable_rows: u64) -> PyResult<Self> {
        if consecutive_stable_rows == 0 {
            return Err(PyValueError::new_err("consecutive_stable_rows must be > 0"));
        }
        Ok(Self {
            inner: StopCondition::SchemaStable {
                consecutive_stable_rows,
            },
        })
    }

    /// Stop when rows_processed / estimated_total >= threshold.
    ///
    /// Only effective when an estimated total row count is available.
    #[staticmethod]
    fn confidence_threshold(threshold: f64) -> PyResult<Self> {
        if !(0.0..=1.0).contains(&threshold) {
            return Err(PyValueError::new_err(
                "threshold must be between 0.0 and 1.0",
            ));
        }
        Ok(Self {
            inner: StopCondition::ConfidenceThreshold(threshold),
        })
    }

    /// Stop when memory usage exceeds this fraction (0.0-1.0) of the limit.
    #[staticmethod]
    fn memory_pressure(threshold: f64) -> PyResult<Self> {
        if !(0.0..=1.0).contains(&threshold) {
            return Err(PyValueError::new_err(
                "threshold must be between 0.0 and 1.0",
            ));
        }
        Ok(Self {
            inner: StopCondition::MemoryPressure(threshold),
        })
    }

    /// Never stop early — process the entire stream.
    #[staticmethod]
    fn never() -> Self {
        Self {
            inner: StopCondition::Never,
        }
    }

    /// Preset: stop after 10K rows or when schema stabilizes (1K stable rows).
    #[staticmethod]
    fn schema_inference() -> Self {
        Self {
            inner: StopCondition::schema_inference(),
        }
    }

    /// Preset: stop after 50K rows, 50MB, or 95% confidence.
    #[staticmethod]
    fn quality_sample() -> Self {
        Self {
            inner: StopCondition::quality_sample(),
        }
    }

    /// Combine with OR — stop when **either** condition triggers.
    fn __or__(&self, other: &PyStopCondition) -> Self {
        // Flatten nested Any conditions for cleaner composition
        let mut conditions = Vec::new();
        match &self.inner {
            StopCondition::Any(v) => conditions.extend(v.clone()),
            c => conditions.push(c.clone()),
        }
        match &other.inner {
            StopCondition::Any(v) => conditions.extend(v.clone()),
            c => conditions.push(c.clone()),
        }
        Self {
            inner: StopCondition::Any(conditions),
        }
    }

    /// Combine with AND — stop when **both** conditions have triggered.
    fn __and__(&self, other: &PyStopCondition) -> Self {
        // Flatten nested All conditions for cleaner composition
        let mut conditions = Vec::new();
        match &self.inner {
            StopCondition::All(v) => conditions.extend(v.clone()),
            c => conditions.push(c.clone()),
        }
        match &other.inner {
            StopCondition::All(v) => conditions.extend(v.clone()),
            c => conditions.push(c.clone()),
        }
        Self {
            inner: StopCondition::All(conditions),
        }
    }

    fn __repr__(&self) -> String {
        format!("StopCondition({:?})", self.inner)
    }
}
