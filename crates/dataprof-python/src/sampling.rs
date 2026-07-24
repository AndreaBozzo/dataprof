use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use dataprof::SamplingStrategy;

/// Python-friendly wrapper for sampling strategies.
///
/// Use static constructors to create a strategy, then pass it to
/// `ProfilerConfig(sampling=...)` or `profile(..., sampling=...)`.
#[pyclass(name = "SamplingStrategy", from_py_object)]
#[derive(Clone)]
pub struct PySamplingStrategy {
    pub(crate) inner: SamplingStrategy,
}

impl PySamplingStrategy {
    pub(crate) fn into_inner(self) -> SamplingStrategy {
        self.inner
    }
}

#[pymethods]
impl PySamplingStrategy {
    /// No sampling — analyze all data.
    #[staticmethod]
    fn none() -> Self {
        Self {
            inner: SamplingStrategy::None,
        }
    }

    /// Simple random sampling with a fixed sample size.
    #[staticmethod]
    fn random(size: usize) -> PyResult<Self> {
        if size == 0 {
            return Err(PyValueError::new_err("size must be > 0"));
        }
        Ok(Self {
            inner: SamplingStrategy::Random { size },
        })
    }

    /// Reservoir sampling for streaming data.
    #[staticmethod]
    fn reservoir(size: usize) -> PyResult<Self> {
        if size == 0 {
            return Err(PyValueError::new_err("size must be > 0"));
        }
        Ok(Self {
            inner: SamplingStrategy::Reservoir { size },
        })
    }

    /// Stratified sampling balanced by category columns.
    #[staticmethod]
    fn stratified(key_columns: Vec<String>, samples_per_stratum: usize) -> PyResult<Self> {
        if key_columns.is_empty() {
            return Err(PyValueError::new_err("key_columns must not be empty"));
        }
        if samples_per_stratum == 0 {
            return Err(PyValueError::new_err("samples_per_stratum must be > 0"));
        }
        Ok(Self {
            inner: SamplingStrategy::Stratified {
                key_columns,
                samples_per_stratum,
            },
        })
    }

    /// Progressive sampling — grows the sample until every numeric column's
    /// mean is precise enough.
    ///
    /// `confidence_level` sets a target relative standard error of
    /// `1 - confidence_level`; at 0.95 sampling stops once each numeric mean is
    /// within 5% of itself. Bounded by `initial_size` and `max_size`.
    #[staticmethod]
    #[pyo3(signature = (initial_size, confidence_level=0.95, max_size=100_000))]
    fn progressive(initial_size: usize, confidence_level: f64, max_size: usize) -> PyResult<Self> {
        if initial_size == 0 {
            return Err(PyValueError::new_err("initial_size must be > 0"));
        }
        if !(0.0..=1.0).contains(&confidence_level) {
            return Err(PyValueError::new_err(
                "confidence_level must be between 0.0 and 1.0",
            ));
        }
        if max_size < initial_size {
            return Err(PyValueError::new_err("max_size must be >= initial_size"));
        }
        Ok(Self {
            inner: SamplingStrategy::Progressive {
                initial_size,
                confidence_level,
                max_size,
            },
        })
    }

    /// Systematic sampling — every Nth row.
    #[staticmethod]
    fn systematic(interval: usize) -> PyResult<Self> {
        if interval == 0 {
            return Err(PyValueError::new_err("interval must be > 0"));
        }
        Ok(Self {
            inner: SamplingStrategy::Systematic { interval },
        })
    }

    /// Keep rows whose `weight_column` holds a number at or above the threshold.
    ///
    /// The caller states what matters — there is no built-in notion of an
    /// important row. Rows where the column is missing or non-numeric are
    /// excluded. This is a filter, so the profile describes the rows that met
    /// the threshold, not the source as a whole.
    #[staticmethod]
    fn importance(weight_column: String, weight_threshold: f64) -> PyResult<Self> {
        if weight_column.is_empty() {
            return Err(PyValueError::new_err("weight_column must not be empty"));
        }
        if !weight_threshold.is_finite() {
            return Err(PyValueError::new_err("weight_threshold must be finite"));
        }
        Ok(Self {
            inner: SamplingStrategy::Importance {
                weight_column,
                weight_threshold,
            },
        })
    }

    /// Multi-stage sampling — a sequence of strategies applied in order.
    #[staticmethod]
    fn multi_stage(stages: Vec<PySamplingStrategy>) -> PyResult<Self> {
        if stages.is_empty() {
            return Err(PyValueError::new_err("stages must not be empty"));
        }
        Ok(Self {
            inner: SamplingStrategy::MultiStage {
                stages: stages.into_iter().map(|s| s.inner).collect(),
            },
        })
    }

    /// Adaptive strategy based on estimated dataset size.
    #[staticmethod]
    #[pyo3(signature = (total_rows=None, file_size_mb=0.0))]
    fn adaptive(total_rows: Option<usize>, file_size_mb: f64) -> Self {
        Self {
            inner: SamplingStrategy::adaptive(total_rows, file_size_mb),
        }
    }

    fn __repr__(&self) -> String {
        format!("SamplingStrategy({})", self.inner.description())
    }
}
