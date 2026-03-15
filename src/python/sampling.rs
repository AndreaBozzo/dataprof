use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::core::sampling::SamplingStrategy;

/// Python-friendly wrapper for sampling strategies.
///
/// Use static constructors to create a strategy, then pass it to
/// `ProfilerConfig(sampling=...)` or `profile(..., sampling=...)`.
#[pyclass(name = "SamplingStrategy")]
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

    /// Progressive sampling — increases sample size until confidence is reached.
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

    /// Importance sampling for anomaly detection.
    #[staticmethod]
    fn importance(weight_threshold: f64) -> PyResult<Self> {
        if !(0.0..=1.0).contains(&weight_threshold) {
            return Err(PyValueError::new_err(
                "weight_threshold must be between 0.0 and 1.0",
            ));
        }
        Ok(Self {
            inner: SamplingStrategy::Importance { weight_threshold },
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
