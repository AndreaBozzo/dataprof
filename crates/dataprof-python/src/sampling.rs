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
    #[pyo3(signature = (size))]
    fn random(size: &Bound<'_, PyAny>) -> PyResult<Self> {
        let size = crate::count_arg::require_nonzero_usize(size, "size")?;
        Ok(Self {
            inner: SamplingStrategy::Random { size },
        })
    }

    /// Reservoir sampling for streaming data.
    #[staticmethod]
    #[pyo3(signature = (size))]
    fn reservoir(size: &Bound<'_, PyAny>) -> PyResult<Self> {
        let size = crate::count_arg::require_nonzero_usize(size, "size")?;
        Ok(Self {
            inner: SamplingStrategy::Reservoir { size },
        })
    }

    /// Systematic sampling — every Nth row.
    #[staticmethod]
    #[pyo3(signature = (interval))]
    fn systematic(interval: &Bound<'_, PyAny>) -> PyResult<Self> {
        let interval = crate::count_arg::require_nonzero_usize(interval, "interval")?;
        Ok(Self {
            inner: SamplingStrategy::Systematic { interval },
        })
    }

    /// Whether this strategy would drop any row.
    ///
    /// `SamplingStrategy.none()` is a no-op, so callers that always pass a
    /// strategy object are not treated as having asked for sampling.
    #[getter]
    fn is_noop(&self) -> bool {
        matches!(self.inner, SamplingStrategy::None)
    }

    fn __repr__(&self) -> String {
        format!("SamplingStrategy({})", self.inner.description())
    }
}
