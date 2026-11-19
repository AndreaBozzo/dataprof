//! Positive counts for the native `SamplingStrategy` and `StopCondition`
//! constructors.
//!
//! PyO3 converts a `usize` or `u64` parameter before the method body runs, so
//! `random(-1)` raises `OverflowError: can't convert negative int to unsigned`
//! and never reaches a check written after that conversion. These helpers
//! extract the value themselves and name the argument and the allowed range.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyInt;

pub(crate) fn require_nonzero_usize(value: &Bound<'_, PyAny>, name: &str) -> PyResult<usize> {
    classify(value.extract::<usize>(), value, name, usize::MAX as u128)
}

pub(crate) fn require_nonzero_u64(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u64> {
    classify(value.extract::<u64>(), value, name, u128::from(u64::MAX))
}

fn classify<T>(
    extracted: PyResult<T>,
    value: &Bound<'_, PyAny>,
    name: &str,
    max: u128,
) -> PyResult<T>
where
    T: Copy + Default + PartialEq,
{
    match extracted {
        Ok(n) if n == T::default() => Err(range_error(name, max, "0")),
        Ok(n) => Ok(n),
        Err(_) => Err(rejected(value, name, max)),
    }
}

fn range_error(name: &str, max: u128, got: &str) -> PyErr {
    PyValueError::new_err(format!(
        "{name} must be an integer between 1 and {max}, got {got}."
    ))
}

fn rejected(value: &Bound<'_, PyAny>, name: &str, max: u128) -> PyErr {
    // `bool` is an `int`, and a bool in range is accepted by `extract` above.
    // A negative or oversized int lands here and keeps the decimal text.
    if value.is_instance_of::<PyInt>() {
        let got = value
            .repr()
            .and_then(|text| text.extract::<String>())
            .unwrap_or_else(|_| "<integer>".to_string());
        return range_error(name, max, &got);
    }
    let kind = value
        .get_type()
        .name()
        .map(|name| name.to_string())
        .unwrap_or_else(|_| "object".to_string());
    PyTypeError::new_err(format!(
        "{name} must be an integer between 1 and {max}, got {kind}."
    ))
}
