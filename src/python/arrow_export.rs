// FFI module: unsafe is required for Arrow C Data Interface PyCapsule operations.
#![allow(unsafe_code)]

//! Arrow PyCapsule Interface for zero-copy data exchange between Rust and Python.
//!
//! This module implements the Arrow C Data Interface PyCapsule protocol, enabling
//! efficient interoperability with pandas, polars, and any Arrow-compatible Python library.
//!
//! ## Ownership Semantics
//!
//! The Arrow PyCapsule Interface requires careful ownership management:
//! - **Export**: PyCapsules contain heap-allocated FFI structs with proper destructors
//!   that call the Arrow release callback if the capsule was never consumed.
//! - **Import**: Consumers must use `FFI_ArrowArray::from_raw()` which nullifies the
//!   release callback, preventing double-free when the capsule is later destroyed.
//!
//! References:
//! - [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! - [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)

use std::sync::Arc;

use arrow::array::{Array, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::engines::columnar::RecordBatchAnalyzer;
use crate::types::{
    ColumnProfile, DataFrameLibrary, DataQualityMetrics, DataSource, QualityReport, ScanInfo,
};

/// PyCapsule name for Arrow schema (null-terminated for C compatibility)
const ARROW_SCHEMA_NAME: &[u8] = b"arrow_schema\0";
/// PyCapsule name for Arrow array (null-terminated for C compatibility)
const ARROW_ARRAY_NAME: &[u8] = b"arrow_array\0";

// ============================================================================
// PyCapsule Destructors for Arrow FFI
// ============================================================================

/// Destructor for FFI_ArrowSchema PyCapsule.
///
/// Per the Arrow PyCapsule Interface spec, the destructor must call the release
/// callback if it hasn't been consumed (release is not null). This handles the
/// case where the capsule is destroyed without being imported by a consumer.
unsafe extern "C" fn pycapsule_schema_destructor(capsule: *mut pyo3::ffi::PyObject) {
    if capsule.is_null() {
        return;
    }
    // Safety: capsule is non-null and was created by PyCapsule_New with ARROW_SCHEMA_NAME
    let ptr =
        unsafe { pyo3::ffi::PyCapsule_GetPointer(capsule, ARROW_SCHEMA_NAME.as_ptr().cast()) };
    if !ptr.is_null() {
        let schema_ptr = ptr as *mut FFI_ArrowSchema;
        // Safety: schema_ptr was heap-allocated via Box::into_raw in __arrow_c_schema__
        unsafe { drop(Box::from_raw(schema_ptr)) };
    }
}

/// Destructor for FFI_ArrowArray PyCapsule.
///
/// Per the Arrow PyCapsule Interface spec, the destructor must call the release
/// callback if it hasn't been consumed (release is not null). This handles the
/// case where the capsule is destroyed without being imported by a consumer.
unsafe extern "C" fn pycapsule_array_destructor(capsule: *mut pyo3::ffi::PyObject) {
    if capsule.is_null() {
        return;
    }
    // Safety: capsule is non-null and was created by PyCapsule_New with ARROW_ARRAY_NAME
    let ptr = unsafe { pyo3::ffi::PyCapsule_GetPointer(capsule, ARROW_ARRAY_NAME.as_ptr().cast()) };
    if !ptr.is_null() {
        let array_ptr = ptr as *mut FFI_ArrowArray;
        // Safety: array_ptr was heap-allocated via Box::into_raw in __arrow_c_array__
        unsafe { drop(Box::from_raw(array_ptr)) };
    }
}

/// Python wrapper for Arrow RecordBatch with PyCapsule protocol support.
///
/// This class enables zero-copy data exchange between Rust and Python
/// by implementing the Arrow PyCapsule Interface.
#[pyclass(name = "RecordBatch")]
pub struct PyRecordBatch {
    inner: RecordBatch,
}

impl PyRecordBatch {
    /// Create from Arrow RecordBatch
    pub fn new(batch: RecordBatch) -> Self {
        Self { inner: batch }
    }

    /// Get reference to inner RecordBatch
    pub fn inner(&self) -> &RecordBatch {
        &self.inner
    }
}

#[pymethods]
impl PyRecordBatch {
    /// Number of rows in the batch
    #[getter]
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Number of columns in the batch
    #[getter]
    fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Column names as a list
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Arrow PyCapsule Interface: export schema.
    ///
    /// Returns a PyCapsule containing the Arrow schema in C Data Interface format.
    /// This allows other Arrow-compatible libraries to inspect the schema.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = self.inner.schema();
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to export schema: {}", e)))?;

        // Heap-allocate for proper ownership transfer via PyCapsule
        let schema_ptr = Box::into_raw(Box::new(ffi_schema));

        // Create PyCapsule with C destructor for Arrow ownership semantics
        // Safety: schema_ptr is valid and heap-allocated
        let capsule = unsafe {
            let cap = pyo3::ffi::PyCapsule_New(
                schema_ptr.cast(),
                ARROW_SCHEMA_NAME.as_ptr().cast(),
                Some(pycapsule_schema_destructor),
            );
            if cap.is_null() {
                // Clean up on failure
                drop(Box::from_raw(schema_ptr));
                return Err(PyRuntimeError::new_err("Failed to create schema PyCapsule"));
            }
            Bound::from_owned_ptr(py, cap)
                .cast_into::<PyCapsule>()
                .map_err(|_| PyRuntimeError::new_err("PyCapsule downcast failed"))?
        };

        Ok(capsule)
    }

    /// Arrow PyCapsule Interface: export array (struct array containing all columns).
    ///
    /// Returns a tuple of (schema_capsule, array_capsule) following the Arrow PyCapsule protocol.
    /// This enables zero-copy data transfer to pandas, polars, pyarrow, and other compatible libraries.
    ///
    /// Per the Arrow PyCapsule Interface spec:
    /// - The capsules contain heap-allocated FFI structs
    /// - Destructors call the release callback if the capsule wasn't consumed
    /// - Consumers must use the appropriate import function which nullifies the release callback
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Py<PyAny>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        // For now, ignore requested_schema (could implement schema negotiation later)
        let _ = requested_schema;

        // Convert RecordBatch to StructArray for FFI export
        let struct_array: arrow::array::StructArray = self.inner.clone().into();
        let array_data = struct_array.into_data();

        // Export via FFI - this creates owned FFI structs with release callbacks
        let (ffi_array, ffi_schema) = to_ffi(&array_data)
            .map_err(|e| PyRuntimeError::new_err(format!("FFI export failed: {}", e)))?;

        // Heap-allocate for proper ownership transfer via PyCapsule
        // The PyCapsule destructor will call Box::from_raw to clean up
        let schema_ptr = Box::into_raw(Box::new(ffi_schema));
        let array_ptr = Box::into_raw(Box::new(ffi_array));

        // Create PyCapsules with C destructors for Arrow ownership semantics
        // Safety: Both pointers are valid and heap-allocated
        let schema_capsule = unsafe {
            let cap = pyo3::ffi::PyCapsule_New(
                schema_ptr.cast(),
                ARROW_SCHEMA_NAME.as_ptr().cast(),
                Some(pycapsule_schema_destructor),
            );
            if cap.is_null() {
                // Clean up both on failure
                drop(Box::from_raw(schema_ptr));
                drop(Box::from_raw(array_ptr));
                return Err(PyRuntimeError::new_err("Failed to create schema PyCapsule"));
            }
            Bound::from_owned_ptr(py, cap)
                .cast_into::<PyCapsule>()
                .map_err(|e| {
                    drop(Box::from_raw(schema_ptr));
                    drop(Box::from_raw(array_ptr));
                    PyRuntimeError::new_err(format!("Schema PyCapsule downcast failed: {}", e))
                })?
        };

        let array_capsule = unsafe {
            let cap = pyo3::ffi::PyCapsule_New(
                array_ptr.cast(),
                ARROW_ARRAY_NAME.as_ptr().cast(),
                Some(pycapsule_array_destructor),
            );
            if cap.is_null() {
                // schema_capsule will be cleaned up when it goes out of scope
                drop(Box::from_raw(array_ptr));
                return Err(PyRuntimeError::new_err("Failed to create array PyCapsule"));
            }
            Bound::from_owned_ptr(py, cap)
                .cast_into::<PyCapsule>()
                .map_err(|e| {
                    drop(Box::from_raw(array_ptr));
                    PyRuntimeError::new_err(format!("Array PyCapsule downcast failed: {}", e))
                })?
        };

        Ok((schema_capsule, array_capsule))
    }

    /// Convert to pandas DataFrame (zero-copy if pyarrow available).
    ///
    /// Requires pyarrow to be installed. Uses the PyCapsule protocol for
    /// efficient zero-copy data transfer.
    fn to_pandas<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Py<PyAny>> {
        // Try pyarrow first for zero-copy
        let pyarrow = py.import("pyarrow").map_err(|_| {
            PyRuntimeError::new_err(
                "pyarrow required for to_pandas(). Install with: pip install pyarrow",
            )
        })?;

        // Use PyCapsule protocol via pa.record_batch(obj) which calls __arrow_c_array__
        // This properly handles ownership transfer per the Arrow PyCapsule Interface spec
        let pa_record_batch = pyarrow.getattr("record_batch")?;
        let pa_batch = pa_record_batch.call1((slf,))?;

        // Convert to pandas
        let df = pa_batch.call_method0("to_pandas")?;
        Ok(df.into())
    }

    /// Convert to polars DataFrame (zero-copy).
    ///
    /// Requires polars to be installed. Polars natively supports the Arrow
    /// PyCapsule protocol for efficient data transfer.
    fn to_polars<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let polars = py.import("polars").map_err(|_| {
            PyRuntimeError::new_err(
                "polars required for to_polars(). Install with: pip install polars",
            )
        })?;

        // Use PyCapsule protocol via pyarrow
        let pyarrow = py.import("pyarrow").map_err(|_| {
            PyRuntimeError::new_err(
                "pyarrow required for to_polars(). Install with: pip install pyarrow",
            )
        })?;

        // pa.record_batch(obj) properly consumes the PyCapsule via __arrow_c_array__
        let pa_record_batch = pyarrow.getattr("record_batch")?;
        let pa_batch = pa_record_batch.call1((slf,))?;

        let pa_table = pyarrow
            .getattr("Table")?
            .call_method1("from_batches", (vec![&pa_batch],))?;

        let df = polars.call_method1("from_arrow", (pa_table,))?;
        Ok(df.into())
    }

    /// String representation
    fn __repr__(&self) -> String {
        format!(
            "RecordBatch(rows={}, columns={}, columns={:?})",
            self.inner.num_rows(),
            self.inner.num_columns(),
            self.column_names()
        )
    }
}

// ============================================================================
// Analysis Functions
// ============================================================================

/// Analyze CSV file and return results as Arrow RecordBatch.
///
/// This function profiles a CSV file and returns the column statistics
/// as an Arrow RecordBatch, enabling zero-copy transfer to pandas/polars.
#[pyfunction]
pub fn analyze_csv_to_arrow(path: &str) -> PyResult<PyRecordBatch> {
    use crate::engines::columnar::ArrowProfiler;
    use std::path::Path;

    let profiler = ArrowProfiler::new();
    let report = profiler
        .analyze_csv_file(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e)))?;

    // Convert column profiles to RecordBatch
    let batch = profiles_to_record_batch(&report.column_profiles)
        .map_err(|e| PyRuntimeError::new_err(format!("Batch conversion failed: {}", e)))?;

    Ok(PyRecordBatch::new(batch))
}

/// Analyze Parquet file and return results as Arrow RecordBatch.
#[cfg(feature = "parquet")]
#[pyfunction]
pub fn analyze_parquet_to_arrow(path: &str) -> PyResult<PyRecordBatch> {
    use crate::analyze_parquet_with_quality;
    use std::path::Path;

    let report = analyze_parquet_with_quality(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Parquet analysis failed: {}", e)))?;

    let batch = profiles_to_record_batch(&report.column_profiles)
        .map_err(|e| PyRuntimeError::new_err(format!("Batch conversion failed: {}", e)))?;

    Ok(PyRecordBatch::new(batch))
}

/// Profile a pandas or polars DataFrame directly.
///
/// This function accepts any object implementing the Arrow PyCapsule protocol,
/// including pandas DataFrames (with pyarrow) and polars DataFrames.
///
/// # Arguments
/// * `df` - A pandas DataFrame, polars DataFrame, or any Arrow-compatible object
/// * `name` - Optional name for identification in reports (default: "dataframe")
///
/// # Returns
/// A PyQualityReport with comprehensive data quality metrics
#[pyfunction]
#[pyo3(signature = (df, name = "dataframe".to_string()))]
pub fn profile_dataframe(
    py: Python<'_>,
    df: Py<PyAny>,
    name: String,
) -> PyResult<super::types::PyQualityReport> {
    let start = std::time::Instant::now();

    // Detect source library
    let source_library = detect_dataframe_library(py, &df)?;

    // Convert DataFrame to RecordBatch via PyCapsule
    let batch = convert_dataframe_to_batch(py, &df, &source_library)?;

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    // Process using existing RecordBatchAnalyzer
    let mut analyzer = RecordBatchAnalyzer::new();
    analyzer
        .process_batch(&batch)
        .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e)))?;

    let column_profiles = analyzer.to_profiles();
    let sample_columns = analyzer.create_sample_columns();

    // Calculate quality metrics
    let data_quality_metrics =
        DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
            .map_err(|e| PyRuntimeError::new_err(format!("Quality metrics failed: {}", e)))?;

    let scan_time_ms = start.elapsed().as_millis();

    // Estimate memory usage before consuming the source library value
    let memory_bytes = estimate_memory_bytes(py, &df, &source_library);

    // Create report with DataFrame data source
    let report = QualityReport::new(
        DataSource::DataFrame {
            name,
            source_library,
            row_count: num_rows,
            column_count: num_cols,
            memory_bytes,
        },
        column_profiles,
        ScanInfo::new(num_rows, num_cols, num_rows, 1.0, scan_time_ms),
        data_quality_metrics,
    );

    Ok(super::types::PyQualityReport::from(&report))
}

/// Profile a PyArrow Table or RecordBatch directly.
///
/// This function is optimized for PyArrow objects and avoids the library
/// detection overhead of `profile_dataframe()`.
///
/// # Arguments
/// * `table` - A pyarrow.Table or pyarrow.RecordBatch
/// * `name` - Optional name for identification in reports (default: "arrow_table")
///
/// # Returns
/// A PyQualityReport with comprehensive data quality metrics
#[pyfunction]
#[pyo3(signature = (table, name = "arrow_table".to_string()))]
pub fn profile_arrow(
    py: Python<'_>,
    table: Py<PyAny>,
    name: String,
) -> PyResult<super::types::PyQualityReport> {
    let start = std::time::Instant::now();

    let bound = table.bind(py);

    // Import directly as PyArrow data (no library detection needed)
    let batch = import_from_pyarrow(py, bound)?;

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    // Estimate memory from pyarrow's nbytes
    let memory_bytes: Option<u64> = bound
        .getattr("nbytes")
        .and_then(|v| v.extract::<u64>())
        .ok();

    // Process using existing RecordBatchAnalyzer
    let mut analyzer = RecordBatchAnalyzer::new();
    analyzer
        .process_batch(&batch)
        .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e)))?;

    let column_profiles = analyzer.to_profiles();
    let sample_columns = analyzer.create_sample_columns();

    // Calculate quality metrics
    let data_quality_metrics =
        DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
            .map_err(|e| PyRuntimeError::new_err(format!("Quality metrics failed: {}", e)))?;

    let scan_time_ms = start.elapsed().as_millis();

    let report = QualityReport::new(
        DataSource::DataFrame {
            name,
            source_library: DataFrameLibrary::PyArrow,
            row_count: num_rows,
            column_count: num_cols,
            memory_bytes,
        },
        column_profiles,
        ScanInfo::new(num_rows, num_cols, num_rows, 1.0, scan_time_ms),
        data_quality_metrics,
    );

    Ok(super::types::PyQualityReport::from(&report))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert column profiles to Arrow RecordBatch for export.
fn profiles_to_record_batch(profiles: &[ColumnProfile]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("column_name", ArrowDataType::Utf8, false),
        Field::new("data_type", ArrowDataType::Utf8, false),
        Field::new("total_count", ArrowDataType::UInt64, false),
        Field::new("null_count", ArrowDataType::UInt64, false),
        Field::new("null_percentage", ArrowDataType::Float64, false),
        Field::new("unique_count", ArrowDataType::UInt64, true),
        Field::new("uniqueness_ratio", ArrowDataType::Float64, false),
    ]));

    let names: StringArray = profiles.iter().map(|p| Some(p.name.as_str())).collect();
    let types: StringArray = profiles
        .iter()
        .map(|p| Some(format!("{:?}", p.data_type)))
        .collect();
    let totals: UInt64Array = profiles
        .iter()
        .map(|p| Some(p.total_count as u64))
        .collect();
    let nulls: UInt64Array = profiles.iter().map(|p| Some(p.null_count as u64)).collect();
    let null_pcts: Float64Array = profiles
        .iter()
        .map(|p| {
            let pct = if p.total_count > 0 {
                (p.null_count as f64 / p.total_count as f64) * 100.0
            } else {
                0.0
            };
            Some(pct)
        })
        .collect();
    let uniques: UInt64Array = profiles
        .iter()
        .map(|p| p.unique_count.map(|u| u as u64))
        .collect();
    let unique_ratios: Float64Array = profiles
        .iter()
        .map(|p| {
            let ratio = match p.unique_count {
                Some(u) if p.total_count > 0 => (u as f64 / p.total_count as f64) * 100.0,
                _ => 0.0,
            };
            Some(ratio)
        })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(names),
            Arc::new(types),
            Arc::new(totals),
            Arc::new(nulls),
            Arc::new(null_pcts),
            Arc::new(uniques),
            Arc::new(unique_ratios),
        ],
    )
    .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
}

/// Detect which DataFrame library the object comes from.
fn detect_dataframe_library(py: Python<'_>, df: &Py<PyAny>) -> PyResult<DataFrameLibrary> {
    let bound = df.bind(py);
    let type_name = bound.get_type().name()?.to_string();
    let module = bound
        .get_type()
        .getattr("__module__")
        .map(|m| m.to_string())
        .unwrap_or_default();

    if module.starts_with("pandas") || (type_name == "DataFrame" && module.contains("pandas")) {
        Ok(DataFrameLibrary::Pandas)
    } else if module.starts_with("polars") {
        Ok(DataFrameLibrary::Polars)
    } else if module.starts_with("pyarrow") {
        Ok(DataFrameLibrary::PyArrow)
    } else {
        Ok(DataFrameLibrary::Custom(format!(
            "{}:{}",
            module, type_name
        )))
    }
}

/// Estimate memory usage of a DataFrame using library-specific methods.
///
/// Returns None if estimation fails (non-critical).
fn estimate_memory_bytes(
    py: Python<'_>,
    df: &Py<PyAny>,
    library: &DataFrameLibrary,
) -> Option<u64> {
    let bound = df.bind(py);

    match library {
        DataFrameLibrary::Pandas => {
            // pandas: df.memory_usage(deep=True).sum()
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("deep", true).ok()?;
            bound
                .call_method("memory_usage", (), Some(&kwargs))
                .ok()
                .and_then(|usage| usage.call_method0("sum").ok())
                .and_then(|val| val.extract::<u64>().ok())
        }
        DataFrameLibrary::Polars => {
            // polars: df.estimated_size()
            bound
                .call_method0("estimated_size")
                .ok()
                .and_then(|val| val.extract::<u64>().ok())
        }
        DataFrameLibrary::PyArrow => {
            // pyarrow: table.nbytes
            bound
                .getattr("nbytes")
                .ok()
                .and_then(|val| val.extract::<u64>().ok())
        }
        DataFrameLibrary::Custom(_) => None,
    }
}

/// Convert DataFrame to Arrow RecordBatch via appropriate method.
fn convert_dataframe_to_batch(
    py: Python<'_>,
    df: &Py<PyAny>,
    source_library: &DataFrameLibrary,
) -> PyResult<RecordBatch> {
    let bound = df.bind(py);

    match source_library {
        DataFrameLibrary::Pandas => convert_pandas_to_batch(py, bound),
        DataFrameLibrary::Polars => convert_polars_to_batch(py, bound),
        DataFrameLibrary::PyArrow => import_from_pyarrow(py, bound),
        DataFrameLibrary::Custom(_) => {
            // Try generic PyCapsule import
            if bound.hasattr("__arrow_c_array__")? {
                import_via_pycapsule(py, bound)
            } else {
                Err(PyTypeError::new_err(format!(
                    "Unsupported DataFrame type: {}. Must implement Arrow PyCapsule protocol.",
                    source_library
                )))
            }
        }
    }
}

/// Convert pandas DataFrame to RecordBatch.
fn convert_pandas_to_batch(py: Python<'_>, df: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let pyarrow = py.import("pyarrow").map_err(|_| {
        PyRuntimeError::new_err(
            "pyarrow required for pandas DataFrames. Install with: pip install pyarrow",
        )
    })?;

    // Convert pandas to pyarrow Table
    let pa_table = pyarrow
        .getattr("Table")?
        .call_method1("from_pandas", (df,))?;

    // Get batches and import the first one
    let batches = pa_table.call_method0("to_batches")?;
    let batch_list: Vec<Py<PyAny>> = batches.extract()?;

    if batch_list.is_empty() {
        return Err(PyValueError::new_err("DataFrame is empty"));
    }

    // Import first batch via PyCapsule
    import_via_pycapsule(py, batch_list[0].bind(py))
}

/// Convert polars DataFrame to RecordBatch.
fn convert_polars_to_batch(py: Python<'_>, df: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    // polars DataFrame has to_arrow() method
    if df.hasattr("to_arrow")? {
        let arrow_data = df.call_method0("to_arrow")?;

        // The result is a pyarrow Table, get first batch
        let batches = arrow_data.call_method0("to_batches")?;
        let batch_list: Vec<Py<PyAny>> = batches.extract()?;

        if batch_list.is_empty() {
            return Err(PyValueError::new_err("DataFrame is empty"));
        }

        import_via_pycapsule(py, batch_list[0].bind(py))
    } else {
        Err(PyRuntimeError::new_err(
            "polars DataFrame doesn't support Arrow export",
        ))
    }
}

/// Import from pyarrow Table or RecordBatch.
fn import_from_pyarrow(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let type_name = obj.get_type().name()?.to_string();

    if type_name == "Table" {
        // Convert Table to batches
        let batches = obj.call_method0("to_batches")?;
        let batch_list: Vec<Py<PyAny>> = batches.extract()?;

        if batch_list.is_empty() {
            return Err(PyValueError::new_err("Table is empty"));
        }

        import_via_pycapsule(py, batch_list[0].bind(py))
    } else if type_name == "RecordBatch" {
        import_via_pycapsule(py, obj)
    } else {
        Err(PyTypeError::new_err(format!(
            "Expected pyarrow Table or RecordBatch, got {}",
            type_name
        )))
    }
}

/// Import RecordBatch via PyCapsule protocol.
///
/// This function properly consumes the PyCapsules by using FFI_ArrowArray::from_raw()
/// which nullifies the release callback in the capsule, preventing double-free when
/// the capsule is later destroyed by Python.
fn import_via_pycapsule(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    // Check for __arrow_c_array__ method
    if !obj.hasattr("__arrow_c_array__")? {
        return Err(PyTypeError::new_err(
            "Object does not implement Arrow PyCapsule interface (__arrow_c_array__)",
        ));
    }

    // Get capsules - pass None for requested_schema
    let result = obj.call_method1("__arrow_c_array__", (py.None(),))?;
    let tuple: (Bound<'_, PyAny>, Bound<'_, PyAny>) = result.extract()?;
    let (schema_capsule, array_capsule) = tuple;

    // Extract PyCapsule references using Bound::cast (non-deprecated)
    let schema_cap: &Bound<'_, PyCapsule> = schema_capsule
        .cast()
        .map_err(|_| PyTypeError::new_err("Expected PyCapsule for schema"))?;
    let array_cap: &Bound<'_, PyCapsule> = array_capsule
        .cast()
        .map_err(|_| PyTypeError::new_err("Expected PyCapsule for array"))?;

    // Get raw pointers from capsules and properly consume them
    // Safety: We trust that pyarrow provides valid Arrow C Data Interface structures.
    // Using from_raw() nullifies the release callback in the original struct,
    // preventing double-free when Python's GC destroys the capsule.
    let array_data = unsafe {
        // Get raw pointers - pointer() returns *mut c_void
        #[allow(deprecated)]
        let ffi_schema_ptr = schema_cap.pointer() as *mut FFI_ArrowSchema;
        #[allow(deprecated)]
        let ffi_array_ptr = array_cap.pointer() as *mut FFI_ArrowArray;

        if ffi_schema_ptr.is_null() || ffi_array_ptr.is_null() {
            return Err(PyRuntimeError::new_err("Null pointer in PyCapsule"));
        }

        // Use from_raw() which takes ownership by nullifying the release callback
        // in the original struct. This is the Arrow-specified way to consume FFI data.
        // After this call, the PyCapsule's destructor will see release=null and do nothing.
        let ffi_array = FFI_ArrowArray::from_raw(ffi_array_ptr);
        let ffi_schema = FFI_ArrowSchema::from_raw(ffi_schema_ptr);

        arrow::ffi::from_ffi(ffi_array, &ffi_schema)
            .map_err(|e| PyRuntimeError::new_err(format!("FFI import failed: {}", e)))?
    };

    // Convert StructArray to RecordBatch
    let struct_array = arrow::array::StructArray::from(array_data);
    let schema = Arc::new(Schema::new(
        struct_array
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));

    RecordBatch::try_new(schema, struct_array.columns().to_vec())
        .map_err(|e| PyRuntimeError::new_err(format!("RecordBatch creation failed: {}", e)))
}
