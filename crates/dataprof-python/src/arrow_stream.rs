//! Incremental, owning import of one-shot Arrow C Stream producers.
//!
//! Drive the callbacks directly: arrow-rs 59's ArrowArrayStreamReader discards
//! schema error text and unwraps missing batch error text. Both are valid error
//! cases that must remain catchable, with the available producer cause intact.
#![allow(unsafe_code)]

use std::ffi::CStr;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchOptions;
use dataprof::{DataSource, EngineType, ExecutionMetadata, MetricPack, TruncationReason};
use dataprof_core::StreamSourceSystem;
use dataprof_parquet::record_batch_analyzer::RecordBatchAnalyzer;
use dataprof_runtime::ReportAssembler;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::config::PyProfilerConfig;
use crate::errors::{analysis_error_to_py, analyzer_error_to_py};
use crate::types::PyProfileReport;

/// Own the moved C struct, releasing it on every success/error path. Calls stay
/// attached to Python because producer callbacks may execute Python code.
struct ImportedStream {
    stream: FFI_ArrowArrayStream,
    schema: SchemaRef,
}

impl ImportedStream {
    fn new(source: &Bound<'_, PyAny>) -> PyResult<Self> {
        let capsule = source.call_method0("__arrow_c_stream__")?;
        let capsule = capsule.cast::<PyCapsule>()?;
        let pointer = capsule
            .pointer_checked(Some(c"arrow_array_stream"))
            .map_err(|_| PyTypeError::new_err("Expected PyCapsule named 'arrow_array_stream'"))?;
        // SAFETY: the named capsule contains an ArrowArrayStream. from_raw
        // moves it and clears the original release callback, so the capsule
        // destructor cannot release our stream a second time.
        let mut stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer.as_ptr().cast()) };
        if stream.release.is_none() {
            return Err(PyValueError::new_err(
                "Arrow stream was already consumed or released",
            ));
        }
        let get_schema = stream.get_schema.ok_or_else(|| {
            PyTypeError::new_err("Arrow stream is missing its get_schema callback")
        })?;
        if stream.get_next.is_none() {
            return Err(PyTypeError::new_err(
                "Arrow stream is missing its get_next callback",
            ));
        }
        let mut schema = FFI_ArrowSchema::empty();
        // SAFETY: stream is owned and the output is an initialized empty FFI struct.
        let status = unsafe { get_schema(&mut stream, &mut schema) };
        if status != 0 {
            return Err(stream_error(source.py(), &mut stream, "schema", status));
        }
        let schema = Schema::try_from(&schema).map_err(|error| {
            PyTypeError::new_err(format!(
                "Arrow stream must expose a record-batch schema: {error}"
            ))
        })?;
        Ok(Self {
            stream,
            schema: Arc::new(schema),
        })
    }

    fn next(&mut self, py: Python<'_>, remaining: Option<usize>) -> PyResult<Option<RecordBatch>> {
        let mut array = FFI_ArrowArray::empty();
        let get_next = self.stream.get_next.expect("validated stream callback");
        // SAFETY: the stream owns its callbacks and array is a valid output slot.
        let status = unsafe { get_next(&mut self.stream, &mut array) };
        if status != 0 {
            return Err(stream_error(py, &mut self.stream, "batch", status));
        }
        if array.is_released() {
            return Ok(None);
        }
        // SAFETY: the producer supplied a C Data Interface array described by
        // the stream schema. Ownership moves into ArrayData, which releases it
        // even if conversion/validation fails. Validate before typed access.
        let data = unsafe {
            arrow::ffi::from_ffi_and_data_type(
                array,
                DataType::Struct(self.schema.fields().clone()),
            )
        }
        .map_err(|error| {
            PyValueError::new_err(format!("Arrow stream batch import failed: {error}"))
        })?;
        // Check structural bounds before slicing unchecked imported children.
        data.validate().map_err(|error| {
            PyValueError::new_err(format!(
                "Arrow stream batch violates the Arrow columnar spec: {error}"
            ))
        })?;
        let data = match remaining {
            Some(limit) => data.slice(0, limit.min(data.len())),
            None => data,
        };
        // Value validation is O(n), so only validate the requested row slice.
        data.validate_full().map_err(|error| {
            PyValueError::new_err(format!(
                "Arrow stream batch violates the Arrow columnar spec: {error}"
            ))
        })?;
        let array = StructArray::from(data);
        if array.null_count() != 0 {
            return Err(PyValueError::new_err(
                "Arrow stream record batches cannot have null rows",
            ));
        }
        RecordBatch::try_new_with_options(
            self.schema.clone(),
            array.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(array.len())),
        )
        .map(Some)
        .map_err(|error| PyValueError::new_err(error.to_string()))
    }
}

/// The C interface carries error text, not a Python exception object. Copy it
/// before releasing the stream, retaining it as the explicit Python cause.
fn stream_error(
    py: Python<'_>,
    stream: &mut FFI_ArrowArrayStream,
    stage: &str,
    status: i32,
) -> PyErr {
    let message = stream.get_last_error.and_then(|callback| {
        // SAFETY: callback belongs to the live stream; the returned string is
        // borrowed only until the next callback, and is copied here immediately.
        let pointer = unsafe { callback(stream) };
        if pointer.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(pointer) }
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    });
    let error =
        PyRuntimeError::new_err(format!("Arrow stream {stage} failed (error code {status})"));
    if let Some(message) = message {
        error.set_cause(py, Some(PyRuntimeError::new_err(message)));
    }
    error
}

fn reject_nested(data_type: &DataType, name: &str) -> PyResult<()> {
    match data_type {
        DataType::Dictionary(_, values) => reject_nested(values, name),
        DataType::RunEndEncoded(_, values) => reject_nested(values.data_type(), name),
        value if value.is_nested() => Err(PyTypeError::new_err(format!(
            "Arrow stream column '{name}' has unsupported nested type {value}; select flat columns before exporting the stream"
        ))),
        _ => Ok(()),
    }
}

pub(crate) fn profile_stream(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    name: String,
    max_rows: Option<usize>,
    config: Option<&PyProfilerConfig>,
) -> PyResult<PyProfileReport> {
    let start = std::time::Instant::now();
    if let Some(config) = config
        && (!matches!(config.engine, EngineType::Auto | EngineType::Columnar)
            || config.chunk_size.is_some()
            || config.memory_limit_mb.is_some()
            || config.format_override.is_some()
            || config.csv_delimiter.is_some()
            || config.csv_flexible.is_some()
            || config.sampling.is_some()
            || config.stop_condition.is_some()
            || config.on_progress.is_some()
            || config.progress_interval_ms.is_some()
            || config.json_error_policy != dataprof::JsonErrorPolicy::Skip)
    {
        return Err(PyValueError::new_err(
            "Arrow streams cannot apply file/transport controls; use max_rows for a row cap",
        ));
    }
    // decode-audit: no-data — absent configuration selects the default metric packs.
    let options = config
        .map(PyProfilerConfig::analysis_options)
        .unwrap_or_default();
    let packs = options.effective_metric_packs();
    let include_quality = MetricPack::include_quality(packs.as_deref());
    let hints = options.semantic_hints();
    hints
        .validate_quality_usage(include_quality)
        .map_err(|error| analysis_error_to_py(&error))?;
    let max_rows =
        max_rows.or_else(|| config.and_then(|config| config.max_rows.map(|n| n as usize)));
    let mut stream = ImportedStream::new(source)?;
    let names: Vec<_> = stream
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    dataprof::validate_unique_column_names(&names, "Arrow stream schema")
        .map_err(|error| analysis_error_to_py(&error))?;
    let indices = options
        .column_indices(&names)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let schema = match &indices {
        Some(indices) => stream
            .schema
            .project(indices)
            .map_err(|error| PyValueError::new_err(error.to_string()))?,
        None => stream.schema.as_ref().clone(),
    };
    for field in schema.fields() {
        reject_nested(field.data_type(), field.name())?;
    }
    hints
        .validate_names(
            &schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| analysis_error_to_py(&error))?;
    let mut analyzer = RecordBatchAnalyzer::new().with_semantic_hints(hints);
    analyzer
        .initialize_schema(&schema)
        .map_err(analyzer_error_to_py)?;
    let mut rows = 0usize;
    let mut capped = false;
    loop {
        if max_rows.is_some_and(|limit| rows >= limit) {
            capped = true;
            break;
        }
        let Some(mut batch) = stream.next(py, max_rows.map(|limit| limit - rows))? else {
            break;
        };
        if let Some(indices) = &indices {
            batch = batch
                .project(indices)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
        rows += batch.num_rows();
        analyzer
            .process_batch(&batch)
            .map_err(analyzer_error_to_py)?;
        py.check_signals()?;
    }
    let mut execution =
        ExecutionMetadata::new(rows, schema.fields().len(), start.elapsed().as_millis())
            .with_engine("columnar");
    if capped {
        execution = execution.with_truncation(TruncationReason::MaxRows(
            max_rows.expect("cap was reached") as u64,
        ));
    }
    let columns = analyzer.to_profiles_with_hints(
        !MetricPack::include_statistics(packs.as_deref()),
        !MetricPack::include_patterns(packs.as_deref()),
        options.locale(),
        hints,
    );
    let mut assembler = ReportAssembler::new(
        DataSource::Stream {
            topic: name,
            batch_id: "0".to_string(),
            partition: None,
            consumer_group: None,
            source_system: StreamSourceSystem::Custom("arrow_c_stream".to_string()),
            session_id: None,
            first_record_at: None,
            last_record_at: None,
        },
        execution,
    )
    .columns(columns)
    .with_row_duplicates(analyzer.row_duplicate_summary())
    .with_row_completeness(analyzer.row_completeness_summary())
    .with_analysis_options(&options);
    if include_quality {
        assembler = assembler
            .with_quality_data(analyzer.create_sample_columns())
            .with_exact_value_hint_bindings(analyzer.semantic_hint_bindings());
    }
    let report = assembler.build();
    crate::errors::validate_report_hints(&report, hints, include_quality)?;
    Ok(PyProfileReport::new(report))
}
