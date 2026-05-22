use std::sync::Arc;

use pyo3::prelude::*;

use dataprof::{ProgressEvent, ProgressSink};

/// A progress event emitted during profiling.
///
/// The ``kind`` field discriminates the event type:
/// ``"started"``, ``"chunk_processed"``, ``"schema_detected"``,
/// ``"finished"``, or ``"warning"``.
#[pyclass(name = "ProgressEvent")]
#[derive(Clone)]
pub struct PyProgressEvent {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub rows_processed: Option<usize>,
    #[pyo3(get)]
    pub bytes_consumed: Option<u64>,
    #[pyo3(get)]
    pub elapsed_ms: Option<u64>,
    #[pyo3(get)]
    pub processing_speed: Option<f64>,
    #[pyo3(get)]
    pub percentage: Option<f64>,
    #[pyo3(get)]
    pub column_names: Option<Vec<String>>,
    #[pyo3(get)]
    pub total_rows: Option<usize>,
    #[pyo3(get)]
    pub total_bytes: Option<u64>,
    #[pyo3(get)]
    pub truncated: Option<bool>,
    #[pyo3(get)]
    pub message: Option<String>,
    #[pyo3(get)]
    pub estimated_total_rows: Option<usize>,
    #[pyo3(get)]
    pub estimated_total_bytes: Option<u64>,
}

#[pymethods]
impl PyProgressEvent {
    fn __repr__(&self) -> String {
        match self.kind.as_str() {
            "chunk_processed" => format!(
                "ProgressEvent(kind='chunk_processed', rows={}, speed={:.0} rows/s)",
                self.rows_processed.unwrap_or(0),
                self.processing_speed.unwrap_or(0.0),
            ),
            "finished" => format!(
                "ProgressEvent(kind='finished', total_rows={}, elapsed={}ms)",
                self.total_rows.unwrap_or(0),
                self.elapsed_ms.unwrap_or(0),
            ),
            _ => format!("ProgressEvent(kind='{}')", self.kind),
        }
    }
}

impl From<ProgressEvent> for PyProgressEvent {
    fn from(event: ProgressEvent) -> Self {
        match event {
            ProgressEvent::Started {
                estimated_total_rows,
                estimated_total_bytes,
            } => Self {
                kind: "started".into(),
                estimated_total_rows,
                estimated_total_bytes,
                rows_processed: None,
                bytes_consumed: None,
                elapsed_ms: None,
                processing_speed: None,
                percentage: None,
                column_names: None,
                total_rows: None,
                total_bytes: None,
                truncated: None,
                message: None,
            },
            ProgressEvent::ChunkProcessed {
                rows_processed,
                bytes_consumed,
                elapsed,
                processing_speed,
                percentage,
            } => Self {
                kind: "chunk_processed".into(),
                rows_processed: Some(rows_processed),
                bytes_consumed: Some(bytes_consumed),
                elapsed_ms: Some(elapsed.as_millis() as u64),
                processing_speed: Some(processing_speed),
                percentage,
                column_names: None,
                total_rows: None,
                total_bytes: None,
                truncated: None,
                message: None,
                estimated_total_rows: None,
                estimated_total_bytes: None,
            },
            ProgressEvent::SchemaDetected { column_names } => Self {
                kind: "schema_detected".into(),
                column_names: Some(column_names),
                rows_processed: None,
                bytes_consumed: None,
                elapsed_ms: None,
                processing_speed: None,
                percentage: None,
                total_rows: None,
                total_bytes: None,
                truncated: None,
                message: None,
                estimated_total_rows: None,
                estimated_total_bytes: None,
            },
            ProgressEvent::Finished {
                total_rows,
                total_bytes,
                elapsed,
                truncated,
            } => Self {
                kind: "finished".into(),
                total_rows: Some(total_rows),
                total_bytes: Some(total_bytes),
                elapsed_ms: Some(elapsed.as_millis() as u64),
                truncated: Some(truncated),
                rows_processed: None,
                bytes_consumed: None,
                processing_speed: None,
                percentage: None,
                column_names: None,
                message: None,
                estimated_total_rows: None,
                estimated_total_bytes: None,
            },
            ProgressEvent::Warning { message } => Self {
                kind: "warning".into(),
                message: Some(message),
                rows_processed: None,
                bytes_consumed: None,
                elapsed_ms: None,
                processing_speed: None,
                percentage: None,
                column_names: None,
                total_rows: None,
                total_bytes: None,
                truncated: None,
                estimated_total_rows: None,
                estimated_total_bytes: None,
            },
        }
    }
}

/// Wrap a Python callable into a `ProgressSink::Callback`.
///
/// The callback re-acquires the GIL on each event, so the caller must
/// release the GIL (via `py.allow_threads`) during the profiling operation.
pub(crate) fn py_callback_to_sink(callback: Arc<Py<PyAny>>) -> ProgressSink {
    ProgressSink::Callback(Arc::new(move |event: ProgressEvent| {
        Python::attach(|py| {
            let py_event = PyProgressEvent::from(event);
            if let Err(e) = callback.call1(py, (py_event,)) {
                // Log but don't propagate — progress callbacks should not crash profiling
                eprintln!("dataprof: progress callback error: {}", e);
            }
        });
    }))
}
