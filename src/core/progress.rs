use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Structured progress events emitted by profiling engines.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Profiling has started. Emitted once before any data is processed.
    Started {
        estimated_total_rows: Option<usize>,
        estimated_total_bytes: Option<u64>,
    },
    /// A chunk of rows has been processed.
    ChunkProcessed {
        rows_processed: usize,
        bytes_consumed: u64,
        elapsed: Duration,
        processing_speed: f64,
        percentage: Option<f64>,
    },
    /// Column schema has been detected (emitted once, after first chunk).
    SchemaDetected { column_names: Vec<String> },
    /// Profiling has finished (successfully or via early stop).
    Finished {
        total_rows: usize,
        total_bytes: u64,
        elapsed: Duration,
        truncated: bool,
    },
    /// A non-fatal warning occurred during processing.
    Warning { message: String },
}

/// How progress events are delivered to the consumer.
///
/// All variants are cheaply cloneable (`Arc` / channel `Sender`).
#[derive(Clone, Default)]
pub enum ProgressSink {
    /// No progress reporting.
    #[default]
    None,
    /// Synchronous callback (for CLI usage, tests, etc.)
    Callback(Arc<dyn Fn(ProgressEvent) + Send + Sync>),
    /// Async channel sender (requires `async-streaming` feature).
    #[cfg(feature = "async-streaming")]
    Channel(tokio::sync::mpsc::Sender<ProgressEvent>),
}

impl fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "ProgressSink::None"),
            Self::Callback(_) => write!(f, "ProgressSink::Callback(..)"),
            #[cfg(feature = "async-streaming")]
            Self::Channel(_) => write!(f, "ProgressSink::Channel(..)"),
        }
    }
}

/// Engine-internal tracker that rate-limits event emission.
pub(crate) struct ProgressTracker {
    sink: ProgressSink,
    start_time: Instant,
    last_update: Instant,
    update_interval: Duration,
    total_rows: usize,
    total_bytes: u64,
    schema_sent: bool,
}

impl ProgressTracker {
    pub fn new(sink: ProgressSink, interval: Duration) -> Self {
        let now = Instant::now();
        Self {
            sink,
            start_time: now,
            last_update: now,
            update_interval: interval,
            total_rows: 0,
            total_bytes: 0,
            schema_sent: false,
        }
    }

    pub fn emit_started(
        &mut self,
        estimated_total_rows: Option<usize>,
        estimated_total_bytes: Option<u64>,
    ) {
        self.emit(ProgressEvent::Started {
            estimated_total_rows,
            estimated_total_bytes,
        });
    }

    /// Emit a chunk-processed event (rate-limited by `update_interval`).
    pub fn emit_chunk(
        &mut self,
        rows_in_chunk: usize,
        bytes_in_chunk: u64,
        estimated_total_rows: Option<usize>,
    ) {
        self.total_rows += rows_in_chunk;
        self.total_bytes += bytes_in_chunk;

        let now = Instant::now();
        if now.duration_since(self.last_update) < self.update_interval {
            return;
        }
        self.last_update = now;

        let elapsed = now.duration_since(self.start_time);
        let processing_speed = if elapsed.as_secs_f64() > 0.0 {
            self.total_rows as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        let percentage = estimated_total_rows
            .filter(|&total| total > 0)
            .map(|total| (self.total_rows as f64 / total as f64) * 100.0);

        self.emit(ProgressEvent::ChunkProcessed {
            rows_processed: self.total_rows,
            bytes_consumed: self.total_bytes,
            elapsed,
            processing_speed,
            percentage,
        });
    }

    /// Emit schema detection (only fires once).
    pub fn emit_schema(&mut self, column_names: Vec<String>) {
        if self.schema_sent {
            return;
        }
        self.schema_sent = true;
        self.emit(ProgressEvent::SchemaDetected { column_names });
    }

    pub fn emit_finished(&mut self, truncated: bool) {
        let elapsed = Instant::now().duration_since(self.start_time);
        self.emit(ProgressEvent::Finished {
            total_rows: self.total_rows,
            total_bytes: self.total_bytes,
            elapsed,
            truncated,
        });
    }

    #[allow(dead_code)]
    pub fn emit_warning(&mut self, message: String) {
        self.emit(ProgressEvent::Warning { message });
    }

    fn emit(&self, event: ProgressEvent) {
        match &self.sink {
            ProgressSink::None => {}
            ProgressSink::Callback(cb) => cb(event),
            #[cfg(feature = "async-streaming")]
            ProgressSink::Channel(tx) => {
                // Best-effort: silently drop if channel is full
                let _ = tx.try_send(event);
            }
        }
    }
}
