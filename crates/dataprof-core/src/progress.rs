use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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
