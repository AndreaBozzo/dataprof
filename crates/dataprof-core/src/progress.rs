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

/// Channel sink holding a sender and an up-front reserved permit for Finished event delivery.
#[cfg(feature = "async-streaming")]
#[derive(Clone)]
pub struct ChannelSink {
    tx: tokio::sync::mpsc::Sender<ProgressEvent>,
    finished_permit: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::OwnedPermit<ProgressEvent>>>>,
}

#[cfg(feature = "async-streaming")]
impl ChannelSink {
    /// Create a new channel sink and attempt to reserve one slot up front for the Finished event.
    pub fn new(tx: tokio::sync::mpsc::Sender<ProgressEvent>) -> Self {
        let permit = tx.clone().try_reserve_owned().ok();
        Self {
            tx,
            finished_permit: Arc::new(std::sync::Mutex::new(permit)),
        }
    }

    /// Access the underlying mpsc Sender.
    pub fn sender(&self) -> &tokio::sync::mpsc::Sender<ProgressEvent> {
        &self.tx
    }
}

/// How progress events are delivered to the consumer.
///
/// All variants are cheaply cloneable (`Arc` / channel `Sender`).
#[derive(Clone, Default)]
pub enum ProgressSink {
    /// No progress reporting.
    #[default]
    None,
    /// Synchronous callback (for Python bindings, tests, embedding, etc.)
    Callback(Arc<dyn Fn(ProgressEvent) + Send + Sync>),
    /// Async channel sender (requires `async-streaming` feature).
    #[cfg(feature = "async-streaming")]
    Channel(ChannelSink),
}

impl ProgressSink {
    /// Whether events go anywhere.
    pub fn is_active(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Construct a channel sink, reserving a slot up front for the terminal Finished event.
    #[cfg(feature = "async-streaming")]
    pub fn channel(tx: tokio::sync::mpsc::Sender<ProgressEvent>) -> Self {
        Self::Channel(ChannelSink::new(tx))
    }

    /// Deliver one event. A full channel drops intermediate events rather than
    /// block profiling, while terminal Finished events use the up-front reserved
    /// permit so they are guaranteed delivery without blocking.
    pub fn emit(&self, event: ProgressEvent) {
        match self {
            Self::None => {}
            Self::Callback(callback) => callback(event),
            #[cfg(feature = "async-streaming")]
            Self::Channel(channel) => {
                if matches!(event, ProgressEvent::Finished { .. }) {
                    if let Ok(mut guard) = channel.finished_permit.lock() {
                        if let Some(permit) = guard.take() {
                            permit.send(event);
                            return;
                        }
                    }
                }
                let _ = channel.tx.try_send(event);
            }
        }
    }
}

#[cfg(feature = "async-streaming")]
impl From<tokio::sync::mpsc::Sender<ProgressEvent>> for ProgressSink {
    fn from(tx: tokio::sync::mpsc::Sender<ProgressEvent>) -> Self {
        Self::channel(tx)
    }
}

#[cfg(feature = "async-streaming")]
impl From<ChannelSink> for ProgressSink {
    fn from(sink: ChannelSink) -> Self {
        Self::Channel(sink)
    }
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
