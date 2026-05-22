/// Reason why profiling was truncated before exhausting the source.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TruncationReason {
    /// Stopped after processing a maximum number of rows.
    MaxRows(u64),
    /// Stopped after consuming a maximum number of bytes.
    MaxBytes(u64),
    /// Stopped due to memory pressure.
    MemoryPressure,
    /// Stopped due to a user-defined stop condition.
    StopCondition(String),
    /// The input stream was closed by the producer.
    StreamClosed,
    /// Stopped due to a timeout.
    Timeout,
}

/// Metadata about the profiling execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionMetadata {
    /// Number of rows actually processed or analyzed.
    pub rows_processed: usize,
    /// Number of bytes consumed from the source, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_consumed: Option<u64>,
    /// Number of columns detected in the data.
    pub columns_detected: usize,
    /// Total execution time in milliseconds.
    pub scan_time_ms: u128,
    /// Throughput in rows per second, auto-calculated when possible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_rows_sec: Option<f64>,
    /// Peak memory usage in megabytes, if tracked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_peak_mb: Option<f64>,
    /// Number of errors encountered during profiling.
    pub error_count: usize,
    /// Whether the entire source was consumed.
    pub source_exhausted: bool,
    /// If the source was not exhausted, why processing stopped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<TruncationReason>,
    /// Whether sampling was applied.
    pub sampling_applied: bool,
    /// Ratio of rows analyzed to total rows when sampling is meaningful.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling_ratio: Option<f64>,
}

impl ExecutionMetadata {
    /// Create new execution metadata with throughput calculated automatically.
    pub fn new(rows_processed: usize, columns_detected: usize, scan_time_ms: u128) -> Self {
        let throughput_rows_sec = if scan_time_ms > 0 {
            Some(rows_processed as f64 / (scan_time_ms as f64 / 1000.0))
        } else {
            None
        };

        Self {
            rows_processed,
            bytes_consumed: None,
            columns_detected,
            scan_time_ms,
            throughput_rows_sec,
            memory_peak_mb: None,
            error_count: 0,
            source_exhausted: true,
            truncation_reason: None,
            sampling_applied: false,
            sampling_ratio: None,
        }
    }

    /// Set sampling information.
    pub fn with_sampling(mut self, ratio: f64) -> Self {
        self.sampling_applied = true;
        self.sampling_ratio = Some(ratio);
        self
    }

    /// Explicitly set whether the source was fully consumed.
    pub fn with_source_exhausted(mut self, exhausted: bool) -> Self {
        self.source_exhausted = exhausted;
        self
    }

    /// Mark the execution as truncated.
    pub fn with_truncation(mut self, reason: TruncationReason) -> Self {
        self.source_exhausted = false;
        self.truncation_reason = Some(reason);
        self
    }

    /// Set the number of bytes consumed from the source.
    pub fn with_bytes_consumed(mut self, bytes: u64) -> Self {
        self.bytes_consumed = Some(bytes);
        self
    }

    /// Set the error count.
    pub fn with_error_count(mut self, count: usize) -> Self {
        self.error_count = count;
        self
    }

    /// Set peak memory usage.
    pub fn with_memory_peak_mb(mut self, mb: f64) -> Self {
        self.memory_peak_mb = Some(mb);
        self
    }
}
