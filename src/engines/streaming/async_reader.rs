use std::io::Read;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::core::errors::DataProfilerError;
use crate::core::profile_builder;
use crate::core::report_assembler::ReportAssembler;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::core::stop_condition::{SchemaStabilityTracker, StopCondition, StopEvaluator};
use crate::core::streaming_stats::StreamingColumnCollection;
use crate::engines::streaming::{ProgressCallback, ProgressTracker};
use crate::types::{DataSource, ExecutionMetadata, FileFormat, ProfileReport, StreamSourceSystem};

use super::async_source::AsyncDataSource;

/// A chunk of parsed records sent through the bounded channel.
struct ParsedChunk {
    /// Rows of field values (each inner Vec is one row).
    records: Vec<Vec<String>>,
    /// Number of raw bytes consumed to produce this chunk (for progress).
    bytes_read: u64,
}

/// Async streaming profiler that accepts [`AsyncDataSource`] instead of file paths.
///
/// Uses a bounded channel between a blocking CSV reader task and an async
/// processing loop to provide natural backpressure: when the processor falls
/// behind, the reader pauses, which propagates TCP pressure back to the source.
///
/// # Architecture
///
/// ```text
/// AsyncDataSource ──► [spawn_blocking: csv::Reader] ──► bounded mpsc ──► [processor loop]
///                       SyncIoBridge + csv crate           capacity N      StreamingColumnCollection
///                       (RFC 4180 compliant)                               ──► QualityReport
/// ```
pub struct AsyncStreamingProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    memory_limit_mb: usize,
    channel_capacity: usize,
    progress_callback: Option<ProgressCallback>,
    stop_condition: StopCondition,
}

impl AsyncStreamingProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            memory_limit_mb: 256,
            channel_capacity: 4,
            progress_callback: None,
            stop_condition: StopCondition::Never,
        }
    }

    pub fn chunk_size(mut self, chunk_size: ChunkSize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn sampling(mut self, strategy: SamplingStrategy) -> Self {
        self.sampling_strategy = strategy;
        self
    }

    pub fn memory_limit_mb(mut self, limit: usize) -> Self {
        self.memory_limit_mb = limit;
        self
    }

    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(1);
        self
    }

    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(super::ProgressInfo) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(callback));
        self
    }

    pub fn stop_condition(mut self, condition: StopCondition) -> Self {
        self.stop_condition = condition;
        self
    }

    /// Profile data from an async source, returning a [`QualityReport`].
    ///
    /// Supports CSV, JSON, and JSONL formats. Parquet async support is tracked separately.
    pub async fn analyze_stream(
        &self,
        source: impl AsyncDataSource,
    ) -> Result<ProfileReport, DataProfilerError> {
        let source_info = source.source_info();
        let format = source_info.format.clone();

        match format {
            FileFormat::Csv | FileFormat::Json | FileFormat::Jsonl => {}
            _ => {
                return Err(DataProfilerError::StreamingError {
                    message: format!(
                        "AsyncStreamingProfiler does not support {:?} format",
                        format
                    ),
                });
            }
        }

        let start = std::time::Instant::now();
        let reader = source.into_async_read().await?;

        let rows_per_chunk = self.rows_per_chunk();
        let (tx, rx) = mpsc::channel::<ParsedChunk>(self.channel_capacity);

        // Bridge the AsyncRead into a sync Read for parsing.
        let sync_reader = tokio_util::io::SyncIoBridge::new(reader);

        // Spawn the reader on a blocking thread — format determines the parser.
        let reader_handle = tokio::task::spawn_blocking(move || match format {
            FileFormat::Csv => Self::reader_task(sync_reader, tx, rows_per_chunk),
            FileFormat::Json | FileFormat::Jsonl => {
                Self::json_reader_task(sync_reader, tx, rows_per_chunk, format)
            }
            _ => unreachable!(),
        });

        // Process chunks on the current task
        let process_result = self.process_chunks(rx, source_info.size_hint).await;

        // If processing failed, abort the reader task to avoid leaking it
        let (_headers, column_stats, total_rows, sampled_rows, total_bytes, truncation_reason) =
            match process_result {
                Ok(result) => result,
                Err(e) => {
                    reader_handle.abort();
                    return Err(e);
                }
            };

        // Wait for the reader task to finish and propagate any errors
        match reader_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(join_err) if join_err.is_cancelled() => {
                // We cancelled it ourselves — fine
            }
            Err(join_err) => {
                return Err(DataProfilerError::StreamingError {
                    message: format!("Reader task panicked: {}", join_err),
                });
            }
        }

        // Build the report
        let column_profiles = profile_builder::profiles_from_streaming(&column_stats);
        let sample_columns = profile_builder::quality_check_samples(&column_stats);

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        let data_source = DataSource::Stream {
            topic: source_info.label,
            batch_id: uuid::Uuid::new_v4().to_string(),
            partition: None,
            consumer_group: None,
            source_system: source_info
                .source_system
                .unwrap_or(StreamSourceSystem::Http),
            session_id: None,
            first_record_at: None,
            last_record_at: None,
        };

        let mut execution = ExecutionMetadata::new(sampled_rows, num_columns, scan_time_ms)
            .with_bytes_consumed(total_bytes);

        if let Some(reason) = truncation_reason {
            execution = execution.with_truncation(reason);
        } else if total_rows > 0 && sampled_rows < total_rows {
            let ratio = sampled_rows as f64 / total_rows as f64;
            execution = execution.with_sampling(ratio).with_source_exhausted(false);
        }

        Ok(ReportAssembler::new(data_source, execution)
            .columns(column_profiles)
            .with_quality_data(sample_columns)
            .build())
    }

    /// Blocking reader task: uses the `csv` crate's RFC 4180-compliant parser
    /// over a `SyncIoBridge` to correctly handle quoted fields with embedded newlines.
    fn reader_task(
        sync_reader: tokio_util::io::SyncIoBridge<
            std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>,
        >,
        tx: mpsc::Sender<ParsedChunk>,
        rows_per_chunk: usize,
    ) -> Result<(), DataProfilerError> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(sync_reader);

        // Send headers as the first chunk
        let headers = csv_reader
            .headers()
            .map_err(|e| DataProfilerError::CsvParsingError {
                message: e.to_string(),
                suggestion: "Check CSV formatting in the stream data".to_string(),
            })?;

        let header_fields: Vec<String> = headers.iter().map(|f| f.to_string()).collect();
        let header_chunk = ParsedChunk {
            records: vec![header_fields],
            bytes_read: 0,
        };
        if tx.blocking_send(header_chunk).is_err() {
            return Ok(());
        }

        // Read data records in chunks
        let mut current_chunk: Vec<Vec<String>> = Vec::with_capacity(rows_per_chunk);
        let mut bytes_in_chunk: u64 = 0;

        for result in csv_reader.records() {
            let record = result.map_err(|e| DataProfilerError::CsvParsingError {
                message: e.to_string(),
                suggestion: "Check CSV formatting in the stream data".to_string(),
            })?;

            bytes_in_chunk += record.as_slice().len() as u64 + 1;
            let fields: Vec<String> = record.iter().map(|f| f.to_string()).collect();
            current_chunk.push(fields);

            if current_chunk.len() >= rows_per_chunk {
                let chunk = ParsedChunk {
                    records: std::mem::replace(
                        &mut current_chunk,
                        Vec::with_capacity(rows_per_chunk),
                    ),
                    bytes_read: bytes_in_chunk,
                };
                bytes_in_chunk = 0;

                if tx.blocking_send(chunk).is_err() {
                    return Ok(());
                }
            }
        }

        // Send remaining records
        if !current_chunk.is_empty() {
            let chunk = ParsedChunk {
                records: current_chunk,
                bytes_read: bytes_in_chunk,
            };
            let _ = tx.blocking_send(chunk);
        }

        Ok(())
    }

    /// Blocking reader task for JSON/JSONL streams.
    ///
    /// For **JSONL**: reads line-by-line (true streaming, bounded memory).
    /// For **JSON array**: streams elements incrementally using `serde_json::Deserializer`,
    /// processing each object without buffering the entire array in memory.
    ///
    /// The first chunk sent contains column names (like the CSV reader task sends headers).
    fn json_reader_task(
        sync_reader: tokio_util::io::SyncIoBridge<
            std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>,
        >,
        tx: mpsc::Sender<ParsedChunk>,
        rows_per_chunk: usize,
        format: FileFormat,
    ) -> Result<(), DataProfilerError> {
        use serde_json::Value;
        use std::io::BufRead;

        let mut buf_reader = std::io::BufReader::new(sync_reader);
        let mut known_columns: Vec<String> = Vec::new();
        let mut current_chunk: Vec<Vec<String>> = Vec::with_capacity(rows_per_chunk);
        let mut known_columns_set: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut bytes_in_chunk: u64 = 0;
        let mut headers_sent = false;

        // Helper closure: convert a JSON object into a row aligned to known_columns.
        // New columns are only registered before headers are sent; once headers have
        // been emitted, the schema is frozen to keep rows aligned with the header set.
        let process_object = |obj: &serde_json::Map<String, Value>,
                              known_cols: &mut Vec<String>,
                              known_cols_set: &mut std::collections::HashSet<String>,
                              is_headers_sent: bool|
         -> Vec<String> {
            // Register new columns only while headers have not been sent yet
            if !is_headers_sent {
                for key in obj.keys() {
                    if known_cols_set.insert(key.clone()) {
                        known_cols.push(key.clone());
                    }
                }
            }
            // Build row aligned to known_cols
            known_cols
                .iter()
                .map(|col| {
                    obj.get(col)
                        .map(|v| match v {
                            Value::Null => String::new(),
                            Value::Bool(b) => b.to_string(),
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => s.to_string(),
                            _ => serde_json::to_string(v).unwrap_or_default(),
                        })
                        .unwrap_or_default()
                })
                .collect()
        };

        // Helper closure: send headers (first chunk) and flush accumulated rows.
        let send_chunk = |chunk: &mut Vec<Vec<String>>,
                          bytes: &mut u64,
                          cols: &[String],
                          headers_sent: &mut bool,
                          tx: &mpsc::Sender<ParsedChunk>|
         -> Result<bool, DataProfilerError> {
            if !*headers_sent && !cols.is_empty() {
                let header_chunk = ParsedChunk {
                    records: vec![cols.to_vec()],
                    bytes_read: 0,
                };
                if tx.blocking_send(header_chunk).is_err() {
                    return Ok(false); // receiver dropped
                }
                *headers_sent = true;
            }

            if !chunk.is_empty() {
                let data_chunk = ParsedChunk {
                    records: std::mem::replace(chunk, Vec::with_capacity(rows_per_chunk)),
                    bytes_read: *bytes,
                };
                *bytes = 0;
                if tx.blocking_send(data_chunk).is_err() {
                    return Ok(false);
                }
            }
            Ok(true)
        };

        match format {
            FileFormat::Jsonl => {
                // True streaming: line-by-line
                let mut reader = buf_reader.take(u64::MAX); // Use take to get a Read trait object
                let de = serde_json::Deserializer::from_reader(&mut reader);
                for value in de.into_iter::<Value>() {
                    let v = value.map_err(|e| DataProfilerError::JsonParsingError {
                        message: e.to_string(),
                    })?;

                    if let Value::Object(obj) = v {
                        let row = process_object(
                            &obj,
                            &mut known_columns,
                            &mut known_columns_set,
                            headers_sent,
                        );
                        bytes_in_chunk += row.iter().map(|s| s.len() as u64 + 4).sum::<u64>();
                        current_chunk.push(row);

                        if current_chunk.len() >= rows_per_chunk
                            && !send_chunk(
                                &mut current_chunk,
                                &mut bytes_in_chunk,
                                &known_columns,
                                &mut headers_sent,
                                &tx,
                            )?
                        {
                            return Ok(());
                        }
                    }
                }
            }
            _ => {
                // JSON array: stream elements efficiently
                let mut found_array = false;
                loop {
                    let mut consume = 0;
                    {
                        let buf = buf_reader.fill_buf().map_err(DataProfilerError::from)?;
                        if buf.is_empty() {
                            break;
                        }
                        for &b in buf {
                            consume += 1;
                            if b == b'[' {
                                found_array = true;
                                break;
                            } else if !b.is_ascii_whitespace() {
                                break;
                            }
                        }
                    }
                    buf_reader.consume(consume);
                    bytes_in_chunk += consume as u64;
                    if found_array || consume == 0 {
                        break;
                    }
                }

                if found_array {
                    loop {
                        let mut consume = 0;
                        let mut found_value = false;
                        let mut end_of_array = false;

                        {
                            let buf = buf_reader.fill_buf().map_err(DataProfilerError::from)?;
                            if buf.is_empty() {
                                break;
                            }
                            for &b in buf {
                                if b.is_ascii_whitespace() || b == b',' {
                                    consume += 1;
                                } else if b == b']' {
                                    end_of_array = true;
                                    consume += 1;
                                    break;
                                } else {
                                    found_value = true;
                                    break;
                                }
                            }
                        }

                        buf_reader.consume(consume);
                        bytes_in_chunk += consume as u64;

                        if end_of_array {
                            break;
                        }

                        if found_value {
                            let mut de = serde_json::Deserializer::from_reader(&mut buf_reader);
                            match serde::Deserialize::deserialize(&mut de) {
                                Ok(Value::Object(obj)) => {
                                    let row = process_object(
                                        &obj,
                                        &mut known_columns,
                                        &mut known_columns_set,
                                        headers_sent,
                                    );
                                    bytes_in_chunk +=
                                        row.iter().map(|s| s.len() as u64 + 4).sum::<u64>();
                                    current_chunk.push(row);

                                    if current_chunk.len() >= rows_per_chunk
                                        && !send_chunk(
                                            &mut current_chunk,
                                            &mut bytes_in_chunk,
                                            &known_columns,
                                            &mut headers_sent,
                                            &tx,
                                        )?
                                    {
                                        return Ok(());
                                    }
                                }
                                Ok(_) => {
                                    // Skip non-object items, approximate 10 bytes progress
                                    bytes_in_chunk += 10;
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Flush remaining
        if !current_chunk.is_empty() || !headers_sent {
            let _ = send_chunk(
                &mut current_chunk,
                &mut bytes_in_chunk,
                &known_columns,
                &mut headers_sent,
                &tx,
            );
        }

        Ok(())
    }

    /// Receive parsed chunks and feed them into StreamingColumnCollection.
    ///
    /// Returns (headers, column_stats, total_rows, sampled_rows, total_bytes_read, truncation_reason).
    async fn process_chunks(
        &self,
        mut rx: mpsc::Receiver<ParsedChunk>,
        size_hint: Option<u64>,
    ) -> Result<
        (
            Vec<String>,
            StreamingColumnCollection,
            usize,
            usize,
            u64,
            Option<crate::types::TruncationReason>,
        ),
        DataProfilerError,
    > {
        let mut column_stats = StreamingColumnCollection::memory_limit(self.memory_limit_mb);
        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());
        let mut total_rows: usize = 0;
        let mut sampled_rows: usize = 0;
        let mut total_bytes: u64 = 0;
        let mut chunk_count: usize = 0;

        // Initialize stop condition evaluator
        let estimated_total = size_hint.map(|total| total / 50); // ~50 bytes per row
        let mut stop_eval = StopEvaluator::new(self.stop_condition.clone());
        if let Some(est) = estimated_total {
            stop_eval = stop_eval.with_estimated_total(est);
        }
        let mut schema_tracker = SchemaStabilityTracker::from_condition(&self.stop_condition);
        let mut truncation_reason: Option<crate::types::TruncationReason> = None;

        // First chunk is always headers
        let header_chunk = rx
            .recv()
            .await
            .ok_or_else(|| DataProfilerError::StreamingError {
                message: "Stream ended before any data was received".to_string(),
            })?;

        total_bytes += header_chunk.bytes_read;

        if header_chunk.records.is_empty() {
            return Err(DataProfilerError::StreamingError {
                message: "Stream header chunk was empty".to_string(),
            });
        }

        let headers: Vec<String> = header_chunk.records.into_iter().next().unwrap_or_default();

        if headers.is_empty() {
            return Err(DataProfilerError::StreamingError {
                message: "No column headers found in stream".to_string(),
            });
        }

        // Estimate total rows for progress (if we know the total size)
        let estimated_total_rows = size_hint.map(|total| {
            (total as usize) / 50 // rough estimate: ~50 bytes per row
        });

        // Process data chunks
        while let Some(chunk) = rx.recv().await {
            total_bytes += chunk.bytes_read;
            chunk_count += 1;
            let chunk_rows = chunk.records.len();
            let chunk_bytes = chunk.bytes_read;

            for (row_idx, values) in chunk.records.into_iter().enumerate() {
                let global_row_idx = total_rows + row_idx;

                // Apply sampling strategy
                if !self
                    .sampling_strategy
                    .should_include(global_row_idx, global_row_idx + 1)
                {
                    continue;
                }

                column_stats.process_record(&headers, values);
                sampled_rows += 1;
            }

            total_rows += chunk_rows;

            // Check memory pressure
            if column_stats.is_memory_pressure() {
                column_stats.reduce_memory_usage();
            }

            // Evaluate stop condition
            let memory_fraction = if self.memory_limit_mb > 0 {
                column_stats.memory_usage_bytes() as f64
                    / (self.memory_limit_mb * 1024 * 1024) as f64
            } else {
                0.0
            };

            if stop_eval.update(chunk_rows as u64, chunk_bytes, memory_fraction) {
                truncation_reason = stop_eval.truncation_reason();
                drop(rx); // signal reader task to stop
                break;
            }

            // Check schema stability
            if let Some(ref mut tracker) = schema_tracker {
                let fingerprint = column_stats.column_type_fingerprint();
                if tracker.update(fingerprint, chunk_rows as u64) {
                    truncation_reason = Some(tracker.truncation_reason());
                    drop(rx);
                    break;
                }
            }

            // Update progress
            progress_tracker.update(
                total_rows,
                estimated_total_rows.or(Some(total_rows + 1000)),
                chunk_count,
            );
        }

        progress_tracker.finish(total_rows);

        Ok((
            headers,
            column_stats,
            total_rows,
            sampled_rows,
            total_bytes,
            truncation_reason,
        ))
    }

    /// Calculate rows per chunk based on ChunkSize config.
    fn rows_per_chunk(&self) -> usize {
        match self.chunk_size {
            ChunkSize::Adaptive => 8192,
            ChunkSize::Fixed(bytes) => {
                // Estimate ~50 bytes per row on average
                (bytes / 50).max(100)
            }
            ChunkSize::Custom(f) => {
                // Use the custom function with a dummy file size (streams don't know size upfront)
                f(0).max(100)
            }
        }
    }
}

impl Default for AsyncStreamingProfiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engines::streaming::async_source::{AsyncSourceInfo, BytesSource};
    use crate::types::DataType;

    fn csv_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo {
                label: "test".into(),
                format: FileFormat::Csv,
                size_hint: Some(data.len() as u64),
                source_system: None,
            },
        )
    }

    #[tokio::test]
    async fn test_basic_csv_profiling() {
        let source = csv_source(b"name,age,salary\nAlice,30,50000\nBob,25,60000\nCarol,35,55000\n");
        let profiler = AsyncStreamingProfiler::new();
        let report = profiler.analyze_stream(source).await.unwrap();

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.execution.columns_detected, 3);

        let age_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("age column");
        assert_eq!(age_col.data_type, DataType::Integer);
        assert_eq!(age_col.total_count, 3);
        assert_eq!(age_col.null_count, 0);
    }

    #[tokio::test]
    async fn test_empty_input() {
        let source = csv_source(b"");
        let profiler = AsyncStreamingProfiler::new();
        let result = profiler.analyze_stream(source).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_headers_only() {
        let source = csv_source(b"name,age,salary\n");
        let profiler = AsyncStreamingProfiler::new();
        let report = profiler.analyze_stream(source).await.unwrap();

        assert_eq!(report.column_profiles.len(), 0);
        assert_eq!(report.execution.rows_processed, 0);
    }

    #[tokio::test]
    async fn test_quoted_newlines_rfc4180() {
        // RFC 4180: fields with embedded newlines must be quoted
        let source =
            csv_source(b"name,bio,age\nAlice,\"likes\ncats\",30\nBob,\"no\nnewlines\njk\",25\n");
        let profiler = AsyncStreamingProfiler::new();
        let report = profiler.analyze_stream(source).await.unwrap();

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.execution.rows_processed, 2);

        let bio_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "bio")
            .expect("bio column");
        assert_eq!(bio_col.total_count, 2);
    }

    #[tokio::test]
    async fn test_large_synthetic_stream() {
        let mut data = String::from("id,value,label\n");
        for i in 0..10_000 {
            data.push_str(&format!("{},{},item_{}\n", i, i * 10 + 5, i));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "large-test".into(),
                format: FileFormat::Csv,
                size_hint: None,
                source_system: None,
            },
        );

        let profiler = AsyncStreamingProfiler::new().memory_limit_mb(16);
        let report = profiler.analyze_stream(source).await.unwrap();

        assert_eq!(report.column_profiles.len(), 3);

        let id_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "id")
            .expect("id column");
        assert_eq!(id_col.data_type, DataType::Integer);
    }

    #[tokio::test]
    async fn test_channel_capacity_one() {
        let source =
            csv_source(b"a,b\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n");
        let profiler = AsyncStreamingProfiler::new().channel_capacity(1);
        let report = profiler.analyze_stream(source).await.unwrap();
        assert_eq!(report.column_profiles.len(), 2);
    }

    #[tokio::test]
    async fn test_progress_callback_fires() {
        let progress_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = progress_count.clone();

        let mut data = String::from("x,y\n");
        for i in 0..1000 {
            data.push_str(&format!("{},{}\n", i, i * 2));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "progress-test".into(),
                format: FileFormat::Csv,
                size_hint: None,
                source_system: None,
            },
        );

        let profiler = AsyncStreamingProfiler::new().progress_callback(move |_info| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });

        let _report = profiler.analyze_stream(source).await.unwrap();
        assert!(progress_count.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn test_unsupported_format_rejected() {
        let source = BytesSource::new(
            bytes::Bytes::from_static(b"{}"),
            AsyncSourceInfo {
                label: "json-test".into(),
                format: FileFormat::Json,
                size_hint: None,
                source_system: None,
            },
        );
        let profiler = AsyncStreamingProfiler::new();
        let result = profiler.analyze_stream(source).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_early_stop_max_rows() {
        let mut data = String::from("id,value\n");
        for i in 0..10_000 {
            data.push_str(&format!("{},val_{}\n", i, i));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "stop-test".into(),
                format: FileFormat::Csv,
                size_hint: None,
                source_system: None,
            },
        );

        let profiler = AsyncStreamingProfiler::new().stop_condition(StopCondition::MaxRows(100));
        let report = profiler.analyze_stream(source).await.unwrap();

        assert!(
            report.execution.rows_processed < 10_000,
            "Should stop before processing all rows, got {}",
            report.execution.rows_processed
        );
        assert!(!report.execution.source_exhausted);
        assert!(matches!(
            report.execution.truncation_reason,
            Some(crate::types::TruncationReason::MaxRows(100))
        ));
    }
}
