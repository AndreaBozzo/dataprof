use std::time::Duration;
use tokio::sync::mpsc;

use dataprof_core::{
    ChunkSize, DataProfilerError, DataSource, ExecutionMetadata, FileFormat, JsonErrorPolicy,
    MetricPack, ProgressSink, QualityDimension, SamplingStrategy, SchemaStabilityTracker,
    SemanticHints, StopCondition, StopEvaluator, StreamSourceSystem, TruncationReason,
};
use dataprof_runtime::{
    ProfileReport, ReportAssembler, StreamingColumnCollection, profile_builder,
};

use super::async_source::AsyncDataSource;
use crate::progress_tracker::ProgressTracker;

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
///                       (RFC 4180 compliant)                               ──► ProfileReport
/// ```
pub struct AsyncStreamingProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    memory_limit_mb: usize,
    channel_capacity: usize,
    progress_sink: ProgressSink,
    progress_interval: Duration,
    stop_condition: StopCondition,
    quality_dimensions: Option<Vec<QualityDimension>>,
    metric_packs: Option<Vec<MetricPack>>,
    semantic_hints: SemanticHints,
    json_error_policy: JsonErrorPolicy,
}

impl AsyncStreamingProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            memory_limit_mb: 256,
            channel_capacity: 4,
            progress_sink: ProgressSink::None,
            progress_interval: Duration::from_millis(500),
            stop_condition: StopCondition::Never,
            quality_dimensions: None,
            metric_packs: None,
            semantic_hints: SemanticHints::default(),
            json_error_policy: JsonErrorPolicy::default(),
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

    pub fn progress(mut self, sink: ProgressSink, interval: Duration) -> Self {
        self.progress_sink = sink;
        self.progress_interval = interval;
        self
    }

    pub fn stop_condition(mut self, condition: StopCondition) -> Self {
        self.stop_condition = condition;
        self
    }

    pub fn quality_dimensions(mut self, dims: Vec<QualityDimension>) -> Self {
        self.quality_dimensions = Some(dims);
        self
    }

    pub fn metric_packs(mut self, packs: Vec<MetricPack>) -> Self {
        self.metric_packs = Some(packs);
        self
    }

    pub fn semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.semantic_hints = hints;
        self
    }

    /// Set how malformed JSON/JSONL records are handled (default: skip and count).
    pub fn json_error_policy(mut self, policy: JsonErrorPolicy) -> Self {
        self.json_error_policy = policy;
        self
    }

    /// Profile data from an async source, returning a [`ProfileReport`].
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
        // The task reports how many malformed records were skipped (0 for CSV,
        // and always 0 in strict JSON mode since the first one aborts).
        let json_error_policy = self.json_error_policy;
        let reader_handle = tokio::task::spawn_blocking(move || match format {
            FileFormat::Csv => Self::reader_task(sync_reader, tx, rows_per_chunk).map(|()| 0usize),
            FileFormat::Json | FileFormat::Jsonl => {
                Self::json_reader_task(sync_reader, tx, rows_per_chunk, format, json_error_policy)
            }
            _ => unreachable!(),
        });

        // Process chunks on the current task
        let process_result = self.process_chunks(rx, source_info.size_hint).await;

        // If processing failed, prefer the reader's error when it also failed:
        // a strict-mode malformed record aborts the reader before any data
        // reaches the processor, which would otherwise surface only as a generic
        // "empty input" error and mask the real cause.
        let (_headers, column_stats, total_rows, sampled_rows, total_bytes, truncation_reason) =
            match process_result {
                Ok(result) => result,
                Err(process_err) => {
                    return match reader_handle.await {
                        Ok(Err(reader_err)) => Err(reader_err),
                        _ => Err(process_err),
                    };
                }
            };

        // Wait for the reader task to finish and propagate any errors
        let malformed_records = match reader_handle.await {
            Ok(Ok(count)) => count,
            Ok(Err(e)) => return Err(e),
            Err(join_err) if join_err.is_cancelled() => {
                // We cancelled it ourselves — fine
                0
            }
            Err(join_err) => {
                return Err(DataProfilerError::StreamingError {
                    message: format!("Reader task panicked: {}", join_err),
                });
            }
        };

        // Build the report
        let packs = self.metric_packs.as_deref();
        let skip_stats = !MetricPack::include_statistics(packs);
        let skip_patterns = !MetricPack::include_patterns(packs);

        let column_profiles = profile_builder::profiles_from_streaming_with_hints(
            &column_stats,
            skip_stats,
            skip_patterns,
            None,
            &self.semantic_hints,
        );
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
            .with_engine("incremental")
            .with_bytes_consumed(total_bytes)
            // Tolerant JSONL scans surface skipped malformed records so callers
            // can distinguish a partial profile from a clean one.
            .with_error_count(malformed_records);

        if let Some(reason) = truncation_reason {
            execution = execution.with_truncation(reason);
        } else if total_rows > 0 && sampled_rows < total_rows {
            let ratio = sampled_rows as f64 / total_rows as f64;
            execution = execution.with_sampling(ratio).with_source_exhausted(false);
        }

        let mut assembler = ReportAssembler::new(data_source, execution)
            .columns(column_profiles)
            .with_quality_data(sample_columns)
            .with_row_duplicates(column_stats.row_duplicate_summary())
            .with_exact_value_hint_bindings(column_stats.semantic_hint_bindings())
            .with_semantic_hints(self.semantic_hints.clone());
        if let Some(ref dims) = self.quality_dimensions {
            assembler = assembler.with_requested_dimensions(dims.clone());
        }
        Ok(assembler.build())
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
        error_policy: JsonErrorPolicy,
    ) -> Result<usize, DataProfilerError> {
        use serde_json::Value;
        use std::io::BufRead;

        let mut buf_reader = std::io::BufReader::new(sync_reader);
        let mut known_columns: Vec<String> = Vec::new();
        let mut current_chunk: Vec<Vec<String>> = Vec::with_capacity(rows_per_chunk);
        let mut known_columns_set: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut bytes_in_chunk: u64 = 0;
        let mut headers_sent = false;
        let mut malformed_records: usize = 0;
        let mut emitted_records: usize = 0;

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
                    // decode-audit: no-data — a key absent from this object is
                    // a missing field, and "" is the profiler's textual null.
                    obj.get(col)
                        .map(|v| match v {
                            Value::Null => String::new(),
                            Value::Bool(b) => b.to_string(),
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => s.to_string(),
                            // decode-audit: impossible — re-serializing an
                            // in-memory Value cannot fail; panic beats a fake null.
                            _ => serde_json::to_string(v)
                                .expect("re-serializing a parsed JSON value cannot fail"),
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
                // True streaming: deserialize one record at a time so a malformed
                // record can be recovered from (skip to next line) or reported,
                // matching the sync `scan_json_from_reader` contract.
                loop {
                    let parsed = {
                        let mut de = serde_json::Deserializer::from_reader(&mut buf_reader);
                        <Value as serde::Deserialize>::deserialize(&mut de)
                    };
                    let v = match parsed {
                        Ok(v) => v,
                        Err(e) if e.classify() == serde_json::error::Category::Eof => break,
                        Err(e) => {
                            if error_policy == JsonErrorPolicy::Strict {
                                return Err(DataProfilerError::JsonParsingError {
                                    message: format!("malformed JSON record: {e}"),
                                });
                            }
                            malformed_records += 1;
                            // Recover by discarding the rest of the offending line.
                            let mut discard = Vec::new();
                            buf_reader
                                .read_until(b'\n', &mut discard)
                                .map_err(DataProfilerError::from)?;
                            continue;
                        }
                    };

                    if let Value::Object(obj) = v {
                        let row = process_object(
                            &obj,
                            &mut known_columns,
                            &mut known_columns_set,
                            headers_sent,
                        );
                        bytes_in_chunk += row.iter().map(|s| s.len() as u64 + 4).sum::<u64>();
                        current_chunk.push(row);
                        emitted_records += 1;

                        if current_chunk.len() >= rows_per_chunk
                            && !send_chunk(
                                &mut current_chunk,
                                &mut bytes_in_chunk,
                                &known_columns,
                                &mut headers_sent,
                                &tx,
                            )?
                        {
                            return Ok(malformed_records);
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
                                    emitted_records += 1;

                                    if current_chunk.len() >= rows_per_chunk
                                        && !send_chunk(
                                            &mut current_chunk,
                                            &mut bytes_in_chunk,
                                            &known_columns,
                                            &mut headers_sent,
                                            &tx,
                                        )?
                                    {
                                        return Ok(malformed_records);
                                    }
                                }
                                Ok(_) => {
                                    // Skip non-object items, approximate 10 bytes progress
                                    bytes_in_chunk += 10;
                                }
                                Err(e) => {
                                    // A corrupt element leaves the array parser unable
                                    // to resync, so we stop here either way; strict mode
                                    // surfaces the failure instead of a partial profile.
                                    if error_policy == JsonErrorPolicy::Strict {
                                        return Err(DataProfilerError::JsonParsingError {
                                            message: format!("malformed JSON record: {e}"),
                                        });
                                    }
                                    malformed_records += 1;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Input made up entirely of malformed records must fail, matching the
        // file/bytes paths, rather than surfacing a generic "empty stream" error.
        if emitted_records == 0 && malformed_records > 0 {
            return Err(DataProfilerError::JsonParsingError {
                message: "No valid JSON records found (all records were malformed)".to_string(),
            });
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

        Ok(malformed_records)
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
            Option<TruncationReason>,
        ),
        DataProfilerError,
    > {
        let mut column_stats = StreamingColumnCollection::memory_limit(self.memory_limit_mb)
            .with_semantic_hints(&self.semantic_hints);
        let mut progress_tracker =
            ProgressTracker::new(self.progress_sink.clone(), self.progress_interval);
        let mut total_rows: usize = 0;
        let mut sampled_rows: usize = 0;
        let mut total_bytes: u64 = 0;

        // Initialize stop condition evaluator
        let estimated_total = size_hint.map(|total| total / 50); // ~50 bytes per row
        let mut stop_eval = StopEvaluator::new(self.stop_condition.clone());
        if let Some(est) = estimated_total {
            stop_eval = stop_eval.with_estimated_total(est);
        }
        let mut schema_tracker = SchemaStabilityTracker::from_condition(&self.stop_condition);
        let mut truncation_reason: Option<TruncationReason> = None;

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

        // decode-audit: impossible — `records` was checked non-empty above, and
        // an empty header row is rejected right below.
        let headers: Vec<String> = header_chunk
            .records
            .into_iter()
            .next()
            .expect("non-empty header chunk has a first record");

        if headers.is_empty() {
            return Err(DataProfilerError::StreamingError {
                message: "No column headers found in stream".to_string(),
            });
        }

        // Estimate total rows for progress (if we know the total size)
        let estimated_total_rows = size_hint.map(|total| {
            (total as usize) / 50 // rough estimate: ~50 bytes per row
        });

        progress_tracker.emit_started(estimated_total_rows, size_hint);
        progress_tracker.emit_schema(headers.clone());

        // Process data chunks
        while let Some(chunk) = rx.recv().await {
            total_bytes += chunk.bytes_read;
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
            progress_tracker.emit_chunk(chunk_rows, chunk_bytes, estimated_total_rows);
        }

        progress_tracker.emit_finished(truncation_reason.is_some());

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
    use dataprof_core::DataType;
    use dataprof_runtime::{AsyncSourceInfo, BytesSource};

    fn csv_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("test", FileFormat::Csv).size_hint(Some(data.len() as u64)),
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
            AsyncSourceInfo::new("large-test", FileFormat::Csv),
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
    async fn test_progress_events_fire() {
        use dataprof_core::{ProgressEvent, ProgressSink};
        use std::sync::Arc;

        let progress_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = progress_count.clone();

        let mut data = String::from("x,y\n");
        for i in 0..1000 {
            data.push_str(&format!("{},{}\n", i, i * 2));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo::new("progress-test", FileFormat::Csv),
        );

        let sink = ProgressSink::Callback(Arc::new(move |_event: ProgressEvent| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));

        let profiler =
            AsyncStreamingProfiler::new().progress(sink, std::time::Duration::from_millis(0));

        let _report = profiler.analyze_stream(source).await.unwrap();
        // Should have at least Started + Finished
        assert!(progress_count.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn test_unsupported_format_rejected() {
        let source = BytesSource::new(
            bytes::Bytes::from_static(b"{}"),
            AsyncSourceInfo::new("json-test", FileFormat::Json),
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
            AsyncSourceInfo::new("stop-test", FileFormat::Csv),
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
            Some(TruncationReason::MaxRows(100))
        ));
    }

    fn jsonl_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("test", FileFormat::Jsonl).size_hint(Some(data.len() as u64)),
        )
    }

    #[tokio::test]
    async fn test_async_jsonl_tolerant_skips_and_counts() {
        // Malformed record in the middle: default policy skips and counts it.
        let source = jsonl_source(b"{\"id\":1}\nnot-json\n{\"id\":2}\n");
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();
        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.error_count, 1);
    }

    #[tokio::test]
    async fn test_async_jsonl_strict_aborts() {
        for data in [
            b"not-json\n{\"id\":1}\n{\"id\":2}\n".as_ref(),
            b"{\"id\":1}\nnot-json\n{\"id\":2}\n".as_ref(),
            b"{\"id\":1}\n{\"id\":2}\nnot-json\n".as_ref(),
        ] {
            let source = BytesSource::new(
                bytes::Bytes::from_static(data),
                AsyncSourceInfo::new("test", FileFormat::Jsonl),
            );
            let result = AsyncStreamingProfiler::new()
                .json_error_policy(JsonErrorPolicy::Strict)
                .analyze_stream(source)
                .await;
            let err = result.expect_err("strict mode must reject malformed records");
            let message = err.to_string();
            assert!(message.to_lowercase().contains("malformed json record"));
            assert!(!message.contains("not-json"), "leaked record: {message}");
        }
    }

    #[tokio::test]
    async fn test_async_jsonl_clean_has_zero_error_count() {
        let source = jsonl_source(b"{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n");
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();
        assert_eq!(report.execution.rows_processed, 3);
        assert_eq!(report.execution.error_count, 0);
    }
}
