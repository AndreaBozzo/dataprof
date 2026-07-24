use std::time::Duration;
use tokio::sync::mpsc;

use dataprof_core::{
    ChunkSize, DataProfilerError, DataSource, ExecutionMetadata, FileFormat, JsonErrorPolicy,
    MetricPack, ProgressSink, QualityDimension, RowSampler, RowView, SamplingStrategy,
    SchemaStabilityTracker, SemanticHints, StopCondition, StopEvaluator, StreamSourceSystem,
    TruncationReason,
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

/// Structural violations the blocking reader task observed while parsing.
///
/// Both counters describe what the *reader* saw, so an early-stopped scan
/// reports the violations within the data it actually read.
#[derive(Default)]
struct ReaderOutcome {
    /// Malformed JSON/JSONL records skipped under the tolerant policy
    /// (always 0 for CSV, and in strict mode where the first one aborts).
    malformed_records: usize,
    /// CSV records whose field count differed from the header (always 0 for
    /// JSON/JSONL, whose rows are aligned to the column set by construction).
    ragged_rows: usize,
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
    csv_flexible: bool,
    csv_delimiter: Option<u8>,
}

/// Bytes sniffed from the head of a stream to detect its CSV delimiter.
///
/// Matches the file path's sample size so the same data yields the same
/// delimiter whether it is read from disk or off a socket.
const DELIMITER_SAMPLE_BYTES: u64 = 4096;

/// Bytes per chunk when [`ChunkSize::Adaptive`] is in effect. A stream has no
/// size to adapt to, so this is a fixed working-set target.
const DEFAULT_CHUNK_BYTES: usize = 512 * 1024;

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
            csv_flexible: true,
            csv_delimiter: None,
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

    /// Set how CSV records whose field count differs from the header are handled.
    ///
    /// `true` (default) recovers them — extra fields dropped, missing fields
    /// padded to null — and counts every one in
    /// [`ExecutionMetadata::ragged_row_count`], matching the incremental file
    /// path. `false` rejects the first such record with a CSV parsing error.
    pub fn csv_flexible(mut self, flexible: bool) -> Self {
        self.csv_flexible = flexible;
        self
    }

    /// Set the CSV field delimiter.
    ///
    /// When unset, it is detected from the head of the stream the same way the
    /// file path detects it, so a semicolon- or tab-separated source profiles
    /// identically over either transport.
    pub fn csv_delimiter(mut self, delimiter: u8) -> Self {
        self.csv_delimiter = Some(delimiter);
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

        let bytes_per_chunk = self.bytes_per_chunk();
        let (tx, rx) = mpsc::channel::<ParsedChunk>(self.channel_capacity);

        // Bridge the AsyncRead into a sync Read for parsing.
        let sync_reader = tokio_util::io::SyncIoBridge::new(reader);

        // Spawn the reader on a blocking thread — format determines the parser.
        // The task reports the structural violations it recovered from, so a
        // report can never describe a repaired scan as a clean one.
        let json_error_policy = self.json_error_policy;
        let csv_flexible = self.csv_flexible;
        let csv_delimiter = self.csv_delimiter;
        let reader_handle = tokio::task::spawn_blocking(move || match format {
            FileFormat::Csv => Self::reader_task(
                sync_reader,
                tx,
                bytes_per_chunk,
                csv_flexible,
                csv_delimiter,
            ),
            FileFormat::Json | FileFormat::Jsonl => {
                Self::json_reader_task(sync_reader, tx, bytes_per_chunk, format, json_error_policy)
                    .map(|malformed_records| ReaderOutcome {
                        malformed_records,
                        ragged_rows: 0,
                    })
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
        let reader_outcome = match reader_handle.await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(e)) => return Err(e),
            Err(join_err) if join_err.is_cancelled() => {
                // We cancelled it ourselves — fine
                ReaderOutcome::default()
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

        // A scan that ran to the end of a source of known length consumed
        // exactly that many bytes. The per-chunk tally is the best available
        // answer for a bounded scan, but it is derived from parsed records and
        // would otherwise leave a complete scan reporting fewer bytes than the
        // source holds.
        let bytes_consumed = match (truncation_reason.is_none(), source_info.size_hint) {
            (true, Some(total)) => total,
            _ => total_bytes,
        };

        let mut execution = ExecutionMetadata::new(sampled_rows, num_columns, scan_time_ms)
            .with_engine("incremental")
            .with_bytes_consumed(bytes_consumed)
            // Tolerant JSONL scans surface skipped malformed records so callers
            // can distinguish a partial profile from a clean one.
            .with_error_count(reader_outcome.malformed_records)
            // Ragged CSV records are recovered, never silently: the count is the
            // same signal the incremental file path reports.
            .with_ragged_row_count(reader_outcome.ragged_rows);

        if let Some(reason) = truncation_reason {
            execution = execution.with_truncation(reason);
        } else if total_rows > 0 && sampled_rows < total_rows {
            // Sampling says which of the rows that were read reached the
            // profile. It says nothing about whether the source ran out —
            // marking a fully consumed source as unexhausted made a complete
            // scan look like an interrupted one.
            execution = execution.with_sampling(sampled_rows as f64 / total_rows as f64);
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

    /// Map a `csv` crate error, pointing a strict-mode field-count failure at
    /// the option that would have recovered it instead.
    fn csv_error(err: &csv::Error, flexible: bool) -> DataProfilerError {
        let suggestion = if !flexible && matches!(err.kind(), csv::ErrorKind::UnequalLengths { .. })
        {
            "Set csv_flexible=true to recover ragged rows; every recovered row is \
             reported in execution.ragged_row_count."
        } else {
            "Check CSV formatting in the stream data"
        };
        DataProfilerError::CsvParsingError {
            message: err.to_string(),
            suggestion: suggestion.to_string(),
        }
    }

    /// Blocking reader task: uses the `csv` crate's RFC 4180-compliant parser
    /// over a `SyncIoBridge` to correctly handle quoted fields with embedded newlines.
    ///
    /// Records whose field count differs from the header are counted and
    /// returned so the report can carry the structural signal; the alignment
    /// itself (padding short rows, dropping extra fields) happens downstream in
    /// `StreamingColumnCollection::process_record`, which keys on the header.
    fn reader_task(
        sync_reader: tokio_util::io::SyncIoBridge<
            std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>,
        >,
        tx: mpsc::Sender<ParsedChunk>,
        bytes_per_chunk: usize,
        flexible: bool,
        delimiter: Option<u8>,
    ) -> Result<ReaderOutcome, DataProfilerError> {
        use std::io::Read;

        let mut builder = csv::ReaderBuilder::new();
        builder.has_headers(true).flexible(flexible);

        // A stream cannot be rewound, so detection reads the head into memory
        // and chains it back in front of the rest — the parser still sees the
        // whole source, byte for byte.
        let source: Box<dyn Read> = match delimiter {
            Some(delimiter) => {
                builder.delimiter(delimiter);
                Box::new(sync_reader)
            }
            None => {
                let mut preamble = Vec::new();
                let mut head = sync_reader.take(DELIMITER_SAMPLE_BYTES);
                head.read_to_end(&mut preamble)
                    .map_err(DataProfilerError::from)?;
                let detected =
                    dataprof_csv::detect_delimiter(std::io::Cursor::new(&preamble)).unwrap_or(b',');
                builder.delimiter(detected);
                Box::new(std::io::Cursor::new(preamble).chain(head.into_inner()))
            }
        };

        let mut csv_reader = builder.from_reader(source);

        // Send headers as the first chunk
        let headers = csv_reader
            .headers()
            .map_err(|e| Self::csv_error(&e, flexible))?;

        let header_fields: Vec<String> = headers.iter().map(|f| f.to_string()).collect();
        let header_len = header_fields.len();
        // Byte counts come from the parser's own position rather than from the
        // parsed fields: a record's fields exclude delimiters, quotes and line
        // endings, so summing them under-reports every row and would leave
        // `bytes_consumed` short of the source even on a complete scan.
        let mut byte_offset = csv_reader.position().byte();
        let header_chunk = ParsedChunk {
            records: vec![header_fields],
            bytes_read: byte_offset,
        };
        let mut outcome = ReaderOutcome::default();
        if tx.blocking_send(header_chunk).is_err() {
            return Ok(outcome);
        }

        // Read data records, emitting a chunk once it holds the configured
        // number of bytes. Chunking by bytes rather than by a row count keeps
        // the working set bounded regardless of how wide the rows are.
        let mut current_chunk: Vec<Vec<String>> = Vec::new();
        let mut bytes_in_chunk: u64 = 0;
        let mut record = csv::StringRecord::new();

        while csv_reader
            .read_record(&mut record)
            .map_err(|e| Self::csv_error(&e, flexible))?
        {
            // A field count differing from the header is a structural violation.
            // Counted here, before the row is queued, so the signal covers every
            // record read rather than only the ones that survive sampling.
            if record.len() != header_len {
                outcome.ragged_rows += 1;
            }

            let position = csv_reader.position().byte();
            bytes_in_chunk += position.saturating_sub(byte_offset);
            byte_offset = position;

            let fields: Vec<String> = record.iter().map(|f| f.to_string()).collect();
            current_chunk.push(fields);

            if bytes_in_chunk as usize >= bytes_per_chunk {
                let chunk = ParsedChunk {
                    records: std::mem::take(&mut current_chunk),
                    bytes_read: bytes_in_chunk,
                };
                bytes_in_chunk = 0;

                if tx.blocking_send(chunk).is_err() {
                    return Ok(outcome);
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

        Ok(outcome)
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
        bytes_per_chunk: usize,
        format: FileFormat,
        error_policy: JsonErrorPolicy,
    ) -> Result<usize, DataProfilerError> {
        use serde_json::Value;
        use std::io::BufRead;

        let mut buf_reader = std::io::BufReader::new(sync_reader);
        let mut known_columns: Vec<String> = Vec::new();
        let mut current_chunk: Vec<Vec<String>> = Vec::new();
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
                    records: std::mem::take(chunk),
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
                    // Distinguish clean exhaustion from an incomplete trailing
                    // value. serde reports both as `Category::Eof`, but only an
                    // empty/whitespace-only remainder is a clean end of stream.
                    let has_value = loop {
                        let whitespace_only_len = {
                            let buf = buf_reader.fill_buf().map_err(DataProfilerError::from)?;
                            if buf.is_empty() {
                                break false;
                            }
                            if buf.iter().all(u8::is_ascii_whitespace) {
                                Some(buf.len())
                            } else {
                                None
                            }
                        };
                        let Some(whitespace_only_len) = whitespace_only_len else {
                            // Leave a mixed whitespace/value buffer untouched so
                            // serde retains its line and column context.
                            break true;
                        };
                        buf_reader.consume(whitespace_only_len);
                    };
                    if !has_value {
                        break;
                    }

                    let parsed = {
                        let mut de = serde_json::Deserializer::from_reader(&mut buf_reader);
                        <Value as serde::Deserialize>::deserialize(&mut de)
                    };
                    let v = match parsed {
                        Ok(v) => v,
                        Err(e) => {
                            if error_policy == JsonErrorPolicy::Strict {
                                return Err(DataProfilerError::JsonParsingError {
                                    message: format!("malformed JSON record: {e}"),
                                });
                            }
                            malformed_records += 1;
                            if e.classify() == serde_json::error::Category::Eof {
                                break;
                            }
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

                        if bytes_in_chunk as usize >= bytes_per_chunk
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

                                    if bytes_in_chunk as usize >= bytes_per_chunk
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
        // Built before any row is read: an unusable strategy must fail before
        // the source is consumed, not after a partial profile exists.
        let mut sampler = RowSampler::new(&self.sampling_strategy)?;
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

        // A row cap is a hard ceiling on what the caller authorized us to
        // analyze, so it is checked per row. Evaluating it only per chunk
        // returned every row of the chunk that crossed it — up to a whole chunk
        // more data than requested, while the report named the smaller limit.
        let row_limit = self.stop_condition.max_rows();

        // Process data chunks
        while let Some(chunk) = rx.recv().await {
            total_bytes += chunk.bytes_read;
            let chunk_rows = chunk.records.len();
            let chunk_bytes = chunk.bytes_read;
            let mut rows_consumed = chunk_rows;
            let mut hit_row_limit = false;

            for (row_idx, values) in chunk.records.into_iter().enumerate() {
                if !sampler.accept(RowView::new(&headers, &values)) {
                    continue;
                }

                if sampler.is_buffered() {
                    // Held rather than folded in: a fixed-size sample is not
                    // final until the stream ends, and statistics cannot be
                    // retracted for a row that is later evicted.
                    sampler.offer(values);
                } else {
                    column_stats.process_record(&headers, values);
                    sampled_rows += 1;
                }

                // A fixed-size strategy folds nothing in until the stream
                // ends, so the cap bounds the rows read and the sample is drawn
                // from those. Counting only folded rows would let the sample
                // exceed the caller's hard ceiling.
                let rows_against_cap = if sampler.is_buffered() {
                    sampler.iterated_rows()
                } else {
                    sampler.sampled_rows()
                };
                if let Some(limit) = row_limit
                    && rows_against_cap as u64 >= limit
                {
                    rows_consumed = row_idx + 1;
                    hit_row_limit = true;
                    break;
                }
            }

            total_rows += rows_consumed;

            if hit_row_limit {
                // Reaching the cap is only a truncation if a row actually
                // remained. Rows left in this chunk prove it outright;
                // otherwise ask the reader for one more chunk, which resolves
                // to `None` when the source is exhausted.
                let more_rows_remain = rows_consumed < chunk_rows
                    || rx.recv().await.is_some_and(|next| !next.records.is_empty());

                if more_rows_remain {
                    stop_eval.update(rows_consumed as u64, chunk_bytes, 0.0);
                    truncation_reason = stop_eval.truncation_reason();
                }
                drop(rx);
                break;
            }

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

        // A fixed-size sample is only final once reading stops, so it is folded
        // in here rather than row by row.
        for values in sampler.take_sample() {
            column_stats.process_record(&headers, values);
            sampled_rows += 1;
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

    /// Bytes to accumulate per chunk, per the configured [`ChunkSize`].
    ///
    /// A stream does not know its own length, so `Custom` is called with `0`.
    fn bytes_per_chunk(&self) -> usize {
        match self.chunk_size {
            ChunkSize::Adaptive => DEFAULT_CHUNK_BYTES,
            ChunkSize::Fixed(bytes) => bytes.max(1),
            ChunkSize::Custom(f) => f(0).max(1),
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

    #[tokio::test]
    async fn test_async_csv_ragged_rows_are_counted() {
        // Row 2 is short, row 3 is over-long: both differ from the 3-column
        // header, both are recovered, and neither may vanish from the report.
        let source =
            csv_source(b"name,age,city\nAlice,25,NYC\nBob,30\nCarol,35,LA,EXTRA\nDave,40,SF\n");
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 4);
        assert_eq!(
            report.execution.ragged_row_count, 2,
            "one short and one over-long row must both count as ragged"
        );
    }

    #[tokio::test]
    async fn test_async_csv_clean_reports_zero_ragged_rows() {
        let source = csv_source(b"name,age,city\nAlice,25,NYC\nBob,30,LA\n");
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.ragged_row_count, 0);
    }

    #[tokio::test]
    async fn test_async_csv_strict_rejects_ragged_rows() {
        let source = csv_source(b"a,b,c\n1,2,3\n4,5\n6,7,8,9\n");
        let err = AsyncStreamingProfiler::new()
            .csv_flexible(false)
            .analyze_stream(source)
            .await
            .expect_err("strict mode must reject a ragged record");

        let message = err.to_string();
        assert!(
            message.contains("csv_flexible"),
            "strict failure must name the recovering option: {message}"
        );
    }

    #[tokio::test]
    async fn test_async_csv_ragged_counted_across_chunk_boundaries() {
        // Ragged rows are counted by the reader task, so the count must not
        // depend on how the stream happens to be split into chunks.
        let mut data = String::from("a,b,c\n");
        for i in 0..500 {
            if i % 100 == 0 {
                data.push_str(&format!("{i},short\n"));
            } else {
                data.push_str(&format!("{i},{i},{i}\n"));
            }
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo::new("chunked-ragged", FileFormat::Csv),
        );
        let report = AsyncStreamingProfiler::new()
            .channel_capacity(1)
            .chunk_size(ChunkSize::Fixed(5_000))
            .analyze_stream(source)
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 500);
        assert_eq!(report.execution.ragged_row_count, 5);
    }

    #[tokio::test]
    async fn test_async_csv_detects_non_comma_delimiter() {
        // Unset delimiter must be sniffed, not assumed: a semicolon file read
        // as comma-separated collapses into one column and profiles as clean.
        let source = csv_source(b"name;age;city\nAlice;25;NYC\nBob;30;LA\n");
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.ragged_row_count, 0);
    }

    #[tokio::test]
    async fn test_async_csv_explicit_delimiter_is_honored() {
        let source = csv_source(b"a|b\n1|2\n3|4\n");
        let report = AsyncStreamingProfiler::new()
            .csv_delimiter(b'|')
            .analyze_stream(source)
            .await
            .unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.execution.rows_processed, 2);
    }

    #[tokio::test]
    async fn test_async_csv_sniffing_keeps_every_row() {
        // The sniffed head is chained back in front of the stream; a source
        // longer than the sample must not lose the rows it was sniffed from.
        let mut data = String::from("id\tvalue\n");
        for i in 0..2_000 {
            data.push_str(&format!("{i}\tvalue_{i}\n"));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo::new("sniffed", FileFormat::Csv),
        );
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.execution.rows_processed, 2_000);
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

    #[tokio::test]
    async fn test_async_jsonl_truncated_final_record_obeys_error_policy() {
        let data = b"{\"id\":1}\n{\"id\":2";

        let report = AsyncStreamingProfiler::new()
            .analyze_stream(jsonl_source(data))
            .await
            .unwrap();
        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.execution.error_count, 1);

        let err = AsyncStreamingProfiler::new()
            .json_error_policy(JsonErrorPolicy::Strict)
            .analyze_stream(jsonl_source(data))
            .await
            .expect_err("strict mode must reject an incomplete trailing record");
        let message = err.to_string().to_lowercase();
        assert!(message.contains("malformed json record"));
        assert!(message.contains("line 2"), "got: {message}");
    }
}
