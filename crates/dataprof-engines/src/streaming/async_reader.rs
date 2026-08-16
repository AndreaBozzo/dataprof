use std::time::Duration;
use tokio::sync::mpsc;

use dataprof_core::{
    AnalysisOptions, ChunkSize, DataProfilerError, DataSource, ExecutionMetadata, FileFormat,
    JsonErrorPolicy, Locale, MetricPack, ProgressSink, QualityDimension, RowSampler, RowView,
    SamplingStrategy, SchemaStabilityTracker, SemanticHints, StopCondition, StopEvaluator,
    StreamSourceSystem, TruncationReason,
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

fn peek_non_whitespace<R: std::io::BufRead>(
    reader: &mut R,
) -> Result<(Option<u8>, usize), DataProfilerError> {
    let mut consumed = 0;
    loop {
        let whitespace_len = {
            let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
            if buf.is_empty() {
                return Ok((None, consumed));
            }
            buf.iter()
                .take_while(|byte| byte.is_ascii_whitespace())
                .count()
        };
        if whitespace_len == 0 {
            let next = reader
                .fill_buf()
                .map_err(DataProfilerError::from)?
                .first()
                .copied();
            return Ok((next, consumed));
        }
        reader.consume(whitespace_len);
        consumed += whitespace_len;
    }
}

/// Build a strict-mode error for a standard JSON document that is not exactly
/// one value. Mirrors `dataprof_json::json_document_error`.
fn json_document_error(err: &serde_json::Error) -> DataProfilerError {
    DataProfilerError::JsonParsingError {
        message: format!(
            // One source line on purpose: see dataprof_json::json_document_error.
            "malformed JSON document: {err}. A JSON source must hold exactly one array or object; for one record per line use format=\"jsonl\""
        ),
    }
}

fn malformed_json_array_error(message: &str) -> DataProfilerError {
    DataProfilerError::JsonParsingError {
        message: format!("malformed JSON array: {message}"),
    }
}

/// One record read from a JSON or JSONL stream.
///
/// This mirrors `dataprof_json::JsonRecord`; the two scanners are kept
/// byte-for-byte equivalent so a payload profiles identically whether it
/// arrives as a file or as an async stream.
enum JsonRecord {
    /// A JSON object — the only value with named fields to profile as a row.
    Object(serde_json::Map<String, serde_json::Value>),
    /// Valid JSON that is not an object, carrying the value's kind for the
    /// error message.
    NonObject(&'static str),
    /// The bytes are not valid JSON.
    Malformed(serde_json::Error),
}

/// Parse one JSONL line as exactly one JSON value.
///
/// `serde_json::from_str` requires the value to fill the whole input, so a line
/// carrying two adjacent or space-separated values is a malformed record rather
/// than two clean ones. Mirrors `dataprof_json::read_jsonl_line`.
fn read_jsonl_line(line: &str) -> JsonRecord {
    match serde_json::from_str::<serde_json::Value>(line.trim()) {
        Ok(serde_json::Value::Object(obj)) => JsonRecord::Object(obj),
        Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
        Err(err) => JsonRecord::Malformed(err),
    }
}

/// Read one JSON value and leave the reader positioned immediately after it.
///
/// `first` is the value's first byte, already located by
/// [`peek_non_whitespace`] but not yet consumed.
fn read_json_record<R: std::io::BufRead>(
    reader: &mut R,
    first: u8,
) -> Result<JsonRecord, DataProfilerError> {
    // A number is the only JSON value that is not self-delimiting: serde finds
    // its end by peeking one byte past it and then drops that byte along with
    // the deserializer. Inside an array that byte is the `,` or `]` the scanner
    // needs next, so numbers are read byte-wise instead.
    if first == b'-' || first.is_ascii_digit() {
        let token = read_number_token(reader)?;
        return Ok(match serde_json::from_str::<serde_json::Value>(&token) {
            Ok(_) => JsonRecord::NonObject("number"),
            Err(err) => JsonRecord::Malformed(err),
        });
    }

    let mut de = serde_json::Deserializer::from_reader(reader);
    Ok(
        match <serde_json::Value as serde::Deserialize>::deserialize(&mut de) {
            Ok(serde_json::Value::Object(obj)) => JsonRecord::Object(obj),
            Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
            Err(err) => JsonRecord::Malformed(err),
        },
    )
}

/// Read the bytes that can spell a JSON number, stopping at the first byte that
/// cannot. The token is returned unvalidated — the caller decides whether it
/// actually parses, so `1.2.3` stays a malformed record rather than a number.
///
/// Leading whitespace is consumed first: the JSONL loop locates the next
/// value's first byte without consuming what precedes it, because serde needs
/// that whitespace to keep its line and column context.
fn read_number_token<R: std::io::BufRead>(reader: &mut R) -> Result<String, DataProfilerError> {
    peek_non_whitespace(reader)?;
    let mut token = String::new();
    loop {
        let (consume, token_ended) = {
            let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
            if buf.is_empty() {
                break;
            }
            let consume = buf
                .iter()
                .take_while(|byte| is_json_number_byte(**byte))
                .count();
            // decode-audit: impossible — every byte accepted above is ASCII.
            token.push_str(
                std::str::from_utf8(&buf[..consume]).expect("JSON number bytes are ASCII"),
            );
            (consume, consume < buf.len())
        };
        reader.consume(consume);
        if token_ended {
            break;
        }
    }
    Ok(token)
}

fn is_json_number_byte(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E')
}

fn json_value_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Build a strict-mode error for valid JSON that is not a row object. This is a
/// distinct category from a syntax failure: the document parsed, it just does
/// not hold records. `position` is 1-based over the records scanned so far.
fn non_object_record_error(kind: &str, position: usize) -> DataProfilerError {
    DataProfilerError::JsonParsingError {
        message: format!(
            "non-object JSON record at position {position}: \
             expected an object with fields to profile, found {kind}"
        ),
    }
}

fn drain_to_end<R: std::io::BufRead>(reader: &mut R) -> Result<usize, DataProfilerError> {
    let mut consumed = 0;
    loop {
        let available = reader.fill_buf().map_err(DataProfilerError::from)?.len();
        if available == 0 {
            return Ok(consumed);
        }
        reader.consume(available);
        consumed += available;
    }
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
    options: AnalysisOptions,
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
            options: AnalysisOptions::default(),
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

    /// Set the whole analysis selection at once.
    ///
    /// Preferred over the individual setters: the async pipeline must apply the
    /// same selection the synchronous engines do, and passing it as one value is
    /// what keeps a new option from reaching only some of them.
    pub fn analysis_options(mut self, options: AnalysisOptions) -> Self {
        self.options = options;
        self
    }

    pub fn quality_dimensions(mut self, dims: Vec<QualityDimension>) -> Self {
        self.options = std::mem::take(&mut self.options).with_quality_dimensions(Some(dims));
        self
    }

    pub fn metric_packs(mut self, packs: Vec<MetricPack>) -> Self {
        self.options = std::mem::take(&mut self.options).with_metric_packs(Some(packs));
        self
    }

    /// Set the locale used to rank detected patterns.
    pub fn locale(mut self, locale: Locale) -> Self {
        self.options = std::mem::take(&mut self.options).with_locale(Some(locale));
        self
    }

    pub fn semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.options = std::mem::take(&mut self.options).with_semantic_hints(hints);
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
        // JSON records with no fields are rows against an empty schema; a CSV
        // stream with no header line has nothing to profile.
        let allow_empty_schema = matches!(format, FileFormat::Json | FileFormat::Jsonl);
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
        let process_result = self
            .process_chunks(rx, source_info.size_hint, allow_empty_schema)
            .await;

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
        let column_profiles = profile_builder::profiles_from_streaming_with_hints(
            &column_stats,
            !self.options.include_statistics(),
            !self.options.include_patterns(),
            self.options.locale(),
            self.options.semantic_hints(),
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

        Ok(ReportAssembler::new(data_source, execution)
            .columns(column_profiles)
            .with_quality_data(sample_columns)
            .with_row_duplicates(column_stats.row_duplicate_summary())
            .with_exact_value_hint_bindings(column_stats.semantic_hint_bindings())
            .with_analysis_options(&self.options)
            .build())
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
        dataprof_core::validate_unique_column_names(&header_fields, "CSV header")?;
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

        let buf_reader = std::io::BufReader::new(sync_reader);
        let mut buf_reader =
            dataprof_core::Utf8BomReader::new(buf_reader).map_err(DataProfilerError::from)?;
        let mut known_columns: Vec<String> = Vec::new();
        let mut current_chunk: Vec<Vec<String>> = Vec::new();
        let mut known_columns_set: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        // The BOM is part of the source and therefore of progress/byte
        // accounting, even though it is not passed to the JSON decoder.
        let mut bytes_in_chunk = buf_reader.stripped_len() as u64;
        let mut headers_sent = false;
        let mut malformed_records: usize = 0;
        let mut emitted_records: usize = 0;

        // Helper closure: convert a JSON object into a row aligned to known_columns.
        // New columns are only registered before headers are sent; once headers have
        // been emitted, the schema is frozen to keep rows aligned with the header set.
        // `serde_json/preserve_order` makes `obj.keys()` yield source field order,
        // so the header set matches the sync scanner's ordering contract.
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
        //
        // Records with no fields are still rows — the file scanner counts them
        // against zero columns — so a header chunk carrying no column names is
        // legal. It is only sent once the source is exhausted (`final_flush`):
        // sending headers freezes the schema, and while input remains a later
        // record may still introduce the columns these rows are missing. Until
        // then the fieldless rows stay buffered rather than being emitted ahead
        // of the headers, where the receiver would read them *as* the headers.
        let send_chunk = |chunk: &mut Vec<Vec<String>>,
                          bytes: &mut u64,
                          cols: &[String],
                          headers_sent: &mut bool,
                          tx: &mpsc::Sender<ParsedChunk>,
                          final_flush: bool|
         -> Result<bool, DataProfilerError> {
            if !*headers_sent && (!cols.is_empty() || final_flush) {
                let header_chunk = ParsedChunk {
                    records: vec![cols.to_vec()],
                    bytes_read: 0,
                };
                if tx.blocking_send(header_chunk).is_err() {
                    return Ok(false); // receiver dropped
                }
                *headers_sent = true;
            }

            if *headers_sent && !chunk.is_empty() {
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
                // True streaming, one physical line at a time: JSONL is
                // line-delimited, so a line is exactly one record and a record
                // never spans lines. This is the same grammar
                // `dataprof_json::scan_json_from_reader` applies, which is what
                // keeps a payload profiling identically over either transport.
                let mut line = String::new();
                // Counts every physical line, blank ones included, so a
                // diagnostic points at the line the user would open.
                let mut line_number = 0usize;
                loop {
                    // Blank lines are separators, not records.
                    let found = loop {
                        line.clear();
                        let read = buf_reader
                            .read_line(&mut line)
                            .map_err(DataProfilerError::from)?;
                        if read == 0 {
                            break false;
                        }
                        line_number += 1;
                        if !line.trim().is_empty() {
                            break true;
                        }
                    };
                    if !found {
                        break;
                    }
                    bytes_in_chunk += line.len() as u64;

                    match read_jsonl_line(&line) {
                        JsonRecord::Object(obj) => {
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
                                    false,
                                )?
                            {
                                return Ok(malformed_records);
                            }
                        }
                        JsonRecord::NonObject(kind) => {
                            if error_policy == JsonErrorPolicy::Strict {
                                return Err(non_object_record_error(
                                    kind,
                                    emitted_records + malformed_records + 1,
                                ));
                            }
                            malformed_records += 1;
                            // Approximate progress for a record that yields no row.
                            bytes_in_chunk += 10;
                        }
                        JsonRecord::Malformed(e) => {
                            if error_policy == JsonErrorPolicy::Strict {
                                // Each line is parsed on its own, so the
                                // decoder's own line number is always 1 and
                                // would mislead; its column is within the line
                                // and so is also the column in the source.
                                return Err(DataProfilerError::JsonParsingError {
                                    message: format!(
                                        "malformed JSON record on line {line_number}, column {}: a JSONL record must be one complete JSON value on one line",
                                        e.column()
                                    ),
                                });
                            }
                            // The offending line has already been consumed in
                            // full, so the next read starts at the next record.
                            malformed_records += 1;
                        }
                    }
                }
            }
            _ => 'document: {
                // A standard JSON document takes either of the two shapes that
                // carry records. An array streams element by element; a single
                // object is one record and is read whole, which is why it may be
                // pretty-printed across lines.
                let (opening, whitespace) = peek_non_whitespace(&mut buf_reader)?;
                bytes_in_chunk += whitespace as u64;
                if opening != Some(b'[') {
                    use std::io::Read as _;
                    let mut text = String::new();
                    buf_reader
                        .read_to_string(&mut text)
                        .map_err(DataProfilerError::from)?;
                    bytes_in_chunk += text.len() as u64;

                    if !text.trim().is_empty() {
                        match serde_json::from_str::<serde_json::Value>(text.trim()) {
                            Ok(serde_json::Value::Object(obj)) => {
                                let row = process_object(
                                    &obj,
                                    &mut known_columns,
                                    &mut known_columns_set,
                                    headers_sent,
                                );
                                current_chunk.push(row);
                                emitted_records += 1;
                            }
                            Ok(value) => {
                                if error_policy == JsonErrorPolicy::Strict {
                                    return Err(non_object_record_error(
                                        json_value_kind(&value),
                                        1,
                                    ));
                                }
                                malformed_records += 1;
                            }
                            Err(e) => {
                                if error_policy == JsonErrorPolicy::Strict {
                                    return Err(json_document_error(&e));
                                }
                                malformed_records += 1;
                            }
                        }
                    }

                    // Fall out to the shared tail so the "nothing profileable"
                    // guard and the final flush still run.
                    break 'document;
                }
                buf_reader.consume(1);
                bytes_in_chunk += 1;

                let mut expect_value = true;
                let mut allow_end = true;
                let mut array_closed = false;
                let mut drain_remainder = false;

                loop {
                    let (next, whitespace) = peek_non_whitespace(&mut buf_reader)?;
                    bytes_in_chunk += whitespace as u64;
                    let Some(next) = next else {
                        if error_policy == JsonErrorPolicy::Strict {
                            return Err(malformed_json_array_error(
                                "unexpected end of input before closing ']'",
                            ));
                        }
                        malformed_records += 1;
                        drain_remainder = true;
                        break;
                    };

                    if expect_value {
                        if next == b']' {
                            if !allow_end {
                                if error_policy == JsonErrorPolicy::Strict {
                                    return Err(malformed_json_array_error(
                                        "trailing comma before closing ']'",
                                    ));
                                }
                                malformed_records += 1;
                                drain_remainder = true;
                                break;
                            }
                            buf_reader.consume(1);
                            bytes_in_chunk += 1;
                            array_closed = true;
                            break;
                        }

                        if next == b',' {
                            if error_policy == JsonErrorPolicy::Strict {
                                return Err(malformed_json_array_error(
                                    "unexpected comma where an array value was required",
                                ));
                            }
                            malformed_records += 1;
                            drain_remainder = true;
                            break;
                        }

                        match read_json_record(&mut buf_reader, next)? {
                            JsonRecord::Object(obj) => {
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
                                        false,
                                    )?
                                {
                                    return Ok(malformed_records);
                                }
                            }
                            JsonRecord::NonObject(kind) => {
                                if error_policy == JsonErrorPolicy::Strict {
                                    return Err(non_object_record_error(
                                        kind,
                                        emitted_records + malformed_records + 1,
                                    ));
                                }
                                // The element was consumed in full, so the array
                                // grammar is still intact and the objects after
                                // it are still profileable.
                                malformed_records += 1;
                                // Approximate progress for a record that yields no row.
                                bytes_in_chunk += 10;
                            }
                            JsonRecord::Malformed(e) => {
                                // A corrupt element leaves the array parser unable
                                // to resync, so we stop here either way; strict mode
                                // surfaces the failure instead of a partial profile.
                                if error_policy == JsonErrorPolicy::Strict {
                                    return Err(DataProfilerError::JsonParsingError {
                                        message: format!("malformed JSON record: {e}"),
                                    });
                                }
                                malformed_records += 1;
                                drain_remainder = true;
                                break;
                            }
                        }
                        expect_value = false;
                    } else {
                        match next {
                            b',' => {
                                buf_reader.consume(1);
                                bytes_in_chunk += 1;
                                expect_value = true;
                                allow_end = false;
                            }
                            b']' => {
                                buf_reader.consume(1);
                                bytes_in_chunk += 1;
                                array_closed = true;
                                break;
                            }
                            _ => {
                                if error_policy == JsonErrorPolicy::Strict {
                                    return Err(malformed_json_array_error(
                                        "expected ',' or ']' after an array value",
                                    ));
                                }
                                malformed_records += 1;
                                drain_remainder = true;
                                break;
                            }
                        }
                    }
                }

                if drain_remainder {
                    bytes_in_chunk += drain_to_end(&mut buf_reader)? as u64;
                } else if array_closed {
                    let (trailing, whitespace) = peek_non_whitespace(&mut buf_reader)?;
                    bytes_in_chunk += whitespace as u64;
                    if trailing.is_some() {
                        if error_policy == JsonErrorPolicy::Strict {
                            return Err(malformed_json_array_error(
                                "non-whitespace content follows the closing ']'",
                            ));
                        }
                        malformed_records += 1;
                        bytes_in_chunk += drain_to_end(&mut buf_reader)? as u64;
                    }
                }
            }
        }

        // Input made up entirely of malformed records must fail, matching the
        // file/bytes paths, rather than surfacing a generic "empty stream" error.
        if emitted_records == 0 && malformed_records > 0 {
            return Err(DataProfilerError::JsonParsingError {
                message: "No valid JSON records found \
                          (every record was malformed or not a JSON object)"
                    .to_string(),
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
                true,
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
        allow_empty_schema: bool,
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
            .with_semantic_hints(self.options.semantic_hints());
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

        // A JSON source may legitimately have no columns: records with no fields
        // are rows against an empty schema. A CSV stream cannot — its first line
        // is the schema, so an empty one means there is nothing to profile.
        if headers.is_empty() && !allow_empty_schema {
            return Err(DataProfilerError::StreamingError {
                message: "No column headers found in stream".to_string(),
            });
        }

        // Headers are the declared schema even when the stream contains no
        // records. Pre-register them so the public async path preserves the
        // same header-only CSV invariant as the file-based engines.
        column_stats.init_columns(&headers);

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
                // A cap of zero rows is met before any row is read. The check
                // below runs after a row is processed, which is right for every
                // positive cap but would let `max_rows(0)` return one row — a
                // cap the caller set and the report then claims to have
                // honoured.
                if row_limit == Some(0) {
                    rows_consumed = row_idx;
                    hit_row_limit = true;
                    break;
                }

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

        let names: Vec<&str> = report
            .column_profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect();
        assert_eq!(names, vec!["name", "age", "salary"]);
        assert!(
            report
                .column_profiles
                .iter()
                .all(|profile| profile.data_type == DataType::String && profile.total_count == 0)
        );
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

    /// A bare object is the second shape a standard JSON document takes, so it
    /// is one record rather than an error (#486) — and a record with no fields
    /// is still a row (#533), on this transport as on every other.
    #[tokio::test]
    async fn test_json_document_reads_a_bare_object_as_one_row() {
        let source = BytesSource::new(
            bytes::Bytes::from_static(b"{}"),
            AsyncSourceInfo::new("json-test", FileFormat::Json),
        );
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .expect("a bare object is a valid JSON document");

        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.execution.error_count, 0);
        assert!(report.column_profiles.is_empty());
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

    fn json_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("test", FileFormat::Json).size_hint(Some(data.len() as u64)),
        )
    }

    #[tokio::test]
    async fn test_async_json_and_jsonl_accept_leading_utf8_bom_and_count_it() {
        for source in [
            json_source(b"\xEF\xBB\xBF[{\"id\":1},{\"id\":2}]"),
            jsonl_source(b"\xEF\xBB\xBF{\"id\":1}\n{\"id\":2}\n"),
        ] {
            let expected_bytes = source.source_info().size_hint.unwrap();
            let report = AsyncStreamingProfiler::new()
                .analyze_stream(source)
                .await
                .unwrap();

            assert_eq!(report.execution.rows_processed, 2);
            assert_eq!(report.execution.error_count, 0);
            assert_eq!(report.execution.bytes_consumed, Some(expected_bytes));
        }
    }

    #[tokio::test]
    async fn test_async_records_with_no_fields_are_rows_against_no_columns() {
        // A record with no fields was read and analysed; nothing was found in
        // it. The file scanner counts it as a row, so the stream must too --
        // and it must not be mistaken for an input holding no records at all.
        for (data, rows) in [
            (jsonl_source(b"{}\n"), 1),
            (jsonl_source(b"{}\n{}\n"), 2),
            (json_source(b"[{}]"), 1),
            (json_source(b"[{},{}]"), 2),
            // No records at all: zero rows against the same zero columns.
            (json_source(b"[]"), 0),
        ] {
            let report = AsyncStreamingProfiler::new()
                .analyze_stream(data)
                .await
                .expect("a fieldless record is well-formed JSON");

            assert_eq!(report.execution.rows_processed, rows);
            assert_eq!(report.execution.columns_detected, 0);
            assert_eq!(report.execution.error_count, 0);
            assert!(report.column_profiles.is_empty());
        }
    }

    #[tokio::test]
    async fn test_async_fieldless_records_do_not_hide_the_columns_around_them() {
        // The schema is discovered from the records, so an empty first record
        // must not freeze it: the fields that follow still become columns, and
        // the fieldless row reads as null across them.
        for data in [
            jsonl_source(b"{}\n{\"a\":1}\n"),
            jsonl_source(b"{\"a\":1}\n{}\n"),
            json_source(b"[{},{\"a\":1}]"),
        ] {
            let report = AsyncStreamingProfiler::new()
                .analyze_stream(data)
                .await
                .unwrap();

            assert_eq!(report.execution.rows_processed, 2);
            assert_eq!(report.column_profiles.len(), 1);
            assert_eq!(report.column_profiles[0].name, "a");
            assert_eq!(report.column_profiles[0].null_count, 1);
        }
    }

    #[tokio::test]
    async fn test_async_jsonl_does_not_strip_later_utf8_bom() {
        let source = jsonl_source(b"{\"id\":1}\n\xEF\xBB\xBF{\"id\":2}\n");
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.execution.error_count, 1);
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

    /// Every JSON value that is not an object, so none of them can be a row.
    /// Numbers appear twice on purpose: they are the one value serde reads from
    /// a reader by peeking one byte past the end.
    const NON_OBJECT_VALUES: &[(&str, &str)] = &[
        ("null", "null"),
        ("boolean", "true"),
        ("number", "42"),
        ("number", "-1.5e3"),
        ("string", r#""text""#),
        ("array", "[1, 2]"),
    ];

    #[tokio::test]
    async fn test_async_tolerant_counts_non_object_records_and_keeps_scanning() {
        for (kind, value) in NON_OBJECT_VALUES {
            for (fmt, data) in [
                (
                    FileFormat::Jsonl,
                    format!("{{\"id\":1}}\n{value}\n{{\"id\":2}}\n"),
                ),
                (
                    FileFormat::Json,
                    format!("[{{\"id\":1}}, {value}, {{\"id\":2}}]"),
                ),
            ] {
                let label = format!("{fmt:?}/{kind}");
                let source =
                    BytesSource::new(bytes::Bytes::from(data), AsyncSourceInfo::new("test", fmt));
                let report = AsyncStreamingProfiler::new()
                    .analyze_stream(source)
                    .await
                    .unwrap();
                assert_eq!(
                    report.execution.rows_processed, 2,
                    "{label}: record after it was lost"
                );
                assert_eq!(
                    report.execution.error_count, 1,
                    "{label}: was silently dropped"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_async_strict_rejects_the_first_non_object_record() {
        for (kind, value) in NON_OBJECT_VALUES {
            for (fmt, data) in [
                (
                    FileFormat::Jsonl,
                    format!("{{\"id\":1}}\n{value}\n{{\"id\":2}}\n"),
                ),
                (
                    FileFormat::Json,
                    format!("[{{\"id\":1}}, {value}, {{\"id\":2}}]"),
                ),
            ] {
                let label = format!("{fmt:?}/{kind}");
                let source =
                    BytesSource::new(bytes::Bytes::from(data), AsyncSourceInfo::new("test", fmt));
                let err = AsyncStreamingProfiler::new()
                    .json_error_policy(JsonErrorPolicy::Strict)
                    .analyze_stream(source)
                    .await
                    .expect_err("strict mode must reject a non-object record");

                let message = err.to_string();
                assert!(
                    message.contains("non-object JSON record"),
                    "{label}: {message}"
                );
                assert!(message.contains("at position 2"), "{label}: {message}");
                assert!(
                    message.contains(&format!("found {kind}")),
                    "{label}: {message}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_async_number_element_does_not_swallow_the_array_delimiter() {
        // serde ends a number by peeking one byte past it and drops that byte
        // with the deserializer; here it is the `,` the array scanner needs.
        let source = json_source(br#"[{"id":1}, 1, 2, 3, {"id":2}, 4, {"id":3}]"#);
        let report = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .unwrap();
        assert_eq!(report.execution.rows_processed, 3);
        assert_eq!(report.execution.error_count, 4);
    }

    #[tokio::test]
    async fn test_async_input_of_only_non_object_records_fails() {
        let source = jsonl_source(b"\"just a string\"\n1\n[2]\n");
        let err = AsyncStreamingProfiler::new()
            .analyze_stream(source)
            .await
            .expect_err("nothing profileable should not return a clean empty profile");

        assert!(
            err.to_string().contains("No valid JSON records found"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_async_json_array_container_grammar_obeys_error_policy() {
        let recoverable_cases = [
            (b"[{\"x\":1}".as_ref(), "missing closing bracket"),
            (b"[{\"x\":1},".as_ref(), "comma followed by EOF"),
            (b"[{\"x\":1} {\"x\":2}]".as_ref(), "missing comma"),
            (b"[{\"x\":1},,{\"x\":2}]".as_ref(), "doubled comma"),
            (b"[{\"x\":1},]".as_ref(), "trailing comma"),
            (b"[{\"x\":1}] trailing".as_ref(), "trailing content"),
            (b"[{\"x\":1}][{\"x\":2}]".as_ref(), "second top-level value"),
        ];

        for (data, case) in recoverable_cases {
            let report = AsyncStreamingProfiler::new()
                .analyze_stream(json_source(data))
                .await
                .expect("tolerant scan should retain the valid prefix");
            assert_eq!(report.execution.rows_processed, 1, "{case}");
            assert_eq!(report.execution.error_count, 1, "{case}");
        }

        let err = AsyncStreamingProfiler::new()
            .analyze_stream(json_source(b"[,{\"x\":1}]"))
            .await
            .expect_err("an array failing before its first value must fail");
        assert!(err.to_string().to_lowercase().contains("malformed"));

        for (data, case) in recoverable_cases
            .into_iter()
            .chain([(b"[,{\"x\":1}]".as_ref(), "leading comma")])
        {
            let err = AsyncStreamingProfiler::new()
                .json_error_policy(JsonErrorPolicy::Strict)
                .analyze_stream(json_source(data))
                .await
                .expect_err("strict mode must reject invalid array grammar");
            assert!(
                err.to_string()
                    .to_lowercase()
                    .contains("malformed json array"),
                "{case}: {err}"
            );
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
