use crate::record_batch_analyzer::BatchRowTracker;
use arrow::array::{Array, StringArray};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{Field, Schema};
use dataprof_core::{
    AnalysisOptions, ColumnProfile, DataProfilerError, DataSource, DataType, ExecutionMetadata,
    FileFormat, Locale, MetricPack, PeakMemorySampler, QualityDimension, SemanticHints,
    TruncationReason, char_len,
};
use dataprof_csv::CsvParserConfig;
use dataprof_metrics::analysis::inference::{infer_type, is_null_like_token};
use dataprof_metrics::{CardinalityEstimator, NumericAccumulator};
use dataprof_runtime::{
    ColumnProfileInput, ExactNumericAggregates, ProfileReport, ReportAssembler,
    StreamReservoirSampler, TextLengths, ValueHintBindingAccumulator, build_column_profile,
};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Sample cap for numeric columns (matches SAMPLE_THRESHOLD in stats::numeric)
const NUMERIC_SAMPLE_CAP: usize = 10_000;

/// What a pass with the `csv` crate learns about a file that Arrow cannot
/// report on its own: see [`ArrowProfiler::pre_scan`].
struct CsvPreScan {
    /// Rows whose field count differs from the header, in either direction.
    ragged_row_count: usize,
    /// Width of the widest record, never less than the header width.
    max_fields: usize,
    /// Whether a record exists past the row cap, which is what makes a profile
    /// truncated rather than merely as long as the file.
    has_more_rows: bool,
    /// Whether the file ended inside a quoted field, when the pass reached
    /// the end of the file.
    unterminated_quote: Option<bool>,
}

/// Everything one Arrow decode of the file accumulated.
struct CsvDecodeOutcome {
    column_analyzers: std::collections::HashMap<String, ColumnAnalyzer>,
    row_tracker: BatchRowTracker,
    hint_bindings: ValueHintBindingAccumulator,
    total_rows: usize,
    /// Rows `arrow-csv` padded to the schema width, cumulative over the decode.
    /// This is the ragged-row count only when no record is *wider* than the
    /// schema, because Arrow aborts on those instead of counting them.
    padded_rows: usize,
    peak_memory_mb: Option<f64>,
    /// Whether the file ended inside a quoted field, when the decode reached
    /// the end of the file. Arrow stops at a row cap, so it may not have.
    unterminated_quote: Option<bool>,
}

/// Why an Arrow decode stopped short.
enum CsvDecodeFailure {
    /// A record whose field count does not match the schema. `arrow-csv` pads a
    /// short record only under `with_truncated_rows`, and never accepts a long
    /// one, so this is what tells the caller a `csv`-crate pass has to run.
    FieldCount(arrow::error::ArrowError),
    Other(DataProfilerError),
}

/// Columnar profiler using Apache Arrow for efficient column-oriented processing
pub struct ArrowProfiler {
    batch_size: usize,
    memory_limit_mb: usize,
    quality_dimensions: Option<Vec<QualityDimension>>,
    metric_packs: Option<Vec<MetricPack>>,
    columns: Option<Vec<String>>,
    csv_config: Option<CsvParserConfig>,
    locale: Option<Locale>,
    semantic_hints: SemanticHints,
}

impl ArrowProfiler {
    pub fn new() -> Self {
        Self {
            batch_size: 8192, // Default batch size for Arrow
            memory_limit_mb: 512,
            quality_dimensions: None,
            metric_packs: None,
            columns: None,
            csv_config: None,
            locale: None,
            semantic_hints: SemanticHints::default(),
        }
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn memory_limit_mb(mut self, limit: usize) -> Self {
        self.memory_limit_mb = limit;
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

    pub fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    pub fn csv_config(mut self, config: CsvParserConfig) -> Self {
        self.csv_config = Some(config);
        self
    }

    pub fn locale(mut self, locale: Locale) -> Self {
        self.locale = Some(locale);
        self
    }

    pub fn semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.semantic_hints = hints;
        self
    }

    /// The `csv`-crate reader configured the way this profile parses the file,
    /// with the row cap it has to honour.
    fn csv_reader_builder(&self) -> (csv::ReaderBuilder, Option<usize>) {
        let mut builder = csv::ReaderBuilder::new();
        let (has_header, flexible, max_rows) = match self.csv_config {
            Some(ref config) => {
                if let Some(delim) = config.delimiter {
                    builder.delimiter(delim);
                }
                builder.quote(config.quote_char);
                if config.trim_whitespace {
                    builder.trim(csv::Trim::All);
                }
                (config.has_header, config.flexible, config.max_rows)
            }
            None => (true, false, None),
        };
        builder.has_headers(has_header);
        // Strict parsing rejects in the pre-scan, one reader earlier than Arrow
        // would, so the caller gets the same field-count diagnostic as every
        // other path instead of Arrow's "incorrect number of fields".
        builder.flexible(flexible);
        (builder, max_rows)
    }

    /// The source wrapped so it reports whether it ended inside a quoted field,
    /// with the delimiter and quote this profile parses it with.
    fn quote_tracked(&self, file: File) -> dataprof_csv::QuoteTrackingReader<File> {
        let (delimiter, quote) = match self.csv_config {
            Some(ref config) => (config.delimiter.unwrap_or(b','), config.quote_char),
            None => (b',', b'"'),
        };
        dataprof_csv::QuoteTrackingReader::new(file, delimiter, quote)
    }

    /// Read the first record and stop: the header row, or, under
    /// `has_header=false`, the first data row, which is what the column count
    /// and the generated `column_N` names come from. Either way this reads one
    /// record rather than the body.
    fn read_headers(&self, file_path: &Path) -> Result<csv::StringRecord, DataProfilerError> {
        let (builder, _) = self.csv_reader_builder();
        let mut reader = builder.from_path(file_path)?;
        Ok(reader.headers()?.clone())
    }

    /// One `csv`-crate pass over the body, taken only when Arrow cannot answer.
    ///
    /// `arrow-csv` reports the rows it padded, so a file whose records are never
    /// wider than the header needs no pass at all. A wider record is different:
    /// Arrow aborts the scan rather than counting it, and nothing in its output
    /// says how wide the widest record is. That is what this recovers, along
    /// with the ragged count for the same file.
    ///
    /// It is a second read of the file on the engine chosen for speed, so the
    /// cost was measured rather than assumed: on a 123 MB / 2M-row CSV it adds
    /// ~1.4s to a ~6.5s profile, about 18% (best of five, warm cache). Keeping
    /// it off the common path is what #549 was about.
    fn pre_scan(&self, file_path: &Path) -> Result<CsvPreScan, DataProfilerError> {
        let (builder, max_rows) = self.csv_reader_builder();

        let source = self.quote_tracked(File::open(file_path)?);
        let quote_outcome = source.outcome();
        let mut reader = builder.from_reader(source);
        let header_width = reader.headers()?.len();

        let mut ragged_row_count = 0;
        let mut max_fields = header_width;
        // The row cap bounds the count too: a ragged row this profile never
        // reaches is not part of it and must not show up in its report.
        let mut rows_scanned = 0;
        let mut record = csv::ByteRecord::new();
        while max_rows.is_none_or(|max| rows_scanned < max)
            && reader.read_byte_record(&mut record)?
        {
            rows_scanned += 1;
            if record.len() != header_width {
                ragged_row_count += 1;
                max_fields = max_fields.max(record.len());
            }
        }

        // One record past the cap, read but not counted. It is the only thing
        // that separates a profile cut short from one that reached the end of
        // its file, and the Arrow decode stops at the cap so it will never see
        // this record itself.
        let has_more_rows = max_rows.is_some() && reader.read_byte_record(&mut record)?;

        Ok(CsvPreScan {
            ragged_row_count,
            max_fields,
            has_more_rows,
            unterminated_quote: quote_outcome.at_end(),
        })
    }

    /// One Arrow decode of the whole file at a given schema width.
    ///
    /// Restartable on purpose: a record wider than `max_fields` aborts the scan
    /// with [`CsvDecodeFailure::FieldCount`], and the caller retries at the
    /// width a `csv`-crate pass found. Every accumulator is built here so a
    /// retry starts from nothing rather than from half a file.
    #[allow(clippy::too_many_arguments)]
    fn decode_csv(
        &self,
        file_path: &Path,
        header_names: &[String],
        max_fields: usize,
        projection: &[usize],
        projected_header_names: &[String],
        has_header: bool,
        max_rows: Option<usize>,
    ) -> Result<CsvDecodeOutcome, CsvDecodeFailure> {
        let header_width = header_names.len();
        let mut fields = Vec::with_capacity(max_fields.max(header_width));
        for header in header_names {
            // Always read raw UTF-8 cells so null-token handling, type inference,
            // and reservoir samples use the original CSV text.
            fields.push(Field::new(header, arrow::datatypes::DataType::Utf8, true));
        }
        // `arrow-csv` has no counterpart to `with_truncated_rows` for a row that
        // is *wider* than the schema: it aborts the scan. Widening the schema to
        // the widest record in the file gives those surplus fields somewhere to
        // land; they are projected away below, which is the same recovery the
        // incremental engine performs when it truncates a record to header width.
        for overflow in header_width..max_fields {
            fields.push(Field::new(
                format!("__dataprof_overflow_{overflow}"),
                arrow::datatypes::DataType::Utf8,
                true,
            ));
        }
        let schema = Arc::new(Schema::new(fields));

        let file = File::open(file_path).map_err(|error| CsvDecodeFailure::Other(error.into()))?;
        let mut arrow_builder = ReaderBuilder::new(schema)
            .with_header(has_header)
            .with_batch_size(self.batch_size);
        // `arrow-csv` offsets the end bound by the header row, so a cap at the
        // very top of the range overflows inside its builder. A cap that size
        // cannot bind any file that exists, so it is left unset rather than
        // special-cased further down: the decode then reads to the end, which
        // is what such a cap asks for.
        if let Some(max) = max_rows.filter(|&max| max < usize::MAX) {
            // Bound the decoder rather than slicing what it hands back. Slicing
            // still reads a whole batch first, so a record past the cap that
            // Arrow refuses to decode, one wider than the schema, failed the
            // profile over a row the profile was never going to report (#753).
            arrow_builder = arrow_builder.with_bounds(0, max);
        }
        if let Some(ref config) = self.csv_config {
            if let Some(delim) = config.delimiter {
                arrow_builder = arrow_builder.with_delimiter(delim);
            }
            arrow_builder = arrow_builder
                .with_quote(config.quote_char)
                .with_truncated_rows(config.flexible);
        }
        let source = self.quote_tracked(file);
        let quote_outcome = source.outcome();
        let mut csv_reader = arrow_builder
            .build(source)
            .map_err(|error| classify_arrow_csv_error(file_path, error))?;

        let mut column_analyzers: std::collections::HashMap<String, ColumnAnalyzer> =
            std::collections::HashMap::new();
        for name in projected_header_names {
            column_analyzers.insert(name.clone(), ColumnAnalyzer::new());
        }

        // `with_bounds` already stops at the cap, so nothing here re-applies it.
        let mut total_rows = 0;
        // Full-stream duplicate-row tracking: without it, files whose sample
        // reservoirs are misaligned (any column with nulls) would silently
        // skip the duplicate component of the uniqueness dimension, breaking
        // cross-engine score parity with the incremental engine.
        let mut row_tracker = BatchRowTracker::default();
        let mut hint_bindings = ValueHintBindingAccumulator::new(&self.semantic_hints);

        let mut memory_sampler = PeakMemorySampler::new();

        for batch_result in csv_reader.by_ref() {
            let mut batch =
                batch_result.map_err(|error| classify_arrow_csv_error(file_path, error))?;

            // Drop overflow and unselected columns before anything observes the
            // batch, so every downstream calculation sees the same projection.
            batch = batch.project(projection).map_err(|error| {
                CsvDecodeFailure::Other(DataProfilerError::arrow_error_from(error))
            })?;

            total_rows += batch.num_rows();
            row_tracker.observe_batch(&batch);

            for (col_idx, column) in batch.columns().iter().enumerate() {
                let schema = batch.schema();
                let field = schema.field(col_idx);

                if let Some(analyzer) = column_analyzers.get_mut(field.name()) {
                    analyzer
                        .process_array(column)
                        .map_err(CsvDecodeFailure::Other)?;
                }
                if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
                    for row_index in 0..values.len() {
                        if !values.is_null(row_index) {
                            hint_bindings.observe(field.name(), values.value(row_index));
                        }
                    }
                }
            }

            // Sample after processing, while the batch and the analyzer state
            // it grew are both resident, so per-batch allocation spikes count
            // toward the peak.
            memory_sampler.sample();
        }

        memory_sampler.sample_now();
        Ok(CsvDecodeOutcome {
            column_analyzers,
            row_tracker,
            hint_bindings,
            total_rows,
            // Read after the final flush: the counter is cumulative across
            // flushes, so this is the whole decode's total.
            padded_rows: csv_reader.truncated_row_count(),
            peak_memory_mb: memory_sampler.peak_mb(),
            unterminated_quote: quote_outcome.at_end(),
        })
    }

    pub fn analyze_csv_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        let start = std::time::Instant::now();
        let file = File::open(file_path)?;
        let file_size_bytes = file.metadata()?.len();
        let _file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Only the header is needed up front. What the body looks like comes
        // from the decoder, and from `pre_scan` only when Arrow cannot say.
        let headers = self.read_headers(file_path)?;
        let has_header = self
            .csv_config
            .as_ref()
            .is_none_or(|config| config.has_header);

        // Reject duplicate headers before building the name-keyed analyzer map,
        // which would otherwise merge two columns into one profile.
        let header_names: Vec<String> = if has_header {
            let names = headers.iter().map(str::to_string).collect::<Vec<_>>();
            dataprof_core::validate_unique_column_names(&names, "CSV header")?;
            names
        } else {
            (0..headers.len())
                .map(|index| format!("column_{index}"))
                .collect()
        };
        let header_width = header_names.len();
        let options = AnalysisOptions::default()
            .with_columns(self.columns.clone())
            .with_metric_packs(self.metric_packs.clone())
            .with_quality_dimensions(self.quality_dimensions.clone())
            .with_locale(self.locale)
            .with_semantic_hints(self.semantic_hints.clone());
        let projection = options
            .column_indices(&header_names)?
            .unwrap_or_else(|| (0..header_width).collect());
        let projected_header_names = projection
            .iter()
            .map(|index| header_names[*index].clone())
            .collect::<Vec<_>>();

        let max_rows = self.csv_config.as_ref().and_then(|config| config.max_rows);

        // A row cap makes the decoder's cumulative counter unusable on its own:
        // rows the decoder pads are counted whether or not the cap keeps them.
        // The pre-scan stops at the cap, so it answers exactly, it reads only as
        // far as the cap rather than to the end of the file, and one record
        // further tells the caller whether the profile was cut short.
        let mut scan = match max_rows {
            Some(_) => Some(self.pre_scan(file_path)?),
            None => None,
        };

        // Decode optimistically at header width. Arrow reports the rows it pads,
        // so nothing else is needed unless a record turns out to be wider than
        // the schema, the one shape Arrow refuses rather than counts. That
        // refusal is what buys the pre-scan, so a file that needs no repair
        // never pays for one.
        let outcome = loop {
            let max_fields = scan.as_ref().map_or(header_width, |scan| scan.max_fields);
            match self.decode_csv(
                file_path,
                &header_names,
                max_fields,
                &projection,
                &projected_header_names,
                has_header,
                max_rows,
            ) {
                Ok(outcome) => break outcome,
                Err(CsvDecodeFailure::Other(error)) => return Err(error),
                Err(CsvDecodeFailure::FieldCount(error)) => {
                    if scan.is_some() {
                        // The schema already covers the widest record the `csv`
                        // crate found, so the two parsers disagree on where
                        // records end.
                        return Err(map_arrow_csv_error(file_path, error));
                    }
                    // Strict parsing rejects inside the pre-scan, with the field
                    // counts named. Flexible parsing gets the width to retry at.
                    let rescan = self.pre_scan(file_path)?;
                    if rescan.max_fields <= header_width {
                        return Err(map_arrow_csv_error(file_path, error));
                    }
                    scan = Some(rescan);
                }
            }
        };

        let CsvDecodeOutcome {
            column_analyzers,
            row_tracker,
            hint_bindings,
            total_rows,
            padded_rows,
            peak_memory_mb,
            unterminated_quote,
        } = outcome;
        let truncated = scan.as_ref().is_some_and(|scan| scan.has_more_rows);
        // Either pass that reached the end of the file answers; they read it
        // with the same quote rules. A capped decode can stop short of the end,
        // and the pre-scan that runs under every cap reads one record past it.
        // The answer is withheld for a truncated profile, which never read the
        // record an unclosed quote swallows.
        let unterminated_quote = unterminated_quote
            .or_else(|| scan.as_ref().and_then(|scan| scan.unterminated_quote))
            .filter(|_| !truncated);
        let flexible = self
            .csv_config
            .as_ref()
            .is_some_and(|config| config.flexible);
        if unterminated_quote == Some(true) && !flexible {
            return Err(dataprof_csv::unterminated_quote_error());
        }
        // A pre-scan, where one ran, is the authority: it saw the wide records
        // Arrow aborted on, and it honoured the row cap. Where none ran, no
        // record was wider than the header and no cap applied, so every ragged
        // row is a row Arrow padded.
        let ragged_row_count = scan.map_or(padded_rows, |scan| scan.ragged_row_count);

        // Convert analyzers to column profiles and extract samples
        // Iterate in header order (from schema) to preserve source column ordering
        let effective_packs = options.effective_metric_packs();
        let packs = effective_packs.as_deref();
        let skip_stats = !MetricPack::include_statistics(packs);
        let skip_patterns = !MetricPack::include_patterns(packs);

        let mut column_profiles = Vec::new();
        let mut sample_columns = std::collections::HashMap::new();

        for name in &projected_header_names {
            if let Some(analyzer) = column_analyzers.get(name) {
                let profile = analyzer.to_column_profile(
                    name.clone(),
                    skip_stats,
                    skip_patterns,
                    options.locale(),
                    options.semantic_hints(),
                );
                column_profiles.push(profile);
                sample_columns.insert(name.clone(), analyzer.get_sample_values());
            }
        }

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        let mut execution = ExecutionMetadata::new(total_rows, num_columns, scan_time_ms)
            .with_engine("columnar")
            .with_ragged_row_count(ragged_row_count);
        if let Some(ended_inside_quotes) = unterminated_quote {
            execution = execution.with_unterminated_quote(ended_inside_quotes);
        }
        if let Some(peak_mb) = peak_memory_mb {
            execution = execution.with_memory_peak_mb(peak_mb);
        }
        if truncated && let Some(max) = max_rows {
            execution = execution.with_truncation(TruncationReason::MaxRows(max as u64));
        }

        let mut assembler = ReportAssembler::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format: FileFormat::Csv,
                size_bytes: file_size_bytes,
                modified_at: None,
                parquet_metadata: None,
            },
            execution,
        )
        .columns(column_profiles)
        .with_row_duplicates(row_tracker.summary())
        .with_row_completeness(row_tracker.completeness_summary());

        if !MetricPack::include_quality(packs) {
            assembler = assembler.skip_quality();
        } else {
            assembler = assembler
                .with_quality_data(sample_columns)
                .with_exact_value_hint_bindings(
                    hint_bindings.bindings(projected_header_names.iter().map(String::as_str)),
                )
                .with_analysis_options(&options);
        }

        Ok(assembler.build())
    }
}

/// Whether Arrow stopped on a record whose field count the schema cannot hold.
///
/// The one Arrow failure a wider schema can fix, and the only one worth a
/// retry, so both the classifier and the mapper below ask the same question
/// here rather than each spelling out the message Arrow happens to use.
fn is_field_count_error(error: &arrow::error::ArrowError) -> bool {
    is_field_count_error_message(&error.to_string())
}

/// The message half of [`is_field_count_error`], so the phrase Arrow uses is
/// written once and the tests can assert it is *not* what reached the caller.
fn is_field_count_error_message(message: &str) -> bool {
    message.contains("incorrect number of fields")
}

/// Separate the one Arrow error a wider schema can fix from every other one.
fn classify_arrow_csv_error(file_path: &Path, error: arrow::error::ArrowError) -> CsvDecodeFailure {
    if is_field_count_error(&error) {
        return CsvDecodeFailure::FieldCount(error);
    }
    CsvDecodeFailure::Other(map_arrow_csv_error(file_path, error))
}

fn map_arrow_csv_error(file_path: &Path, error: arrow::error::ArrowError) -> DataProfilerError {
    let message = error.to_string();

    if is_field_count_error(&error) {
        let suggestion = format!(
            "The columnar engine sizes its schema from a pre-scan of '{}', so a row Arrow still finds ragged means the two parsers disagree on where records end — most often unbalanced quotes or an embedded newline. Use engine='auto' or engine='incremental', which parse the file only once.",
            file_path.display()
        );
        return DataProfilerError::csv_parsing_with_source(message, suggestion, error);
    }

    DataProfilerError::arrow_with_source(message, error)
}

impl Default for ArrowProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Analyzer for the UTF-8 arrays produced by `ArrowProfiler::analyze_csv_file`.
/// Typed Arrow inputs are handled separately by `record_batch_analyzer`.
struct ColumnAnalyzer {
    total_count: usize,
    null_count: usize,
    cardinality: CardinalityEstimator,
    // Numeric statistics
    numeric: NumericAccumulator,
    // Text statistics
    min_length: usize,
    max_length: usize,
    total_length: usize,
    // Reservoir sample for pattern detection and order statistics
    sample_values: StreamReservoirSampler,
    date_matched_values: usize,
}

impl ColumnAnalyzer {
    fn new() -> Self {
        Self {
            total_count: 0,
            null_count: 0,
            cardinality: CardinalityEstimator::new(),
            numeric: NumericAccumulator::new(),
            min_length: usize::MAX,
            max_length: 0,
            total_length: 0,
            sample_values: StreamReservoirSampler::new(NUMERIC_SAMPLE_CAP),
            date_matched_values: 0,
        }
    }

    fn offer_sample(&mut self, value: String) {
        if dataprof_metrics::value_matches_hint(&value, dataprof_core::SemanticHintKind::Temporal) {
            self.date_matched_values += 1;
        }
        self.sample_values.offer(value);
    }

    fn process_array(&mut self, array: &dyn Array) -> Result<(), DataProfilerError> {
        // The CSV reader has an explicit Utf8 schema; reject a violated
        // invariant instead of casting values and losing their original text.
        let strings = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataProfilerError::arrow_error(&format!(
                    "Arrow CSV profiler expected a Utf8 array, got {}",
                    array.data_type()
                ))
            })?;
        self.total_count += strings.len();
        self.null_count += strings.null_count();
        self.process_string_array(strings);
        Ok(())
    }

    fn process_string_array(&mut self, array: &StringArray) {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                if is_null_like_token(value) {
                    self.null_count += 1;
                    continue;
                }
                self.update_text_stats(value);
                // CSV cells always arrive as strings. Keep exact aggregates
                // over every finite number, independent of the sample.
                // NumericAccumulator::update requires finite input, so NaN,
                // infinities, and overflowing literals are excluded here.
                // decode-audit: no-data — a cell that does not parse is a
                // non-numeric value, excluded from numeric stats by design.
                if let Some(number) = value.trim().parse::<f64>().ok().filter(|n| n.is_finite()) {
                    self.numeric.update(number);
                }

                self.cardinality.insert(value);

                self.offer_sample(value.to_string());
            }
        }
    }

    /// Exact aggregates over every numeric value processed, independent of the
    /// bounded reservoir sample. `None` when the column saw no numeric values.
    fn exact_numeric_aggregates(&self) -> Option<ExactNumericAggregates> {
        let (min, max) = (self.numeric.min()?, self.numeric.max()?);
        Some(ExactNumericAggregates {
            min,
            max,
            mean: self.numeric.mean(),
            std_dev: self.numeric.sample_std_dev(),
            variance: self.numeric.sample_variance(),
            count: self.numeric.count() as usize,
        })
    }

    fn update_text_stats(&mut self, value: &str) {
        // Unicode scalar values, not UTF-8 bytes: see `dataprof_core::text_units`.
        let len = char_len(value);
        self.min_length = self.min_length.min(len);
        self.max_length = self.max_length.max(len);
        self.total_length += len;
    }

    fn to_column_profile(
        &self,
        name: String,
        skip_statistics: bool,
        skip_patterns: bool,
        locale: Option<Locale>,
        semantic_hints: &SemanticHints,
    ) -> ColumnProfile {
        let data_type = if semantic_hints.is_identifier_column(&name) {
            DataType::Identifier
        } else {
            infer_type(self.sample_values.samples())
        };
        let avg_length = if self.total_count > self.null_count {
            self.total_length as f64 / (self.total_count - self.null_count) as f64
        } else {
            0.0
        };

        build_column_profile(ColumnProfileInput {
            name,
            data_type,
            total_count: self.total_count,
            null_count: self.null_count,
            unique_count: Some(self.cardinality.estimate()),
            unique_count_is_approximate: Some(self.cardinality.is_approximate()),
            sample_values: self.sample_values.samples(),
            text_lengths: Some(TextLengths {
                min_length: self.min_length,
                max_length: self.max_length,
                avg_length,
            }),
            boolean_counts: None,
            skip_statistics,
            skip_patterns,
            locale,
            exact_numeric: self.exact_numeric_aggregates(),
            exact_date_matches: Some(self.date_matched_values),
        })
    }

    /// Get collected sample values for quality metrics calculation
    fn get_sample_values(&self) -> Vec<String> {
        self.sample_values.samples().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dataprof_core::ColumnStats;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_arrow_profiler() -> Result<(), DataProfilerError> {
        // Create a test CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary,active")?;
        writeln!(temp_file, "Alice,25,50000.0,true")?;
        writeln!(temp_file, "Bob,30,60000.5,false")?;
        writeln!(temp_file, "Charlie,35,70000.0,true")?;
        temp_file.flush()?;

        // Test Arrow profiler
        let profiler = ArrowProfiler::new();
        let report = profiler.analyze_csv_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 4);

        // Find age column and verify it's detected as numeric
        let age_column = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("Age column should exist");

        assert_eq!(age_column.total_count, 3);
        assert_eq!(
            age_column.data_type,
            DataType::Integer,
            "age column should be detected as Integer"
        );

        Ok(())
    }

    #[test]
    fn test_memory_peak_is_populated() -> Result<(), DataProfilerError> {
        // Regression for #419: memory_peak_mb was declared but never set,
        // so every report shipped null for a headline instrumentation field.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,value")?;
        for i in 1..=100 {
            writeln!(temp_file, "{},val_{}", i, i)?;
        }
        temp_file.flush()?;

        let report = ArrowProfiler::new().analyze_csv_file(temp_file.path())?;

        let peak = report
            .execution
            .memory_peak_mb
            .expect("columnar engine must report peak memory");
        assert!(peak > 0.0, "peak memory must be positive, got {peak}");

        Ok(())
    }

    #[test]
    fn test_unique_count_not_capped_for_high_cardinality_csv() -> Result<(), DataProfilerError> {
        // Regression for the columnar hard cap: the Arrow CSV path used to stop
        // counting distinct values at 1,000 and expose that cap as the exact
        // count, wrecking uniqueness-based quality metrics for large columns.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id")?;
        let rows = 50_000;
        for id in 0..rows {
            writeln!(temp_file, "{}", id)?;
        }
        temp_file.flush()?;

        let profiler = ArrowProfiler::new();
        let report = profiler.analyze_csv_file(temp_file.path())?;

        let id_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "id")
            .expect("id column should exist");

        let unique = id_col.unique_count.expect("unique_count should be present");
        assert_ne!(unique, 1_000, "must not expose the old hard cap as exact");
        let error = (unique as f64 - rows as f64).abs() / rows as f64;
        assert!(error < 0.05, "{rows} distinct ids estimated as {unique}");

        Ok(())
    }

    #[test]
    fn test_value_hint_binding_covers_full_stream() -> Result<(), DataProfilerError> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "value")?;
        writeln!(temp_file, "1")?;
        for _ in 0..10_050 {
            writeln!(temp_file, "not-a-number")?;
        }
        temp_file.flush()?;

        let hints = SemanticHints::new(vec!["value".to_string()], vec![]);
        let report = ArrowProfiler::new()
            .semantic_hints(hints)
            .analyze_csv_file(temp_file.path())?;
        let binding = report
            .semantic_hint_bindings
            .iter()
            .find(|binding| binding.column == "value")
            .expect("positive hint binding");

        assert_eq!(binding.checked_values, 10_051);
        assert_eq!(binding.matched_values, 1);
        assert!(binding.exact);
        Ok(())
    }

    #[test]
    fn test_arrow_profiler_csv_with_mixed_columns() -> Result<(), DataProfilerError> {
        // The Arrow CSV profiler reads all columns as Utf8 and then infers types.
        // Numeric-looking Utf8 columns that also have float-typed Arrow data
        // get properly typed. Test with a mixed CSV.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,score")?;
        for i in 1..=20 {
            writeln!(temp_file, "Person{},{}", i, i * 10)?;
        }
        temp_file.flush()?;

        let profiler = ArrowProfiler::new();
        let report = profiler.analyze_csv_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 2);

        let score_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "score")
            .expect("score column should exist");

        assert_eq!(score_col.total_count, 20);

        // Numeric Utf8 columns should now be detected as Integer
        assert_eq!(
            score_col.data_type,
            DataType::Integer,
            "score column should be detected as Integer"
        );

        // Verify numeric stats are computed
        match &score_col.stats {
            ColumnStats::Numeric(n) => {
                assert!((n.min - 10.0).abs() < 0.01, "min should be 10");
                assert!((n.max - 200.0).abs() < 0.01, "max should be 200");
                assert!((n.mean - 105.0).abs() < 0.01, "mean should be 105");
                assert!(n.skewness.is_some(), "skewness should be computed");
                assert!(n.kurtosis.is_some(), "kurtosis should be computed");
            }
            other => panic!("score column should have Numeric stats, got {:?}", other),
        }

        // Verify name column is still String
        let name_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "name")
            .expect("name column should exist");
        assert_eq!(name_col.data_type, DataType::String);

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_numeric_inference_float() -> Result<(), DataProfilerError> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "label,value")?;
        for i in 1..=20 {
            writeln!(temp_file, "item{},{:.2}", i, i as f64 * 1.5)?;
        }
        temp_file.flush()?;

        let profiler = ArrowProfiler::new();
        let report = profiler.analyze_csv_file(temp_file.path())?;

        let value_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "value")
            .expect("value column should exist");

        assert_eq!(
            value_col.data_type,
            DataType::Float,
            "value column with decimals should be detected as Float"
        );
        assert!(
            matches!(&value_col.stats, ColumnStats::Numeric(..)),
            "Float column should have Numeric stats"
        );

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_allows_truncated_rows_when_flexible() -> Result<(), DataProfilerError> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,Rome")?;
        writeln!(temp_file, "Bob,30")?;
        temp_file.flush()?;

        let profiler = ArrowProfiler::new().csv_config(CsvParserConfig::default());
        let report = profiler.analyze_csv_file(temp_file.path())?;

        let city_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "city")
            .expect("city column should exist");

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(city_col.total_count, 2);
        assert_eq!(city_col.null_count, 1);
        // Padding the row is the recovery; reporting it is the contract (#470).
        assert_eq!(report.execution.ragged_row_count, 1);

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_headerless_csv_keeps_the_first_record() -> Result<(), DataProfilerError>
    {
        let mut temp_file = NamedTempFile::new()?;
        write!(temp_file, "1,Alice\n2,Bob")?;
        temp_file.flush()?;

        let report = ArrowProfiler::new()
            .csv_config(CsvParserConfig::default().has_header(false))
            .analyze_csv_file(temp_file.path())?;

        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.ragged_row_count, 0);
        assert_eq!(
            report
                .column_profiles
                .iter()
                .map(|column| (column.name.as_str(), column.total_count))
                .collect::<Vec<_>>(),
            [("column_0", 2), ("column_1", 2)]
        );
        Ok(())
    }

    #[test]
    fn test_arrow_profiler_recovers_and_counts_extra_fields() -> Result<(), DataProfilerError> {
        // Arrow aborts on a row wider than its schema, so the pre-scan widens
        // the schema and the surplus fields are projected away — the same
        // recovery the incremental engine performs, with the same count.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,Rome")?;
        writeln!(temp_file, "Bob,30,Milan,unexpected")?;
        temp_file.flush()?;

        let profiler = ArrowProfiler::new().csv_config(CsvParserConfig::default());
        let report = profiler.analyze_csv_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.ragged_row_count, 1);

        let city_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "city")
            .expect("city column should exist");
        assert_eq!(city_col.total_count, 2);
        assert_eq!(
            city_col.null_count, 0,
            "the dropped field is the 4th, not city"
        );

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_ragged_count_respects_the_row_cap() -> Result<(), DataProfilerError> {
        // The ragged row sits past the cap, so this profile never reads it and
        // must not report it.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,Rome")?;
        writeln!(temp_file, "Bob,30")?;
        temp_file.flush()?;

        let config = CsvParserConfig {
            max_rows: Some(1),
            ..CsvParserConfig::default()
        };
        let report = ArrowProfiler::new()
            .csv_config(config)
            .analyze_csv_file(temp_file.path())?;

        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.execution.ragged_row_count, 0);

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_ignores_a_wide_row_past_the_row_cap() -> Result<(), DataProfilerError> {
        // The wide row sits past the cap, so this profile never reads it. Arrow
        // refuses a record wider than its schema, so decoding a whole batch and
        // slicing afterwards failed the profile over a row it was discarding.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,Rome")?;
        writeln!(temp_file, "Bob,30,Milan,unexpected")?;
        temp_file.flush()?;

        let config = CsvParserConfig {
            max_rows: Some(1),
            ..CsvParserConfig::default()
        };
        let report = ArrowProfiler::new()
            .csv_config(config)
            .analyze_csv_file(temp_file.path())?;

        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.execution.ragged_row_count, 0);
        assert!(matches!(
            report.execution.truncation_reason,
            Some(TruncationReason::MaxRows(1))
        ));

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_does_not_truncate_a_file_that_ends_at_the_cap()
    -> Result<(), DataProfilerError> {
        // A cap the file happens to reach exactly is not a truncation, and the
        // row past the cap is the only thing that tells the two apart.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,Rome")?;
        writeln!(temp_file, "Bob,30,Milan")?;
        temp_file.flush()?;

        let config = CsvParserConfig {
            max_rows: Some(2),
            ..CsvParserConfig::default()
        };
        let report = ArrowProfiler::new()
            .csv_config(config)
            .analyze_csv_file(temp_file.path())?;

        assert_eq!(report.execution.rows_processed, 2);
        assert!(
            report.execution.truncation_reason.is_none(),
            "a file of exactly `max_rows` rows was not cut short: {:?}",
            report.execution.truncation_reason
        );

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_truncates_at_a_cap_inside_a_batch() -> Result<(), DataProfilerError> {
        // The cap falls inside a batch, so the decoder has to stop mid-batch
        // and the row count must land on the cap rather than a batch boundary.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        for row in 0..10 {
            writeln!(temp_file, "Name{row},{row},Rome")?;
        }
        temp_file.flush()?;

        let config = CsvParserConfig {
            max_rows: Some(3),
            ..CsvParserConfig::default()
        };
        let report = ArrowProfiler::new()
            .batch_size(4)
            .csv_config(config)
            .analyze_csv_file(temp_file.path())?;

        assert_eq!(report.execution.rows_processed, 3);
        assert!(matches!(
            report.execution.truncation_reason,
            Some(TruncationReason::MaxRows(3))
        ));

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_accepts_a_row_cap_at_the_top_of_the_range()
    -> Result<(), DataProfilerError> {
        // `arrow-csv` adds the header offset to the end bound it is given, so
        // handing it `usize::MAX` overflows inside the builder. A cap nothing
        // can reach has to behave as no cap rather than panicking.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,Rome")?;
        writeln!(temp_file, "Bob,30,Milan")?;
        temp_file.flush()?;

        for has_header in [true, false] {
            let config = CsvParserConfig {
                max_rows: Some(usize::MAX),
                has_header,
                ..CsvParserConfig::default()
            };
            let report = ArrowProfiler::new()
                .csv_config(config)
                .analyze_csv_file(temp_file.path())?;

            // The header row counts as data when it is not a header.
            let expected = if has_header { 2 } else { 3 };
            assert_eq!(report.execution.rows_processed, expected, "{has_header}");
            assert!(
                report.execution.truncation_reason.is_none(),
                "{has_header}: {:?}",
                report.execution.truncation_reason
            );
        }

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_counts_nothing_for_a_rectangular_file() -> Result<(), DataProfilerError>
    {
        // A trailing empty field is a present field. Arrow renders it as null,
        // exactly like a padded one, which is why the count cannot come from
        // Arrow's output — and why this case has to be pinned.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,Rome")?;
        writeln!(temp_file, "Carol,35,")?;
        temp_file.flush()?;

        let report = ArrowProfiler::new()
            .csv_config(CsvParserConfig::default())
            .analyze_csv_file(temp_file.path())?;

        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.ragged_row_count, 0);

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_counts_a_padded_row_from_an_earlier_batch() {
        // The decoder's counter is cumulative across flushes, so a padded row in
        // a batch that is not the last one only survives if the count is read
        // after the final flush rather than off the batch that carried it.
        let mut temp_file = NamedTempFile::new().expect("temp file should be created");
        writeln!(temp_file, "name,age,city").expect("header should write");
        writeln!(temp_file, "Alice,25").expect("ragged row should write");
        for row in 0..6 {
            writeln!(temp_file, "Name{row},{row},Rome").expect("row should write");
        }
        temp_file.flush().expect("temp file should flush");

        let report = ArrowProfiler::new()
            .batch_size(2)
            .csv_config(CsvParserConfig::default())
            .analyze_csv_file(temp_file.path())
            .expect("flexible parsing should pad the ragged row");

        assert_eq!(report.execution.rows_processed, 7);
        assert_eq!(report.execution.ragged_row_count, 1);
    }

    #[test]
    fn test_arrow_profiler_counts_every_padded_row_across_batches() {
        // One padded row per batch, so a count taken from any single batch, or
        // reset between them, lands short of seven.
        let mut temp_file = NamedTempFile::new().expect("temp file should be created");
        writeln!(temp_file, "name,age,city").expect("header should write");
        for row in 0..7 {
            writeln!(temp_file, "Name{row},{row}").expect("ragged row should write");
        }
        temp_file.flush().expect("temp file should flush");

        let report = ArrowProfiler::new()
            .batch_size(1)
            .csv_config(CsvParserConfig::default())
            .analyze_csv_file(temp_file.path())
            .expect("flexible parsing should pad every ragged row");

        assert_eq!(report.execution.rows_processed, 7);
        assert_eq!(report.execution.ragged_row_count, 7);
    }

    #[test]
    fn test_arrow_profiler_strict_rejects_a_short_row_with_field_counts() {
        // The wide-row case is covered below. A short row reaches Arrow first
        // now that the pre-scan no longer runs ahead of it, so the diagnostic
        // has to come back from the fallback pass rather than from Arrow.
        let mut temp_file = NamedTempFile::new().expect("temp file should be created");
        writeln!(temp_file, "name,age,city").expect("header should write");
        writeln!(temp_file, "Alice,25,Rome").expect("row should write");
        writeln!(temp_file, "Bob,30").expect("short row should write");
        temp_file.flush().expect("temp file should flush");

        let config = CsvParserConfig {
            flexible: false,
            ..CsvParserConfig::default()
        };
        let error = ArrowProfiler::new()
            .csv_config(config)
            .analyze_csv_file(temp_file.path())
            .expect_err("strict parsing must reject a short row");

        match error {
            DataProfilerError::CsvParsingError { message, .. } => {
                // Name both counts, the way the `csv` crate does and Arrow does
                // not, so passing Arrow's "expected 3 got 2" through unmapped
                // would fail here.
                assert!(
                    message.contains("found record with 2 fields")
                        && message.contains("has 3 fields"),
                    "{message}"
                );
                assert!(!is_field_count_error_message(&message), "{message}");
            }
            other => panic!("expected CsvParsingError, got {other:?}"),
        }
    }

    #[test]
    fn test_arrow_profiler_strict_rejects_ragged_rows_with_field_counts() {
        // Strict parsing still rejects, but the pre-scan gets there first, so
        // the diagnostic names the field counts the way every other path does
        // instead of Arrow's opaque "incorrect number of fields".
        let mut temp_file = NamedTempFile::new().expect("temp file should be created");
        writeln!(temp_file, "name,age,city").expect("header should write");
        writeln!(temp_file, "Alice,25,Rome").expect("row should write");
        writeln!(temp_file, "Bob,30,Milan,unexpected").expect("ragged row should write");
        temp_file.flush().expect("temp file should flush");

        let config = CsvParserConfig {
            flexible: false,
            ..CsvParserConfig::default()
        };
        let error = ArrowProfiler::new()
            .csv_config(config)
            .analyze_csv_file(temp_file.path())
            .expect_err("strict parsing must reject a ragged row");

        match error {
            DataProfilerError::CsvParsingError { message, .. } => {
                assert!(
                    message.contains("found record with 4 fields")
                        && message.contains("has 3 fields"),
                    "{message}"
                );
                assert!(!is_field_count_error_message(&message), "{message}");
            }
            other => panic!("expected CsvParsingError, got {other:?}"),
        }
    }

    #[test]
    fn test_typed_array_is_rejected_without_changing_column_state() -> Result<(), DataProfilerError>
    {
        let mut analyzer = ColumnAnalyzer::new();
        let typed = arrow::array::Float64Array::from(vec![Some(42.0), None, Some(f64::NAN)]);
        let error = analyzer
            .process_array(&typed)
            .expect_err("the CSV analyzer must reject typed input");
        assert!(
            error
                .to_string()
                .contains("Arrow CSV profiler expected a Utf8 array, got Float64"),
            "{error}"
        );

        // Rejection must happen before any counters or accumulators change.
        analyzer.process_array(&StringArray::from(vec!["1", "2"]))?;
        let profile = analyzer.to_column_profile(
            "value".to_string(),
            false,
            false,
            None,
            &SemanticHints::default(),
        );
        assert_eq!(profile.total_count, 2);
        assert_eq!(profile.null_count, 0);
        assert_eq!(profile.unique_count, Some(2));
        let ColumnStats::Numeric(stats) = profile.stats else {
            panic!("expected numeric statistics from the subsequent UTF-8 input");
        };
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 2.0);
        assert_eq!(stats.mean, 1.5);
        Ok(())
    }

    #[test]
    fn test_boolean_and_all_null_csv_columns() -> Result<(), DataProfilerError> {
        let mut csv = NamedTempFile::new()?;
        writeln!(csv, "flag,all_null")?;
        for flag in ["true", "false", "TRUE", "", "True", "False"] {
            writeln!(csv, "{flag},")?;
        }
        csv.flush()?;

        let report = ArrowProfiler::new()
            .batch_size(2)
            .analyze_csv_file(csv.path())?;
        let flag = &report.column_profiles[0];
        assert_eq!(flag.data_type, DataType::Boolean);
        assert_eq!(flag.total_count, 6);
        assert_eq!(flag.null_count, 1);
        match &flag.stats {
            ColumnStats::Boolean(stats) => {
                assert_eq!(stats.true_count, 3);
                assert_eq!(stats.false_count, 2);
                assert!((stats.true_ratio - 0.6).abs() < 0.001);
            }
            other => panic!("expected Boolean stats, got {other:?}"),
        }

        let all_null = &report.column_profiles[1];
        assert_eq!(all_null.total_count, 6);
        assert_eq!(all_null.null_count, 6);
        assert_eq!(all_null.unique_count, Some(0));
        Ok(())
    }
}
