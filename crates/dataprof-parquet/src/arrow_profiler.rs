use crate::record_batch_analyzer::BatchRowTracker;
use arrow::array::*;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::*;
use dataprof_core::{
    ColumnProfile, DataProfilerError, DataSource, DataType, ExecutionMetadata, FileFormat,
    MetricPack, PeakMemorySampler, QualityDimension, SemanticHints, TruncationReason,
};
use dataprof_csv::CsvParserConfig;
use dataprof_metrics::CardinalityEstimator;
use dataprof_metrics::analysis::inference::{infer_type, is_null_like_token};
use dataprof_runtime::{
    ColumnProfileInput, ExactNumericAggregates, ProfileReport, ReportAssembler,
    StreamReservoirSampler, TextLengths, ValueHintBindingAccumulator, build_column_profile,
};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Sample cap for numeric columns (matches SAMPLE_THRESHOLD in stats::numeric)
const NUMERIC_SAMPLE_CAP: usize = 10_000;

/// Columnar profiler using Apache Arrow for efficient column-oriented processing
pub struct ArrowProfiler {
    batch_size: usize,
    memory_limit_mb: usize,
    quality_dimensions: Option<Vec<QualityDimension>>,
    metric_packs: Option<Vec<MetricPack>>,
    csv_config: Option<CsvParserConfig>,
    locale: Option<String>,
    semantic_hints: SemanticHints,
}

impl ArrowProfiler {
    pub fn new() -> Self {
        Self {
            batch_size: 8192, // Default batch size for Arrow
            memory_limit_mb: 512,
            quality_dimensions: None,
            metric_packs: None,
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

    pub fn csv_config(mut self, config: CsvParserConfig) -> Self {
        self.csv_config = Some(config);
        self
    }

    pub fn locale(mut self, locale: String) -> Self {
        self.locale = Some(locale);
        self
    }

    pub fn semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.semantic_hints = hints;
        self
    }

    pub fn analyze_csv_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        let start = std::time::Instant::now();
        let file = File::open(file_path)?;
        let file_size_bytes = file.metadata()?.len();
        let _file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Read first to infer schema from headers
        let mut header_builder = csv::ReaderBuilder::new();
        header_builder.has_headers(true);
        if let Some(ref config) = self.csv_config {
            if let Some(delim) = config.delimiter {
                header_builder.delimiter(delim);
            }
            header_builder.flexible(config.flexible);
            header_builder.quote(config.quote_char);
            if config.trim_whitespace {
                header_builder.trim(csv::Trim::All);
            }
        }
        let mut reader = header_builder.from_path(file_path)?;

        // Get headers to create Arrow schema
        let headers = reader.headers()?.clone();
        let mut fields = Vec::new();
        for header in headers.iter() {
            // Start with string type, Arrow will convert during processing
            fields.push(Field::new(header, arrow::datatypes::DataType::Utf8, true));
        }
        let schema = Arc::new(Schema::new(fields));

        // Now create Arrow reader with proper schema
        let file = File::open(file_path)?;
        let mut arrow_builder = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_batch_size(self.batch_size);
        if let Some(ref config) = self.csv_config {
            if let Some(delim) = config.delimiter {
                arrow_builder = arrow_builder.with_delimiter(delim);
            }
            arrow_builder = arrow_builder
                .with_quote(config.quote_char)
                .with_truncated_rows(config.flexible);
        }
        let csv_reader = arrow_builder
            .build(file)
            .map_err(|error| map_arrow_csv_error(file_path, &error))?;

        // Process data in columnar batches
        let mut column_analyzers: std::collections::HashMap<String, ColumnAnalyzer> =
            std::collections::HashMap::new();

        for field in schema.fields() {
            column_analyzers.insert(
                field.name().to_string(),
                ColumnAnalyzer::new(field.data_type()),
            );
        }

        let mut total_rows = 0;
        // Full-stream duplicate-row tracking: without it, files whose sample
        // reservoirs are misaligned (any column with nulls) would silently
        // skip the duplicate component of the uniqueness dimension — breaking
        // cross-engine score parity with the incremental engine.
        let mut row_tracker = BatchRowTracker::default();
        let mut hint_bindings = ValueHintBindingAccumulator::new(&self.semantic_hints);

        // The Arrow CSV reader has no row cap, so enforce it here: stop once the
        // limit is reached and slice the batch that straddles it, keeping the row
        // count exact rather than rounding up to a batch boundary.
        let max_rows = self.csv_config.as_ref().and_then(|config| config.max_rows);
        let mut truncated = false;
        let mut memory_sampler = PeakMemorySampler::new();

        for batch_result in csv_reader {
            let mut batch = batch_result.map_err(|error| map_arrow_csv_error(file_path, &error))?;

            if let Some(max) = max_rows {
                if total_rows >= max {
                    truncated = true;
                    break;
                }
                let remaining = max - total_rows;
                if batch.num_rows() > remaining {
                    batch = batch.slice(0, remaining);
                    truncated = true;
                }
            }

            total_rows += batch.num_rows();
            row_tracker.observe_batch(&batch);

            // Process each column in the batch
            for (col_idx, column) in batch.columns().iter().enumerate() {
                let schema = batch.schema();
                let field = schema.field(col_idx);

                if let Some(analyzer) = column_analyzers.get_mut(field.name()) {
                    analyzer.process_array(column)?;
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

        // Convert analyzers to column profiles and extract samples
        // Iterate in header order (from schema) to preserve source column ordering
        let packs = self.metric_packs.as_deref();
        let skip_stats = !MetricPack::include_statistics(packs);
        let skip_patterns = !MetricPack::include_patterns(packs);

        let mut column_profiles = Vec::new();
        let mut sample_columns = std::collections::HashMap::new();

        for header in headers.iter() {
            let name = header.to_string();
            if let Some(analyzer) = column_analyzers.get(&name) {
                let profile = analyzer.to_column_profile(
                    name.clone(),
                    skip_stats,
                    skip_patterns,
                    self.locale.as_deref(),
                    &self.semantic_hints,
                );
                column_profiles.push(profile);
                sample_columns.insert(name, analyzer.get_sample_values());
            }
        }

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        memory_sampler.sample();
        let mut execution =
            ExecutionMetadata::new(total_rows, num_columns, scan_time_ms).with_engine("columnar");
        if let Some(peak_mb) = memory_sampler.peak_mb() {
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
        .with_row_duplicates(row_tracker.summary());

        if MetricPack::include_quality(packs) {
            assembler = assembler
                .with_quality_data(sample_columns)
                .with_exact_value_hint_bindings(hint_bindings.bindings(headers.iter()))
                .with_semantic_hints(self.semantic_hints.clone());
            if let Some(ref dims) = self.quality_dimensions {
                assembler = assembler.with_requested_dimensions(dims.clone());
            }
        } else {
            assembler = assembler.skip_quality();
        }

        Ok(assembler.build())
    }
}

fn map_arrow_csv_error(file_path: &Path, error: &arrow::error::ArrowError) -> DataProfilerError {
    let message = error.to_string();

    if message.contains("incorrect number of fields") {
        return DataProfilerError::CsvParsingError {
            message,
            suggestion: format!(
                "The explicit columnar CSV engine currently requires rectangular rows. '{}' contains ragged records that the Arrow backend cannot recover from. Use engine='auto' or engine='incremental' for malformed CSV, or clean the file before forcing engine='columnar'.",
                file_path.display()
            ),
        };
    }

    DataProfilerError::ArrowError { message }
}

impl Default for ArrowProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Column analyzer for Arrow arrays
struct ColumnAnalyzer {
    data_type: arrow::datatypes::DataType,
    total_count: usize,
    null_count: usize,
    cardinality: CardinalityEstimator,
    // Numeric statistics
    min_value: Option<f64>,
    max_value: Option<f64>,
    sum: f64,
    sum_squares: f64,
    numeric_count: usize,
    // Text statistics
    min_length: usize,
    max_length: usize,
    total_length: usize,
    // Boolean statistics
    true_count: usize,
    false_count: usize,
    // Reservoir sample for pattern detection and order statistics
    sample_values: StreamReservoirSampler,
}

impl ColumnAnalyzer {
    fn new(data_type: &arrow::datatypes::DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            total_count: 0,
            null_count: 0,
            cardinality: CardinalityEstimator::new(),
            min_value: None,
            max_value: None,
            sum: 0.0,
            sum_squares: 0.0,
            numeric_count: 0,
            min_length: usize::MAX,
            max_length: 0,
            total_length: 0,
            true_count: 0,
            false_count: 0,
            sample_values: StreamReservoirSampler::new(NUMERIC_SAMPLE_CAP),
        }
    }

    fn process_array(&mut self, array: &dyn Array) -> Result<(), DataProfilerError> {
        self.total_count += array.len();
        // logical_null_count, not null_count: a NullArray has no validity
        // buffer, so the physical count reports 0 nulls for an all-null array.
        self.null_count += array.logical_null_count();

        match array.data_type() {
            arrow::datatypes::DataType::Null => {
                // Every slot of a NullArray is null, but is_null() is false on
                // all of them (no validity buffer), so the string fallback
                // below would fabricate one value per null slot.
            }
            arrow::datatypes::DataType::Float64 => {
                if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    self.process_float64_array(float_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to Float64Array".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Float32 => {
                if let Some(float_array) = array.as_any().downcast_ref::<Float32Array>() {
                    self.process_float32_array(float_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to Float32Array".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Int64 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    self.process_int64_array(int_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to Int64Array".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Int32 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
                    self.process_int32_array(int_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to Int32Array".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    self.process_string_array(string_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to StringArray".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                if let Some(string_array) = array.as_any().downcast_ref::<LargeStringArray>() {
                    self.process_large_string_array(string_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to LargeStringArray".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Boolean => {
                if let Some(bool_array) = array.as_any().downcast_ref::<BooleanArray>() {
                    self.process_boolean_array(bool_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to BooleanArray".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Date32 => {
                if let Some(date_array) = array.as_any().downcast_ref::<Date32Array>() {
                    self.process_date32_array(date_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to Date32Array".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Date64 => {
                if let Some(date_array) = array.as_any().downcast_ref::<Date64Array>() {
                    self.process_date64_array(date_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast to Date64Array".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Timestamp(_, _) => {
                if let Some(ts_array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    self.process_timestamp_array(ts_array)?;
                } else if let Some(ts_array) =
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    self.process_timestamp_micro_array(ts_array)?;
                } else if let Some(ts_array) =
                    array.as_any().downcast_ref::<TimestampMillisecondArray>()
                {
                    self.process_timestamp_milli_array(ts_array)?;
                } else if let Some(ts_array) = array.as_any().downcast_ref::<TimestampSecondArray>()
                {
                    self.process_timestamp_second_array(ts_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast Timestamp array".to_string(),
                    });
                }
            }
            arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::LargeBinary => {
                if let Some(bin_array) = array.as_any().downcast_ref::<BinaryArray>() {
                    self.process_binary_array(bin_array)?;
                } else if let Some(bin_array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                    self.process_large_binary_array(bin_array)?;
                } else {
                    return Err(DataProfilerError::ArrowError {
                        message: "Failed to downcast Binary array".to_string(),
                    });
                }
            }
            _ => {
                // For other types, convert to string and process
                self.process_as_string_array(array)?;
            }
        }

        Ok(())
    }

    fn process_float64_array(&mut self, array: &Float64Array) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                self.update_numeric_stats(value);

                // Add to unique values (with limit)
                self.cardinality.insert_owned(value.to_string());

                // Keep samples for pattern detection
                self.sample_values.offer(value.to_string());
            }
        }
        Ok(())
    }

    fn process_float32_array(&mut self, array: &Float32Array) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                self.cardinality.insert_owned(value.to_string());

                self.sample_values.offer(value.to_string());
            }
        }
        Ok(())
    }

    fn process_int64_array(&mut self, array: &Int64Array) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                self.cardinality.insert_owned(value.to_string());

                self.sample_values.offer(value.to_string());
            }
        }
        Ok(())
    }

    fn process_int32_array(&mut self, array: &Int32Array) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                self.cardinality.insert_owned(value.to_string());

                self.sample_values.offer(value.to_string());
            }
        }
        Ok(())
    }

    fn process_string_array(&mut self, array: &StringArray) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                if is_null_like_token(value) {
                    self.null_count += 1;
                    continue;
                }
                self.update_text_stats(value);
                // Utf8 columns often carry numeric content (the Arrow CSV
                // reader may deliver strings): keep the exact accumulators
                // fed so numeric stats never fall back to the sample.
                // decode-audit: no-data — a cell that does not parse is a
                // non-numeric value, excluded from numeric stats by design.
                if let Some(number) = value.parse::<f64>().ok().filter(|n| n.is_finite()) {
                    self.update_numeric_stats(number);
                }

                self.cardinality.insert(value);

                self.sample_values.offer(value.to_string());
            }
        }
        Ok(())
    }

    fn process_large_string_array(
        &mut self,
        array: &LargeStringArray,
    ) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                if is_null_like_token(value) {
                    self.null_count += 1;
                    continue;
                }
                self.update_text_stats(value);
                // decode-audit: no-data — a cell that does not parse is a
                // non-numeric value, excluded from numeric stats by design.
                if let Some(number) = value.parse::<f64>().ok().filter(|n| n.is_finite()) {
                    self.update_numeric_stats(number);
                }

                self.cardinality.insert(value);

                self.sample_values.offer(value.to_string());
            }
        }
        Ok(())
    }

    fn process_boolean_array(&mut self, array: &BooleanArray) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                if value {
                    self.true_count += 1;
                } else {
                    self.false_count += 1;
                }
                let value_str = if value { "True" } else { "False" };

                self.cardinality.insert(value_str);

                self.sample_values.offer(value_str.to_string());
            }
        }
        Ok(())
    }

    fn process_date32_array(&mut self, array: &Date32Array) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                // Date32 represents days since epoch
                let date_str = format!("date32:{}", value);
                self.update_text_stats(&date_str);

                self.cardinality.insert(&date_str);

                self.sample_values.offer(date_str);
            }
        }
        Ok(())
    }

    fn process_date64_array(&mut self, array: &Date64Array) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                // Date64 represents milliseconds since epoch
                let date_str = format!("date64:{}", value);
                self.update_text_stats(&date_str);

                self.cardinality.insert(&date_str);

                self.sample_values.offer(date_str);
            }
        }
        Ok(())
    }

    fn process_timestamp_array(
        &mut self,
        array: &TimestampNanosecondArray,
    ) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_ns:{}", value);
                self.update_text_stats(&ts_str);

                self.cardinality.insert(&ts_str);

                self.sample_values.offer(ts_str);
            }
        }
        Ok(())
    }

    fn process_timestamp_micro_array(
        &mut self,
        array: &TimestampMicrosecondArray,
    ) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_us:{}", value);
                self.update_text_stats(&ts_str);

                self.cardinality.insert(&ts_str);

                self.sample_values.offer(ts_str);
            }
        }
        Ok(())
    }

    fn process_timestamp_milli_array(
        &mut self,
        array: &TimestampMillisecondArray,
    ) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_ms:{}", value);
                self.update_text_stats(&ts_str);

                self.cardinality.insert(&ts_str);

                self.sample_values.offer(ts_str);
            }
        }
        Ok(())
    }

    fn process_timestamp_second_array(
        &mut self,
        array: &TimestampSecondArray,
    ) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_s:{}", value);
                self.update_text_stats(&ts_str);

                self.cardinality.insert(&ts_str);

                self.sample_values.offer(ts_str);
            }
        }
        Ok(())
    }

    fn process_binary_array(&mut self, array: &BinaryArray) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                // Convert to hex string manually (first 8 bytes)
                let sample_bytes = &value[..value.len().min(8)];
                let hex_str = format!(
                    "0x{}",
                    sample_bytes
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>()
                );
                self.update_text_stats(&hex_str);

                self.cardinality.insert(&hex_str);

                self.sample_values.offer(hex_str);
            }
        }
        Ok(())
    }

    fn process_large_binary_array(
        &mut self,
        array: &LargeBinaryArray,
    ) -> Result<(), DataProfilerError> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                // Convert to hex string manually (first 8 bytes)
                let sample_bytes = &value[..value.len().min(8)];
                let hex_str = format!(
                    "0x{}",
                    sample_bytes
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>()
                );
                self.update_text_stats(&hex_str);

                self.cardinality.insert(&hex_str);

                self.sample_values.offer(hex_str);
            }
        }
        Ok(())
    }

    fn process_as_string_array(&mut self, array: &dyn Array) -> Result<(), DataProfilerError> {
        // Convert any array type to string for processing using Arrow's display functionality
        use arrow::util::display::array_value_to_string;

        for i in 0..array.len() {
            if !array.is_null(i) {
                // Use Arrow's built-in conversion to string
                let value = array_value_to_string(array, i)
                    .unwrap_or_else(|_| format!("<type:{}>", array.data_type()));
                self.update_text_stats(&value);

                self.sample_values.offer(value.clone());

                self.cardinality.insert_owned(value);
            }
        }
        Ok(())
    }

    fn update_numeric_stats(&mut self, value: f64) {
        self.sum += value;
        self.sum_squares += value * value;
        self.numeric_count += 1;

        self.min_value = Some(match self.min_value {
            Some(min) => min.min(value),
            None => value,
        });

        self.max_value = Some(match self.max_value {
            Some(max) => max.max(value),
            None => value,
        });
    }

    /// Exact aggregates over every numeric value processed, independent of the
    /// bounded reservoir sample. `None` when the column saw no numeric values.
    fn exact_numeric_aggregates(&self) -> Option<ExactNumericAggregates> {
        let (min, max) = (self.min_value?, self.max_value?);
        if self.numeric_count == 0 {
            return None;
        }
        let mean = self.sum / self.numeric_count as f64;
        // Clamp: sum-of-squares variance can go slightly negative from
        // floating-point cancellation.
        let variance = dataprof_metrics::stats::numeric::calculate_variance(
            self.sum_squares,
            self.sum,
            self.numeric_count,
        )
        .max(0.0);
        Some(ExactNumericAggregates {
            min,
            max,
            mean,
            std_dev: variance.sqrt(),
            variance,
            count: self.numeric_count,
        })
    }

    fn update_text_stats(&mut self, value: &str) {
        let len = value.len();
        self.min_length = self.min_length.min(len);
        self.max_length = self.max_length.max(len);
        self.total_length += len;
    }

    fn to_column_profile(
        &self,
        name: String,
        skip_statistics: bool,
        skip_patterns: bool,
        locale: Option<&str>,
        semantic_hints: &SemanticHints,
    ) -> ColumnProfile {
        let data_type = if semantic_hints.is_identifier_column(&name) {
            DataType::Identifier
        } else {
            self.infer_data_type()
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
            boolean_counts: if matches!(self.data_type, arrow::datatypes::DataType::Boolean) {
                Some((self.true_count, self.false_count))
            } else {
                None
            },
            skip_statistics,
            skip_patterns,
            locale,
            exact_numeric: self.exact_numeric_aggregates(),
        })
    }

    fn infer_data_type(&self) -> DataType {
        match &self.data_type {
            arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => {
                DataType::Float
            }
            arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int16
            | arrow::datatypes::DataType::Int8 => DataType::Integer,
            arrow::datatypes::DataType::Boolean => DataType::Boolean,
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                // Reuse the shared inference logic for consistent type detection
                // across all engines (dates before numerics, 100% match threshold)
                infer_type(self.sample_values.samples())
            }
            _ => DataType::String,
        }
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

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_reports_clear_error_for_extra_fields() {
        let mut temp_file = NamedTempFile::new().expect("temp file should be created");
        writeln!(temp_file, "name,age,city").expect("header should write");
        writeln!(temp_file, "Alice,25,Rome").expect("row should write");
        writeln!(temp_file, "Bob,30,Milan,unexpected").expect("ragged row should write");
        temp_file.flush().expect("temp file should flush");

        let profiler = ArrowProfiler::new().csv_config(CsvParserConfig::default());
        let error = profiler
            .analyze_csv_file(temp_file.path())
            .expect_err("extra fields should still fail in the Arrow CSV backend");

        match error {
            DataProfilerError::CsvParsingError {
                message,
                suggestion,
            } => {
                assert!(message.contains("incorrect number of fields"));
                assert!(suggestion.contains("engine='auto'"));
                assert!(suggestion.contains("engine='incremental'"));
            }
            other => panic!("expected CsvParsingError, got {other:?}"),
        }
    }

    #[test]
    fn test_boolean_column_detection_and_stats() {
        use arrow::array::BooleanArray;
        use arrow::datatypes::DataType as ArrowDataType;

        // Create a ColumnAnalyzer for a Boolean column
        let mut analyzer = ColumnAnalyzer::new(&ArrowDataType::Boolean);

        // Construct a BooleanArray with some nulls
        let array = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false),
        ]);

        analyzer
            .process_array(&array)
            .expect("should process boolean array");

        assert_eq!(analyzer.total_count, 6);
        assert_eq!(analyzer.null_count, 1);
        assert_eq!(analyzer.true_count, 3);
        assert_eq!(analyzer.false_count, 2);

        let profile = analyzer.to_column_profile(
            "flag".to_string(),
            false,
            false,
            None,
            &SemanticHints::default(),
        );
        assert_eq!(profile.data_type, DataType::Boolean);
        assert_eq!(profile.total_count, 6);
        assert_eq!(profile.null_count, 1);

        match &profile.stats {
            ColumnStats::Boolean(b) => {
                assert_eq!(b.true_count, 3);
                assert_eq!(b.false_count, 2);
                assert!((b.true_ratio - 0.6).abs() < 0.001);
            }
            other => panic!("expected Boolean stats, got {:?}", other),
        }
    }

    #[test]
    fn test_null_array_profiles_as_all_nulls() {
        use arrow::array::NullArray;
        use arrow::datatypes::DataType as ArrowDataType;

        let mut analyzer = ColumnAnalyzer::new(&ArrowDataType::Null);
        analyzer
            .process_array(&NullArray::new(5))
            .expect("should process null array");

        let profile = analyzer.to_column_profile(
            "all_null".to_string(),
            false,
            false,
            None,
            &SemanticHints::default(),
        );
        assert_eq!(profile.total_count, 5);
        assert_eq!(profile.null_count, 5);
        assert_eq!(profile.unique_count, Some(0));
    }
}
