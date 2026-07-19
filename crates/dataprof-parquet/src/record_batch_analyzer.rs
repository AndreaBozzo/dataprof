//! Shared utilities for analyzing Apache Arrow RecordBatches.

use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use dataprof_core::{
    ColumnProfile, DataType, SemanticHintBinding, SemanticHintKind, SemanticHints,
};
use dataprof_metrics::CardinalityEstimator;
use dataprof_metrics::analysis::inference::is_null_like_token;
use dataprof_metrics::{RowDuplicateSummary, infer_type};
use dataprof_runtime::{
    ColumnProfileInput, ExactNumericAggregates, RowUniquenessTracker, StreamReservoirSampler,
    TextLengths, build_column_profile,
};
use std::collections::HashMap;
use std::fmt::Write as _;

const NUMERIC_SAMPLE_CAP: usize = 10_000;

/// Full-stream duplicate-row tracking over Arrow batches.
///
/// Row signatures use the same length-prefixed encoding as the incremental
/// engine's `StreamingColumnCollection::process_record`, with a null slot
/// contributing an empty value — so on inputs both engines can read (CSV),
/// duplicate counts agree across engines. Columns fold in schema order,
/// which is stable across the batches of one source.
///
/// When a column's values cannot be rendered (an Arrow type without a
/// formatter), tracking disables itself and [`Self::summary`] returns `None`
/// — the duplicate component reads "not assessed". Substituting placeholder
/// values instead would fabricate or mask duplicates, which is worse than
/// admitting the metric was skipped; and the profile as a whole must not
/// fail over an auxiliary metric.
#[derive(Default)]
pub(crate) struct BatchRowTracker {
    tracker: RowUniquenessTracker,
    disabled: bool,
}

impl BatchRowTracker {
    pub(crate) fn observe_batch(&mut self, batch: &RecordBatch) {
        if self.disabled {
            return;
        }
        if observe_batch_rows(&mut self.tracker, batch).is_err() {
            self.disabled = true;
        }
    }

    /// Full-stream row-duplicate counts; `None` when no rows were seen or a
    /// batch could not be signed (partial counts would be misleading).
    pub(crate) fn summary(&self) -> Option<RowDuplicateSummary> {
        if self.disabled {
            return None;
        }
        self.tracker.summary()
    }
}

/// Fold every row of a batch into the tracker as a canonical signature.
/// See [`BatchRowTracker`] for the encoding and failure contract.
fn observe_batch_rows(tracker: &mut RowUniquenessTracker, batch: &RecordBatch) -> Result<()> {
    use arrow::datatypes::DataType as ArrowDataType;
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    // Null must render as an empty field ("0:"), matching the incremental
    // engine — the default formatter would render a visible placeholder.
    let options = FormatOptions::default().with_null("");
    let formatters: Vec<Option<ArrayFormatter>> = batch
        .columns()
        .iter()
        .map(|column| match column.data_type() {
            // A NullArray has no validity buffer; every slot is null.
            ArrowDataType::Null => Ok(None),
            _ => ArrayFormatter::try_new(column.as_ref(), &options).map(Some),
        })
        .collect::<std::result::Result<_, _>>()
        .map_err(|error| anyhow::anyhow!("row signature formatting failed: {error}"))?;

    let mut value_buffer = String::new();
    for row_index in 0..batch.num_rows() {
        let mut row_signature = String::new();
        for (column, formatter) in batch.columns().iter().zip(&formatters) {
            value_buffer.clear();
            if let Some(formatter) = formatter
                && !column.is_null(row_index)
            {
                write!(value_buffer, "{}", formatter.value(row_index))
                    .map_err(|error| anyhow::anyhow!("row signature formatting failed: {error}"))?;
            }
            let _ = write!(row_signature, "{}:", value_buffer.len());
            row_signature.push_str(&value_buffer);
        }
        tracker.observe(row_signature);
    }

    Ok(())
}

/// Analyzer for processing multiple RecordBatches and building column profiles.
pub struct RecordBatchAnalyzer {
    column_analyzers: HashMap<String, ColumnAnalyzer>,
    total_rows: usize,
    row_tracker: BatchRowTracker,
    semantic_hints: SemanticHints,
}

impl RecordBatchAnalyzer {
    pub fn new() -> Self {
        Self {
            column_analyzers: HashMap::new(),
            total_rows: 0,
            row_tracker: BatchRowTracker::default(),
            semantic_hints: SemanticHints::default(),
        }
    }

    pub fn with_semantic_hints(mut self, hints: &SemanticHints) -> Self {
        self.semantic_hints = hints.clone();
        self
    }

    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.total_rows += batch.num_rows();
        self.row_tracker.observe_batch(batch);

        for (column_index, column) in batch.columns().iter().enumerate() {
            let schema = batch.schema();
            let field = schema.field(column_index);
            let column_name = field.name().to_string();
            let positive_hint = self.semantic_hints.is_positive_column(&column_name);
            let temporal_hint = self.semantic_hints.is_temporal_column(&column_name);

            let analyzer = self.column_analyzers.entry(column_name).or_insert_with(|| {
                ColumnAnalyzer::with_value_hints(field.data_type(), positive_hint, temporal_hint)
            });

            analyzer.process_array(column)?;
        }

        Ok(())
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Full-stream row-duplicate counts, or `None` when no rows were seen or
    /// the batches could not be signed (see [`BatchRowTracker`]).
    pub fn row_duplicate_summary(&self) -> Option<RowDuplicateSummary> {
        self.row_tracker.summary()
    }

    pub fn to_profiles(
        &self,
        skip_statistics: bool,
        skip_patterns: bool,
        locale: Option<&str>,
    ) -> Vec<ColumnProfile> {
        self.to_profiles_with_hints(
            skip_statistics,
            skip_patterns,
            locale,
            &SemanticHints::default(),
        )
    }

    pub fn to_profiles_with_hints(
        &self,
        skip_statistics: bool,
        skip_patterns: bool,
        locale: Option<&str>,
        semantic_hints: &SemanticHints,
    ) -> Vec<ColumnProfile> {
        let mut profiles = Vec::new();
        for (name, analyzer) in &self.column_analyzers {
            profiles.push(analyzer.to_column_profile(
                name.clone(),
                skip_statistics,
                skip_patterns,
                locale,
                semantic_hints,
            ));
        }
        profiles
    }

    pub fn create_sample_columns(&self) -> HashMap<String, Vec<String>> {
        let mut samples = HashMap::new();
        for (name, analyzer) in &self.column_analyzers {
            samples.insert(name.clone(), analyzer.sample_values.samples().to_vec());
        }
        samples
    }

    /// Exact value-driven semantic-hint evidence over every processed batch.
    pub fn semantic_hint_bindings(&self) -> Vec<SemanticHintBinding> {
        let positive = self
            .semantic_hints
            .positive_columns
            .iter()
            .filter_map(|column| {
                self.column_analyzers
                    .get(column)
                    .map(|analyzer| analyzer.hint_binding(column, SemanticHintKind::Positive))
            });
        let temporal = self
            .semantic_hints
            .temporal_columns
            .iter()
            .filter_map(|column| {
                self.column_analyzers
                    .get(column)
                    .map(|analyzer| analyzer.hint_binding(column, SemanticHintKind::Temporal))
            });
        positive.chain(temporal).collect()
    }
}

impl Default for RecordBatchAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ColumnAnalyzer {
    data_type: arrow::datatypes::DataType,
    total_count: usize,
    null_count: usize,
    cardinality: CardinalityEstimator,
    min_value: Option<f64>,
    max_value: Option<f64>,
    sum: f64,
    sum_squares: f64,
    numeric_count: usize,
    min_length: usize,
    max_length: usize,
    total_length: usize,
    true_count: usize,
    false_count: usize,
    sample_values: StreamReservoirSampler,
    positive_hint: bool,
    temporal_hint: bool,
    hint_checked_values: usize,
    positive_matched_values: usize,
    temporal_matched_values: usize,
    date_matched_values: usize,
}

impl ColumnAnalyzer {
    pub fn new(data_type: &arrow::datatypes::DataType) -> Self {
        Self::with_value_hints(data_type, false, false)
    }

    fn with_value_hints(
        data_type: &arrow::datatypes::DataType,
        positive_hint: bool,
        temporal_hint: bool,
    ) -> Self {
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
            positive_hint,
            temporal_hint,
            hint_checked_values: 0,
            positive_matched_values: 0,
            temporal_matched_values: 0,
            date_matched_values: 0,
        }
    }

    fn offer_sample(&mut self, value: String) {
        if !is_null_like_token(value.trim()) {
            let matches_date = self.should_track_date_matches()
                && dataprof_metrics::value_matches_hint(&value, SemanticHintKind::Temporal);
            if matches_date {
                self.date_matched_values += 1;
            }
            if self.positive_hint || self.temporal_hint {
                self.hint_checked_values += 1;
                if self.positive_hint
                    && dataprof_metrics::value_matches_hint(&value, SemanticHintKind::Positive)
                {
                    self.positive_matched_values += 1;
                }
                if self.temporal_hint && matches_date {
                    self.temporal_matched_values += 1;
                }
            }
        }
        self.sample_values.offer(value);
    }

    fn should_track_date_matches(&self) -> bool {
        matches!(
            &self.data_type,
            arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::LargeUtf8
                | arrow::datatypes::DataType::Date32
                | arrow::datatypes::DataType::Date64
                | arrow::datatypes::DataType::Timestamp(_, _)
        )
    }

    fn hint_binding(&self, column: &str, kind: SemanticHintKind) -> SemanticHintBinding {
        let matched_values = match kind {
            SemanticHintKind::Positive => self.positive_matched_values,
            SemanticHintKind::Temporal => self.temporal_matched_values,
            SemanticHintKind::Identifier => self.hint_checked_values,
        };
        SemanticHintBinding {
            column: column.to_string(),
            kind,
            checked_values: self.hint_checked_values,
            matched_values,
            exact: true,
        }
    }

    pub fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        self.total_count += array.len();
        // logical_null_count, not null_count: a NullArray has no validity
        // buffer, so the physical count reports 0 nulls for an all-null array.
        self.null_count += array.logical_null_count();

        match array.data_type() {
            arrow::datatypes::DataType::Null => {
                // Every slot of a NullArray is null, but is_null() is false on
                // all of them (no validity buffer), so the generic path below
                // would fabricate one string value per null slot.
            }
            arrow::datatypes::DataType::Float64 => {
                if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    self.process_float64_array(float_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Float64Array"));
                }
            }
            arrow::datatypes::DataType::Float32 => {
                if let Some(float_array) = array.as_any().downcast_ref::<Float32Array>() {
                    self.process_float32_array(float_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Float32Array"));
                }
            }
            arrow::datatypes::DataType::Int64 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    self.process_int64_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Int64Array"));
                }
            }
            arrow::datatypes::DataType::Int32 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
                    self.process_int32_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Int32Array"));
                }
            }
            arrow::datatypes::DataType::Int16 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int16Array>() {
                    self.process_int16_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Int16Array"));
                }
            }
            arrow::datatypes::DataType::Int8 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int8Array>() {
                    self.process_int8_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Int8Array"));
                }
            }
            arrow::datatypes::DataType::UInt64 => {
                if let Some(int_array) = array.as_any().downcast_ref::<UInt64Array>() {
                    self.process_uint64_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to UInt64Array"));
                }
            }
            arrow::datatypes::DataType::UInt32 => {
                if let Some(int_array) = array.as_any().downcast_ref::<UInt32Array>() {
                    self.process_uint32_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to UInt32Array"));
                }
            }
            arrow::datatypes::DataType::UInt16 => {
                if let Some(int_array) = array.as_any().downcast_ref::<UInt16Array>() {
                    self.process_uint16_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to UInt16Array"));
                }
            }
            arrow::datatypes::DataType::UInt8 => {
                if let Some(int_array) = array.as_any().downcast_ref::<UInt8Array>() {
                    self.process_uint8_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to UInt8Array"));
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    self.process_string_array(string_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to StringArray"));
                }
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                if let Some(string_array) = array.as_any().downcast_ref::<LargeStringArray>() {
                    self.process_large_string_array(string_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to LargeStringArray"));
                }
            }
            arrow::datatypes::DataType::Boolean => {
                if let Some(bool_array) = array.as_any().downcast_ref::<BooleanArray>() {
                    self.process_boolean_array(bool_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to BooleanArray"));
                }
            }
            arrow::datatypes::DataType::Date32 => {
                if let Some(date_array) = array.as_any().downcast_ref::<Date32Array>() {
                    self.process_date32_array(date_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Date32Array"));
                }
            }
            arrow::datatypes::DataType::Date64 => {
                if let Some(date_array) = array.as_any().downcast_ref::<Date64Array>() {
                    self.process_date64_array(date_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Date64Array"));
                }
            }
            arrow::datatypes::DataType::Timestamp(_, _) => {
                self.process_timestamp_array(array)?;
            }
            arrow::datatypes::DataType::Binary => {
                if let Some(binary_array) = array.as_any().downcast_ref::<BinaryArray>() {
                    self.process_binary_array(binary_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to BinaryArray"));
                }
            }
            arrow::datatypes::DataType::LargeBinary => {
                if let Some(binary_array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                    self.process_large_binary_array(binary_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to LargeBinaryArray"));
                }
            }
            arrow::datatypes::DataType::Decimal128(_, _) => {
                if let Some(decimal_array) = array.as_any().downcast_ref::<Decimal128Array>() {
                    self.process_decimal128_array(decimal_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Decimal128Array"));
                }
            }
            arrow::datatypes::DataType::Decimal256(_, _) => {
                if let Some(decimal_array) = array.as_any().downcast_ref::<Decimal256Array>() {
                    self.process_decimal256_array(decimal_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Decimal256Array"));
                }
            }
            arrow::datatypes::DataType::Duration(_) => {
                self.process_duration_array(array)?;
            }
            _ => {
                self.process_generic_array(array)?;
            }
        }

        Ok(())
    }

    fn process_float64_array(&mut self, array: &Float64Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                self.update_numeric_stats(value);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_float32_array(&mut self, array: &Float32Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_int64_array(&mut self, array: &Int64Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_int32_array(&mut self, array: &Int32Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_int16_array(&mut self, array: &Int16Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_int8_array(&mut self, array: &Int8Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_uint64_array(&mut self, array: &UInt64Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_uint32_array(&mut self, array: &UInt32Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_uint16_array(&mut self, array: &UInt16Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_uint8_array(&mut self, array: &UInt8Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);
                self.cardinality.insert_owned(value.to_string());
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_string_array(&mut self, array: &StringArray) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
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
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_large_string_array(&mut self, array: &LargeStringArray) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
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
                self.offer_sample(value.to_string());
            }
        }
        Ok(())
    }

    fn process_boolean_array(&mut self, array: &BooleanArray) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let value = array.value(index);
                if value {
                    self.true_count += 1;
                } else {
                    self.false_count += 1;
                }
                let value_str = if value { "True" } else { "False" };
                self.cardinality.insert(value_str);
                self.offer_sample(value_str.to_string());
            }
        }
        Ok(())
    }

    fn process_date32_array(&mut self, array: &Date32Array) -> Result<()> {
        let formatter = ArrayFormatter::try_new(array, &Default::default())?;
        for index in 0..array.len() {
            if !array.is_null(index) {
                let days = array.value(index);
                self.update_numeric_stats(days as f64);
                let date_str = formatter.value(index).to_string();
                self.cardinality.insert(&date_str);
                self.offer_sample(date_str);
            }
        }
        Ok(())
    }

    fn process_date64_array(&mut self, array: &Date64Array) -> Result<()> {
        let formatter = ArrayFormatter::try_new(array, &Default::default())?;
        for index in 0..array.len() {
            if !array.is_null(index) {
                let millis = array.value(index);
                self.update_numeric_stats(millis as f64);
                let datetime_str = formatter.value(index).to_string();
                self.cardinality.insert(&datetime_str);
                self.offer_sample(datetime_str);
            }
        }
        Ok(())
    }

    fn process_timestamp_array(&mut self, array: &dyn Array) -> Result<()> {
        use arrow::array::PrimitiveArray;

        let formatter = ArrayFormatter::try_new(array, &Default::default())?;

        if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampNanosecondType>>()
        {
            for index in 0..ts_array.len() {
                if !ts_array.is_null(index) {
                    let ts_value = ts_array.value(index);
                    self.update_numeric_stats(ts_value as f64);
                    let ts_str = formatter.value(index).to_string();
                    self.cardinality.insert(&ts_str);
                    self.offer_sample(ts_str);
                }
            }
        } else if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampMicrosecondType>>()
        {
            for index in 0..ts_array.len() {
                if !ts_array.is_null(index) {
                    let ts_value = ts_array.value(index);
                    self.update_numeric_stats(ts_value as f64);
                    let ts_str = formatter.value(index).to_string();
                    self.cardinality.insert(&ts_str);
                    self.offer_sample(ts_str);
                }
            }
        } else if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampMillisecondType>>()
        {
            for index in 0..ts_array.len() {
                if !ts_array.is_null(index) {
                    let ts_value = ts_array.value(index);
                    self.update_numeric_stats(ts_value as f64);
                    let ts_str = formatter.value(index).to_string();
                    self.cardinality.insert(&ts_str);
                    self.offer_sample(ts_str);
                }
            }
        } else if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampSecondType>>()
        {
            for index in 0..ts_array.len() {
                if !ts_array.is_null(index) {
                    let ts_value = ts_array.value(index);
                    self.update_numeric_stats(ts_value as f64);
                    let ts_str = formatter.value(index).to_string();
                    self.cardinality.insert(&ts_str);
                    self.offer_sample(ts_str);
                }
            }
        }

        Ok(())
    }

    fn process_binary_array(&mut self, array: &BinaryArray) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let bytes = array.value(index);
                let len = bytes.len();
                self.update_text_stats(&format!("<binary:{}>", len));
                self.cardinality.insert_owned(format!("len:{}", len));
                self.offer_sample(format!("<binary:{} bytes>", len));
            }
        }
        Ok(())
    }

    fn process_large_binary_array(&mut self, array: &LargeBinaryArray) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let bytes = array.value(index);
                let len = bytes.len();
                self.update_text_stats(&format!("<binary:{}>", len));
                self.cardinality.insert_owned(format!("len:{}", len));
                self.offer_sample(format!("<binary:{} bytes>", len));
            }
        }
        Ok(())
    }

    fn process_decimal128_array(&mut self, array: &Decimal128Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let decimal_value = array.value(index);
                self.update_numeric_stats(decimal_value as f64);
                let decimal_str = format!("dec128:{}", decimal_value);
                self.cardinality.insert(&decimal_str);
                self.offer_sample(decimal_str);
            }
        }
        Ok(())
    }

    fn process_decimal256_array(&mut self, array: &Decimal256Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let decimal_str = format!("dec256:value_{}", index);
                self.update_text_stats(&decimal_str);
                self.cardinality.insert(&decimal_str);
                self.offer_sample(decimal_str);
            }
        }
        Ok(())
    }

    fn process_duration_array(&mut self, array: &dyn Array) -> Result<()> {
        use arrow::array::PrimitiveArray;

        if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationNanosecondType>>()
        {
            for index in 0..dur_array.len() {
                if !dur_array.is_null(index) {
                    let dur_value = dur_array.value(index);
                    self.update_numeric_stats(dur_value as f64);
                    let dur_str = format!("dur:{}ns", dur_value);
                    self.cardinality.insert(&dur_str);
                    self.offer_sample(dur_str);
                }
            }
        } else if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationMicrosecondType>>()
        {
            for index in 0..dur_array.len() {
                if !dur_array.is_null(index) {
                    let dur_value = dur_array.value(index);
                    self.update_numeric_stats(dur_value as f64);
                    let dur_str = format!("dur:{}us", dur_value);
                    self.cardinality.insert(&dur_str);
                    self.offer_sample(dur_str);
                }
            }
        } else if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationMillisecondType>>()
        {
            for index in 0..dur_array.len() {
                if !dur_array.is_null(index) {
                    let dur_value = dur_array.value(index);
                    self.update_numeric_stats(dur_value as f64);
                    let dur_str = format!("dur:{}ms", dur_value);
                    self.cardinality.insert(&dur_str);
                    self.offer_sample(dur_str);
                }
            }
        } else if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationSecondType>>()
        {
            for index in 0..dur_array.len() {
                if !dur_array.is_null(index) {
                    let dur_value = dur_array.value(index);
                    self.update_numeric_stats(dur_value as f64);
                    let dur_str = format!("dur:{}s", dur_value);
                    self.cardinality.insert(&dur_str);
                    self.offer_sample(dur_str);
                }
            }
        }

        Ok(())
    }

    fn process_generic_array(&mut self, array: &dyn Array) -> Result<()> {
        match ArrayFormatter::try_new(array, &Default::default()) {
            Ok(formatter) => {
                for index in 0..array.len() {
                    if !array.is_null(index) {
                        let value_str = formatter.value(index).to_string();
                        self.update_text_stats(&value_str);
                        self.offer_sample(value_str.clone());
                        self.cardinality.insert_owned(value_str);
                    }
                }
            }
            Err(_) => {
                for index in 0..array.len() {
                    if !array.is_null(index) {
                        let value_str = format!("<{}:value_{}>", array.data_type(), index);
                        self.update_text_stats(&value_str);
                        self.offer_sample(value_str.clone());
                        self.cardinality.insert_owned(value_str);
                    }
                }
            }
        }
        Ok(())
    }

    fn update_numeric_stats(&mut self, value: f64) {
        self.sum += value;
        self.sum_squares += value * value;
        self.numeric_count += 1;

        self.min_value = Some(match self.min_value {
            Some(min_value) => min_value.min(value),
            None => value,
        });

        self.max_value = Some(match self.max_value {
            Some(max_value) => max_value.max(value),
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

    pub fn to_column_profile(
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
            exact_date_matches: Some(self.date_matched_values),
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
            | arrow::datatypes::DataType::Int8
            | arrow::datatypes::DataType::UInt64
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::UInt16
            | arrow::datatypes::DataType::UInt8 => DataType::Integer,
            arrow::datatypes::DataType::Date32
            | arrow::datatypes::DataType::Date64
            | arrow::datatypes::DataType::Timestamp(_, _) => DataType::Date,
            arrow::datatypes::DataType::Decimal128(_, _)
            | arrow::datatypes::DataType::Decimal256(_, _) => DataType::Float,
            arrow::datatypes::DataType::Duration(_) => DataType::Integer,
            arrow::datatypes::DataType::Boolean => DataType::Boolean,
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                infer_type(self.sample_values.samples())
            }
            _ => DataType::String,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use dataprof_core::ColumnStats;
    use std::sync::Arc;

    #[test]
    fn test_nulls_are_excluded_from_numeric_stats() {
        // A null slot still holds a physical value in its buffer -- 0.0 here.
        // Folding that into the statistics silently corrupts min, mean and
        // uniqueness for every nullable numeric column.
        let schema = Arc::new(Schema::new(vec![
            Field::new("score", ArrowDataType::Float64, true),
            Field::new("rank", ArrowDataType::Int64, true),
        ]));

        let scores = Float64Array::from(vec![Some(100.0), None, Some(1.0), None, Some(2.0)]);
        let ranks = Int64Array::from(vec![Some(7), None, Some(9), Some(8), None]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(scores), Arc::new(ranks)]).unwrap();

        let mut analyzer = RecordBatchAnalyzer::new();
        analyzer.process_batch(&batch).unwrap();
        let profiles = analyzer.to_profiles(false, false, None);

        let score_col = profiles.iter().find(|p| p.name == "score").unwrap();
        assert_eq!(score_col.null_count, 2);
        assert_eq!(score_col.unique_count, Some(3));
        match &score_col.stats {
            ColumnStats::Numeric(stats) => {
                assert!((stats.min - 1.0).abs() < 1e-9, "min was {}", stats.min);
                assert!((stats.max - 100.0).abs() < 1e-9, "max was {}", stats.max);
                // (100 + 1 + 2) / 3, not (100 + 0 + 1 + 0 + 2) / 5
                assert!(
                    (stats.mean - 34.333_333).abs() < 1e-5,
                    "mean was {}",
                    stats.mean
                );
            }
            _ => panic!("Expected Numeric stats for score column"),
        }

        let rank_col = profiles.iter().find(|p| p.name == "rank").unwrap();
        assert_eq!(rank_col.null_count, 2);
        match &rank_col.stats {
            ColumnStats::Numeric(stats) => {
                assert!((stats.min - 7.0).abs() < 1e-9, "min was {}", stats.min);
                // (7 + 9 + 8) / 3, not (7 + 0 + 9 + 8 + 0) / 5
                assert!((stats.mean - 8.0).abs() < 1e-9, "mean was {}", stats.mean);
            }
            _ => panic!("Expected Numeric stats for rank column"),
        }
    }

    #[test]
    fn test_all_null_column_profiles_as_all_nulls() {
        // A NullArray has no validity buffer: physically null_count() is 0 and
        // is_null() is false everywhere. Profiled naively, an all-null column
        // reports zero nulls and one fabricated unique value.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "all_null",
            ArrowDataType::Null,
            true,
        )]));
        let nulls = arrow::array::NullArray::new(5);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(nulls)]).unwrap();

        let mut analyzer = RecordBatchAnalyzer::new();
        analyzer.process_batch(&batch).unwrap();
        let profiles = analyzer.to_profiles(false, false, None);

        let col = profiles.iter().find(|p| p.name == "all_null").unwrap();
        assert_eq!(col.total_count, 5);
        assert_eq!(col.null_count, 5);
        assert_eq!(col.unique_count, Some(0));
    }

    #[test]
    fn test_record_batch_analyzer_numeric_stats() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("score", ArrowDataType::Float64, false),
            Field::new("rank", ArrowDataType::Int64, false),
            Field::new("label", ArrowDataType::Utf8, false),
        ]));

        let scores = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]);
        let ranks = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let labels = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(scores), Arc::new(ranks), Arc::new(labels)],
        )
        .unwrap();

        let mut analyzer = RecordBatchAnalyzer::new();
        analyzer.process_batch(&batch).unwrap();

        let profiles = analyzer.to_profiles(false, false, None);
        assert_eq!(profiles.len(), 3);

        let score_col = profiles
            .iter()
            .find(|profile| profile.name == "score")
            .unwrap();
        match &score_col.stats {
            ColumnStats::Numeric(stats) => {
                assert!((stats.min - 10.0).abs() < 0.01);
                assert!((stats.max - 80.0).abs() < 0.01);
                assert!((stats.mean - 45.0).abs() < 0.01);
                assert!(stats.std_dev > 0.0);
                assert!(stats.median.is_some());
                assert!(stats.skewness.is_some());
                assert!(stats.kurtosis.is_some());
                assert!(stats.coefficient_of_variation.is_some());
                assert!(stats.skewness.unwrap().abs() < 0.5);
            }
            _ => panic!("Expected Numeric stats for score column"),
        }

        let rank_col = profiles
            .iter()
            .find(|profile| profile.name == "rank")
            .unwrap();
        match &rank_col.stats {
            ColumnStats::Numeric(stats) => {
                assert!(stats.skewness.is_some(), "rank should have skewness");
                assert!(stats.kurtosis.is_some(), "rank should have kurtosis");
            }
            _ => panic!("Expected Numeric stats for rank column"),
        }

        let label_col = profiles
            .iter()
            .find(|profile| profile.name == "label")
            .unwrap();
        match &label_col.stats {
            ColumnStats::Text(..) => {}
            _ => panic!("Expected Text stats for label column"),
        }
    }

    fn unique_count_for_distinct_ints(distinct: i64) -> usize {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let ids = Int64Array::from((0..distinct).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap();

        let mut analyzer = RecordBatchAnalyzer::new();
        analyzer.process_batch(&batch).unwrap();
        let profiles = analyzer.to_profiles(false, false, None);
        profiles
            .iter()
            .find(|p| p.name == "id")
            .unwrap()
            .unique_count
            .unwrap()
    }

    #[test]
    fn test_unique_count_is_not_capped_at_1000() {
        // Regression for the columnar hard cap: a high-cardinality column used to
        // report exactly 1,000 distinct values, corrupting quality scores. Small
        // columns must stay exact; large ones must estimate near the truth and
        // never freeze at the old cap.
        assert_eq!(unique_count_for_distinct_ints(999), 999);
        assert_eq!(unique_count_for_distinct_ints(1_000), 1_000);
        assert_eq!(unique_count_for_distinct_ints(1_001), 1_001);
        assert_eq!(unique_count_for_distinct_ints(10_000), 10_000);

        let large = unique_count_for_distinct_ints(100_000);
        assert_ne!(large, 1_000, "must not expose the old hard cap as exact");
        let error = (large as f64 - 100_000.0).abs() / 100_000.0;
        assert!(error < 0.05, "100k distinct estimated as {large}");
    }
}
