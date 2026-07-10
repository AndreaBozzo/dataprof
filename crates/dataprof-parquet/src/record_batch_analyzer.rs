//! Shared utilities for analyzing Apache Arrow RecordBatches.

use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use dataprof_core::{ColumnProfile, DataType, SemanticHints};
use dataprof_metrics::analysis::inference::is_null_like_token;
use dataprof_metrics::infer_type;
use dataprof_runtime::{ColumnProfileInput, TextLengths, build_column_profile};
use std::collections::HashMap;

const NUMERIC_SAMPLE_CAP: usize = 10_000;

/// Analyzer for processing multiple RecordBatches and building column profiles.
pub struct RecordBatchAnalyzer {
    column_analyzers: HashMap<String, ColumnAnalyzer>,
    total_rows: usize,
}

impl RecordBatchAnalyzer {
    pub fn new() -> Self {
        Self {
            column_analyzers: HashMap::new(),
            total_rows: 0,
        }
    }

    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.total_rows += batch.num_rows();

        for (column_index, column) in batch.columns().iter().enumerate() {
            let schema = batch.schema();
            let field = schema.field(column_index);
            let column_name = field.name().to_string();

            let analyzer = self
                .column_analyzers
                .entry(column_name)
                .or_insert_with(|| ColumnAnalyzer::new(field.data_type()));

            analyzer.process_array(column)?;
        }

        Ok(())
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
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
            samples.insert(name.clone(), analyzer.sample_values.clone());
        }
        samples
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
    unique_values: std::collections::HashSet<String>,
    min_value: Option<f64>,
    max_value: Option<f64>,
    sum: f64,
    sum_squares: f64,
    min_length: usize,
    max_length: usize,
    total_length: usize,
    true_count: usize,
    false_count: usize,
    sample_values: Vec<String>,
}

impl ColumnAnalyzer {
    pub fn new(data_type: &arrow::datatypes::DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            total_count: 0,
            null_count: 0,
            unique_values: std::collections::HashSet::new(),
            min_value: None,
            max_value: None,
            sum: 0.0,
            sum_squares: 0.0,
            min_length: usize::MAX,
            max_length: 0,
            total_length: 0,
            true_count: 0,
            false_count: 0,
            sample_values: Vec::new(),
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value_str.to_string());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value_str.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_date32_array(&mut self, array: &Date32Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let days = array.value(index);
                self.update_numeric_stats(days as f64);
                let date_str = format!("1970-01-01+{}days", days);
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(date_str.clone());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(date_str);
                }
            }
        }
        Ok(())
    }

    fn process_date64_array(&mut self, array: &Date64Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let millis = array.value(index);
                self.update_numeric_stats(millis as f64);
                let datetime_str = format!("1970-01-01T00:00:00.000+{}ms", millis);
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(datetime_str.clone());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(datetime_str);
                }
            }
        }
        Ok(())
    }

    fn process_timestamp_array(&mut self, array: &dyn Array) -> Result<()> {
        use arrow::array::PrimitiveArray;

        if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampNanosecondType>>()
        {
            for index in 0..ts_array.len() {
                if !ts_array.is_null(index) {
                    let ts_value = ts_array.value(index);
                    self.update_numeric_stats(ts_value as f64);
                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(ts_str);
                    }
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
                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(ts_str);
                    }
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
                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(ts_str);
                    }
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
                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(ts_str);
                    }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(format!("len:{}", len));
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(format!("<binary:{} bytes>", len));
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(format!("len:{}", len));
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(format!("<binary:{} bytes>", len));
                }
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
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(decimal_str.clone());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(decimal_str);
                }
            }
        }
        Ok(())
    }

    fn process_decimal256_array(&mut self, array: &Decimal256Array) -> Result<()> {
        for index in 0..array.len() {
            if !array.is_null(index) {
                let decimal_str = format!("dec256:value_{}", index);
                self.update_text_stats(&decimal_str);
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(decimal_str.clone());
                }
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(decimal_str);
                }
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
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(dur_str);
                    }
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
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(dur_str);
                    }
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
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(dur_str);
                    }
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
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }
                    if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                        self.sample_values.push(dur_str);
                    }
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
                        if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                            self.sample_values.push(value_str.clone());
                        }
                        if self.unique_values.len() < 1000 {
                            self.unique_values.insert(value_str);
                        }
                    }
                }
            }
            Err(_) => {
                for index in 0..array.len() {
                    if !array.is_null(index) {
                        let value_str = format!("<{}:value_{}>", array.data_type(), index);
                        self.update_text_stats(&value_str);
                        if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                            self.sample_values.push(value_str.clone());
                        }
                        if self.unique_values.len() < 1000 {
                            self.unique_values.insert(value_str);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn update_numeric_stats(&mut self, value: f64) {
        self.sum += value;
        self.sum_squares += value * value;

        self.min_value = Some(match self.min_value {
            Some(min_value) => min_value.min(value),
            None => value,
        });

        self.max_value = Some(match self.max_value {
            Some(max_value) => max_value.max(value),
            None => value,
        });
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
            unique_count: Some(self.unique_values.len()),
            sample_values: &self.sample_values,
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
                infer_type(&self.sample_values)
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
}
