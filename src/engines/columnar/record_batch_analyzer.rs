//! Shared utilities for analyzing Apache Arrow RecordBatches
//!
//! This module provides reusable components for processing Arrow data
//! from both CSV (via arrow::csv) and Parquet files.

use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

use crate::types::{ColumnProfile, ColumnStats, DataType};

// Additional Arrow utilities for display formatting
use arrow::util::display::ArrayFormatter;

/// Analyzer for processing multiple RecordBatches and building column profiles
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

    /// Process a RecordBatch, updating statistics for all columns
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.total_rows += batch.num_rows();

        // Process each column in the batch
        for (col_idx, column) in batch.columns().iter().enumerate() {
            let schema = batch.schema();
            let field = schema.field(col_idx);
            let column_name = field.name().to_string();

            let analyzer = self
                .column_analyzers
                .entry(column_name)
                .or_insert_with(|| ColumnAnalyzer::new(field.data_type()));

            analyzer.process_array(column)?;
        }

        Ok(())
    }

    /// Get the total number of rows processed
    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Convert the analyzers to column profiles
    pub fn to_profiles(&self) -> Vec<ColumnProfile> {
        let mut profiles = Vec::new();
        for (name, analyzer) in &self.column_analyzers {
            let profile = analyzer.to_column_profile(name.clone());
            profiles.push(profile);
        }
        profiles
    }

    /// Create empty sample columns for quality metrics calculation
    pub fn create_sample_columns(&self) -> HashMap<String, Vec<String>> {
        let mut samples = HashMap::new();
        for name in self.column_analyzers.keys() {
            samples.insert(name.clone(), Vec::new());
        }
        samples
    }
}

impl Default for RecordBatchAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Column analyzer for Arrow arrays - processes statistics incrementally
pub struct ColumnAnalyzer {
    data_type: arrow::datatypes::DataType,
    total_count: usize,
    null_count: usize,
    unique_values: std::collections::HashSet<String>,
    // Numeric statistics
    min_value: Option<f64>,
    max_value: Option<f64>,
    sum: f64,
    sum_squares: f64,
    // Text statistics
    min_length: usize,
    max_length: usize,
    total_length: usize,
    // Sample values for pattern detection
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
            sample_values: Vec::new(),
        }
    }

    pub fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        self.total_count += array.len();
        self.null_count += array.null_count();

        match array.data_type() {
            // Floating point types
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
            // Signed integer types
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
            // Unsigned integer types
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
            // String types
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
            // Boolean type
            arrow::datatypes::DataType::Boolean => {
                if let Some(bool_array) = array.as_any().downcast_ref::<BooleanArray>() {
                    self.process_boolean_array(bool_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to BooleanArray"));
                }
            }
            // Date and time types
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
            // Binary types
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
            // Decimal types
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
            // Duration types
            arrow::datatypes::DataType::Duration(_) => {
                self.process_duration_array(array)?;
            }
            // Complex types - use fallback with improved extraction
            _ => {
                // For other types, use improved generic handling
                self.process_generic_array(array)?;
            }
        }

        Ok(())
    }

    fn process_float64_array(&mut self, array: &Float64Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                self.update_numeric_stats(value);

                // Add to unique values (with limit)
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                // Keep samples for pattern detection
                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_float32_array(&mut self, array: &Float32Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_int64_array(&mut self, array: &Int64Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_int32_array(&mut self, array: &Int32Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_int16_array(&mut self, array: &Int16Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_int8_array(&mut self, array: &Int8Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_uint64_array(&mut self, array: &UInt64Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_uint32_array(&mut self, array: &UInt32Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_uint16_array(&mut self, array: &UInt16Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_uint8_array(&mut self, array: &UInt8Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_string_array(&mut self, array: &StringArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                self.update_text_stats(value);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_large_string_array(&mut self, array: &LargeStringArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                self.update_text_stats(value);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_boolean_array(&mut self, array: &BooleanArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                let value_str = value.to_string();
                self.update_text_stats(&value_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value_str.clone());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value_str);
                }
            }
        }
        Ok(())
    }

    fn process_date32_array(&mut self, array: &Date32Array) -> Result<()> {
        // Date32: days since Unix epoch (1970-01-01)
        for i in 0..array.len() {
            if !array.is_null(i) {
                let days = array.value(i);
                // Convert to approximate year for statistics
                let value_f64 = days as f64;
                self.update_numeric_stats(value_f64);

                // Format as date string for display
                let date_str = format!("1970-01-01+{}days", days);
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(date_str.clone());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(date_str);
                }
            }
        }
        Ok(())
    }

    fn process_date64_array(&mut self, array: &Date64Array) -> Result<()> {
        // Date64: milliseconds since Unix epoch
        for i in 0..array.len() {
            if !array.is_null(i) {
                let millis = array.value(i);
                let value_f64 = millis as f64;
                self.update_numeric_stats(value_f64);

                // Format as datetime string
                let datetime_str = format!("1970-01-01T00:00:00.000+{}ms", millis);
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(datetime_str.clone());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(datetime_str);
                }
            }
        }
        Ok(())
    }

    fn process_timestamp_array(&mut self, array: &dyn Array) -> Result<()> {
        // Generic timestamp processing - extract as i64 and treat as numeric
        use arrow::array::PrimitiveArray;

        // Timestamp arrays are primitive arrays of i64
        if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampNanosecondType>>()
        {
            for i in 0..ts_array.len() {
                if !ts_array.is_null(i) {
                    let ts_value = ts_array.value(i);
                    let value_f64 = ts_value as f64;
                    self.update_numeric_stats(value_f64);

                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(ts_str);
                    }
                }
            }
        } else if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampMicrosecondType>>()
        {
            for i in 0..ts_array.len() {
                if !ts_array.is_null(i) {
                    let ts_value = ts_array.value(i);
                    let value_f64 = ts_value as f64;
                    self.update_numeric_stats(value_f64);

                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(ts_str);
                    }
                }
            }
        } else if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampMillisecondType>>()
        {
            for i in 0..ts_array.len() {
                if !ts_array.is_null(i) {
                    let ts_value = ts_array.value(i);
                    let value_f64 = ts_value as f64;
                    self.update_numeric_stats(value_f64);

                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(ts_str);
                    }
                }
            }
        } else if let Some(ts_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::TimestampSecondType>>()
        {
            for i in 0..ts_array.len() {
                if !ts_array.is_null(i) {
                    let ts_value = ts_array.value(i);
                    let value_f64 = ts_value as f64;
                    self.update_numeric_stats(value_f64);

                    let ts_str = format!("ts:{}", ts_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(ts_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(ts_str);
                    }
                }
            }
        }

        Ok(())
    }

    fn process_binary_array(&mut self, array: &BinaryArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let bytes = array.value(i);
                let len = bytes.len();
                self.update_text_stats(&format!("<binary:{}>", len));

                // For binary, we track length distributions
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(format!("len:{}", len));
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(format!("<binary:{} bytes>", len));
                }
            }
        }
        Ok(())
    }

    fn process_large_binary_array(&mut self, array: &LargeBinaryArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let bytes = array.value(i);
                let len = bytes.len();
                self.update_text_stats(&format!("<binary:{}>", len));

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(format!("len:{}", len));
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(format!("<binary:{} bytes>", len));
                }
            }
        }
        Ok(())
    }

    fn process_decimal128_array(&mut self, array: &Decimal128Array) -> Result<()> {
        // Decimal128: fixed-point decimal with precision and scale
        for i in 0..array.len() {
            if !array.is_null(i) {
                let decimal_value = array.value(i);
                // Convert to f64 for statistics (may lose precision)
                let value_f64 = decimal_value as f64;
                self.update_numeric_stats(value_f64);

                let decimal_str = format!("dec128:{}", decimal_value);
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(decimal_str.clone());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(decimal_str);
                }
            }
        }
        Ok(())
    }

    fn process_decimal256_array(&mut self, array: &Decimal256Array) -> Result<()> {
        // Decimal256: larger fixed-point decimal
        for i in 0..array.len() {
            if !array.is_null(i) {
                // Decimal256 is stored as i256, we'll represent as string
                let decimal_str = format!("dec256:value_{}", i);
                self.update_text_stats(&decimal_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(decimal_str.clone());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(decimal_str);
                }
            }
        }
        Ok(())
    }

    fn process_duration_array(&mut self, array: &dyn Array) -> Result<()> {
        // Duration is stored as i64 with different units (seconds, millis, micros, nanos)
        use arrow::array::PrimitiveArray;

        if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationNanosecondType>>()
        {
            for i in 0..dur_array.len() {
                if !dur_array.is_null(i) {
                    let dur_value = dur_array.value(i);
                    let value_f64 = dur_value as f64;
                    self.update_numeric_stats(value_f64);

                    let dur_str = format!("dur:{}ns", dur_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(dur_str);
                    }
                }
            }
        } else if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationMicrosecondType>>()
        {
            for i in 0..dur_array.len() {
                if !dur_array.is_null(i) {
                    let dur_value = dur_array.value(i);
                    let value_f64 = dur_value as f64;
                    self.update_numeric_stats(value_f64);

                    let dur_str = format!("dur:{}us", dur_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(dur_str);
                    }
                }
            }
        } else if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationMillisecondType>>()
        {
            for i in 0..dur_array.len() {
                if !dur_array.is_null(i) {
                    let dur_value = dur_array.value(i);
                    let value_f64 = dur_value as f64;
                    self.update_numeric_stats(value_f64);

                    let dur_str = format!("dur:{}ms", dur_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(dur_str);
                    }
                }
            }
        } else if let Some(dur_array) = array
            .as_any()
            .downcast_ref::<PrimitiveArray<arrow::datatypes::DurationSecondType>>()
        {
            for i in 0..dur_array.len() {
                if !dur_array.is_null(i) {
                    let dur_value = dur_array.value(i);
                    let value_f64 = dur_value as f64;
                    self.update_numeric_stats(value_f64);

                    let dur_str = format!("dur:{}s", dur_value);
                    if self.unique_values.len() < 1000 {
                        self.unique_values.insert(dur_str.clone());
                    }

                    if self.sample_values.len() < 100 {
                        self.sample_values.push(dur_str);
                    }
                }
            }
        }

        Ok(())
    }

    fn process_generic_array(&mut self, array: &dyn Array) -> Result<()> {
        // Use Arrow's display formatting for unsupported types (List, Struct, Map, etc.)
        // This provides actual values instead of placeholder strings
        match ArrayFormatter::try_new(array, &Default::default()) {
            Ok(formatter) => {
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        // Format the value using Arrow's formatter
                        let value_str = formatter.value(i).to_string();
                        self.update_text_stats(&value_str);

                        if self.sample_values.len() < 100 {
                            self.sample_values.push(value_str.clone());
                        }

                        if self.unique_values.len() < 1000 {
                            self.unique_values.insert(value_str);
                        }
                    }
                }
            }
            Err(_) => {
                // If formatter fails, fall back to generic placeholder
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        let value_str = format!("<{}:value_{}>", array.data_type(), i);
                        self.update_text_stats(&value_str);

                        if self.sample_values.len() < 100 {
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
            Some(min) => min.min(value),
            None => value,
        });

        self.max_value = Some(match self.max_value {
            Some(max) => max.max(value),
            None => value,
        });
    }

    fn update_text_stats(&mut self, value: &str) {
        let len = value.len();
        self.min_length = self.min_length.min(len);
        self.max_length = self.max_length.max(len);
        self.total_length += len;
    }

    pub fn to_column_profile(&self, name: String) -> ColumnProfile {
        let data_type = self.infer_data_type();

        let stats = match data_type {
            DataType::Integer | DataType::Float => ColumnStats::Numeric {
                min: self.min_value.unwrap_or(0.0),
                max: self.max_value.unwrap_or(0.0),
                mean: if self.total_count > self.null_count {
                    self.sum / (self.total_count - self.null_count) as f64
                } else {
                    0.0
                },
            },
            DataType::String | DataType::Date => {
                let avg_length = if self.total_count > self.null_count {
                    self.total_length as f64 / (self.total_count - self.null_count) as f64
                } else {
                    0.0
                };

                ColumnStats::Text {
                    min_length: if self.min_length == usize::MAX {
                        0
                    } else {
                        self.min_length
                    },
                    max_length: self.max_length,
                    avg_length,
                }
            }
        };

        // Detect patterns using sample values
        let patterns = crate::detect_patterns(&self.sample_values);

        ColumnProfile {
            name,
            data_type,
            null_count: self.null_count,
            total_count: self.total_count,
            unique_count: Some(self.unique_values.len()),
            stats,
            patterns,
        }
    }

    fn infer_data_type(&self) -> DataType {
        match &self.data_type {
            // Floating point types
            arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => {
                DataType::Float
            }
            // Integer types (signed and unsigned)
            arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int16
            | arrow::datatypes::DataType::Int8
            | arrow::datatypes::DataType::UInt64
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::UInt16
            | arrow::datatypes::DataType::UInt8 => DataType::Integer,
            // Date and timestamp types - preserve as Date
            arrow::datatypes::DataType::Date32
            | arrow::datatypes::DataType::Date64
            | arrow::datatypes::DataType::Timestamp(_, _) => DataType::Date,
            // Decimal types - treat as Float for now
            arrow::datatypes::DataType::Decimal128(_, _)
            | arrow::datatypes::DataType::Decimal256(_, _) => DataType::Float,
            // Duration - treat as Integer (numeric elapsed time)
            arrow::datatypes::DataType::Duration(_) => DataType::Integer,
            // String types
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                // Check if it looks like dates (for string-encoded dates)
                let sample_size = self.sample_values.len().min(50);
                if sample_size > 0 {
                    let date_like_count = self
                        .sample_values
                        .iter()
                        .take(sample_size)
                        .filter(|s| self.looks_like_date(s))
                        .count();

                    if date_like_count as f64 / sample_size as f64 > 0.7 {
                        DataType::Date
                    } else {
                        DataType::String
                    }
                } else {
                    DataType::String
                }
            }
            // Binary types and complex types - treat as String
            _ => DataType::String,
        }
    }

    fn looks_like_date(&self, value: &str) -> bool {
        use regex::Regex;

        let date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{2}/\d{2}/\d{4}$",
            r"^\d{2}-\d{2}-\d{4}$",
        ];

        date_patterns.iter().any(|pattern| {
            Regex::new(pattern)
                .map(|re| re.is_match(value))
                .unwrap_or(false)
        })
    }
}
