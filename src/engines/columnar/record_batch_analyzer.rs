//! Shared utilities for analyzing Apache Arrow RecordBatches
//!
//! This module provides reusable components for processing Arrow data
//! from both CSV (via arrow::csv) and Parquet files.

use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

use crate::types::{ColumnProfile, ColumnStats, DataType};

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
            _ => {
                // For other types, convert to string and process
                self.process_as_string_array(array)?;
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

    fn process_as_string_array(&mut self, array: &dyn Array) -> Result<()> {
        // Convert any array type to string for processing
        for i in 0..array.len() {
            if !array.is_null(i) {
                // This is a simplified approach - in practice we'd need more sophisticated conversion
                let value = format!("value_{}", i); // Placeholder
                self.update_text_stats(&value);

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.clone());
                }

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value);
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
            arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => {
                DataType::Float
            }
            arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int16
            | arrow::datatypes::DataType::Int8 => DataType::Integer,
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                // Check if it looks like dates
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
