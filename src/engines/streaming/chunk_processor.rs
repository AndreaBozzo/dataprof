//! Chunk processor for streaming CSV analysis
//!
//! Separates chunk processing logic from StreamingProfiler (God Object refactoring)

use csv::StringRecord;
use std::collections::HashMap;

use crate::core::sampling::SamplingStrategy;

/// Statistics about chunk processing
#[derive(Debug, Clone, Default)]
pub struct ProcessingStats {
    pub total_rows_processed: usize,
    pub total_chunks: usize,
    pub bytes_processed: u64,
}

/// Handles chunked processing of CSV records
pub struct ChunkProcessor {
    sampling_strategy: SamplingStrategy,
    chunk_size: usize,
    current_chunk: HashMap<String, Vec<String>>,
    rows_in_current_chunk: usize,
    stats: ProcessingStats,
}

impl ChunkProcessor {
    pub fn new(chunk_size: usize, sampling_strategy: SamplingStrategy) -> Self {
        Self {
            sampling_strategy,
            chunk_size,
            current_chunk: HashMap::new(),
            rows_in_current_chunk: 0,
            stats: ProcessingStats::default(),
        }
    }

    /// Initialize chunk storage with headers
    pub fn initialize_headers(&mut self, headers: &[String]) {
        self.current_chunk.clear();
        for header in headers {
            self.current_chunk.insert(header.clone(), Vec::new());
        }
    }

    /// Process a single record, returns true if chunk is full
    pub fn process_record(
        &mut self,
        record: &StringRecord,
        headers: &[String],
        row_index: usize,
    ) -> bool {
        // Apply sampling strategy
        if !self
            .sampling_strategy
            .should_include(row_index, self.stats.total_rows_processed + 1)
        {
            return false;
        }

        // Add record to current chunk
        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                if let Some(column_data) = self.current_chunk.get_mut(header) {
                    column_data.push(field.to_string());
                }
            }
        }

        self.rows_in_current_chunk += 1;
        self.stats.total_rows_processed += 1;

        // Check if chunk is full
        self.rows_in_current_chunk >= self.chunk_size
    }

    /// Flush current chunk to aggregated data
    pub fn flush_chunk(&mut self, all_data: &mut HashMap<String, Vec<String>>) {
        if self.rows_in_current_chunk == 0 {
            return;
        }

        // Append chunk data to all_data
        for (column_name, chunk_values) in &self.current_chunk {
            if let Some(all_values) = all_data.get_mut(column_name) {
                all_values.extend(chunk_values.iter().cloned());
            }
        }

        // Update stats
        self.stats.total_chunks += 1;
        self.stats.bytes_processed +=
            (self.rows_in_current_chunk * self.current_chunk.len() * 50) as u64;

        // Clear chunk
        for values in self.current_chunk.values_mut() {
            values.clear();
        }
        self.rows_in_current_chunk = 0;
    }

    /// Get current processing statistics
    pub fn stats(&self) -> &ProcessingStats {
        &self.stats
    }

    /// Get rows in current chunk
    pub fn rows_in_current_chunk(&self) -> usize {
        self.rows_in_current_chunk
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_processor_basic() {
        let mut processor = ChunkProcessor::new(2, SamplingStrategy::None);
        let headers = vec!["col1".to_string(), "col2".to_string()];

        processor.initialize_headers(&headers);

        // Process first record
        let record = StringRecord::from(vec!["a", "b"]);
        assert!(!processor.process_record(&record, &headers, 0));
        assert_eq!(processor.rows_in_current_chunk(), 1);

        // Process second record - chunk should be full
        let record = StringRecord::from(vec!["c", "d"]);
        assert!(processor.process_record(&record, &headers, 1));
        assert_eq!(processor.rows_in_current_chunk(), 2);
    }

    #[test]
    fn test_flush_chunk() {
        let mut processor = ChunkProcessor::new(2, SamplingStrategy::None);
        let headers = vec!["col1".to_string()];
        processor.initialize_headers(&headers);

        let record = StringRecord::from(vec!["value"]);
        processor.process_record(&record, &headers, 0);

        let mut all_data = HashMap::new();
        all_data.insert("col1".to_string(), Vec::new());

        processor.flush_chunk(&mut all_data);

        assert_eq!(all_data.get("col1").unwrap().len(), 1);
        assert_eq!(processor.rows_in_current_chunk(), 0);
        assert_eq!(processor.stats().total_chunks, 1);
    }
}
