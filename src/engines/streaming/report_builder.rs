//! Report builder for streaming analysis
//!
//! Separates report building logic from StreamingProfiler (God Object refactoring)

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::analysis::analyze_column;
use crate::engines::streaming::chunk_processor::ProcessingStats;
use crate::types::{
    ColumnProfile, DataQualityMetrics, DataSource, ExecutionMetadata, FileFormat, QualityReport,
};

/// Builds quality reports from processed data
pub struct ReportBuilder {
    file_path: String,
    file_size_bytes: u64,
    start_time: Instant,
    estimated_total_rows: Option<usize>,
}

impl ReportBuilder {
    pub fn new(file_path: &Path, file_size_mb: f64, estimated_total_rows: Option<usize>) -> Self {
        Self {
            file_path: file_path.display().to_string(),
            file_size_bytes: (file_size_mb * 1_048_576.0) as u64,
            start_time: Instant::now(),
            estimated_total_rows,
        }
    }

    /// Build a complete quality report from column data
    pub fn build(
        &self,
        column_data: HashMap<String, Vec<String>>,
        stats: &ProcessingStats,
    ) -> Result<QualityReport> {
        // Analyze columns
        let column_profiles = self.analyze_columns(&column_data);

        // Calculate comprehensive ISO 8000/25012 quality metrics
        let data_quality_metrics =
            DataQualityMetrics::calculate_from_data(&column_data, &column_profiles)
                .unwrap_or_else(|_| DataQualityMetrics::empty());

        // Calculate metrics
        let scan_time_ms = self.start_time.elapsed().as_millis();
        let num_columns = column_profiles.len();

        let mut execution =
            ExecutionMetadata::new(stats.total_rows_processed, num_columns, scan_time_ms)
                .with_bytes_consumed(stats.bytes_processed);

        // If we have an estimated total and processed fewer rows, sampling was applied
        if let Some(total) = self.estimated_total_rows
            && total > 0
            && stats.total_rows_processed < total
        {
            let ratio = stats.total_rows_processed as f64 / total as f64;
            execution = execution.with_sampling(ratio);
        }

        Ok(QualityReport::new(
            DataSource::File {
                path: self.file_path.clone(),
                format: FileFormat::Csv,
                size_bytes: self.file_size_bytes,
                modified_at: None,
                parquet_metadata: None,
            },
            column_profiles,
            execution,
            data_quality_metrics,
        ))
    }

    /// Analyze all columns and return profiles
    fn analyze_columns(&self, column_data: &HashMap<String, Vec<String>>) -> Vec<ColumnProfile> {
        let mut profiles = Vec::new();
        for (name, data) in column_data {
            let profile = analyze_column(name, data);
            profiles.push(profile);
        }
        profiles
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_report_builder() {
        let path = PathBuf::from("test.csv");
        // estimated_total_rows = None means total_rows will be set from stats.total_rows_processed
        let builder = ReportBuilder::new(&path, 1.0, None);

        let mut column_data = HashMap::new();
        column_data.insert("col1".to_string(), vec!["1".to_string(), "2".to_string()]);

        let stats = ProcessingStats {
            total_rows_processed: 2,
            total_chunks: 1,
            bytes_processed: 100,
        };

        let report = builder.build(column_data, &stats).unwrap();

        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.columns_detected, 1);
        assert_eq!(report.execution.bytes_consumed, Some(100));
        assert!(report.execution.source_exhausted);
        assert!(!report.execution.sampling_applied);
    }
}
