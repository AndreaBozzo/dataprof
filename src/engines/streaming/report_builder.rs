//! Report builder for streaming analysis
//!
//! Separates report building logic from StreamingProfiler (God Object refactoring)

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::analysis::analyze_column;
use crate::engines::streaming::chunk_processor::ProcessingStats;
use crate::types::{ColumnProfile, FileInfo, QualityReport, ScanInfo};
use crate::QualityChecker;

/// Builds quality reports from processed data
pub struct ReportBuilder {
    file_path: String,
    file_size_mb: f64,
    start_time: Instant,
    estimated_total_rows: Option<usize>,
}

impl ReportBuilder {
    pub fn new(file_path: &Path, file_size_mb: f64, estimated_total_rows: Option<usize>) -> Self {
        Self {
            file_path: file_path.display().to_string(),
            file_size_mb,
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

        // Check quality issues
        let issues = QualityChecker::check_columns(&column_profiles, &column_data);

        // Calculate comprehensive ISO 8000/25012 quality metrics
        let quality_metrics =
            crate::types::DataQualityMetrics::calculate_from_data(&column_data, &column_profiles)
                .ok(); // Convert Result to Option, ignore errors for now

        // Calculate metrics
        let scan_time_ms = self.start_time.elapsed().as_millis();
        let sampling_ratio = self.calculate_sampling_ratio(stats.total_rows_processed);

        Ok(QualityReport {
            file_info: FileInfo {
                path: self.file_path.clone(),
                total_rows: Some(stats.total_rows_processed),
                total_columns: column_profiles.len(),
                file_size_mb: self.file_size_mb,
            },
            column_profiles,
            issues,
            scan_info: ScanInfo {
                rows_scanned: stats.total_rows_processed,
                sampling_ratio,
                scan_time_ms,
            },
            data_quality_metrics: quality_metrics,
        })
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

    /// Calculate sampling ratio
    fn calculate_sampling_ratio(&self, rows_processed: usize) -> f64 {
        if let Some(total) = self.estimated_total_rows {
            if total > 0 {
                return rows_processed as f64 / total as f64;
            }
        }
        1.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_report_builder() {
        let path = PathBuf::from("test.csv");
        let builder = ReportBuilder::new(&path, 1.0, Some(100));

        let mut column_data = HashMap::new();
        column_data.insert("col1".to_string(), vec!["1".to_string(), "2".to_string()]);

        let stats = ProcessingStats {
            total_rows_processed: 2,
            total_chunks: 1,
            bytes_processed: 100,
        };

        let report = builder.build(column_data, &stats).unwrap();

        assert_eq!(report.file_info.total_rows, Some(2));
        assert_eq!(report.file_info.total_columns, 1);
        assert_eq!(report.scan_info.rows_scanned, 2);
    }
}
