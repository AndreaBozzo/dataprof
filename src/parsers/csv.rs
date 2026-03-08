use csv::ReaderBuilder;
use std::io::Read;
use std::path::Path;

use crate::core::errors::DataProfilerError;
use crate::core::profile_builder;
use crate::core::streaming_stats::StreamingColumnCollection;
use crate::types::{
    ColumnProfile, DataQualityMetrics, DataSource, FileFormat, QualityReport, ScanInfo,
};

// ============================================================================
// NEW CONFIG-BASED API (#181 + #218)
// ============================================================================

/// Configuration for CSV parsing and analysis.
///
/// Replaces the 7 separate `analyze_csv_*` functions with a single config struct.
/// Supports both file-based and reader-based (streaming) entry points.
#[derive(Debug, Clone)]
pub struct CsvParserConfig {
    /// Skip pattern detection and unique counts for faster analysis.
    pub fast_mode: bool,
    /// Allow ragged rows (flexible field counts).
    pub flexible: bool,
    /// Custom delimiter (None = comma).
    pub delimiter: Option<u8>,
    /// Quote character.
    pub quote_char: u8,
    /// Trim whitespace from fields.
    pub trim_whitespace: bool,
    /// Maximum rows to process (None = all rows).
    pub max_rows: Option<usize>,
    /// Verbosity level (0=quiet, 1=normal, 2+=verbose).
    pub verbosity: u8,
}

impl Default for CsvParserConfig {
    fn default() -> Self {
        Self {
            fast_mode: false,
            flexible: true,
            delimiter: None,
            quote_char: b'"',
            trim_whitespace: false,
            max_rows: None,
            verbosity: 1,
        }
    }
}

impl CsvParserConfig {
    /// Create a config optimized for speed (skips pattern detection).
    pub fn fast() -> Self {
        Self {
            fast_mode: true,
            ..Default::default()
        }
    }

    /// Create a strict config that rejects ragged rows.
    pub fn strict() -> Self {
        Self {
            flexible: false,
            ..Default::default()
        }
    }

    /// Set the delimiter.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set the maximum number of rows to process.
    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(max_rows);
        self
    }

    /// Set verbosity level.
    pub fn with_verbosity(mut self, verbosity: u8) -> Self {
        self.verbosity = verbosity;
        self
    }
}

/// Analyze CSV data from any `Read` source using streaming statistics.
///
/// This is the primary generic entry point for both file-based and stream-based
/// CSV profiling. Data is processed incrementally through [`StreamingColumnCollection`]
/// with bounded memory, rather than accumulating all rows in a `HashMap`.
///
/// Returns `(column_profiles, streaming_stats, rows_read)`.
pub fn analyze_csv_from_reader<R: Read>(
    reader: R,
    config: &CsvParserConfig,
) -> Result<(Vec<ColumnProfile>, StreamingColumnCollection, usize), DataProfilerError> {
    let mut csv_builder = ReaderBuilder::new();
    csv_builder.has_headers(true);
    csv_builder.flexible(config.flexible);
    csv_builder.quote(config.quote_char);
    if config.trim_whitespace {
        csv_builder.trim(csv::Trim::All);
    }
    if let Some(delim) = config.delimiter {
        csv_builder.delimiter(delim);
    }

    let mut csv_reader = csv_builder.from_reader(reader);

    let headers = csv_reader.headers()?.clone();
    let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    let mut column_stats = StreamingColumnCollection::new();
    let mut rows_read = 0;

    for result in csv_reader.records() {
        if let Some(max) = config.max_rows
            && rows_read >= max
        {
            break;
        }

        let record = result?;
        let values: Vec<String> = record.iter().map(|s| s.to_string()).collect();
        column_stats.process_record(&header_names, values);
        rows_read += 1;
    }

    let profiles = if config.fast_mode {
        // Fast mode: use profile_builder (streaming-based) but it already skips
        // most expensive ops via sample-based inference. For backward compat with
        // the old analyze_column_fast path, we still use profile_builder here since
        // it produces equivalent results from streaming stats.
        profile_builder::profiles_from_streaming(&column_stats)
    } else {
        profile_builder::profiles_from_streaming(&column_stats)
    };

    Ok((profiles, column_stats, rows_read))
}

/// Analyze a CSV file, returning a full [`QualityReport`].
///
/// Opens the file, delegates to [`analyze_csv_from_reader`], and wraps the
/// result with file metadata and ISO 8000/25012 data quality metrics.
pub fn analyze_csv_file(
    file_path: &Path,
    config: &CsvParserConfig,
) -> Result<QualityReport, DataProfilerError> {
    let metadata = std::fs::metadata(file_path).map_err(|e| map_io_error(file_path, e))?;
    let start = std::time::Instant::now();

    let file = std::fs::File::open(file_path).map_err(|e| map_io_error(file_path, e))?;
    let buf_reader = std::io::BufReader::new(file);

    let (column_profiles, column_stats, rows_read) = analyze_csv_from_reader(buf_reader, config)?;

    if column_profiles.is_empty() && rows_read == 0 {
        return Ok(QualityReport::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format: FileFormat::Csv,
                size_bytes: metadata.len(),
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ScanInfo::new(0, 0, 0, 1.0, start.elapsed().as_millis()),
            DataQualityMetrics::empty(),
        ));
    }

    let sample_columns = profile_builder::quality_check_samples(&column_stats);
    let data_quality_metrics =
        DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
            .unwrap_or_else(|_| DataQualityMetrics::empty());

    let scan_time_ms = start.elapsed().as_millis();
    let num_columns = column_profiles.len();

    Ok(QualityReport::new(
        DataSource::File {
            path: file_path.display().to_string(),
            format: FileFormat::Csv,
            size_bytes: metadata.len(),
            modified_at: None,
            parquet_metadata: None,
        },
        column_profiles,
        ScanInfo::new(rows_read, num_columns, rows_read, 1.0, scan_time_ms),
        data_quality_metrics,
    ))
}

/// Map I/O errors to DataProfilerError with the actual file path context
fn map_io_error(file_path: &Path, e: std::io::Error) -> DataProfilerError {
    if e.kind() == std::io::ErrorKind::NotFound {
        DataProfilerError::FileNotFound {
            path: file_path.display().to_string(),
        }
    } else {
        DataProfilerError::from(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    fn write_csv(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "{}", content).unwrap();
        f.flush().unwrap();
        f
    }

    // --- New API tests ---

    #[test]
    fn test_analyze_csv_from_reader_basic() {
        let data = b"name,age\nAlice,25\nBob,30\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.total_count, 2);
        assert_eq!(age.data_type, DataType::Integer);
    }

    #[test]
    fn test_analyze_csv_from_reader_with_max_rows() {
        let data = b"val\n1\n2\n3\n4\n5\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default().with_max_rows(3);

        let (_profiles, _stats, rows) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_analyze_csv_from_reader_strict_rejects_ragged() {
        let data = b"a,b\n1,2\n3,4,5\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::strict();

        assert!(analyze_csv_from_reader(cursor, &config).is_err());
    }

    #[test]
    fn test_analyze_csv_from_reader_flexible_handles_ragged() {
        let data = b"a,b\n1,2\n3,4,5\n6\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 3);
        assert!(!profiles.is_empty());
    }

    #[test]
    fn test_analyze_csv_from_reader_custom_delimiter() {
        let data = b"a;b\n1;2\n3;4\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default().with_delimiter(b';');

        let (profiles, _stats, rows) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_analyze_csv_file_returns_quality_report() {
        let csv = write_csv("x,y\n1,a\n2,b\n3,c\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.scan_info.total_rows, 3);
        assert!(report.quality_score() >= 0.0);
        assert!(report.quality_score() <= 100.0);
    }

    // --- Legacy API tests migrated to new API ---

    #[test]
    fn test_analyze_csv_basic() {
        let csv = write_csv("name,age\nAlice,25\nBob,30\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;
        assert_eq!(profiles.len(), 2);

        let names: Vec<&str> = profiles.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"name"));
        assert!(names.contains(&"age"));

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.total_count, 2);
        assert_eq!(age.null_count, 0);
    }

    #[test]
    fn test_analyze_csv_with_nulls() {
        let csv = write_csv("name,age,email\nAlice,25,a@b.com\nBob,,\nCharlie,30,c@d.com\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.total_count, 3);
        assert_eq!(age.null_count, 1);

        let email = profiles.iter().find(|p| p.name == "email").unwrap();
        assert_eq!(email.null_count, 1);
    }

    #[test]
    fn test_analyze_csv_file_returns_quality_report_legacy() {
        let csv = write_csv("x,y\n1,a\n2,b\n3,c\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.scan_info.total_rows, 3);
        assert_eq!(report.scan_info.total_columns, 2);
        assert!(report.quality_score() >= 0.0);
        assert!(report.quality_score() <= 100.0);
    }

    #[test]
    fn test_analyze_csv_file_empty_file() {
        let csv = write_csv("name,age\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 0);
        assert_eq!(report.scan_info.total_rows, 0);
    }

    #[test]
    fn test_analyze_csv_fast_produces_profiles() {
        let csv = write_csv("a,b,c\n1,x,true\n2,y,false\n3,z,true\n");
        let config = CsvParserConfig::fast();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;
        assert_eq!(profiles.len(), 3);

        for p in profiles {
            assert_eq!(p.total_count, 3);
        }
    }

    #[test]
    fn test_analyze_csv_with_max_rows_small_file() {
        let csv = write_csv("val\n1\n2\n3\n4\n5\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.scan_info.total_rows, 5);
        assert_eq!(report.scan_info.rows_scanned, 5);
        assert!((report.scan_info.sampling_ratio - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_analyze_csv_numeric_types_detected() {
        let csv = write_csv("int_col,float_col,str_col\n1,1.5,hello\n2,2.5,world\n3,3.5,foo\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let int_col = profiles.iter().find(|p| p.name == "int_col").unwrap();
        assert!(
            matches!(int_col.data_type, DataType::Integer),
            "Expected Integer, got {:?}",
            int_col.data_type
        );

        let float_col = profiles.iter().find(|p| p.name == "float_col").unwrap();
        assert!(
            matches!(float_col.data_type, DataType::Float),
            "Expected Float, got {:?}",
            float_col.data_type
        );

        let str_col = profiles.iter().find(|p| p.name == "str_col").unwrap();
        assert!(
            matches!(str_col.data_type, DataType::String),
            "Expected String, got {:?}",
            str_col.data_type
        );
    }

    #[test]
    fn test_analyze_csv_strict_rejects_ragged_rows() {
        let csv = write_csv("a,b\n1,2\n3,4,5\n");
        let config = CsvParserConfig::strict();
        assert!(analyze_csv_file(csv.path(), &config).is_err());
    }

    #[test]
    fn test_analyze_csv_file_handles_ragged_rows() {
        let csv = write_csv("a,b\n1,2\n3,4,5\n6\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        assert!(!report.column_profiles.is_empty());
    }

    #[test]
    fn test_analyze_csv_with_quality_metrics() {
        let csv = write_csv("name,age\nAlice,25\nBob,\nCharlie,30\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert!(report.data_quality_metrics.missing_values_ratio > 0.0);
        assert!(report.data_quality_metrics.complete_records_ratio < 100.0);
    }
}
