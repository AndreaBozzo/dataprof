use csv::ReaderBuilder;
use std::io::Read;
use std::path::Path;

use crate::core::errors::DataProfilerError;
use crate::core::profile_builder;
use crate::core::streaming_stats::StreamingColumnCollection;
use crate::types::{
    ColumnProfile, DataQualityMetrics, DataSource, ExecutionMetadata, FileFormat, QualityReport,
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
}

impl Default for CsvParserConfig {
    fn default() -> Self {
        Self {
            flexible: true,
            delimiter: None,
            quote_char: b'"',
            trim_whitespace: false,
            max_rows: None,
        }
    }
}

impl CsvParserConfig {
    /// Create a config optimized for speed (skips pattern detection).
    #[deprecated(
        note = "fast() is currently equivalent to default(). Feature will be removed or refactored."
    )]
    pub fn fast() -> Self {
        Self {
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

    /// Set maximum number of rows to evaluate.
    pub fn max_rows(mut self, max: Option<usize>) -> Self {
        self.max_rows = max;
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
) -> Result<
    (
        Vec<ColumnProfile>,
        StreamingColumnCollection,
        usize,
        Vec<String>,
    ),
    DataProfilerError,
> {
    let mut csv_builder = ReaderBuilder::new();
    csv_builder.has_headers(true);
    csv_builder.flexible(config.flexible);
    csv_builder.quote(config.quote_char);
    if config.trim_whitespace {
        csv_builder.trim(csv::Trim::All);
    }
    let mut actual_delimiter = config.delimiter;

    let boxed_reader: Box<dyn std::io::Read + '_> = if config.delimiter.is_none() {
        let mut preamble = Vec::new();
        let mut take = reader.take(4096);
        take.read_to_end(&mut preamble)?;

        let preamble_str = String::from_utf8_lossy(&preamble);
        let lines: Vec<String> = preamble_str.lines().map(|s| s.to_string()).collect();

        let delimiters = [b',', b';', b'\t', b'|'];
        let mut best_del = b',';
        let mut max_consistency = 0;
        let mut max_fields = 0;

        let count_fields = |line: &str, del: char| -> usize {
            let mut count = 1;
            let mut in_quotes = false;
            let mut chars = line.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                    }
                } else if c == del && !in_quotes {
                    count += 1;
                }
            }
            count
        };

        for &d in &delimiters {
            let del_char = d as char;
            let mut counts = std::collections::HashMap::new();
            let mut total_fields = 0;

            let sample_lines = std::cmp::min(lines.len(), 5);
            for line in lines.iter().take(sample_lines) {
                let c = count_fields(line, del_char);
                *counts.entry(c).or_insert(0) += 1;
                total_fields += c;
            }

            let consistency = counts.values().max().copied().unwrap_or(0);
            let avg_fields = if sample_lines > 0 {
                total_fields / sample_lines
            } else {
                0
            };

            let should_update = if avg_fields > 1 && max_fields <= 1 {
                true
            } else if avg_fields <= 1 && max_fields > 1 {
                false
            } else {
                consistency > max_consistency
                    || (consistency == max_consistency && avg_fields > max_fields)
            };

            if should_update {
                max_consistency = consistency;
                max_fields = avg_fields;
                best_del = d;
            }
        }

        actual_delimiter = Some(best_del);
        Box::new(std::io::Cursor::new(preamble).chain(take.into_inner()))
    } else {
        Box::new(reader)
    };

    if let Some(delim) = actual_delimiter {
        csv_builder.delimiter(delim);
    }

    let mut csv_reader = csv_builder.from_reader(boxed_reader);

    let headers = csv_reader.headers()?;
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
        let mut values: Vec<String> = record.iter().map(|s| s.to_string()).collect();

        // Ensure values align with headers, even when using flexible (ragged) rows.
        // Note: missing fields are padded with empty strings, which downstream
        // StreamingColumnCollection treats as nulls — this conflates "absent" with
        // "explicitly empty", which is acceptable for profiling purposes.
        let header_len = header_names.len();
        if values.len() < header_len {
            values.resize(header_len, String::new());
        } else if values.len() > header_len {
            values.truncate(header_len);
        }

        column_stats.process_record(&header_names, values);
        rows_read += 1;
    }

    let profiles = profile_builder::profiles_from_streaming(&column_stats);

    Ok((profiles, column_stats, rows_read, header_names))
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

    let (column_profiles, column_stats, rows_read, header_names) =
        analyze_csv_from_reader(buf_reader, config)?;

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
            ExecutionMetadata::new(0, header_names.len(), start.elapsed().as_millis()),
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
        ExecutionMetadata::new(rows_read, num_columns, scan_time_ms),
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

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
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
        let config = CsvParserConfig::default().max_rows(Some(3));

        let (_profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
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

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 3);
        assert!(!profiles.is_empty());
    }

    #[test]
    fn test_analyze_csv_from_reader_custom_delimiter() {
        let data = b"a;b\n1;2\n3;4\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default().with_delimiter(b';');

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_analyze_csv_file_returns_quality_report() {
        let csv = write_csv("x,y\n1,a\n2,b\n3,c\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.execution.rows_processed, 3);
        assert!(report.quality_score() >= 0.0);
        assert!(report.quality_score() <= 100.0);
    }

    #[test]
    fn test_csv_auto_detects_delimiter_from_reader() {
        let data = b"name;age\nAlice;25\nBob;30\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.total_count, 2);
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
        assert_eq!(report.execution.rows_processed, 3);
        assert_eq!(report.execution.columns_detected, 2);
        assert!(report.quality_score() >= 0.0);
        assert!(report.quality_score() <= 100.0);
    }

    #[test]
    fn test_analyze_csv_file_empty_file() {
        let csv = write_csv("name,age\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 0);
        assert_eq!(report.execution.rows_processed, 0);
    }

    #[test]
    #[allow(deprecated)]
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

        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.execution.rows_processed, 5);
        assert!((report.execution.sampling_ratio.unwrap_or(1.0) - 1.0).abs() < 0.01);
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
        let csv = write_csv(
            "name,age\n\
             Alice,25\nBob,\nCharlie,30\nDave,\nEve,28\n\
             Frank,\nGrace,35\nHeidi,40\nIvan,\nJudy,22\n",
        );
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        // 4 out of 10 records have a missing "age" field
        let age = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .unwrap();
        assert_eq!(age.null_count, 4);
        assert_eq!(age.total_count, 10);
        // Quality score should be computable (>= 0)
        assert!(report.quality_score() >= 0.0);
    }
}
