use csv::ReaderBuilder;
use std::io::Read;
use std::path::Path;

use dataprof_core::{
    AnalysisOptions, ColumnProfile, DataProfilerError, DataSource, ExecutionMetadata, FileFormat,
    TruncationReason,
};
use dataprof_runtime::{
    ProfileReport, ReportAssembler, StreamingColumnCollection, profile_builder,
};

mod encoding;
mod memmap_reader;
mod quote_state;
#[doc(hidden)]
pub mod row_count;

pub use encoding::{EncodingDiagnostic, diagnose_non_utf8};
pub use memmap_reader::MemoryMappedCsvReader;
pub use quote_state::{QuoteOutcome, QuoteTrackingReader, unterminated_quote_error};

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
    /// Whether the first row contains column headers.
    /// When `false`, columns are named `column_0`, `column_1`, etc.
    pub has_header: bool,
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
            has_header: true,
            max_rows: None,
        }
    }
}

impl CsvParserConfig {
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

    /// Set whether the first row contains column headers.
    pub fn has_header(mut self, yes: bool) -> Self {
        self.has_header = yes;
        self
    }

    /// Set maximum number of rows to evaluate.
    pub fn max_rows(mut self, max: Option<usize>) -> Self {
        self.max_rows = max;
        self
    }
}

/// Count the fields in a record for a given delimiter, respecting quoted
/// sections, and how many of them at the end are empty.
///
/// Returns `(fields, trailing_empty)`. A field is empty when it decodes to
/// nothing, as the readers decode it: nothing stands between its delimiters,
/// or it is a quoted empty string `""`. Judging the raw text instead counted
/// `""` as content, and a row ending in `;""` disagreed with its header
/// (#1102).
///
/// A quote opens a quoted section only at the start of a field, as in the
/// readers and [`split_sample_records`]. Opening one at any quote hid every
/// delimiter after `id"x`, and a `id"x;value` header was read as one column.
fn count_fields(line: &str, delimiter: char) -> (usize, usize) {
    let decodes_empty = |field: &str| field.is_empty() || field == "\"\"";
    let mut fields = 1;
    let mut trailing_empty = 0;
    let mut field_start = 0;
    let mut in_quotes = false;
    let mut chars = line.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if ch == '"' && in_quotes {
            if chars.peek().map(|&(_, next)| next) == Some('"') {
                chars.next();
            } else {
                in_quotes = false;
            }
        } else if ch == '"' && index == field_start {
            in_quotes = true;
        } else if ch == delimiter && !in_quotes {
            trailing_empty = if decodes_empty(&line[field_start..index]) {
                trailing_empty + 1
            } else {
                0
            };
            fields += 1;
            field_start = index + ch.len_utf8();
        }
    }
    trailing_empty = if decodes_empty(&line[field_start..]) {
        trailing_empty + 1
    } else {
        0
    };
    (fields, trailing_empty)
}

/// Whether a record reads at the header's width without losing or inventing a
/// value: it has exactly that many fields, or more whose extras are all empty,
/// so cutting it to the header drops nothing (#1096).
fn agrees_with_header(fields: usize, trailing_empty: usize, header_fields: usize) -> bool {
    fields == header_fields || (fields > header_fields && fields - trailing_empty <= header_fields)
}

/// Split raw CSV sample text into logical records, honoring quoted sections
/// so an embedded newline inside quotes does not start a new record.
///
/// A record ends at LF, CRLF or a lone CR outside quotes, as in the readers.
/// Splitting on LF alone read a CR-only file as one header record (#1097).
///
/// As in the readers, a quote opens a quoted section only at the start of a
/// field; anywhere else it is a literal character. The delimiter is not known
/// yet, so a field starts at a record start or after any candidate. Opening
/// one at any quote read a header such as `id;height"in;note` as never ending,
/// and detection refused a file the readers read (#1098 review).
///
/// The returned flag is true when the final record was not terminated by a
/// newline outside quotes — i.e. the text ends mid-record (or exactly at a
/// record's end with no trailing newline, which a caller sampling a fixed
/// byte budget cannot distinguish from a cut).
fn split_sample_records(text: &str) -> (Vec<&str>, bool) {
    let bytes = text.as_bytes();
    let mut records = Vec::new();
    let mut in_quotes = false;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'"' if in_quotes => {
                if bytes.get(i + 1) == Some(&b'"') {
                    i += 1; // escaped quote inside a quoted section
                } else {
                    in_quotes = false;
                }
            }
            b'"' if i == start || b",;\t|".contains(&bytes[i - 1]) => in_quotes = true,
            b'\n' if !in_quotes => {
                let end = if i > start && bytes[i - 1] == b'\r' {
                    i - 1
                } else {
                    i
                };
                records.push(&text[start..end]);
                start = i + 1;
            }
            b'\r' if !in_quotes && bytes.get(i + 1) != Some(&b'\n') => {
                records.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let last_unterminated = start < text.len();
    if last_unterminated {
        records.push(text[start..].trim_end_matches('\r'));
    }
    (records, last_unterminated)
}

/// Bytes delimiter detection reads when the header record ends within them.
const DETECTION_SAMPLE_BYTES: usize = 4096;

/// Most bytes delimiter detection reads to see the whole header record.
const MAX_HEADER_SAMPLE_BYTES: usize = 1 << 20;

/// Index of the first non-blank record, the one the readers take as the header.
fn header_index(records: &[&str]) -> Option<usize> {
    records.iter().position(|record| !record.trim().is_empty())
}

/// Whether `head`, the start of a source, holds its header record up to the
/// newline that ends it.
fn header_is_complete(head: &[u8]) -> bool {
    let sample = head.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(head);
    let text = String::from_utf8_lossy(sample);
    let (records, last_unterminated) = split_sample_records(&text);
    header_index(&records).is_some_and(|index| index + 1 < records.len() || !last_unterminated)
}

/// Read the head of a source that delimiter detection scores: 4 KB, or more
/// when the header record runs past that, doubling up to 1 MiB. Each read
/// takes one byte past its target, so the returned flag says for certain
/// whether the head was cut before the end of the source.
///
/// A fixed 4 KB window scored the start of a longer header as the whole of
/// it; no candidate split the fragment, and the file was read as one column
/// with no error (#1098).
fn read_detection_head<R: Read>(reader: &mut R) -> std::io::Result<(Vec<u8>, bool)> {
    let mut head = Vec::new();
    let mut target = DETECTION_SAMPLE_BYTES;
    loop {
        let wanted = (target + 1 - head.len()) as u64;
        reader.by_ref().take(wanted).read_to_end(&mut head)?;
        let cut = head.len() > target;
        if !cut || target == MAX_HEADER_SAMPLE_BYTES || header_is_complete(&head) {
            return Ok((head, cut));
        }
        target = (target * 2).min(MAX_HEADER_SAMPLE_BYTES);
    }
}

const HEADER_TOO_LONG: &str =
    "The CSV header record is longer than 1 MiB, so its delimiter could not be detected.";
const HEADER_TOO_LONG_HINT: &str =
    "Set the delimiter explicitly (csv_delimiter) to read this file.";

/// Detect the delimiter of a source and return it with the bytes read to
/// detect it, so a caller that cannot rewind the source can chain them back
/// in front of the rest.
///
/// Detection reads 4 KB, or up to 1 MiB when the header record is longer. A
/// header still incomplete at 1 MiB is an error, never a guess.
pub fn detect_delimiter_with_head<R: Read>(
    reader: &mut R,
) -> Result<(u8, Vec<u8>), DataProfilerError> {
    let (head, cut) = read_detection_head(reader)?;
    match score_head(&head, cut) {
        Some(delimiter) => Ok((delimiter, head)),
        None => Err(DataProfilerError::csv_structure(
            HEADER_TOO_LONG,
            HEADER_TOO_LONG_HINT,
        )),
    }
}

/// Detect the most likely CSV delimiter by sampling 4 KB of data, or more
/// when the header record is longer (see [`detect_delimiter_with_head`]).
///
/// The sample is split into logical records with quote handling, so quoted
/// fields containing embedded newlines or delimiter characters do not skew
/// detection. Tests comma, semicolon, tab, and pipe over the first 5 non-blank
/// records, measured against the first one, the header: it sets the column
/// count, and the readers cut or pad a wider or narrower record to it, or
/// reject it under strict parsing. A delimiter that splits the header always
/// beats one that does not; then more records agreeing with the header win,
/// then more records at exactly the header's width, then a wider header, and
/// comma wins a tie. A record agrees when it reads at the header's width
/// without losing a value: the same field count, or extra fields that are all
/// empty, as an exporter's trailing delimiter leaves.
///
/// Judging by the data rows alone let a delimiter that never occurs in the
/// header win, and the readers then cut every row back to its first field: a
/// one-column file of `123|456` keys was profiled as `123` with fabricated
/// duplicates (#1092).
///
/// A header record still incomplete after 1 MiB is an `InvalidData` error.
pub fn detect_delimiter<R: Read>(mut reader: R) -> std::io::Result<u8> {
    let (head, cut) = read_detection_head(&mut reader)?;
    score_head(&head, cut).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{HEADER_TOO_LONG} {HEADER_TOO_LONG_HINT}"),
        )
    })
}

/// Score the candidate delimiters over a detection head, or `None` when the
/// head was cut inside its header record.
fn score_head(head: &[u8], cut: bool) -> Option<u8> {
    // A UTF-8 BOM marks the encoding, as in the readers, and is no part of
    // the first record: left in, a BOM followed by a blank line was scored as
    // a one-field header and outvoted the real one.
    let sample = head.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(head);
    let preamble_str = String::from_utf8_lossy(sample);
    let (mut records, last_unterminated) = split_sample_records(&preamble_str);
    // A head cut mid-record ends in a fragment; never score it. A head ending
    // on a clean newline keeps all its records. A fragment that is the header
    // itself has no delimiter to find.
    if cut && last_unterminated {
        if header_index(&records) == Some(records.len() - 1) {
            return None;
        }
        records.pop();
    }

    // Blank records carry no delimiter signal: a header followed by trailing
    // blank lines must not outvote the true delimiter with single-field
    // "records" (gh #559).
    let scored: Vec<&str> = records
        .into_iter()
        .filter(|record| !record.trim().is_empty())
        .take(5)
        .collect();
    let Some((header, rows)) = scored.split_first() else {
        return Some(b',');
    };

    let delimiters = *b",;\t|";
    let mut best_delimiter = b',';
    // (splits the header into >1 field, records agreeing with the header,
    //  records at exactly the header's width, header field count) — compared
    //  lexicographically, first wins ties. Exact width only breaks a tie: a
    //  candidate that reads every record as it stands beats one that needs
    //  trailing empty fields cut to agree.
    let mut best_score = (false, 0usize, 0usize, 0usize);

    for &delimiter in &delimiters {
        let (header_fields, _) = count_fields(header, delimiter as char);
        let (mut agreeing, mut exact) = (1, 1);
        for record in rows {
            let (fields, trailing_empty) = count_fields(record, delimiter as char);
            if agrees_with_header(fields, trailing_empty, header_fields) {
                agreeing += 1;
            }
            if fields == header_fields {
                exact += 1;
            }
        }

        let score = (header_fields > 1, agreeing, exact, header_fields);
        if score > best_score {
            best_score = score;
            best_delimiter = delimiter;
        }
    }

    Some(best_delimiter)
}

/// Detect the most likely CSV delimiter from a file path.
pub fn detect_delimiter_from_path(path: &Path) -> std::io::Result<u8> {
    let file = std::fs::File::open(path)?;
    detect_delimiter(std::io::BufReader::new(file))
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
    let scan = scan_csv(reader, config)?;
    let profiles = profile_builder::profiles_from_streaming(&scan.column_stats, false, false, None);

    Ok((
        profiles,
        scan.column_stats,
        scan.rows_read,
        scan.header_names,
    ))
}

/// What one pass over CSV input read, and how much of the input that was.
struct CsvScan {
    column_stats: StreamingColumnCollection,
    header_names: Vec<String>,
    rows_read: usize,
    /// Records whose field count differs from the header's, padded or cut to
    /// the header width before profiling.
    ragged_rows: usize,
    /// Set when `max_rows` stopped the pass with records left in the input.
    truncation: Option<TruncationReason>,
}

fn scan_csv<R: Read>(reader: R, config: &CsvParserConfig) -> Result<CsvScan, DataProfilerError> {
    let mut csv_builder = ReaderBuilder::new();
    csv_builder.has_headers(config.has_header);
    csv_builder.flexible(config.flexible);
    csv_builder.quote(config.quote_char);
    if config.trim_whitespace {
        csv_builder.trim(csv::Trim::All);
    }
    let mut actual_delimiter = config.delimiter;

    let boxed_reader: Box<dyn std::io::Read + '_> = if config.delimiter.is_none() {
        let mut reader = reader;
        let (detected, head) = detect_delimiter_with_head(&mut reader)?;
        actual_delimiter = Some(detected);

        Box::new(std::io::Cursor::new(head).chain(reader))
    } else {
        Box::new(reader)
    };

    if let Some(delimiter) = actual_delimiter {
        csv_builder.delimiter(delimiter);
    }

    let mut csv_reader = csv_builder.from_reader(boxed_reader);

    let header_names: Vec<String> = if config.has_header {
        let headers = csv_reader.headers()?;
        let names: Vec<String> = headers.iter().map(|header| header.to_string()).collect();
        // Reject duplicate headers before profiling: process_record keys on name,
        // so a repeat would merge two source columns into one inflated profile.
        dataprof_core::validate_unique_column_names(&names, "CSV header")?;
        names
    } else {
        let headers = csv_reader.headers()?;
        (0..headers.len())
            .map(|index| format!("column_{index}"))
            .collect()
    };

    let mut column_stats = StreamingColumnCollection::new();
    // A header declares the columns even when no data records follow it.
    column_stats.init_columns(&header_names);
    let mut rows_read = 0;
    let mut ragged_rows = 0;
    let mut truncation = None;

    for result in csv_reader.records() {
        if let Some(max_rows) = config.max_rows
            && rows_read >= max_rows
        {
            // A record past the cap means the cap, not the end of the input,
            // stopped the pass. That record is neither profiled nor judged.
            truncation = Some(TruncationReason::MaxRows(max_rows as u64));
            break;
        }

        let record = result?;
        let mut values: Vec<String> = record.iter().map(|value| value.to_string()).collect();

        let header_len = header_names.len();
        if values.len() != header_len {
            ragged_rows += 1;
        }
        if values.len() < header_len {
            values.resize(header_len, String::new());
        } else if values.len() > header_len {
            values.truncate(header_len);
        }

        column_stats.process_record(&header_names, values);
        rows_read += 1;
    }

    Ok(CsvScan {
        column_stats,
        header_names,
        rows_read,
        ragged_rows,
        truncation,
    })
}

/// Analyze a CSV file, returning a full [`ProfileReport`].
///
/// Opens the file, reads it as [`analyze_csv_from_reader`] does, and wraps the
/// result with file metadata, ISO 8000/25012 data quality metrics, and the
/// scan's own record: how many rows were ragged, and whether `max_rows` left
/// rows unread.
pub fn analyze_csv_file(
    file_path: &Path,
    config: &CsvParserConfig,
) -> Result<ProfileReport, DataProfilerError> {
    let metadata = std::fs::metadata(file_path).map_err(|error| map_io_error(file_path, error))?;
    dataprof_core::binary_input::reject_binary_input(file_path)?;
    let start = std::time::Instant::now();

    let file = std::fs::File::open(file_path).map_err(|error| map_io_error(file_path, error))?;
    let buf_reader = std::io::BufReader::new(file);

    let scan = scan_csv(buf_reader, config)?;
    let column_stats = &scan.column_stats;
    let column_profiles =
        profile_builder::profiles_from_streaming(column_stats, false, false, None);

    let file_source = DataSource::File {
        path: file_path.display().to_string(),
        format: FileFormat::Csv,
        size_bytes: metadata.len(),
        modified_at: None,
        parquet_metadata: None,
    };

    let sample_columns = profile_builder::quality_check_samples(column_stats);
    let scan_time_ms = start.elapsed().as_millis();
    let num_columns = column_profiles.len();

    let mut execution = ExecutionMetadata::new(scan.rows_read, num_columns, scan_time_ms)
        .with_engine("csv")
        .with_ragged_row_count(scan.ragged_rows);
    if let Some(reason) = scan.truncation {
        execution = execution.with_truncation(reason);
    }

    // This path takes no analysis options: it profiles with the defaults,
    // so it records them rather than leaving its settings unknown.
    Ok(ReportAssembler::new(file_source, execution)
        .with_analysis_options(&AnalysisOptions::default())
        .columns(column_profiles)
        .with_quality_data(sample_columns)
        .with_row_duplicates(column_stats.row_duplicate_summary())
        .with_row_completeness(column_stats.row_completeness_summary())
        .build())
}

fn map_io_error(file_path: &Path, error: std::io::Error) -> DataProfilerError {
    if error.kind() == std::io::ErrorKind::NotFound {
        DataProfilerError::FileNotFound {
            path: file_path.display().to_string(),
        }
    } else {
        DataProfilerError::from(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dataprof_core::DataType;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    fn write_csv(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", content).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_analyze_csv_from_reader_basic() {
        let data = b"name,age\nAlice,25\nBob,30\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
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
        assert!(report.quality_score().unwrap() >= 0.0);
        assert!(report.quality_score().unwrap() <= 100.0);
    }

    #[test]
    fn test_csv_auto_detects_delimiter_from_reader() {
        let data = b"name;age\nAlice;25\nBob;30\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.total_count, 2);
    }

    #[test]
    fn test_analyze_csv_basic() {
        let csv = write_csv("name,age\nAlice,25\nBob,30\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;
        assert_eq!(profiles.len(), 2);

        let names: Vec<&str> = profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect();
        assert!(names.contains(&"name"));
        assert!(names.contains(&"age"));

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.total_count, 2);
        assert_eq!(age.null_count, 0);
    }

    #[test]
    fn test_analyze_csv_with_nulls() {
        let csv = write_csv("name,age,email\nAlice,25,a@b.com\nBob,,\nCharlie,30,c@d.com\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.total_count, 3);
        assert_eq!(age.null_count, 1);

        let email = profiles
            .iter()
            .find(|profile| profile.name == "email")
            .unwrap();
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
        assert!(report.quality_score().unwrap() >= 0.0);
        assert!(report.quality_score().unwrap() <= 100.0);
    }

    #[test]
    fn test_analyze_csv_file_header_only_preserves_declared_columns() {
        let csv = write_csv("name,age\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(
            report
                .column_profiles
                .iter()
                .map(|column| (column.name.as_str(), column.data_type.clone()))
                .collect::<Vec<_>>(),
            vec![("name", DataType::String), ("age", DataType::String)]
        );
        assert_eq!(report.execution.rows_processed, 0);
        assert!(report.quality.is_some());
    }

    #[test]
    fn test_analyze_csv_file_empty_file_has_no_columns() {
        let csv = write_csv("");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert!(report.column_profiles.is_empty());
        assert_eq!(report.execution.rows_processed, 0);
        assert_eq!(report.execution.columns_detected, 0);
        assert_eq!(
            report.quality_status,
            dataprof_runtime::QualityAnalysisStatus::Computed
        );
        assert!(report.quality.is_some());
        assert!(report.quality_score().is_none());
    }

    #[test]
    fn test_analyze_csv_with_max_rows_small_file() {
        let csv = write_csv("val\n1\n2\n3\n4\n5\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.execution.columns_detected, 1);
        assert!((report.execution.sampling_ratio.unwrap_or(1.0) - 1.0).abs() < 0.01);
    }

    #[test]
    fn a_row_cap_that_leaves_rows_unread_is_a_truncation() {
        let csv = write_csv("val\n1\n2\n3\n4\n5\n");
        let config = CsvParserConfig::default().max_rows(Some(2));
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 2);
        assert!(!report.execution.source_exhausted);
        assert_eq!(
            report.execution.truncation_reason,
            Some(TruncationReason::MaxRows(2))
        );
    }

    #[test]
    fn a_row_cap_at_the_last_row_is_a_complete_scan() {
        let csv = write_csv("val\n1\n2\n");
        let config = CsvParserConfig::default().max_rows(Some(2));
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 2);
        assert!(report.execution.source_exhausted);
        assert_eq!(report.execution.truncation_reason, None);
    }

    #[test]
    fn short_and_long_rows_are_counted_as_ragged() {
        let csv = write_csv("a,b,c\n1,2,3\n4,5\n6,7,8,9\n10,11,12\n");
        let report = analyze_csv_file(csv.path(), &CsvParserConfig::default()).unwrap();

        assert_eq!(report.execution.rows_processed, 4);
        assert_eq!(report.execution.ragged_row_count, 2);
    }

    #[test]
    fn a_ragged_row_past_the_row_cap_is_not_counted() {
        let csv = write_csv("a,b,c\n1,2,3\n4,5\n");
        let config = CsvParserConfig::default().max_rows(Some(1));
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.execution.ragged_row_count, 0);
        assert_eq!(
            report.execution.truncation_reason,
            Some(TruncationReason::MaxRows(1))
        );
    }

    #[test]
    fn test_analyze_csv_numeric_types_detected() {
        let csv = write_csv("int_col,float_col,str_col\n1,1.5,hello\n2,2.5,world\n3,3.5,foo\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let int_col = profiles
            .iter()
            .find(|profile| profile.name == "int_col")
            .unwrap();
        assert!(matches!(int_col.data_type, DataType::Integer));

        let float_col = profiles
            .iter()
            .find(|profile| profile.name == "float_col")
            .unwrap();
        assert!(matches!(float_col.data_type, DataType::Float));

        let str_col = profiles
            .iter()
            .find(|profile| profile.name == "str_col")
            .unwrap();
        assert!(matches!(str_col.data_type, DataType::String));
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

        let age = report
            .column_profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.null_count, 4);
        assert_eq!(age.total_count, 10);
        assert!(report.quality_score().unwrap() >= 0.0);
    }

    #[test]
    fn test_detect_delimiter_comma() {
        let data = b"name,age,city\nAlice,25,NYC\nBob,30,London\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_semicolon() {
        let data = b"name;age;city\nAlice;25;NYC\nBob;30;London\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_pipe() {
        let data = b"name|age|city\nAlice|25|NYC\nBob|30|London\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b'|');
    }

    #[test]
    fn test_detect_delimiter_tab() {
        let data = b"name\tage\tcity\nAlice\t25\tNYC\nBob\t30\tLondon\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b'\t');
    }

    #[test]
    fn test_detect_delimiter_from_path_semicolon() {
        let csv = write_csv("id;name;salary\n1;Alice;50000\n2;Bob;60000\n");
        assert_eq!(detect_delimiter_from_path(csv.path()).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_blank_records_do_not_outvote_header() {
        // A header-only file with trailing blank lines: the blank records are
        // single-field for every candidate, so counting them made `;` win on
        // perfect consistency and collapse the whole header into one column
        // (gh #559).
        let data = b"name,age\n\n\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');

        let semicolon = b"name;age\n\n";
        assert_eq!(
            detect_delimiter(Cursor::new(semicolon.as_ref())).unwrap(),
            b';'
        );
    }

    #[test]
    fn test_detect_delimiter_quoted_fields() {
        let data = b"name;desc;val\n\"hello;world\";foo;1\nbar;baz;2\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_quoted_embedded_newline() {
        // A quoted cell containing a newline must not break record splitting:
        // before the quote-aware sniffer this file was detected as semicolon
        // (zero occurrences, "perfectly consistent" single-field records) and
        // profiled as a single column named "id,comment".
        let data = b"id,comment\n1,\"line one\nline two\"\n2,\"normal\"\n3,\"has \"\"quotes\"\" inside\"\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_absent_candidate_never_beats_real_one() {
        // One ragged record makes comma imperfect (consistency 3/4); an absent
        // delimiter is uniformly single-field (4/4) and must still lose.
        let data = b"a,b,c\n1,2,3\nragged line without delimiters\n4,5,6\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_single_column_falls_back_to_comma() {
        let data = b"value\n1\n2\n3\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_two_column_semicolon_with_commas_in_text() {
        // Free text containing commas must not outvote the true delimiter.
        let data = b"id;note\n1;hello, world\n2;foo, bar, baz\n3;plain\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_never_splits_rows_the_header_does_not() {
        // Pipe splits both data rows and not the header; the readers cut rows
        // to the header's width, so choosing it kept only `123` (gh #1092).
        let data = b"key\n123|456\n123|789\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_header_split_beats_uniform_rows() {
        // A trailing delimiter on every data row: comma splits the header,
        // which no other candidate does.
        let data = b"a,b\n1,2,\n3,4,\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_trailing_empty_fields_still_agree() {
        // Semicolon rows carry a trailing empty field, comma occurs inside the
        // header name and a decimal. Counting the empty extras against
        // semicolon let comma win and invent a null column (gh #1096).
        let data = b"id;amount,EUR\n1;10,50;\n2;20;\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_quoted_empty_trailing_field_still_agrees() {
        // `""` decodes to an empty field in every reader; judged on its raw
        // text it counted as content and comma won again (gh #1102).
        let data = b"id;amount,EUR\n1;10,50;\"\"\n2;20;\"\"\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_exact_width_beats_cut_trailing_fields() {
        // Both candidates split the header and keep every value; semicolon
        // reads each row as it stands, comma only after cutting an empty
        // trailing field. Comma won the tie and renamed the columns.
        let data = b"id,part;value\nx;y,z,\nu;v,w,\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_skips_a_utf8_bom() {
        // A BOM then a blank line: the BOM alone was scored as a one-field
        // header, and comma won over the real semicolon header.
        let data = b"\xEF\xBB\xBF\nid;value\n1;2\n3;4\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
        let no_blank = b"\xEF\xBB\xBFid;value\n1;2\n";
        assert_eq!(
            detect_delimiter(Cursor::new(no_blank.as_ref())).unwrap(),
            b';'
        );
    }

    #[test]
    fn test_detect_delimiter_lone_cr_ends_a_record() {
        // CR-only line endings: read as one header record, pipe split it and
        // won, and the keys were cut to `123` (gh #1097).
        let data = b"key\r123|456\r123|789\r";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
        let semicolon = b"id;value\r1;10\r2;20\r";
        assert_eq!(
            detect_delimiter(Cursor::new(semicolon.as_ref())).unwrap(),
            b';'
        );
    }

    #[test]
    fn test_count_fields_reports_trailing_empty_fields() {
        assert_eq!(count_fields("1;10,50;", ';'), (3, 1));
        assert_eq!(count_fields("a;;", ';'), (3, 2));
        assert_eq!(count_fields("a;\"\"", ';'), (2, 1));
        assert_eq!(count_fields("a;\"\"\"\"", ';'), (2, 0));
        assert_eq!(count_fields("a; ", ';'), (2, 0));
        assert_eq!(count_fields("a;b", ';'), (2, 0));
        assert_eq!(count_fields("\"x;y\";", ';'), (2, 1));
    }

    #[test]
    fn test_split_sample_records_quote_aware() {
        let (records, last_unterminated) = split_sample_records("a,b\r\n1,\"x\ny\"\n2,z\n");
        assert_eq!(records, vec!["a,b", "1,\"x\ny\"", "2,z"]);
        assert!(!last_unterminated);

        let (records, last_unterminated) = split_sample_records("a,b\n1,\"cut mid-fie");
        assert_eq!(records, vec!["a,b", "1,\"cut mid-fie"]);
        assert!(last_unterminated);

        let (records, last_unterminated) = split_sample_records("a,b\r1,\"x\ry\"\r2,z\r");
        assert_eq!(records, vec!["a,b", "1,\"x\ry\"", "2,z"]);
        assert!(!last_unterminated);

        let (records, _) = split_sample_records("a\rb\r\nc\n");
        assert_eq!(records, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_detect_delimiter_full_sample_ending_on_record_boundary() {
        // Exactly 4096 bytes ending with a clean newline: the final record is
        // complete and must be scored, not dropped as a fragment.
        let mut data = Vec::from(&b"a;b;c\n"[..]);
        while data.len() < 4090 {
            data.extend_from_slice(b"1;2;3\n");
        }
        while data.len() < 4095 {
            data.push(b'#');
        }
        data.push(b'\n');
        assert_eq!(data.len(), 4096);
        assert_eq!(
            detect_delimiter(Cursor::new(data.as_slice())).unwrap(),
            b';'
        );
    }

    #[test]
    fn test_detect_delimiter_reads_past_the_sample_to_end_the_header() {
        // #1098: the first `;` is past byte 4096, so a 4 KB sample held a
        // fragment of the header that no candidate split.
        let issue = format!("\"{}\";value\n1;10\n2;20\n", "x".repeat(4094));
        assert_eq!(
            detect_delimiter(Cursor::new(issue.as_bytes())).unwrap(),
            b';'
        );

        // A header record ending exactly at the 4 KB boundary needs no more
        // than the one byte read past it.
        let mut exact = "a".repeat(4093).into_bytes();
        exact.extend_from_slice(b";b\n1;2\n");
        let mut reader = Cursor::new(exact.as_slice());
        let (delimiter, head) = detect_delimiter_with_head(&mut reader).unwrap();
        assert_eq!((delimiter, head.len()), (b';', 4097));
    }

    #[test]
    fn test_detect_delimiter_with_head_returns_every_byte_it_read() {
        let data = format!("{};value\n1;2\n", "a".repeat(20_000));
        let mut reader = Cursor::new(data.as_bytes());
        let (delimiter, head) = detect_delimiter_with_head(&mut reader).unwrap();
        assert_eq!(delimiter, b';');
        assert_eq!(head.len(), 32_768.min(data.len()));
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!([head, rest].concat(), data.as_bytes());
    }

    #[test]
    fn test_detect_delimiter_refuses_a_header_past_the_bound() {
        let data = format!("{};value\n1;2\n", "h".repeat(MAX_HEADER_SAMPLE_BYTES));
        let error = detect_delimiter(Cursor::new(data.as_bytes())).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("csv_delimiter"), "{error}");
        let error = detect_delimiter_with_head(&mut Cursor::new(data.as_bytes())).unwrap_err();
        assert!(matches!(error, DataProfilerError::CsvParsingError { .. }));

        // A whole source no longer than the bound is never refused.
        let short = "h".repeat(MAX_HEADER_SAMPLE_BYTES);
        assert_eq!(
            detect_delimiter(Cursor::new(short.as_bytes())).unwrap(),
            b','
        );
    }

    #[test]
    fn test_detect_delimiter_mid_field_quote_is_literal() {
        // The readers take a quote inside a field as a literal, so the header
        // ends at its newline. Taken as opening a quote, it never ended, and a
        // source past the 1 MiB bound was refused.
        let mut data = b"id;height\"in;note\n".to_vec();
        while data.len() <= MAX_HEADER_SAMPLE_BYTES {
            data.extend_from_slice(b"1;5;x\n");
        }
        assert_eq!(
            detect_delimiter(Cursor::new(data.as_slice())).unwrap(),
            b';'
        );

        let (records, last_unterminated) = split_sample_records("a;b\"c;d\n1;\"x\ny\";2\n");
        assert_eq!(records, vec!["a;b\"c;d", "1;\"x\ny\";2"]);
        assert!(!last_unterminated);

        // Field counting follows the same rule: the readers split `id"x;value`
        // into two fields, and a header no candidate split fell back to comma.
        assert_eq!(count_fields("id\"x;value", ';'), (2, 0));
        assert_eq!(count_fields("\"a;\"\"b\";c", ';'), (2, 0));
        assert_eq!(
            detect_delimiter(Cursor::new(&b"id\"x;value\n1;2\n3;4\n"[..])).unwrap(),
            b';'
        );
    }
}
