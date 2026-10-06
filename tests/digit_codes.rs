//! Codes written in digits are not quantities (#814).
//!
//! Zero-padded codes and `YYYYMMDD` dates parse as integers, so they were typed
//! `integer` and given a mean: postal codes averaged, leading zeros gone from
//! every statistic. A column with a value that keeps a leading zero, or whose
//! numeric values are all `YYYYMMDD` dates, is now typed `string`. The decision
//! is made by the shared inference both engine families call, so every engine
//! and input format has to type the same values the same way.
//!
//! `python/tests/test_digit_codes.py` is the Python twin.

use std::io::Write;

use dataprof::{
    ColumnStats, CsvParserConfig, DataType, EngineType, ProfileReport, Profiler, analyze_column,
    analyze_csv_file, infer_schema,
};
use tempfile::NamedTempFile;

const NAMES: [&str; 6] = [
    "leading_zero",
    "es_postal",
    "compact_date",
    "departure_time",
    "quantity",
    "amount_cents",
];

/// The three columns from #814, HHMM times in a column named like a date,
/// then two controls: a count that includes a bare `0`, and eight-digit amounts
/// that are not calendar dates.
const ROWS: [[&str; 6]; 6] = [
    ["00123", "28013", "20240115", "0930", "3", "12345678"],
    ["00456", "08001", "20240216", "1415", "12", "23456789"],
    ["00789", "41001", "20240317", "0805", "0", "34567891"],
    ["01234", "46001", "20240418", "2210", "25", "45678912"],
    ["05678", "48001", "20240519", "1130", "7", "56789123"],
    ["09999", "50001", "20240620", "0645", "100", "67891234"],
];

const EXPECTED: [(&str, DataType); 6] = [
    ("leading_zero", DataType::String),
    ("es_postal", DataType::String),
    ("compact_date", DataType::String),
    ("departure_time", DataType::String),
    ("quantity", DataType::Integer),
    ("amount_cents", DataType::Integer),
];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", NAMES.join(",")).unwrap();
    for row in ROWS {
        writeln!(file, "{}", row.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    ROWS.iter()
        .map(|row| {
            let fields: Vec<String> = NAMES
                .iter()
                .zip(row)
                .map(|(name, value)| format!(r#""{name}":"{value}""#))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

fn json_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".json").unwrap();
    write!(file, "[{}]", json_records().join(",")).unwrap();
    file.flush().unwrap();
    file
}

fn jsonl_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(file, "{}", json_records().join("\n")).unwrap();
    file.flush().unwrap();
    file
}

/// Every column as `Utf8`: the values reach the profiler as text, as from a
/// CSV converted without a schema.
#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(
        NAMES
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = (0..NAMES.len())
        .map(|index| {
            Arc::new(StringArray::from(
                ROWS.iter().map(|row| row[index]).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format.
fn reports() -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture();
    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let mut files = vec![("json", json_fixture()), ("jsonl", jsonl_fixture())];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

/// Collects every wrong column on every path before failing, so a regression
/// names all the paths it reaches rather than the first.
#[test]
fn every_path_types_digit_codes_as_text() {
    let mut wrong = Vec::new();
    for (label, report) in reports() {
        // The type is written and read back, not re-inferred on load.
        let document = serde_json::to_value(&report).expect("serializes");
        let restored: ProfileReport =
            serde_json::from_value(document).expect("the report reads back");

        for (name, expected) in EXPECTED {
            for (side, report) in [("", &report), (" read back", &restored)] {
                let column = report
                    .column_profiles
                    .iter()
                    .find(|column| column.name == name)
                    .unwrap_or_else(|| panic!("[{label}] {name} column{side}"));
                // Text statistics are the ones without a mean.
                let stats_match = match expected {
                    DataType::String => matches!(column.stats, ColumnStats::Text(_)),
                    _ => matches!(column.stats, ColumnStats::Numeric(_)),
                };
                if column.data_type != expected || !stats_match {
                    wrong.push(format!(
                        "[{label}] {name}{side}: {:?} with {:?}",
                        column.data_type, column.stats
                    ));
                }
            }
        }

        // `compact_date` and `departure_time` are text now, and their names
        // hold them to date forms. They were scored as numbers before, and
        // retyping them must not cost them their consistency.
        let consistency = report
            .quality
            .as_ref()
            .and_then(|quality| quality.metrics.consistency.as_ref())
            .unwrap_or_else(|| panic!("[{label}] consistency assessed"));
        if consistency.data_type_consistency != 100.0 {
            wrong.push(format!(
                "[{label}] data_type_consistency {}",
                consistency.data_type_consistency
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// `infer_schema` answers the type without a full profile, from a bounded
/// sample of each format.
#[test]
fn schema_inference_types_them_the_same_way() {
    let mut files = vec![
        ("csv", csv_fixture()),
        ("json", json_fixture()),
        ("jsonl", jsonl_fixture()),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let schema = infer_schema(file.path()).unwrap_or_else(|e| panic!("[{label}] {e}"));
        for (name, expected) in EXPECTED {
            let column = schema
                .columns
                .iter()
                .find(|column| column.name == name)
                .unwrap_or_else(|| panic!("[{label}] {name} column"));
            assert_eq!(column.data_type, expected, "[{label}] {name}");
        }
    }
}

/// The database connectors profile a column through `analyze_column`, which
/// builds its profile separately from the file engines.
#[test]
fn column_analysis_types_them_the_same_way() {
    for (index, (name, expected)) in EXPECTED.into_iter().enumerate() {
        let values: Vec<String> = ROWS.iter().map(|row| row[index].to_string()).collect();
        assert_eq!(analyze_column(name, &values).data_type, expected, "{name}");
    }
}
