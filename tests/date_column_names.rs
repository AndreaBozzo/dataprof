//! Date rules read a column name by its words, and mixed date formats are
//! counted whatever the name (#869).
//!
//! The name rule used to match English substrings: `date` inside
//! `candidate_name`, `time` inside `lifetime_tier`. A string column so named
//! was held to date forms, so clean names scored 0% consistency and could fail
//! a quality gate. The mixed-format count ran only under those names, so
//! `data_ordine` or `bestelldatum` holding ISO and slash dates reported none.
//! Both rules live in the shared metrics crate, so every engine and input
//! format has to agree.
//!
//! `python/tests/test_date_column_names.py` is the Python twin.

use std::io::Write;

use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
use tempfile::NamedTempFile;

const PEOPLE: [&str; 6] = [
    "Anna Rossi",
    "Luca Bianchi",
    "Marco Verdi",
    "Giulia Neri",
    "Paolo Russo",
    "Sara Gallo",
];

/// Three ISO dates, two slash dates, one junk value: two minority-format
/// values, whatever the column is called.
const MIXED_DATES: [&str; 6] = [
    "2024-01-15",
    "2024-02-20",
    "2024-03-05",
    "15/04/2024",
    "20/05/2024",
    "unknown",
];

fn csv_fixture(names: &[&str], columns: &[&[&str]]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", names.join(",")).unwrap();
    for row in 0..columns[0].len() {
        let cells: Vec<&str> = columns.iter().map(|column| column[row]).collect();
        writeln!(file, "{}", cells.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records(names: &[&str], columns: &[&[&str]]) -> Vec<String> {
    (0..columns[0].len())
        .map(|row| {
            let fields: Vec<String> = names
                .iter()
                .zip(columns)
                .map(|(name, column)| format!(r#""{name}":"{}""#, column[row]))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

fn text_fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

/// Every column as `Utf8`, as from a CSV converted without a schema.
#[cfg(feature = "parquet")]
fn parquet_fixture(names: &[&str], columns: &[&[&str]]) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|column| Arc::new(StringArray::from(column.to_vec())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format.
fn reports(names: &[&str], columns: &[&[&str]]) -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture(names, columns);
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

    let records = json_records(names, columns);
    let mut files = vec![
        (
            "json",
            text_fixture(".json", format!("[{}]", records.join(","))),
        ),
        ("jsonl", text_fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(names, columns)));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

fn consistency(report: &ProfileReport, label: &str) -> (f64, usize) {
    let metrics = report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.consistency.as_ref())
        .unwrap_or_else(|| panic!("[{label}] consistency assessed"));
    (metrics.data_type_consistency, metrics.format_violations)
}

/// Collects every wrong (path, column) before failing, so a regression names
/// all the paths it reaches rather than the first.
#[test]
fn names_that_merely_contain_a_date_word_are_not_held_to_dates() {
    let mut wrong = Vec::new();
    for name in [
        "full_name",
        "candidate_name",
        "validated_by",
        "lifetime_tier",
        "created_by",
        "time_zone",
        "birth_place",
    ] {
        for (label, report) in reports(&[name], &[&PEOPLE]) {
            let (score, _) = consistency(&report, &label);
            if score != 100.0 {
                wrong.push(format!("[{label}] {name}: data_type_consistency {score}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The name rule still applies where a word of the name is a date word: a
/// string column so named is held to date forms.
#[test]
fn a_name_with_a_date_word_still_holds_its_column_to_dates() {
    let mut wrong = Vec::new();
    for name in ["order_date", "created_at", "startTime", "date_of_birth"] {
        for (label, report) in reports(&[name], &[&PEOPLE]) {
            let (score, _) = consistency(&report, &label);
            if score != 0.0 {
                wrong.push(format!("[{label}] {name}: data_type_consistency {score}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn mixed_date_formats_are_counted_under_any_name() {
    let mut wrong = Vec::new();
    for name in [
        "order_date",
        "date_de_commande",
        "data_ordine",
        "bestelldatum",
        "fecha_pedido",
    ] {
        for (label, report) in reports(&[name], &[&MIXED_DATES]) {
            let (_, violations) = consistency(&report, &label);
            if violations != 2 {
                wrong.push(format!("[{label}] {name}: format_violations {violations}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
