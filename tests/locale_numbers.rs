//! Numbers written with a decimal comma or digit grouping (#433).
//!
//! dataprof does not parse `1.234,56` as a number, so a column of them is typed
//! `string` with no numeric statistics, and nothing said why. The profile now
//! counts them in `locale_number_count` and the `locale_numbers` finding names
//! the column. The count is taken by the shared column builders, so every
//! engine and input format has to report the same one for the same values.
//!
//! `python/tests/test_locale_numbers.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CsvParserConfig, DataType, EngineType, FindingCode, ProfileReport, Profiler, analyze_column,
    analyze_csv_file,
};
use tempfile::NamedTempFile;

/// Three columns as an Italian spreadsheet export writes them.
const ROWS: [(&str, &str, &str); 6] = [
    ("1.234,56", "10,50", "consegna rapida"),
    ("2.345,67", "20,00", "1,5"),
    ("9.876,54", "7,25", "fragile"),
    ("12.000,00", "3,10", "ok"),
    ("500,00", "1,99", "da verificare"),
    ("1.000.000,01", "0,99", "urgente"),
];

/// Per column: how many values are locale-formatted numbers, and whether the
/// finding reports the column. `note` holds one `1,5` among free text, which
/// is too few of its text values to report.
const EXPECTED: [(&str, usize, bool); 3] = [
    ("importo", 6, true),
    ("prezzo", 6, true),
    ("note", 1, false),
];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "importo;prezzo;note").unwrap();
    for (importo, prezzo, note) in ROWS {
        writeln!(file, "{importo};{prezzo};{note}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    ROWS.iter()
        .map(|(importo, prezzo, note)| {
            format!(r#"{{"importo":"{importo}","prezzo":"{prezzo}","note":"{note}"}}"#)
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

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("importo", DataType::Utf8, false),
        Field::new("prezzo", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ROWS.map(|row| row.0).to_vec())),
            Arc::new(StringArray::from(ROWS.map(|row| row.1).to_vec())),
            Arc::new(StringArray::from(ROWS.map(|row| row.2).to_vec())),
        ],
    )
    .unwrap();

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

#[test]
fn every_path_counts_the_same_locale_numbers() {
    for (label, report) in reports() {
        let findings = report.findings();
        for (name, count, reported) in EXPECTED {
            let column = report
                .column_profiles
                .iter()
                .find(|column| column.name == name)
                .unwrap_or_else(|| panic!("[{label}] {name} column"));
            assert_eq!(
                column.data_type,
                DataType::String,
                "[{label}] {name} must not be read as numeric"
            );
            assert_eq!(column.locale_number_count, Some(count), "[{label}] {name}");
            // A subset of the text values, over the same values.
            let text = column.type_homogeneity.expect("classified").text;
            assert!(count <= text, "[{label}] {name}: {count} > {text} text");

            let found = findings.findings.iter().any(|finding| {
                finding.code == FindingCode::LocaleNumbers
                    && finding.column.as_deref() == Some(name)
            });
            assert_eq!(found, reported, "[{label}] {name} finding");
        }

        // Written, and read back as the same count rather than as absence.
        let document = serde_json::to_value(&report).expect("serializes");
        let columns = document["column_profiles"].as_array().expect("columns");
        let importo = columns
            .iter()
            .find(|column| column["name"] == "importo")
            .expect("importo serialized");
        assert_eq!(importo["locale_number_count"], 6, "[{label}] serialized");
        let restored: ProfileReport =
            serde_json::from_value(document).expect("the report reads back");
        assert_eq!(restored.findings(), findings, "[{label}] read back");
    }
}

/// The database connectors profile a column through `analyze_column`, which
/// builds its profile separately from the file engines.
#[test]
fn column_analysis_counts_them_too() {
    for (index, (name, count, _)) in EXPECTED.into_iter().enumerate() {
        let values: Vec<String> = ROWS
            .iter()
            .map(|row| [row.0, row.1, row.2][index].to_string())
            .collect();
        let profile = analyze_column(name, &values);
        assert_eq!(profile.locale_number_count, Some(count), "{name}");
    }
}

/// A float column keeps its type when a few values use a decimal comma. Those
/// values are counted invalid and left out of every statistic, which the
/// finding reports because they are all of the column's text values.
#[test]
fn a_numeric_column_reports_the_decimal_commas_it_left_out() {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "peso").unwrap();
    for value in ["1.5", "2.5", "3,5", "4.5", "5.5", "6.5"] {
        writeln!(file, "{value}").unwrap();
    }
    file.flush().unwrap();

    let report = Profiler::new().analyze_file(file.path()).expect("profiles");
    let peso = &report.column_profiles[0];
    assert_eq!(peso.data_type, DataType::Float);
    assert_eq!(peso.invalid_count, Some(1));
    assert_eq!(peso.locale_number_count, Some(1));
    assert!(
        report
            .findings()
            .findings
            .iter()
            .any(|finding| finding.code == FindingCode::LocaleNumbers)
    );
}
