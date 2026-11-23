//! A column's type and lexical forms are counted over every value read (#788).
//!
//! The file, Arrow and streaming engines keep 10,000 values of each column.
//! They used to type a text column and classify its values from those, while
//! the same data read in memory was typed from every value. One zero-padded
//! code outside the sample, or a share at a type threshold, then typed the
//! column otherwise by input route, and every check that follows the type
//! followed it. Each engine now counts the lexical facts of every value it
//! reads, so every route reports the type, `type_homogeneity`,
//! `locale_number_count` and `zero_padded_count` of every value. Zero-padded
//! values make digit codes from 1% of the numbers, so one stray code no
//! longer retypes a column of quantities.
//!
//! `python/tests/test_exact_type_counters.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CsvParserConfig, DataType, EngineType, ProfileReport, Profiler, TypeHomogeneity,
    analyze_column, analyze_csv_file,
};
use tempfile::NamedTempFile;

/// Rows: four times the sample, so a sample sees a quarter of the values.
const ROWS: usize = 40_000;

/// Laid out in this order, then read in another: coprime with `ROWS`.
const ORDER: usize = 7_919;

/// Values laid end to end as `(value, count)` blocks, then permuted.
fn placed(blocks: &[(&str, usize)]) -> Vec<String> {
    let values: Vec<&str> = blocks
        .iter()
        .flat_map(|&(value, count)| std::iter::repeat_n(value, count))
        .collect();
    assert_eq!(values.len(), ROWS);
    (0..ROWS)
        .map(|row| values[(row * ORDER) % ROWS].to_string())
        .collect()
}

struct Expected {
    name: &'static str,
    data_type: DataType,
    forms: TypeHomogeneity,
    locale_numbers: usize,
    zero_padded: usize,
}

fn forms(numeric: usize, date: usize, boolean: usize, text: usize) -> TypeHomogeneity {
    TypeHomogeneity {
        numeric,
        date,
        boolean,
        text,
    }
}

/// The columns and what every value of each makes it.
fn columns() -> Vec<(Vec<String>, Expected)> {
    // Integers with one stray zero-padded code: still quantities.
    let mut codes: Vec<String> = (0..ROWS).map(|row| (1 + row % 900).to_string()).collect();
    codes[30_001] = "007".to_string();
    // Postal codes, exactly 1% of them written with a leading zero: codes.
    let postcodes = placed(&[("10121", 39_600), ("00184", 400)]);
    // Free text holding two locale-formatted numbers late in the column.
    let mut notes: Vec<String> = (0..ROWS).map(|row| format!("item {row}")).collect();
    notes[35_000] = "1.234,56".to_string();
    notes[39_999] = "10,50".to_string();
    vec![
        (
            codes,
            Expected {
                name: "code",
                data_type: DataType::Integer,
                forms: forms(ROWS, 0, 0, 0),
                locale_numbers: 0,
                zero_padded: 1,
            },
        ),
        (
            postcodes,
            Expected {
                name: "postcode",
                data_type: DataType::String,
                forms: forms(ROWS, 0, 0, 0),
                locale_numbers: 0,
                zero_padded: 400,
            },
        ),
        // Exactly 80% numbers: a float column needs more.
        (
            placed(&[("12", 32_000), ("n/a?", 8_000)]),
            Expected {
                name: "numeric_share",
                data_type: DataType::String,
                forms: forms(32_000, 0, 0, 8_000),
                locale_numbers: 0,
                zero_padded: 0,
            },
        ),
        // Exactly 70% dates: a date column needs more.
        (
            placed(&[("2024-03-01", 28_000), ("soon", 12_000)]),
            Expected {
                name: "date_share",
                data_type: DataType::String,
                forms: forms(0, 28_000, 0, 12_000),
                locale_numbers: 0,
                zero_padded: 0,
            },
        ),
        // Exactly 90% booleans: enough for a boolean column.
        (
            placed(&[("true", 36_000), ("maybe", 4_000)]),
            Expected {
                name: "boolean_share",
                data_type: DataType::Boolean,
                forms: forms(0, 0, 36_000, 4_000),
                locale_numbers: 0,
                zero_padded: 0,
            },
        ),
        (
            notes,
            Expected {
                name: "note",
                data_type: DataType::String,
                forms: forms(0, 0, 0, ROWS),
                locale_numbers: 2,
                zero_padded: 0,
            },
        ),
    ]
}

fn csv_fixture(columns: &[(Vec<String>, Expected)]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    let names: Vec<&str> = columns.iter().map(|(_, e)| e.name).collect();
    writeln!(file, "{}", names.join(";")).unwrap();
    for row in 0..ROWS {
        let cells: Vec<&str> = columns.iter().map(|(v, _)| v[row].as_str()).collect();
        writeln!(file, "{}", cells.join(";")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records(columns: &[(Vec<String>, Expected)]) -> Vec<String> {
    (0..ROWS)
        .map(|row| {
            let fields: Vec<String> = columns
                .iter()
                .map(|(values, e)| format!(r#""{}":"{}""#, e.name, values[row]))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

fn json_fixture(columns: &[(Vec<String>, Expected)]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".json").unwrap();
    write!(file, "[{}]", json_records(columns).join(",")).unwrap();
    file.flush().unwrap();
    file
}

fn jsonl_fixture(columns: &[(Vec<String>, Expected)]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(file, "{}", json_records(columns).join("\n")).unwrap();
    file.flush().unwrap();
    file
}

/// Text columns in Parquet are re-inferred from their values, through the
/// record-batch analyzer.
#[cfg(feature = "parquet")]
fn parquet_fixture(columns: &[(Vec<String>, Expected)]) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(_, e)| Field::new(e.name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|(values, _)| Arc::new(StringArray::from(values.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format.
fn reports(columns: &[(Vec<String>, Expected)]) -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture(columns);
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

    let mut files = vec![
        ("json", json_fixture(columns)),
        ("jsonl", jsonl_fixture(columns)),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(columns)));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

#[test]
fn every_route_types_and_classifies_every_value() {
    let columns = columns();
    let mut wrong = Vec::new();
    for (label, report) in reports(&columns) {
        assert_eq!(
            report
                .metric_semantics
                .as_ref()
                .and_then(|semantics| semantics.value_facts),
            Some(dataprof::ValueFacts::EveryValue),
            "[{label}] records value_facts"
        );
        for (_, expected) in &columns {
            let Some(column) = report
                .column_profiles
                .iter()
                .find(|column| column.name == expected.name)
            else {
                wrong.push(format!("[{label}] {}: missing", expected.name));
                continue;
            };
            let got = (
                &column.data_type,
                column.type_homogeneity,
                column.locale_number_count,
                column.zero_padded_count,
            );
            let want = (
                &expected.data_type,
                Some(expected.forms),
                Some(expected.locale_numbers),
                Some(expected.zero_padded),
            );
            if got != want {
                wrong.push(format!(
                    "[{label}] {}: {got:?}, want {want:?}",
                    expected.name
                ));
            }
        }
    }
    // The database connectors profile a column through `analyze_column`,
    // which holds the whole column.
    for (values, expected) in &columns {
        let column = analyze_column(expected.name, values);
        let got = (
            &column.data_type,
            column.type_homogeneity,
            column.locale_number_count,
            column.zero_padded_count,
        );
        let want = (
            &expected.data_type,
            Some(expected.forms),
            Some(expected.locale_numbers),
            Some(expected.zero_padded),
        );
        if got != want {
            wrong.push(format!(
                "[column] {}: {got:?}, want {want:?}",
                expected.name
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn a_full_scan_decides_the_rules_that_read_lexical_forms() {
    // The forms cover every value read, so a file larger than the sample,
    // read in full, decides `mixed_types` and `locale_numbers` instead of
    // leaving them open. Patterns still come from the sample, so
    // `sensitive_pattern` stays open on a column larger than it.
    use dataprof::{FindingCode, QualityPolicy, Verdict};

    let columns = columns();
    let csv = csv_fixture(&columns);
    let report = Profiler::new().analyze_file(csv.path()).expect("profiles");
    let verdict = |code| {
        QualityPolicy::new()
            .forbid_findings([code])
            .evaluate(&report)
            .unwrap()
            .verdict
    };
    let mut wrong = Vec::new();
    for code in [FindingCode::MixedTypes, FindingCode::LocaleNumbers] {
        let got = verdict(code);
        if got == Verdict::Inconclusive {
            wrong.push(format!("{code:?}: {got:?}"));
        }
    }
    let got = verdict(FindingCode::SensitivePattern);
    if got != Verdict::Inconclusive {
        wrong.push(format!("SensitivePattern: {got:?}"));
    }
    // Without `pattern_values_checked` (#1399) the rule falls back to the
    // sample size, and the columns here are past it.
    let mut document = serde_json::to_value(&report).unwrap();
    for column in document["column_profiles"].as_array_mut().unwrap() {
        column
            .as_object_mut()
            .unwrap()
            .remove("pattern_values_checked");
    }
    let uncounted: ProfileReport = serde_json::from_value(document).unwrap();
    let got = QualityPolicy::new()
        .forbid_findings([FindingCode::SensitivePattern])
        .evaluate(&uncounted)
        .unwrap()
        .verdict;
    if got != Verdict::Inconclusive {
        wrong.push(format!("SensitivePattern without the count: {got:?}"));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn the_patterns_own_count_decides_sensitive_pattern() {
    // #1399: a report records how many values its patterns were detected
    // over. Memory pressure can shrink a streaming sample below 10,000, so a
    // 6,000-value column may carry patterns from 5,000 of them; the count
    // says so, and the rule stays open. Patterns over every value decide.
    use dataprof::{FindingCode, QualityPolicy, Verdict};

    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "contact").unwrap();
    for row in 0..6_000 {
        writeln!(file, "user{row}@example.org").unwrap();
    }
    file.flush().unwrap();
    let report = Profiler::new().analyze_file(file.path()).expect("profiles");
    assert_eq!(
        report.column_profiles[0].pattern_values_checked,
        Some(6_000)
    );

    let with_checked = |checked: usize| {
        let mut document = serde_json::to_value(&report).unwrap();
        document["column_profiles"][0]["pattern_values_checked"] = checked.into();
        let report: ProfileReport = serde_json::from_value(document).unwrap();
        QualityPolicy::new()
            .forbid_findings([FindingCode::SensitivePattern])
            .evaluate(&report)
            .unwrap()
            .verdict
    };
    assert_eq!(with_checked(6_000), Verdict::Fail, "every value checked");
    assert_eq!(
        with_checked(5_000),
        Verdict::Inconclusive,
        "a shrunken sample"
    );
}
