//! `infer_schema()` and `profile()` must name the same type for the same
//! column, in every format (#693).
//!
//! A Parquet writer that did not type its input leaves dates, integers and
//! booleans in `Utf8` columns — what any CSV-to-Parquet export produces.
//! `profile()` re-infers those from the values (#661), so a schema read from
//! the file metadata alone disagreed with the full profiler about three of the
//! five columns below, and with `infer_schema()` on the identical data as CSV.

#![cfg(feature = "parquet")]

use std::io::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, Int64Array, StringArray, Time32MillisecondArray};
use arrow::datatypes::{Field, Int8Type, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{DataType, Profiler, analyze_structure, infer_schema};
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

/// Fifty rows, wide enough for the sample to be representative and short enough
/// to stay under every row cap in the partial paths.
const ROWS: usize = 50;

const COLUMNS: [&str; 5] = ["when", "ident", "numish", "boolish", "real_int"];

fn when(row: usize) -> String {
    format!("2026-01-{:02}T03:04:05Z", row % 28 + 1)
}

fn ident(row: usize) -> String {
    format!("ORD-{row}")
}

fn numish(row: usize) -> String {
    (row * 3).to_string()
}

fn boolish(row: usize) -> String {
    if row.is_multiple_of(2) {
        "true"
    } else {
        "false"
    }
    .to_string()
}

fn real_int(row: usize) -> i64 {
    row as i64
}

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", COLUMNS.join(",")).unwrap();
    for row in 0..ROWS {
        writeln!(
            file,
            "{},{},{},{},{}",
            when(row),
            ident(row),
            numish(row),
            boolish(row),
            real_int(row)
        )
        .unwrap();
    }
    file.flush().unwrap();
    file
}

/// The same data as Parquet, with every text column written `Utf8` — the shape
/// a writer produces when its input was untyped.
fn parquet_fixture() -> NamedTempFile {
    let text = |value: fn(usize) -> String| -> ArrayRef {
        Arc::new(StringArray::from(
            (0..ROWS).map(value).collect::<Vec<String>>(),
        ))
    };

    write_parquet(vec![
        ("when", text(when)),
        ("ident", text(ident)),
        ("numish", text(numish)),
        ("boolish", text(boolish)),
        (
            "real_int",
            Arc::new(Int64Array::from(
                (0..ROWS).map(real_int).collect::<Vec<i64>>(),
            )),
        ),
    ])
}

/// The same data again, with the text columns dictionary-encoded — what
/// `pandas.DataFrame.to_parquet` writes for a categorical column. The encoding
/// is not the type, so the schema must not change (#661).
fn dictionary_parquet_fixture() -> NamedTempFile {
    let text = |value: fn(usize) -> String| -> ArrayRef {
        let values: DictionaryArray<Int8Type> = (0..ROWS)
            .map(value)
            .collect::<Vec<String>>()
            .iter()
            .map(|value| Some(value.as_str()))
            .collect();
        Arc::new(values)
    };

    write_parquet(vec![
        ("when", text(when)),
        ("ident", text(ident)),
        ("numish", text(numish)),
        ("boolish", text(boolish)),
        (
            "real_int",
            Arc::new(Int64Array::from(
                (0..ROWS).map(real_int).collect::<Vec<i64>>(),
            )),
        ),
    ])
}

fn write_parquet(columns: Vec<(&str, ArrayRef)>) -> NamedTempFile {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<Field>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn schema_types(file: &NamedTempFile) -> Vec<(String, DataType)> {
    infer_schema(file.path())
        .unwrap()
        .columns
        .into_iter()
        .map(|column| (column.name, column.data_type))
        .collect()
}

fn profile_types(file: &NamedTempFile) -> Vec<(String, DataType)> {
    Profiler::new()
        .analyze_file(file.path())
        .unwrap()
        .column_profiles
        .into_iter()
        .map(|profile| (profile.name, profile.data_type))
        .collect()
}

fn structure_types(file: &NamedTempFile) -> Vec<(String, DataType)> {
    analyze_structure(file.path(), None)
        .unwrap()
        .columns
        .into_iter()
        .map(|column| (column.name, column.data_type))
        .collect()
}

/// What the values say, which is what CSV already reported and what `profile()`
/// reports for every format.
fn expected() -> Vec<(String, DataType)> {
    vec![
        ("when".to_string(), DataType::Date),
        ("ident".to_string(), DataType::String),
        ("numish".to_string(), DataType::Integer),
        ("boolish".to_string(), DataType::Boolean),
        ("real_int".to_string(), DataType::Integer),
    ]
}

/// The headline case: one dataset, two formats, four surfaces, one answer.
///
/// Before the fix the three string-encoded columns reported `string` from
/// `infer_schema()` and `analyze_structure()` on the Parquet file, while
/// `profile()` on the same file and all four surfaces on the CSV reported
/// `date`, `integer` and `boolean`.
#[test]
fn infer_schema_agrees_with_profile_across_csv_and_parquet() {
    let csv = csv_fixture();
    let parquet = parquet_fixture();

    for (label, file) in [("csv", &csv), ("parquet", &parquet)] {
        assert_eq!(schema_types(file), expected(), "infer_schema on {label}");
        assert_eq!(profile_types(file), expected(), "profile on {label}");
        assert_eq!(
            structure_types(file),
            expected(),
            "analyze_structure on {label}"
        );
    }
}

/// A type the schema path answered on its own, differently from the profiler.
///
/// `Time32`/`Time64` had an arm in the schema path's Arrow map (`date`) and
/// none in the profiler's, which renders the values and re-infers them as
/// `string`. Both surfaces read one map now, so the arm cannot exist on one
/// side only — and no row is read to establish it, since the values are not
/// what decided it.
#[test]
fn infer_schema_agrees_with_profile_on_a_time_column() {
    let file = write_parquet(vec![(
        "clock",
        Arc::new(Time32MillisecondArray::from(
            (0..ROWS)
                .map(|row| (row * 1_000) as i32)
                .collect::<Vec<i32>>(),
        )) as ArrayRef,
    )]);

    let expected = vec![("clock".to_string(), DataType::String)];
    assert_eq!(profile_types(&file), expected);
    assert_eq!(schema_types(&file), expected);
    assert_eq!(structure_types(&file), expected);
    assert_eq!(infer_schema(file.path()).unwrap().rows_sampled, 0);
}

/// A zero-row sample of a file that does have text columns is not a settled
/// schema, and must not claim to be. The CSV and JSON paths report the same
/// clause for the same reason.
#[test]
fn a_zero_row_sample_is_not_a_stable_schema() {
    let parquet = parquet_fixture();

    let report = analyze_structure(parquet.path(), Some(0)).unwrap();
    assert_eq!(report.rows_sampled, 0);
    assert!(report.truncated);
    assert!(!report.source_exhausted);
}

/// An empty file is settled, though: there is no row left that could change a
/// type, so nothing is truncated and the text column keeps the type an empty
/// profile reports.
#[test]
fn an_empty_parquet_file_is_stable_without_reading_rows() {
    let empty = write_parquet(vec![
        (
            "id",
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        ),
        (
            "label",
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
        ),
    ]);

    let schema = infer_schema(empty.path()).unwrap();
    assert_eq!(schema.rows_sampled, 0);
    assert!(schema.schema_stable);
    assert_eq!(
        schema
            .columns
            .into_iter()
            .map(|column| (column.name, column.data_type))
            .collect::<Vec<_>>(),
        profile_types(&empty)
    );
}

/// The physical encoding a Parquet writer chose must not reach the schema
/// either: a dictionary is a compression of the values, not a type.
#[test]
fn infer_schema_ignores_dictionary_encoding() {
    let dictionary = dictionary_parquet_fixture();

    assert_eq!(schema_types(&dictionary), expected());
    assert_eq!(profile_types(&dictionary), expected());
}

/// The value sample is bounded, and its cost is disclosed: `rows_sampled`
/// reports the rows read, and `schema_stable` is false once the sample stopped
/// short of the file.
#[test]
fn parquet_schema_reports_the_rows_it_read() {
    let parquet = parquet_fixture();

    let schema = infer_schema(parquet.path()).unwrap();
    assert_eq!(schema.rows_sampled, ROWS);
    assert!(schema.schema_stable, "the whole file was read");

    // The structural pass takes its own cap, and says so rather than reporting
    // a type as if the whole file had backed it.
    let capped = analyze_structure(parquet.path(), Some(10)).unwrap();
    assert_eq!(capped.rows_sampled, 10);
    assert!(capped.truncated);
    assert_eq!(capped.truncation_reason.as_deref(), Some("max_rows(10)"));
    assert!(!capped.source_exhausted);

    // Provenance separates the two sources the file is typed from.
    let provenance: Vec<(String, String)> = capped
        .columns
        .into_iter()
        .map(|column| (column.name, column.provenance))
        .collect();
    assert_eq!(
        provenance,
        vec![
            ("when".to_string(), "sample".to_string()),
            ("ident".to_string(), "sample".to_string()),
            ("numish".to_string(), "sample".to_string()),
            ("boolish".to_string(), "sample".to_string()),
            ("real_int".to_string(), "metadata".to_string()),
        ]
    );
}
