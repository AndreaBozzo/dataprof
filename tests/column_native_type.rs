//! Each column records the type its source declared (#906).
//!
//! A report kept only the type dataprof profiled a column as, so an Arrow
//! `decimal128(12, 2)` read as `Float` and a `timestamp[us, tz=UTC]` as `Date`,
//! and nothing said the column had kept its declared type. Each column now
//! carries `native_type`: the declaration, in the spelling of the type system
//! it belongs to, or `None` where the source declares none.
//!
//! The database connectors are covered in `crates/dataprof-db/tests/native_types.rs`,
//! the remote Parquet reader in `tests/remote_parquet_versions.rs`.
//! `python/tests/test_column_native_type.py` is the Python twin.

use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Int64Array, ListArray,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{EngineType, NativeType, ProfileReport, Profiler};
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

/// Each column as the Arrow writer declares it, and the name pyarrow prints
/// for that declaration.
const DECLARED: [(&str, &str); 9] = [
    ("amount", "decimal128(12, 2)"),
    ("ordered_at", "timestamp[us, tz=UTC]"),
    ("id", "int64"),
    ("name", "string"),
    (
        "status",
        "dictionary<values=string, indices=int32, ordered=0>",
    ),
    ("tags", "list<item: int64>"),
    ("point", "struct<x: int64, label: string not null>"),
    ("day", "date32[day]"),
    ("flag", "bool"),
];

fn batch() -> RecordBatch {
    let amount: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(1234), None])
            .with_precision_and_scale(12, 2)
            .unwrap(),
    );
    let ordered_at: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(1_767_225_600_000_000), None])
            .with_timezone("UTC"),
    );
    let status: DictionaryArray<Int32Type> = vec!["new", "done"].into_iter().collect();
    let tags =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1), Some(2)]), None]);
    let point = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        amount,
        ordered_at,
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec![Some("a"), None])),
        Arc::new(status),
        Arc::new(tags),
        Arc::new(point),
        Arc::new(Date32Array::from(vec![Some(20_454), None])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
    ];
    let fields: Fields = DECLARED
        .iter()
        .zip(&columns)
        .map(|((name, _), column)| Field::new(*name, column.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn parquet_file() -> NamedTempFile {
    let batch = batch();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every column whose recorded declaration is not `expected`'s, named with
/// its route, so one failing run shows each wrong (route, column).
fn mismatches(
    route: &str,
    report: &ProfileReport,
    expected: impl Fn(&str) -> Option<NativeType>,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for column in &report.column_profiles {
        let want = expected(&column.name);
        if column.native_type != want {
            wrong.push(format!(
                "{route}, {}: {:?} != {want:?}",
                column.name, column.native_type
            ));
        }
    }
    if report.column_profiles.is_empty() {
        wrong.push(format!("{route}: no columns"));
    }
    wrong
}

fn arrow_declared(column: &str) -> Option<NativeType> {
    DECLARED
        .iter()
        .find(|(name, _)| *name == column)
        .map(|(_, type_name)| NativeType::new(NativeType::ARROW, *type_name))
}

#[test]
fn a_parquet_column_records_its_declared_arrow_type() {
    // The byte-buffer route reads through the same parser; the Python twin
    // profiles it.
    let file = parquet_file();
    let report = Profiler::new()
        .analyze_file(file.path())
        .expect("parquet file");
    let wrong = mismatches("parquet file", &report, arrow_declared);
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn text_sources_declare_no_type() {
    let csv = "id,amount,ordered_at\n1,12.34,2026-01-01\n2,5.00,\n";
    let jsonl = "{\"id\": 1, \"amount\": 12.34}\n{\"id\": 2, \"amount\": 5.0}\n";
    let mut wrong = Vec::new();
    for (suffix, text) in [(".csv", csv), (".jsonl", jsonl), (".json", "[{\"id\": 1}]")] {
        let mut file = NamedTempFile::with_suffix(suffix).unwrap();
        file.write_all(text.as_bytes()).unwrap();
        file.flush().unwrap();
        let engines: &[EngineType] = if suffix == ".csv" {
            &[
                EngineType::Auto,
                EngineType::Incremental,
                EngineType::Columnar,
            ]
        } else {
            &[EngineType::Auto]
        };
        for engine in engines {
            let report = Profiler::new()
                .engine(*engine)
                .analyze_file(file.path())
                .unwrap_or_else(|error| panic!("{suffix} {engine:?}: {error}"));
            wrong.extend(mismatches(&format!("{suffix} {engine:?}"), &report, |_| {
                None
            }));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn both_dialects_carry_the_declaration_and_an_old_report_reads_as_unrecorded() {
    let file = parquet_file();
    let report = Profiler::new().analyze_file(file.path()).expect("parquet");

    let document: serde_json::Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let mut wrong = Vec::new();
    for (dialect, columns) in [
        ("document", &document["column_profiles"]),
        ("summary", &summary["columns"]),
    ] {
        for column in columns.as_array().unwrap() {
            let name = column["name"].as_str().unwrap();
            let (_, type_name) = DECLARED.iter().find(|(n, _)| *n == name).unwrap();
            let want = serde_json::json!({"system": "arrow", "name": type_name});
            if column["native_type"] != want {
                wrong.push(format!("{dialect}, {name}: {}", column["native_type"]));
            }
        }
    }

    let back: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    wrong.extend(mismatches("document read back", &back, arrow_declared));

    let mut old = document;
    for column in old["column_profiles"].as_array_mut().unwrap() {
        column.as_object_mut().unwrap().remove("native_type");
    }
    let old: ProfileReport = serde_json::from_value(old).unwrap();
    wrong.extend(mismatches("document without the field", &old, |_| None));
    let rewritten: serde_json::Value = serde_json::from_str(&old.to_json().unwrap()).unwrap();
    if rewritten.to_string().contains("native_type") {
        wrong.push("an unrecorded declaration was written back".to_string());
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
