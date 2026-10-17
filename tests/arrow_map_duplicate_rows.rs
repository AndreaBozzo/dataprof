//! Duplicate rows holding an Arrow map (#943).
//!
//! A row signature encodes a nested value in Arrow's row format, which writes
//! a map's entries in stored order. Arrow and Parquet do not give that order a
//! meaning, so two maps holding the same entries in a different order are the
//! same value, and two rows that differ only in it are duplicates. The JSON
//! readers already compare objects with their keys sorted (#767).
//!
//! `python/tests/test_arrow_map_duplicate_rows.py` is the Python twin.
#![cfg(feature = "parquet")]

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{ProfileReport, Profiler};
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

type Entries<'a> = &'a [(&'a str, i64)];

/// One map per row; `None` is a null map.
fn map_column(rows: &[Option<Entries>]) -> ArrayRef {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in *entries {
                    builder.keys().append_value(key);
                    builder.values().append_value(*value);
                }
                builder.append(true).unwrap();
            }
            None => builder.append(false).unwrap(),
        }
    }
    Arc::new(builder.finish())
}

/// One list of maps per row.
fn list_of_maps(rows: &[&[Entries]]) -> ArrayRef {
    let map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    let mut builder = ListBuilder::new(map);
    for maps in rows {
        for entries in *maps {
            for (key, value) in *entries {
                builder.values().keys().append_value(key);
                builder.values().values().append_value(*value);
            }
            builder.values().append(true).unwrap();
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

/// One struct `{m: map}` per row.
fn struct_of_map(rows: &[Entries]) -> ArrayRef {
    let maps = map_column(&rows.iter().copied().map(Some).collect::<Vec<_>>());
    let field = Arc::new(Field::new("m", maps.data_type().clone(), true));
    Arc::new(StructArray::from(vec![(field, maps)]))
}

/// One map per row whose values are maps.
fn map_of_maps(rows: &[&[(&str, Entries)]]) -> ArrayRef {
    let inner = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    let mut builder = MapBuilder::new(None, StringBuilder::new(), inner);
    for entries in rows {
        for (key, inner_entries) in *entries {
            builder.keys().append_value(key);
            for (inner_key, value) in *inner_entries {
                builder.values().keys().append_value(inner_key);
                builder.values().values().append_value(*value);
            }
            builder.values().append(true).unwrap();
        }
        builder.append(true).unwrap();
    }
    Arc::new(builder.finish())
}

/// A case: the `value` column beside an `id` of 1 on every row, and the
/// duplicate rows it holds.
struct Case {
    name: &'static str,
    value: ArrayRef,
    duplicates: usize,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "entries in a different order",
            value: map_column(&[Some(&[("a", 1), ("b", 2)]), Some(&[("b", 2), ("a", 1)])]),
            duplicates: 1,
        },
        Case {
            name: "same keys, different values",
            value: map_column(&[Some(&[("a", 1), ("b", 2)]), Some(&[("b", 1), ("a", 2)])]),
            duplicates: 0,
        },
        Case {
            name: "a null map against an empty map",
            value: map_column(&[None, Some(&[])]),
            duplicates: 0,
        },
        Case {
            name: "maps inside a list",
            value: list_of_maps(&[
                &[&[("a", 1), ("b", 2)], &[("c", 3)]],
                &[&[("b", 2), ("a", 1)], &[("c", 3)]],
            ]),
            duplicates: 1,
        },
        Case {
            name: "a map inside a struct",
            value: struct_of_map(&[&[("a", 1), ("b", 2)], &[("b", 2), ("a", 1)]]),
            duplicates: 1,
        },
        Case {
            name: "a map inside a map value",
            value: map_of_maps(&[
                &[("k", &[("a", 1), ("b", 2)]), ("j", &[])],
                &[("j", &[]), ("k", &[("b", 2), ("a", 1)])],
            ]),
            duplicates: 1,
        },
    ]
}

fn parquet(value: &ArrayRef) -> NamedTempFile {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1; value.len()]));
    let schema = Arc::new(Schema::new(Fields::from(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", value.data_type().clone(), true),
    ])));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, value.clone()]).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn duplicates(report: &ProfileReport) -> Option<usize> {
    report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.uniqueness.as_ref())
        .map(|uniqueness| uniqueness.duplicate_rows)
}

/// Every wrong case at once, so one run shows which shapes the fix reaches.
#[test]
fn maps_compare_by_their_entries_not_their_order() {
    let mut wrong = Vec::new();
    for case in cases() {
        let file = parquet(&case.value);
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{}] {e}", case.name));
        let found = duplicates(&report);
        if found != Some(case.duplicates) {
            wrong.push(format!(
                "{}: {found:?}, expected {}",
                case.name, case.duplicates
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "wrong duplicate counts:\n{}",
        wrong.join("\n")
    );
}
