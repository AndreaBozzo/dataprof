//! A struct, list or map column is not analyzed beyond its counts (#637).
//!
//! Before this, a nested Parquet column was profiled as Arrow's display string
//! (`{city: Rome, zip: 100}`), while the JSON path profiled the same record as
//! compact JSON (`{"city":"Rome","zip":100}`). The lengths, distinct counts and
//! patterns of a container therefore depended on the file format it arrived in,
//! and described a serialisation rather than the data in either.

#![cfg(feature = "parquet")]

use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeListArray, ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{ColumnProfile, ColumnStats, DataType, ProfileReport, Profiler};
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

/// The same four records, as JSON Lines.
///
/// Row 2 has a null struct and a null list; row 3 has a struct holding a null
/// child and an empty list. A null child is where the two renderings used to
/// disagree most: `null` in JSON, the empty string in Arrow's formatter.
///
/// `lat` puts a `.` and a `,` into each struct's JSON text, which the format
/// check over sampled values counts as a mixed-separator violation. Only the
/// JSON path samples that text, so the quality comparison below fails unless
/// container values are kept out of the value-level checks.
const RECORDS: &str = r#"{"id": 1, "address": {"city": "Rome", "zip": 100, "lat": 41.9}, "tags": ["a", "b"]}
{"id": 2, "address": null, "tags": null}
{"id": 3, "address": {"city": null, "zip": 200, "lat": 45.4}, "tags": []}
{"id": 4, "address": {"city": "Milan", "zip": 300, "lat": 45.5}, "tags": ["c"]}
"#;

/// The same records as one Arrow batch.
fn batch() -> RecordBatch {
    let address_fields = Fields::from(vec![
        Field::new("city", ArrowDataType::Utf8, true),
        Field::new("zip", ArrowDataType::Int64, true),
        Field::new("lat", ArrowDataType::Float64, true),
    ]);
    let address = StructArray::new(
        address_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("Rome"),
                None,
                None,
                Some("Milan"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(100),
                None,
                Some(200),
                Some(300),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(41.9),
                None,
                Some(45.4),
                Some(45.5),
            ])) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    );

    let tag_field = Arc::new(Field::new("item", ArrowDataType::Utf8, true));
    let tags = ListArray::new(
        Arc::clone(&tag_field),
        OffsetBuffer::from_lengths([2, 0, 0, 1]),
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
        Some(NullBuffer::from(vec![true, false, true, true])),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("address", ArrowDataType::Struct(address_fields), true),
        Field::new("tags", ArrowDataType::List(tag_field), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(address),
            Arc::new(tags),
        ],
    )
    .unwrap()
}

fn write_parquet(batch: &RecordBatch) -> NamedTempFile {
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    file
}

fn write_jsonl(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn profile(path: &std::path::Path) -> ProfileReport {
    Profiler::new()
        .analyze_file(path)
        .expect("profile should succeed")
}

fn column<'a>(report: &'a ProfileReport, name: &str) -> &'a ColumnProfile {
    report
        .column_profiles
        .iter()
        .find(|profile| profile.name == name)
        .unwrap_or_else(|| panic!("no column {name}"))
}

/// Everything a profile says about a column, not a hand-picked subset.
fn observable(profile: &ColumnProfile) -> String {
    format!("{profile:?}")
}

#[track_caller]
fn assert_counts_only(profile: &ColumnProfile, total: usize, nulls: usize) {
    assert_eq!(profile.data_type, DataType::Nested, "{}", profile.name);
    assert_eq!(profile.total_count, total, "{}", profile.name);
    assert_eq!(profile.null_count, nulls, "{}", profile.name);
    assert_eq!(profile.unique_count, None, "{}", profile.name);
    assert_eq!(
        profile.unique_count_is_approximate, None,
        "{}",
        profile.name
    );
    assert_eq!(profile.invalid_count, None, "{}", profile.name);
    assert_eq!(profile.type_homogeneity, None, "{}", profile.name);
    assert!(
        matches!(profile.stats, ColumnStats::None),
        "{}: {:?}",
        profile.name,
        profile.stats
    );
    assert!(profile.patterns.is_none(), "{}", profile.name);
}

/// The same records profile identically from Parquet and from JSON Lines.
#[test]
fn nested_columns_profile_the_same_from_parquet_and_json() {
    let parquet = write_parquet(&batch());
    let jsonl = write_jsonl(RECORDS);
    let from_parquet = profile(parquet.path());
    let from_json = profile(jsonl.path());

    for name in ["id", "address", "tags"] {
        assert_eq!(
            observable(column(&from_parquet, name)),
            observable(column(&from_json, name)),
            "{name} profiles differently from Parquet and from JSON"
        );
    }
    assert_counts_only(column(&from_parquet, "address"), 4, 1);
    assert_counts_only(column(&from_parquet, "tags"), 4, 1);

    let quality = |report: &ProfileReport| {
        serde_json::to_value(report.quality.as_ref().map(|quality| &quality.metrics)).unwrap()
    };
    assert_eq!(quality(&from_parquet), quality(&from_json));
}

/// Every container shape is counted and nothing more, a null container
/// included. Only JSON's two shapes exist on the JSON path; the rest can only
/// arrive typed.
#[test]
fn every_container_shape_is_counted_and_not_analyzed() {
    let large_list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    let vectors = FixedSizeListArray::new(
        Arc::new(Field::new("item", ArrowDataType::Float32, true)),
        2,
        Arc::new(Float32Array::from(vec![0.1, 0.2, 0.0, 0.0, 0.5, 0.6])),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let map = {
        let mut builder = arrow::array::MapBuilder::new(
            None,
            arrow::array::StringBuilder::new(),
            arrow::array::Int32Builder::new(),
        );
        builder.keys().append_value("a");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.keys().append_value("b");
        builder.values().append_value(2);
        builder.append(true).unwrap();
        builder.finish()
    };
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3]))),
        ("large_list", Arc::new(large_list)),
        ("vectors", Arc::new(vectors)),
        ("map", Arc::new(map) as Arc<MapArray> as ArrayRef),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema,
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap();

    let parquet = write_parquet(&batch);
    let report = profile(parquet.path());
    for name in ["large_list", "vectors", "map"] {
        assert_counts_only(column(&report, name), 3, 1);
    }
    assert_eq!(column(&report, "id").data_type, DataType::Integer);
}

/// A JSON column that mixes containers with scalars has no typed twin, so it
/// stays text. Only a column whose every non-null value is a container is
/// `Nested`.
#[test]
fn a_json_column_mixing_containers_and_scalars_stays_text() {
    let jsonl = write_jsonl(
        "{\"v\": {\"a\": 1}, \"w\": [1]}\n{\"v\": \"plain\", \"w\": null}\n{\"v\": null, \"w\": \"\"}\n",
    );
    let report = profile(jsonl.path());
    assert_eq!(column(&report, "v").data_type, DataType::String);
    // An empty string is null-like, so `w` holds only containers.
    assert_counts_only(column(&report, "w"), 3, 2);
}

/// An identifier hint types text, so it cannot bind to a container. Both paths
/// refuse it the same way, rather than one of them bringing back the
/// statistics of a rendering.
#[test]
fn an_identifier_hint_on_a_container_is_refused_on_every_path() {
    let parquet = write_parquet(&batch());
    let jsonl = write_jsonl(RECORDS);
    let hinted = |path: &std::path::Path| {
        Profiler::new()
            .identifier_columns(vec!["address".to_string()])
            .analyze_file(path)
            .expect_err("a hint that binds to nothing is refused")
            .to_string()
    };
    let from_parquet = hinted(parquet.path());
    assert!(
        from_parquet.contains("'address' (identifier_columns): 0 of 3 value(s) matched"),
        "{from_parquet}"
    );
    assert_eq!(from_parquet, hinted(jsonl.path()));
}
