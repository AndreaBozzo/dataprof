//! The physical encoding a writer chose must not change the profile (#647).
//!
//! `pandas.DataFrame.to_parquet` on a categorical column writes
//! `Dictionary(Int8, Utf8)`; pyarrow writes `Utf8View` for `string_view`. Both
//! hold the values a different writer would have put in a plain `Utf8` column,
//! and all three have to profile the same way.

#![cfg(feature = "parquet")]

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Float32Array, Int32Array,
    LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{ColumnProfile, ColumnStats, DataType, Profiler};
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

/// Sixty values that are digits, so a text encoding has something to be
/// re-inferred *into* and a failure to re-infer is visible as `String`.
fn digits() -> Vec<String> {
    (0..60).map(|i| (i % 9 + 1).to_string()).collect()
}

fn write(array: ArrayRef) -> NamedTempFile {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn profile(array: ArrayRef) -> ColumnProfile {
    let file = write(array);
    let report = Profiler::new()
        .analyze_file(file.path())
        .expect("profile should succeed");
    report.column_profiles.into_iter().next().unwrap()
}

/// Everything a profile says about a column, so a divergence anywhere in it
/// fails rather than only the fields a test remembered to name.
fn observable(profile: &ColumnProfile) -> String {
    format!(
        "type={:?} total={} null={} invalid={:?} unique={:?} stats={:?}",
        profile.data_type,
        profile.total_count,
        profile.null_count,
        profile.invalid_count,
        profile.unique_count,
        profile.stats,
    )
}

#[track_caller]
fn assert_same_profile(baseline_name: &str, baseline: ArrayRef, cases: Vec<(&str, ArrayRef)>) {
    let expected = profile(baseline);
    for (name, array) in cases {
        let actual = profile(array);
        assert_eq!(
            observable(&actual),
            observable(&expected),
            "{name} profiles differently from {baseline_name}"
        );
    }
}

/// The headline case: the same sixty digit strings, written five ways.
///
/// Before the fix `plain` re-inferred as `integer` with a numeric block while
/// `dictionary` and `string_view` reported `string`, length statistics, and
/// `invalid_count` absent rather than `0`.
#[test]
fn every_string_encoding_profiles_as_the_plain_column_does() {
    let values = digits();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();

    let baseline: ArrayRef = Arc::new(StringArray::from(refs.clone()));
    assert_eq!(
        profile(Arc::clone(&baseline)).data_type,
        DataType::Integer,
        "the baseline must re-infer, or this test proves nothing"
    );

    let mut dictionary =
        arrow::array::builder::StringDictionaryBuilder::<arrow::datatypes::Int8Type>::new();
    for value in &refs {
        dictionary.append_value(value);
    }

    assert_same_profile(
        "Utf8",
        baseline,
        vec![
            ("LargeUtf8", Arc::new(LargeStringArray::from(refs.clone()))),
            ("Utf8View", Arc::new(StringViewArray::from(refs.clone()))),
            ("Dictionary(Int8, Utf8)", Arc::new(dictionary.finish())),
        ],
    );
}

/// A dictionary of a *numeric* value type must profile as that numeric type,
/// not as its rendering. This one reported `string` with length statistics.
#[test]
fn a_dictionary_of_integers_profiles_as_integers() {
    let values: Vec<i32> = (0..60).map(|i| i % 9 + 1).collect();

    let mut dictionary = arrow::array::builder::PrimitiveDictionaryBuilder::<
        arrow::datatypes::Int8Type,
        arrow::datatypes::Int32Type,
    >::new();
    for value in &values {
        dictionary.append_value(*value);
    }

    assert_same_profile(
        "Int32",
        Arc::new(Int32Array::from(values)),
        vec![("Dictionary(Int8, Int32)", Arc::new(dictionary.finish()))],
    );
}

/// `Float16` had no arm, so it fell through to text and reported length
/// statistics for a numeric column. Every `f16` is exact in `f32`.
#[test]
fn half_precision_profiles_as_the_float_it_is() {
    let values: Vec<f32> = (0..60).map(|i| (i % 9 + 1) as f32).collect();
    let plain: ArrayRef = Arc::new(Float32Array::from(values));
    // Cast rather than reach for `half` directly: these values are all exact in
    // f16, so the round trip changes nothing.
    let halves =
        arrow::compute::kernels::cast::cast(&plain, &arrow::datatypes::DataType::Float16).unwrap();

    assert_same_profile("Float32", plain, vec![("Float16", halves)]);
}

/// The two binary layouts disagreed: the same three bytes reported `max_length`
/// 6 through the generic fallback and 10 through the `Binary` arm, because the
/// two paths measured different renderings. What a binary column *should*
/// report is #645; this asserts only that one answer reaches both.
#[test]
fn the_binary_layouts_agree_with_each_other() {
    let values: Vec<&[u8]> = (0..60).map(|_| b"abc".as_slice()).collect();

    assert_same_profile(
        "Binary",
        Arc::new(BinaryArray::from(values.clone())),
        vec![("BinaryView", Arc::new(BinaryViewArray::from(values)))],
    );
}

/// A time of day carries no date, so it stays text — but it must be *re-inferred*
/// text rather than text by default, and its date-match count must be a real
/// zero rather than a count that was never taken.
#[test]
fn a_time_of_day_is_text_because_it_is_not_a_date() {
    let times: Vec<i64> = (0..60).map(|i| (i % 24) * 3_600_000_000).collect();
    let array: ArrayRef = Arc::new(arrow::array::Time64MicrosecondArray::from(times));

    let profile = profile(array);
    assert_eq!(profile.data_type, DataType::String);
    assert!(
        matches!(profile.stats, ColumnStats::Text(_)),
        "expected text statistics, got {:?}",
        profile.stats
    );
}

/// Re-inferring a column from its rendering is only sound when the rendering is
/// the value. `FixedSizeBinary` formats as **hex**, so reading the samples back
/// as data turns the three bytes `abc` into the integer 616263 and reports a
/// numeric column with that mean — a decode artifact become a plausible number,
/// which is the one thing a profiler must never do.
#[test]
fn a_binary_rendering_is_never_re_inferred_as_the_number_it_spells() {
    let values: Vec<&[u8]> = (0..60).map(|_| b"abc".as_slice()).collect();
    let array: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap());

    let profile = profile(array);
    assert_eq!(
        profile.data_type,
        DataType::String,
        "a hex rendering was read back as data"
    );
    assert!(
        matches!(profile.stats, ColumnStats::Text(_)),
        "expected text statistics, got {:?}",
        profile.stats
    );
}
