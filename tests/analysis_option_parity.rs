//! Analysis-option parity across formats and transports (#494).
//!
//! `metrics`, `quality_dimensions`, and `locale` are requests about *what to
//! compute*. A caller who deselects quality must get no quality whichever format
//! and transport carried the data, and a locale must reach pattern detection
//! everywhere or nowhere. The JSON, Parquet, and async paths used to accept
//! these options and drop them, so the same logical rows profiled differently
//! depending on how they were stored.
//!
//! Every test here is table-driven over the same five records in four formats.

use std::io::Write;

use dataprof::{
    ColumnStats, EngineType, Locale, MetricPack, ProfileReport, Profiler, QualityDimension,
};
use tempfile::NamedTempFile;

/// Five records with an Italian postal code column. `cap` is what makes locale
/// observable: without a locale both `CAP (IT)` and `ZIP Code (US)` match a
/// five-digit string, and `locale="IT"` must suppress the US pattern.
const RECORDS: [(u32, &str, f64); 5] = [
    (1, "20121", 10.5),
    (2, "00184", 21.0),
    (3, "10121", 33.5),
    (4, "80132", 42.0),
    (5, "50122", 55.5),
];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "id,cap,amount").unwrap();
    for (id, cap, amount) in RECORDS {
        writeln!(file, "{id},{cap},{amount}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    RECORDS
        .iter()
        .map(|(id, cap, amount)| format!(r#"{{"id":{id},"cap":"{cap}","amount":{amount}}}"#))
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

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("cap", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                RECORDS.iter().map(|r| r.0 as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                RECORDS.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                RECORDS.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every format holding the same five records. Parquet joins the table only when
/// the feature is compiled in.
fn fixtures() -> Vec<(&'static str, NamedTempFile)> {
    let mut all = vec![
        ("csv", csv_fixture()),
        ("json", json_fixture()),
        ("jsonl", jsonl_fixture()),
    ];
    #[cfg(feature = "parquet")]
    all.push(("parquet", parquet_fixture()));
    all
}

fn pattern_names(report: &ProfileReport, column: &str) -> Option<Vec<String>> {
    let profile = report
        .column_profiles
        .iter()
        .find(|c| c.name == column)
        .unwrap_or_else(|| panic!("column {column} missing from report"));
    profile
        .patterns
        .as_ref()
        .map(|patterns| patterns.iter().map(|p| p.name.clone()).collect())
}

#[test]
fn column_projection_matches_full_profiles_on_every_format() {
    for (label, file) in fixtures() {
        let full = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] full profiling failed: {e}"));
        let projected = Profiler::new()
            // Deliberately reverse request order: report order is source order.
            .columns(vec!["amount".to_string(), "id".to_string()])
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] projected profiling failed: {e}"));

        let names = projected
            .column_profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["id", "amount"], "[{label}] projection/order");

        for profile in &projected.column_profiles {
            let full_profile = full
                .column_profiles
                .iter()
                .find(|candidate| candidate.name == profile.name)
                .expect("selected column exists in full profile");
            assert_eq!(
                serde_json::to_value(profile).unwrap(),
                serde_json::to_value(full_profile).unwrap(),
                "[{label}] selected column changed under projection: {}",
                profile.name
            );
        }

        let quality = projected
            .quality
            .expect("non-row quality remains available");
        assert!(
            quality.metrics.completeness.is_none(),
            "[{label}] full-row completeness must be withheld under projection"
        );
        assert!(
            quality.metrics.uniqueness.is_none(),
            "[{label}] full-row duplicates must be withheld under projection"
        );
    }
}

#[test]
fn both_csv_engines_apply_the_same_projection() {
    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let file = csv_fixture();
        let report = Profiler::new()
            .engine(engine)
            .columns(vec!["cap".to_string()])
            .analyze_file(file.path())
            .unwrap_or_else(|error| panic!("[{engine:?}] profiling failed: {error}"));
        assert_eq!(
            report
                .column_profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>(),
            ["cap"],
            "{engine:?} ignored the projection"
        );
    }
}

#[test]
fn empty_projection_returns_no_columns_on_every_format() {
    for (label, file) in fixtures() {
        let report = Profiler::new()
            .columns(vec![])
            .analyze_file(file.path())
            .unwrap_or_else(|error| panic!("[{label}] empty projection failed: {error}"));

        assert!(
            report.column_profiles.is_empty(),
            "[{label}] an empty projection must return no profiled columns"
        );
    }
}

#[test]
fn unknown_projected_columns_fail_on_every_format() {
    for (label, file) in fixtures() {
        let error = Profiler::new()
            .columns(vec!["missing".to_string()])
            .analyze_file(file.path())
            .expect_err("unknown selected column must fail");
        assert!(
            matches!(
                error,
                dataprof::DataProfilerError::InvalidConfiguration { .. }
            ),
            "[{label}] unknown projection should remain a typed configuration error: {error}"
        );
        assert!(
            error.to_string().contains("missing"),
            "[{label}] error should name the unknown column: {error}"
        );
    }
}

#[test]
fn schema_pack_omits_statistics_patterns_and_quality_on_every_format() {
    for (label, file) in fixtures() {
        let report = Profiler::new()
            .metric_packs(vec![MetricPack::Schema])
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] profiling failed: {e}"));

        assert!(
            report.quality.is_none(),
            "[{label}] quality must be absent when the quality pack is deselected"
        );
        for profile in &report.column_profiles {
            assert!(
                matches!(profile.stats, ColumnStats::None),
                "[{label}] column {} kept statistics under metrics=[schema]",
                profile.name
            );
            assert!(
                profile.patterns.is_none(),
                "[{label}] column {} kept patterns under metrics=[schema]",
                profile.name
            );
        }
        // Schema itself is still reported — this is a narrowed profile, not an
        // empty one.
        assert_eq!(
            report.column_profiles.len(),
            3,
            "[{label}] schema pack must still report every column"
        );
    }
}

#[test]
fn empty_dimension_selection_yields_absent_quality_on_every_format() {
    for (label, file) in fixtures() {
        let report = Profiler::new()
            .quality_dimensions(vec![])
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] profiling failed: {e}"));

        assert!(
            report.quality.is_none(),
            "[{label}] quality_dimensions=[] must mean 'not analyzed', not an empty assessment"
        );
        assert!(
            report.quality_score().is_none(),
            "[{label}] a report with no quality assessment has no score"
        );
        // Deselecting quality says nothing about the other packs.
        assert!(
            !matches!(
                report
                    .column_profiles
                    .iter()
                    .find(|c| c.name == "amount")
                    .unwrap()
                    .stats,
                ColumnStats::None
            ),
            "[{label}] statistics must survive an empty dimension selection"
        );
    }
}

#[test]
fn a_narrowed_dimension_selection_still_reports_quality() {
    // The counterpart to the test above: asking for *some* dimension is a
    // request to analyze, so quality is present. Only an empty request means
    // "assess nothing".
    for (label, file) in fixtures() {
        let report = Profiler::new()
            .quality_dimensions(vec![QualityDimension::Completeness])
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] profiling failed: {e}"));

        assert!(
            report.quality.is_some(),
            "[{label}] a narrowed dimension selection is still an analysis"
        );
    }
}

#[test]
fn locale_reaches_pattern_detection_on_every_format() {
    let mut without_locale = Vec::new();
    let mut with_locale = Vec::new();

    for (label, file) in fixtures() {
        let plain = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] profiling failed: {e}"));
        let localized = Profiler::new()
            .locale(Locale::It)
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] localized profiling failed: {e}"));

        let plain_patterns = pattern_names(&plain, "cap").expect("patterns detected by default");
        let localized_patterns =
            pattern_names(&localized, "cap").expect("patterns detected by default");

        assert!(
            plain_patterns.iter().any(|p| p.contains("ZIP Code (US)")),
            "[{label}] without a locale the US pattern should still match, got {plain_patterns:?}"
        );
        assert!(
            !localized_patterns
                .iter()
                .any(|p| p.contains("ZIP Code (US)")),
            "[{label}] locale=IT must suppress the US pattern, got {localized_patterns:?}"
        );
        assert!(
            localized_patterns.iter().any(|p| p.contains("CAP (IT)")),
            "[{label}] locale=IT must keep the Italian pattern, got {localized_patterns:?}"
        );

        without_locale.push((label, plain_patterns));
        with_locale.push((label, localized_patterns));
    }

    // Parity, not just presence: every format must reach the same conclusion.
    for table in [&without_locale, &with_locale] {
        let (first_label, first) = &table[0];
        for (label, patterns) in &table[1..] {
            assert_eq!(
                first, patterns,
                "{first_label} and {label} disagree on detected patterns"
            );
        }
    }
}

#[cfg(feature = "async-streaming")]
mod async_transport {
    use super::*;

    /// The async pipeline must apply the same selection the sync one does.
    /// CSV/JSON/JSONL stream through the async reader; Parquet is delegated to
    /// the blocking parser, and that delegation used to drop the options.
    fn profile_async(profiler: Profiler, path: &std::path::Path) -> ProfileReport {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { profiler.profile_file(path).await })
            .expect("async profiling should succeed")
    }

    #[test]
    fn async_paths_honour_the_schema_pack() {
        for (label, file) in fixtures() {
            let report = profile_async(
                Profiler::new().metric_packs(vec![MetricPack::Schema]),
                file.path(),
            );

            assert!(
                report.quality.is_none(),
                "[{label}, async] quality must be absent under metrics=[schema]"
            );
            for profile in &report.column_profiles {
                assert!(
                    matches!(profile.stats, ColumnStats::None),
                    "[{label}, async] column {} kept statistics",
                    profile.name
                );
                assert!(
                    profile.patterns.is_none(),
                    "[{label}, async] column {} kept patterns",
                    profile.name
                );
            }
        }
    }

    #[test]
    fn async_paths_honour_an_empty_dimension_selection() {
        for (label, file) in fixtures() {
            let report = profile_async(Profiler::new().quality_dimensions(vec![]), file.path());
            assert!(
                report.quality.is_none(),
                "[{label}, async] quality_dimensions=[] must yield no quality"
            );
        }
    }

    #[test]
    fn async_paths_honour_the_locale_and_match_the_sync_result() {
        for (label, file) in fixtures() {
            let sync = Profiler::new()
                .locale(Locale::It)
                .analyze_file(file.path())
                .unwrap_or_else(|e| panic!("[{label}] sync profiling failed: {e}"));
            let async_ = profile_async(Profiler::new().locale(Locale::It), file.path());

            assert_eq!(
                pattern_names(&sync, "cap"),
                pattern_names(&async_, "cap"),
                "[{label}] sync and async disagree on locale-ranked patterns"
            );
            assert!(
                !pattern_names(&async_, "cap")
                    .unwrap()
                    .iter()
                    .any(|p| p.contains("ZIP Code (US)")),
                "[{label}, async] locale=IT must suppress the US pattern"
            );
        }
    }
}
