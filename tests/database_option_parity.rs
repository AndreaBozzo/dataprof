//! Analysis-option parity for database queries (#536).
//!
//! #494 made `metrics`, `quality_dimensions`, and `locale` take effect on every
//! file format and transport. The database path was left out of that pass and
//! kept ignoring all three: it hardcoded quality on and called pattern
//! detection with no locale, so a query and a CSV holding the same rows
//! disagreed about what had been analyzed.
//!
//! These tests profile the same five records from SQLite and from a CSV file
//! and require the two to agree.

#![cfg(feature = "sqlite")]

use std::io::Write;

use dataprof::{
    ColumnStats, DatabaseConfig, MetricPack, ProfileReport, Profiler, QualityDimension,
    SemanticHints,
};
use tempfile::NamedTempFile;

/// `cap` is what makes locale observable: a five-digit string matches both
/// `CAP (IT)` and `ZIP Code (US)`, and `locale="IT"` must suppress the US
/// pattern. `amount` gives the fixture a numeric column to carry statistics.
const RECORDS: [(i64, &str, f64); 5] = [
    (1, "20121", 10.5),
    (2, "00184", 21.0),
    (3, "10121", 33.5),
    (4, "80132", 42.0),
    (5, "50122", 55.5),
];

const QUERY: &str = "SELECT * FROM parity";

/// A SQLite file holding [`RECORDS`]. The `TempDir` must outlive the profiling
/// call, so it is returned alongside the path.
async fn sqlite_fixture() -> (tempfile::TempDir, String) {
    use sqlx::sqlite::SqlitePoolOptions;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("parity.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{db_path}"))
        .await
        .unwrap();

    sqlx::query("CREATE TABLE parity (id INTEGER, cap TEXT, amount REAL)")
        .execute(&pool)
        .await
        .unwrap();
    for (id, cap, amount) in RECORDS {
        sqlx::query("INSERT INTO parity (id, cap, amount) VALUES (?, ?, ?)")
            .bind(id)
            .bind(cap)
            .bind(amount)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    (dir, db_path)
}

/// The same records as a CSV, so the database result can be checked against a
/// path that already honours the whole selection.
fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "id,cap,amount").unwrap();
    for (id, cap, amount) in RECORDS {
        writeln!(file, "{id},{cap},{amount}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn sqlite_profiler(db_path: &str) -> Profiler {
    Profiler::new().database(DatabaseConfig {
        connection_string: db_path.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    })
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

#[tokio::test]
async fn schema_pack_omits_statistics_patterns_and_quality() {
    let (_dir, db_path) = sqlite_fixture().await;

    let report = sqlite_profiler(&db_path)
        .metric_packs(vec![MetricPack::Schema])
        .analyze_query(QUERY)
        .await
        .expect("query profiling should succeed");

    assert!(
        report.quality.is_none(),
        "quality must be absent when the quality pack is deselected"
    );
    for profile in &report.column_profiles {
        assert!(
            matches!(profile.stats, ColumnStats::None),
            "column {} kept statistics under metrics=[schema]",
            profile.name
        );
        assert!(
            profile.patterns.is_none(),
            "column {} kept patterns under metrics=[schema]",
            profile.name
        );
    }
    // Schema itself is still reported — a narrowed profile, not an empty one.
    assert_eq!(report.column_profiles.len(), 3);
}

#[tokio::test]
async fn empty_dimension_selection_yields_absent_quality() {
    let (_dir, db_path) = sqlite_fixture().await;

    let report = sqlite_profiler(&db_path)
        .quality_dimensions(vec![])
        .analyze_query(QUERY)
        .await
        .expect("query profiling should succeed");

    assert!(
        report.quality.is_none(),
        "quality_dimensions=[] must mean 'not analyzed', not an empty assessment"
    );
    assert!(
        report.quality_score().is_none(),
        "a report with no quality assessment has no score"
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
        "statistics must survive an empty dimension selection"
    );
}

#[tokio::test]
async fn a_narrowed_dimension_selection_still_reports_quality() {
    let (_dir, db_path) = sqlite_fixture().await;

    let report = sqlite_profiler(&db_path)
        .quality_dimensions(vec![QualityDimension::Completeness])
        .analyze_query(QUERY)
        .await
        .expect("query profiling should succeed");

    assert!(
        report.quality.is_some(),
        "a narrowed dimension selection is still an analysis"
    );
}

#[tokio::test]
async fn locale_reaches_pattern_detection_and_matches_csv() {
    let (_dir, db_path) = sqlite_fixture().await;
    let csv = csv_fixture();

    let plain = sqlite_profiler(&db_path)
        .analyze_query(QUERY)
        .await
        .expect("query profiling should succeed");
    let localized = sqlite_profiler(&db_path)
        .locale("IT")
        .analyze_query(QUERY)
        .await
        .expect("localized query profiling should succeed");

    let plain_patterns = pattern_names(&plain, "cap").expect("patterns detected by default");
    let localized_patterns =
        pattern_names(&localized, "cap").expect("patterns detected by default");

    assert!(
        plain_patterns.iter().any(|p| p.contains("ZIP Code (US)")),
        "without a locale the US pattern should still match, got {plain_patterns:?}"
    );
    assert!(
        !localized_patterns
            .iter()
            .any(|p| p.contains("ZIP Code (US)")),
        "locale=IT must suppress the US pattern, got {localized_patterns:?}"
    );

    // The product contract: the same rows profile the same way whether they
    // came out of a query or off disk.
    let csv_localized = Profiler::new()
        .locale("IT")
        .analyze_file(csv.path())
        .expect("csv profiling should succeed");
    assert_eq!(
        pattern_names(&localized, "cap"),
        pattern_names(&csv_localized, "cap"),
        "a query and a CSV holding the same rows disagree on locale-ranked patterns"
    );
}

#[tokio::test]
async fn no_quality_entry_point_keeps_statistics_and_patterns() {
    // `analyze_query_no_quality` is now "the configured selection minus the
    // quality pack", so it must still narrow nothing else — and must still
    // honour a locale.
    let (_dir, db_path) = sqlite_fixture().await;

    let report = sqlite_profiler(&db_path)
        .locale("IT")
        .analyze_query_no_quality(QUERY)
        .await
        .expect("query profiling should succeed");

    assert!(report.quality.is_none(), "this entry point skips quality");
    let patterns = pattern_names(&report, "cap").expect("patterns still detected");
    assert!(
        !patterns.iter().any(|p| p.contains("ZIP Code (US)")),
        "locale must still apply without the quality pack, got {patterns:?}"
    );
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
        "statistics must survive skipping quality"
    );
}

/// A connection string that is recognised as SQLite but cannot be opened, so
/// any error other than the expected one proves the call got further than it
/// should have.
const UNOPENABLE_DB: &str = "/nonexistent/path/to.db";

#[tokio::test]
async fn semantic_hints_are_rejected_before_connecting() {
    // The connectors cannot apply hints, so asking for them is an error rather
    // than a silent no-op — and the rejection must come before anything is
    // opened.
    let profiler = Profiler::new()
        .database(DatabaseConfig {
            connection_string: UNOPENABLE_DB.to_string(),
            load_credentials_from_env: false,
            ..Default::default()
        })
        .positive_columns(vec!["amount".to_string()]);

    let error = profiler
        .analyze_query(QUERY)
        .await
        .expect_err("semantic hints are unsupported for database profiling");
    assert!(
        error.to_string().contains("positive_columns"),
        "expected the hint rejection, got: {error}"
    );
}

#[tokio::test]
async fn the_database_entry_point_rejects_hints_itself() {
    // The check lives at the boundary that lacks the capability, not only in
    // `Profiler`. `analyze_database_with_options` is public, so a caller
    // reaching it directly must get the same error rather than a profile with
    // the hints silently dropped.
    let options = dataprof::AnalysisOptions::default()
        .with_semantic_hints(SemanticHints::new(vec!["amount".to_string()], vec![]));
    let config = DatabaseConfig {
        connection_string: UNOPENABLE_DB.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    };

    let error = dataprof::analyze_database_with_options(config, QUERY, &options)
        .await
        .expect_err("semantic hints are unsupported for database profiling");
    assert!(
        error.to_string().contains("positive_columns"),
        "expected the hint rejection, got: {error}"
    );
}
