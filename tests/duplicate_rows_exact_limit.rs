//! A configurable exact limit for the duplicate-row count (#1292).
//!
//! Past one million distinct rows the row tracker's count became an estimate,
//! and an estimate witnesses no duplicate, so `max_duplicate_rows` could not
//! fail on any large source. A caller can now set the limit as a row count:
//! every route that counts whole rows honours it, the memory budget does not
//! cut it short, the report records it next to the approximate flag, and a
//! check left undecided by an estimate names the setting.
//!
//! `python/tests/test_duplicate_rows_exact_limit.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckStatus, EngineType, FindingCode, ProfileReport, Profiler, QualityPolicy, UniquenessMetrics,
};
use tempfile::NamedTempFile;

/// Distinct rows in the small source, and how many of them appear twice.
const DISTINCT: usize = 3_000;
const DUPLICATES: usize = 40;
/// Under the small source's distinct rows, so the count must be estimated.
const LOW_LIMIT: usize = 1_000;
/// Over them, so the count must be exact.
const HIGH_LIMIT: usize = 5_000;
const DEFAULT_LIMIT: usize = 1_000_000;

/// `id,label` rows `0..distinct`, then rows `0..duplicates` again.
fn csv_text(distinct: usize, duplicates: usize) -> String {
    let mut text = String::with_capacity((distinct + duplicates) * 16);
    text.push_str("id,label\n");
    for id in (0..distinct).chain(0..duplicates) {
        text.push_str(&format!("{id},row-{id}\n"));
    }
    text
}

fn json_records() -> Vec<String> {
    (0..DISTINCT)
        .chain(0..DUPLICATES)
        .map(|id| format!(r#"{{"id":{id},"label":"row-{id}"}}"#))
        .collect()
}

fn file(suffix: &str, contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_file() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let ids: Vec<i64> = (0..DISTINCT as i64).chain(0..DUPLICATES as i64).collect();
    let labels: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn profiler(limit: Option<usize>) -> Profiler {
    match limit {
        Some(limit) => Profiler::new().duplicate_rows_exact_limit(limit),
        None => Profiler::new(),
    }
}

/// Every engine over the CSV, every other file format, and the async routes.
fn reports(limit: Option<usize>) -> Vec<(String, ProfileReport)> {
    let csv = file(".csv", &csv_text(DISTINCT, DUPLICATES));
    let mut reports = Vec::new();
    let mut engines = vec![EngineType::Auto, EngineType::Incremental];
    #[cfg(feature = "arrow")]
    engines.push(EngineType::Columnar);
    for engine in engines {
        let report = profiler(limit)
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let mut files = vec![
        (
            "json",
            file(".json", &format!("[{}]", json_records().join(","))),
        ),
        ("jsonl", file(".jsonl", &json_records().join("\n"))),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_file()));
    for (label, file) in &files {
        let report = profiler(limit)
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }

    #[cfg(feature = "async-streaming")]
    {
        use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let bytes = BytesSource::new(
            bytes::Bytes::from(csv_text(DISTINCT, DUPLICATES)),
            AsyncSourceInfo::new("pipe", FileFormat::Csv),
        );
        let stream = rt
            .block_on(profiler(limit).profile_stream(bytes))
            .expect("async stream");
        reports.push(("async stream".to_string(), stream));
        let async_file = rt
            .block_on(profiler(limit).profile_file(csv.path()))
            .expect("async file");
        reports.push(("async file".to_string(), async_file));
    }
    reports
}

fn uniqueness(report: &ProfileReport) -> &UniquenessMetrics {
    report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.uniqueness.as_ref())
        .expect("uniqueness assessed")
}

/// What each route got wrong for `limit`, collected before asserting so one
/// run shows every route.
fn wrong_routes(limit: Option<usize>, approximate: bool) -> Vec<String> {
    let recorded = limit.unwrap_or(DEFAULT_LIMIT);
    let mut wrong = Vec::new();
    for (label, report) in reports(limit) {
        let metrics = uniqueness(&report);
        if metrics.duplicate_rows_approximate != approximate {
            wrong.push(format!(
                "{label}: approximate {} (count {})",
                metrics.duplicate_rows_approximate, metrics.duplicate_rows
            ));
        }
        if !approximate && metrics.duplicate_rows != DUPLICATES {
            wrong.push(format!("{label}: {} duplicates", metrics.duplicate_rows));
        }
        // An estimate is filed as sampled and decides nothing; an exact
        // count fails the gate on the duplicates it saw.
        let sampled = report
            .quality
            .as_ref()
            .and_then(|quality| quality.sampled_dimensions())
            .unwrap_or_default();
        if sampled.contains(&"duplicate_rows".to_string()) != approximate {
            wrong.push(format!("{label}: sampled {sampled:?}"));
        }
        let check = QualityPolicy::new()
            .max_duplicate_rows(0)
            .evaluate(&report)
            .unwrap()
            .checks
            .remove(0);
        let decided = check.status == CheckStatus::Failed;
        if decided == approximate {
            wrong.push(format!(
                "{label}: gate {:?}: {}",
                check.status, check.message
            ));
        }
        // The estimate is bounded, so a score requirement any source meets
        // still passes rather than turning inconclusive.
        let floor = QualityPolicy::new()
            .min_quality_score(0.0)
            .evaluate(&report)
            .unwrap()
            .checks
            .remove(0);
        if floor.status != CheckStatus::Passed {
            wrong.push(format!(
                "{label}: min_quality_score(0) {:?}: {}",
                floor.status, floor.message
            ));
        }
        let bounded = report
            .quality
            .as_ref()
            .and_then(|quality| quality.score_bounds())
            .is_some();
        if approximate && !bounded {
            wrong.push(format!("{label}: no score bounds for the estimate"));
        }
        if metrics.duplicate_rows_exact_limit != Some(recorded) {
            wrong.push(format!(
                "{label}: recorded {:?}",
                metrics.duplicate_rows_exact_limit
            ));
        }
        // Written in both dialects, and read back as the same limit.
        let document = serde_json::to_value(&report).unwrap();
        let written = &document["quality"]["metrics"]["uniqueness"]["duplicate_rows_exact_limit"];
        let summary: serde_json::Value =
            serde_json::from_str(&report.summary_json().unwrap()).unwrap();
        let summarized = &summary["quality"]["uniqueness"]["duplicate_rows_exact_limit"];
        if *written != recorded || *summarized != recorded {
            wrong.push(format!(
                "{label}: written {written} summarized {summarized}"
            ));
        }
        let restored: ProfileReport = serde_json::from_value(document).unwrap();
        if uniqueness(&restored).duplicate_rows_exact_limit != Some(recorded) {
            wrong.push(format!("{label}: read back wrong"));
        }
    }
    wrong
}

#[test]
fn every_route_estimates_past_a_lowered_limit_and_records_it() {
    let wrong = wrong_routes(Some(LOW_LIMIT), true);
    assert!(wrong.is_empty(), "routes off the limit: {wrong:#?}");
}

#[test]
fn every_route_counts_exactly_under_a_limit_and_records_it() {
    let wrong = wrong_routes(Some(HIGH_LIMIT), false);
    assert!(wrong.is_empty(), "routes off the limit: {wrong:#?}");
}

#[test]
fn without_a_limit_every_route_records_the_default() {
    let wrong = wrong_routes(None, false);
    assert!(wrong.is_empty(), "routes off the default: {wrong:#?}");
}

#[cfg(feature = "sqlite")]
#[test]
fn a_database_query_honours_the_limit() {
    use dataprof::DatabaseConfig;
    use sqlx::sqlite::SqlitePoolOptions;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("duplicates.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();
    rt.block_on(async {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{db_path}"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER, label TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for id in (0..DISTINCT).chain(0..DUPLICATES) {
            sqlx::query("INSERT INTO t VALUES (?, ?)")
                .bind(id as i64)
                .bind(format!("row-{id}"))
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
    });
    let query = |limit: usize| {
        rt.block_on(
            Profiler::new()
                .duplicate_rows_exact_limit(limit)
                .database(DatabaseConfig {
                    connection_string: db_path.clone(),
                    load_credentials_from_env: false,
                    ..Default::default()
                })
                .analyze_query("SELECT * FROM t"),
        )
        .unwrap()
    };
    let low = query(LOW_LIMIT);
    assert!(uniqueness(&low).duplicate_rows_approximate);
    assert_eq!(uniqueness(&low).duplicate_rows_exact_limit, Some(LOW_LIMIT));
    let high = query(HIGH_LIMIT);
    assert!(!uniqueness(&high).duplicate_rows_approximate);
    assert_eq!(uniqueness(&high).duplicate_rows, DUPLICATES);
    assert_eq!(
        uniqueness(&high).duplicate_rows_exact_limit,
        Some(HIGH_LIMIT)
    );
}

/// The memory budget spills the row tracker on the engines that hold one; a
/// limit the caller set is kept past it, so the verdict does not depend on
/// the machine's memory. The kept set is outside the budget, so it does not
/// spill a column set the budget holds either.
#[test]
fn a_set_limit_survives_the_memory_budget() {
    // 400,000 distinct rows hold about 6 MB of fingerprints, over a 2 MB
    // budget; column `c` has 20,000 values, which the budget holds alone.
    let mut text = "a,b,c\n".to_string();
    for row in (0..400_000).chain(0..9) {
        text.push_str(&format!("{},{},{}\n", row % 400, row / 400, row % 20_000));
    }
    let csv = file(".csv", &text);
    let c_estimated = |report: &ProfileReport| {
        report
            .column_profiles
            .iter()
            .find(|profile| profile.name == "c")
            .and_then(|profile| profile.unique_count_is_approximate)
    };
    let mut engines = vec![EngineType::Incremental];
    #[cfg(feature = "arrow")]
    engines.push(EngineType::Columnar);
    let mut wrong = Vec::new();
    for engine in engines {
        let run = |limit: Option<usize>| {
            profiler(limit)
                .engine(engine)
                .memory_limit_mb(2)
                .analyze_file(csv.path())
                .unwrap_or_else(|e| panic!("[{engine:?}] {e}"))
        };
        let spilled = run(None);
        if !uniqueness(&spilled).duplicate_rows_approximate {
            wrong.push(format!(
                "{engine:?}: the budget no longer spills; test reaches nothing"
            ));
        }
        if c_estimated(&spilled) != Some(true) {
            wrong.push(format!(
                "{engine:?}: column c no longer spills; test reaches nothing"
            ));
        }
        let kept = run(Some(500_000));
        let metrics = uniqueness(&kept);
        if metrics.duplicate_rows_approximate || metrics.duplicate_rows != 9 {
            wrong.push(format!(
                "{engine:?}: approximate {} count {}",
                metrics.duplicate_rows_approximate, metrics.duplicate_rows
            ));
        }
        if c_estimated(&kept) != Some(false) {
            wrong.push(format!(
                "{engine:?}: the kept row set spilled column c: {:?}",
                c_estimated(&kept)
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The replay in #1292 at the smallest size that crosses the default: past
/// one million distinct rows `max_duplicate_rows 0` is undecided and names
/// the setting; with the limit raised it fails on the true count.
#[test]
fn a_raised_limit_decides_max_duplicate_rows_past_one_million_rows() {
    let csv = file(".csv", &csv_text(DEFAULT_LIMIT + 100, 7));
    let run = |limit: Option<usize>| {
        profiler(limit)
            .engine(EngineType::Incremental)
            .analyze_file(csv.path())
            .unwrap()
    };
    let policy = QualityPolicy::new()
        .max_duplicate_rows(0)
        .forbid_findings([FindingCode::DuplicateRows]);
    let hint = format!(
        "It was counted under a duplicate_rows_exact_limit of {DEFAULT_LIMIT} distinct rows; \
         a limit of {}, the rows checked, counts every row exactly",
        DEFAULT_LIMIT + 107
    );

    // Both the threshold and the forbidden finding are undecided, and say why.
    let estimated = policy.evaluate(&run(None)).unwrap();
    assert_eq!(estimated.checks.len(), 2);
    for check in &estimated.checks {
        assert!(check.not_evaluated().is_some(), "{check:?}");
        assert!(check.message.ends_with(&hint), "{}", check.message);
    }

    let exact = run(Some(2_000_000));
    assert_eq!(uniqueness(&exact).duplicate_rows, 7);
    let decided = policy.evaluate(&exact).unwrap();
    for check in &decided.checks {
        assert_eq!(check.status, CheckStatus::Failed, "{check:?}");
        assert!(
            !check.message.contains("duplicate_rows_exact_limit"),
            "{}",
            check.message
        );
    }
}
