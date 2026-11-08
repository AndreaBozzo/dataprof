//! A fixed timeliness reference date, recorded in the report (#903).
//!
//! Timeliness compared dates with today's, so an unchanged file changed score
//! as the calendar moved and a gate on it could not be reproduced. A caller
//! can now fix the date, every input route measures against it, and the
//! report records the clock it used under `quality.timeliness.clock`, which
//! `compare()` lists under `configuration`.
//!
//! `python/tests/test_timeliness_reference_date.py` is the Python twin.

use std::io::Write;

use dataprof::{
    EngineType, NaiveDate, ProfileReport, Profiler, ReportDelta, ReportSide, TimelinessClock,
    TimelinessClockMode,
};
use tempfile::NamedTempFile;

/// Five years before the fixed date is 2021-06-10 (1826 days, one of them
/// 29 February 2024), so the first value is the one stale date, and the last
/// is the one future date. Today's clock reads both 2021 dates as stale.
const DATES: [&str; 4] = ["2021-06-09", "2021-06-10", "2026-06-10", "2026-06-11"];
const STALE_RATIO: f64 = 25.0;
const FUTURE_DATES: usize = 1;

fn fixed() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 6, 10).unwrap()
}

fn csv_text() -> String {
    let mut text = "id,updated_at\n".to_string();
    for (id, date) in DATES.iter().enumerate() {
        text.push_str(&format!("{id},{date}\n"));
    }
    text
}

fn file(suffix: &str, contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    DATES
        .iter()
        .enumerate()
        .map(|(id, date)| format!(r#"{{"id":{id},"updated_at":"{date}"}}"#))
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_file() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("updated_at", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..DATES.len() as i64)),
            Arc::new(StringArray::from(DATES.to_vec())),
        ],
    )
    .unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn profiler(reference_date: Option<NaiveDate>) -> Profiler {
    let profiler = Profiler::new().temporal_columns(vec!["updated_at".to_string()]);
    match reference_date {
        Some(date) => profiler.reference_date(date),
        None => profiler,
    }
}

/// Every engine over the CSV, every other file format, and the async routes.
fn reports(reference_date: Option<NaiveDate>) -> Vec<(String, ProfileReport)> {
    let csv = file(".csv", &csv_text());
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler(reference_date)
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
        let report = profiler(reference_date)
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
            bytes::Bytes::from(csv_text()),
            AsyncSourceInfo::new("pipe", FileFormat::Csv),
        );
        let stream = rt
            .block_on(profiler(reference_date).profile_stream(bytes))
            .expect("async stream");
        reports.push(("async stream".to_string(), stream));
        let async_file = rt
            .block_on(profiler(reference_date).profile_file(csv.path()))
            .expect("async file");
        reports.push(("async file".to_string(), async_file));
    }
    reports
}

fn timeliness(report: &ProfileReport) -> &dataprof::TimelinessMetrics {
    report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.timeliness.as_ref())
        .expect("timeliness assessed")
}

#[test]
fn every_route_measures_against_a_fixed_date_and_records_it() {
    let expected = TimelinessClock::new(TimelinessClockMode::Fixed, fixed(), 5.0);
    let mut wrong = Vec::new();
    for (label, report) in reports(Some(fixed())) {
        let metrics = timeliness(&report);
        if metrics.clock != Some(expected) {
            wrong.push(format!("{label}: clock {:?}", metrics.clock));
        }
        if metrics.stale_data_ratio != STALE_RATIO || metrics.future_dates_count != FUTURE_DATES {
            wrong.push(format!(
                "{label}: stale {} future {}",
                metrics.stale_data_ratio, metrics.future_dates_count
            ));
        }
        // Written in both dialects, and read back as the same clock.
        let document = serde_json::to_value(&report).unwrap();
        let written = &document["quality"]["metrics"]["timeliness"]["clock"];
        let summary: serde_json::Value =
            serde_json::from_str(&report.summary_json().unwrap()).unwrap();
        let summarized = &summary["quality"]["timeliness"]["clock"];
        let clock = serde_json::json!({
            "mode": "fixed",
            "reference_date": "2026-06-10",
            "timezone": "UTC",
            "max_data_age_years": 5.0,
        });
        if *written != clock || *summarized != clock {
            wrong.push(format!(
                "{label}: written {written} summarized {summarized}"
            ));
        }
        let restored: ProfileReport = serde_json::from_value(document).unwrap();
        if timeliness(&restored).clock != Some(expected) {
            wrong.push(format!(
                "{label}: read back {:?}",
                timeliness(&restored).clock
            ));
        }
    }
    assert!(wrong.is_empty(), "routes off the fixed clock: {wrong:#?}");
}

#[test]
fn without_a_date_every_route_records_todays_rolling_clock() {
    let before = chrono::Utc::now().date_naive();
    let reports = reports(None);
    let after = chrono::Utc::now().date_naive();
    let mut wrong = Vec::new();
    for (label, report) in reports {
        let clock = timeliness(&report).clock.expect("recorded");
        if clock.mode != TimelinessClockMode::Rolling
            || !(before..=after).contains(&clock.reference_date)
        {
            wrong.push(format!("{label}: {clock:?}"));
        }
    }
    assert!(wrong.is_empty(), "routes off today's clock: {wrong:#?}");
}

#[cfg(feature = "sqlite")]
#[test]
fn a_database_query_measures_against_the_fixed_date() {
    use dataprof::DatabaseConfig;
    use sqlx::sqlite::SqlitePoolOptions;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("timeliness.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();
    let report = rt.block_on(async {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{db_path}"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER, updated_at TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for (id, date) in DATES.iter().enumerate() {
            sqlx::query("INSERT INTO t VALUES (?, ?)")
                .bind(id as i64)
                .bind(*date)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
        // Database profiling refuses semantic hints, so the date column is
        // found by its inferred type.
        Profiler::new()
            .reference_date(fixed())
            .database(DatabaseConfig {
                connection_string: db_path.clone(),
                load_credentials_from_env: false,
                ..Default::default()
            })
            .analyze_query("SELECT * FROM t")
            .await
            .unwrap()
    });
    let metrics = timeliness(&report);
    assert_eq!(
        metrics.clock,
        Some(TimelinessClock::new(
            TimelinessClockMode::Fixed,
            fixed(),
            5.0
        ))
    );
    assert_eq!(metrics.stale_data_ratio, STALE_RATIO);
}

fn csv_report(reference_date: NaiveDate) -> ProfileReport {
    let csv = file(".csv", &csv_text());
    profiler(Some(reference_date))
        .analyze_file(csv.path())
        .unwrap()
}

#[test]
fn compare_lists_the_clock_from_either_dialect() {
    let a = csv_report(fixed());
    let b = csv_report(fixed().pred_opt().unwrap());
    let summary =
        |report: &ProfileReport| ReportSide::from_summary_json(&report.summary_json().unwrap());

    let native = ReportDelta::between(&a, &b).configuration.timeliness_clock;
    assert_eq!(native.same, Some(false));
    assert_eq!(native.a.map(|clock| clock.reference_date), Some(fixed()));

    let flat = ReportDelta::between_sides(&summary(&a).unwrap(), &summary(&b).unwrap())
        .configuration
        .timeliness_clock;
    assert_eq!(flat.same, Some(false), "the flat summary carries the clock");
    assert_eq!(
        ReportDelta::between_sides(&summary(&a).unwrap(), &ReportSide::from_report(&a))
            .configuration
            .timeliness_clock
            .same,
        Some(true)
    );
}

#[test]
fn a_summary_without_a_clock_is_unrecorded_and_a_null_one_is_malformed() {
    let report = csv_report(fixed());
    let mut summary: serde_json::Value =
        serde_json::from_str(&report.summary_json().unwrap()).unwrap();

    summary["quality"]["timeliness"]
        .as_object_mut()
        .unwrap()
        .remove("clock");
    let old = ReportSide::from_summary_json(&summary.to_string()).expect("reads");
    let delta = ReportDelta::between_sides(&old, &ReportSide::from_report(&report));
    assert_eq!(delta.configuration.timeliness_clock.a, None);
    assert_eq!(delta.configuration.timeliness_clock.same, None);

    summary["quality"]["timeliness"]["clock"] = serde_json::Value::Null;
    let error = ReportSide::from_summary_json(&summary.to_string())
        .expect_err("explicit null is malformed")
        .to_string();
    assert!(error.contains("quality.timeliness.clock"), "{error}");
}

#[test]
fn a_document_with_a_null_clock_is_malformed() {
    let mut document = serde_json::to_value(csv_report(fixed())).unwrap();
    document["quality"]["metrics"]["timeliness"]["clock"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<ProfileReport>(document).is_err());
}

/// A sampled run computes timeliness on the reservoir, through a second code
/// path, and has to record the same clock.
#[test]
fn a_sampled_run_records_the_fixed_clock() {
    use dataprof::{MetricConfidence, SamplingStrategy};

    let mut text = "id,updated_at\n".to_string();
    for id in 0..400 {
        text.push_str(&format!("{id},{}\n", DATES[id % DATES.len()]));
    }
    let csv = file(".csv", &text);
    let report = profiler(Some(fixed()))
        .engine(EngineType::Incremental)
        .sampling(SamplingStrategy::Systematic { interval: 4 })
        .analyze_file(csv.path())
        .unwrap();
    let quality = report.quality.as_ref().expect("quality");
    assert!(
        matches!(quality.confidence, MetricConfidence::Mixed { .. }),
        "the sampled path: {:?}",
        quality.confidence
    );
    assert_eq!(
        timeliness(&report).clock,
        Some(TimelinessClock::new(
            TimelinessClockMode::Fixed,
            fixed(),
            5.0
        ))
    );
}
