//! Datetimes written with a space for the `T` and a UTC offset (#1335).
//!
//! Python's `str()` of a timezone-aware datetime, a pandas `Timestamp` and
//! many SQL exports write `2026-06-01 12:00:00+00:00`. The date grammar took
//! the offset only after a `T`, so these columns were typed as text: the date
//! rules could not decide, and a date-named column scored 0 consistency. Both
//! inferrers share the grammar, so every engine and input format has to type
//! the same values as dates and read the same UTC dates from them.
//!
//! `python/tests/test_spaced_offset_datetimes.py` is the Python twin; it covers
//! the in-memory routes, where aware `datetime` cells are rendered with
//! `str()`.

use std::io::Write;

use dataprof::{
    ColumnStats, DataType, EngineType, NaiveDate, ProfileReport, Profiler, QualityPolicy,
};
use serde_json::Value;
use tempfile::NamedTempFile;

/// Per column: two values, and the UTC dates they fall on (oldest, newest).
/// Each case moves a different part of the form: the offset carries `01:00`
/// back across midnight, a negative offset carries the evening forward, a
/// fraction sits before the offset, `Z`, and a naive value with a fraction.
const CASES: [(&str, [&str; 2], &str, &str); 5] = [
    (
        "created_at",
        ["2026-05-31 12:00:00+00:00", "2026-06-01 12:00:00+00:00"],
        "2026-05-31",
        "2026-06-01",
    ),
    (
        "east",
        ["2026-06-01 01:00:00+02:00", "2026-06-02 01:00:00+02:00"],
        "2026-05-31",
        "2026-06-01",
    ),
    (
        "west",
        [
            "2026-05-30 20:00:00.123456-05:00",
            "2026-05-31 21:00:00-05:00",
        ],
        "2026-05-31",
        "2026-06-01",
    ),
    (
        "zulu",
        ["2026-05-31 08:00:00Z", "2026-06-01 08:00:00Z"],
        "2026-05-31",
        "2026-06-01",
    ),
    (
        "naive",
        ["2026-05-31 12:00:00.000005", "2026-06-01 12:00:00.000005"],
        "2026-05-31",
        "2026-06-01",
    ),
];

fn fixture(suffix: &str, text: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(text.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn records() -> Vec<String> {
    (0..2)
        .map(|row| {
            let fields: Vec<String> = CASES
                .iter()
                .map(|(name, values, _, _)| format!("\"{name}\": \"{}\"", values[row]))
                .collect();
            format!("{{{}}}", fields.join(", "))
        })
        .collect()
}

/// The same values through every file route.
fn reports() -> Vec<(String, ProfileReport)> {
    let profiler = || Profiler::new().reference_date(day("2026-06-01"));
    let header: Vec<&str> = CASES.iter().map(|(name, ..)| *name).collect();
    let rows: Vec<String> = (0..2)
        .map(|row| {
            CASES
                .iter()
                .map(|(_, values, _, _)| values[row])
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    let csv = fixture(
        ".csv",
        &format!("{}\n{}\n", header.join(","), rows.join("\n")),
    );
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[csv {engine:?}] {e}"));
        reports.push((format!("csv {engine:?}"), report));
    }
    let records = records();
    for (label, file) in [
        (
            "json",
            fixture(".json", &format!("[{}]", records.join(","))),
        ),
        ("jsonl", fixture(".jsonl", &(records.join("\n") + "\n"))),
    ] {
        let report = profiler()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    #[cfg(feature = "async-streaming")]
    {
        use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};
        let text = format!("{}\n{}\n", header.join(","), rows.join("\n"));
        let source = BytesSource::new(
            bytes::Bytes::from(text),
            AsyncSourceInfo::new("upload", FileFormat::Csv),
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = rt.block_on(profiler().profile_stream(source)).unwrap();
        reports.push(("async csv bytes".to_string(), report));
    }
    reports
}

fn day(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

/// Every route types each column as a date, reads the UTC dates the offsets
/// give, and decides `max_date_age` and `max_date_gap` on them. The
/// date-named column scores full consistency.
#[test]
fn spaced_datetimes_with_an_offset_are_dates_on_every_route() {
    let mut wrong = Vec::new();
    for (route, report) in reports() {
        for (name, _, oldest, newest) in CASES {
            let Some(profile) = report.column_profiles.iter().find(|p| p.name == name) else {
                wrong.push(format!("[{route}] {name}: not profiled"));
                continue;
            };
            if profile.data_type != DataType::Date {
                wrong.push(format!("[{route}] {name}: typed {:?}", profile.data_type));
                continue;
            }
            match &profile.stats {
                ColumnStats::DateTime(stats)
                    if (stats.min_datetime.as_str(), stats.max_datetime.as_str())
                        == (oldest, newest) => {}
                ColumnStats::DateTime(stats) => wrong.push(format!(
                    "[{route}] {name}: dates {} to {}",
                    stats.min_datetime, stats.max_datetime
                )),
                other => wrong.push(format!("[{route}] {name}: stats {other:?}")),
            }
            let result = QualityPolicy::new()
                .max_date_age(name, 0)
                .max_date_gap(name, 1)
                .evaluate(&report)
                .unwrap();
            let checks: Vec<(Value, Value)> = result
                .checks
                .iter()
                .map(|check| {
                    let check = serde_json::to_value(check).unwrap();
                    (check["status"].clone(), check["observed"].clone())
                })
                .collect();
            let expected = vec![
                (Value::from("passed"), Value::from(0)),
                (Value::from("passed"), Value::from(1)),
            ];
            if checks != expected {
                wrong.push(format!("[{route}] {name}: checks {checks:?}"));
            }
        }
        let consistency = report
            .quality
            .as_ref()
            .and_then(|quality| quality.metrics.consistency.as_ref())
            .map(|metrics| metrics.data_type_consistency);
        if consistency != Some(100.0) {
            wrong.push(format!("[{route}] data_type_consistency {consistency:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// An offset chrono cannot turn into an instant keeps the column text, so no
/// date column counts every value invalid: an hour of 24, a minute of 60.
#[test]
fn an_out_of_range_offset_is_not_a_date() {
    let csv = fixture(
        ".csv",
        "t\n2026-06-01 12:00:00+24:00\n2026-06-01 12:00:00+00:60\n",
    );
    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap();
        assert_eq!(
            report.column_profiles[0].data_type,
            DataType::String,
            "[{engine:?}]"
        );
    }
}
