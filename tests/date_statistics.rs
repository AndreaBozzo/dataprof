//! Date statistics over every value read, not the retained sample (#1297).
//!
//! The file, Arrow and streaming engines keep at most 10,000 values of each
//! column. A date column's `min_datetime`, `max_datetime`, `duration_days`,
//! distributions and slash order used to come from those values, so a date
//! outside them was missing from the report while the same data profiled in
//! memory reported it. Every route must now report what the in-memory column
//! analysis reports over all of the values.
//!
//! `python/tests/test_date_statistics.py` is the Python twin.

use std::io::Write;

use dataprof::{
    ColumnStats, CsvParserConfig, DateStatistics, EngineType, ProfileReport, Profiler,
    analyze_column, analyze_csv_file,
};
use serde_json::Value;
use tempfile::NamedTempFile;

const ROWS: usize = 40_000;
const COLUMNS: [&str; 3] = ["published_at", "us_date", "logged_at"];

/// Rows past the sample, each holding the one value that moves a statistic:
/// the newest and the oldest date, the only value that decides the slash
/// order (12/31 parses only month-first), and the only hour 23.
const NEWEST_ROW: usize = 25_000;
const OLDEST_ROW: usize = 33_000;
const MONTH_FIRST_ROW: usize = 31_000;
const LATE_HOUR_ROW: usize = 37_000;

fn row(i: usize) -> [String; 3] {
    let start = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let published_at = match i {
        NEWEST_ROW => "2026-09-29".to_string(),
        OLDEST_ROW => "2019-03-04".to_string(),
        _ => (start + chrono::Duration::days((i % 500) as i64)).to_string(),
    };
    // Day and month both at most 12, so every value but one parses both ways.
    let us_date = match i {
        MONTH_FIRST_ROW => "12/31/2024".to_string(),
        _ => format!("{:02}/{:02}/2024", i % 12 + 1, i / 12 % 12 + 1),
    };
    let logged_at = match i {
        LATE_HOUR_ROW => "2021-05-05T23:00:00".to_string(),
        _ => format!("2021-05-{:02}T10:00:00", i % 28 + 1),
    };
    [published_at, us_date, logged_at]
}

fn rows() -> Vec<[String; 3]> {
    (0..ROWS).map(row).collect()
}

fn column_values(index: usize) -> Vec<String> {
    rows().into_iter().map(|row| row[index].clone()).collect()
}

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", COLUMNS.join(",")).unwrap();
    for row in rows() {
        writeln!(file, "{}", row.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    rows()
        .iter()
        .map(|row| {
            format!(
                r#"{{"published_at":"{}","us_date":"{}","logged_at":"{}"}}"#,
                row[0], row[1], row[2]
            )
        })
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
    for record in json_records() {
        writeln!(file, "{record}").unwrap();
    }
    file.flush().unwrap();
    file
}

/// Text columns, or `published_at` as a typed Arrow date (`Date32`), which
/// the record batch analyzer renders and reads like any other value.
#[cfg(feature = "parquet")]
fn parquet_fixture(typed_dates: bool) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Date32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (index, name) in COLUMNS.iter().enumerate() {
        let values = column_values(index);
        if typed_dates && index == 0 {
            fields.push(Field::new(*name, DataType::Date32, false));
            let days: Vec<i32> = values
                .iter()
                .map(|value| {
                    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap();
                    (date - epoch).num_days() as i32
                })
                .collect();
            arrays.push(Arc::new(Date32Array::from(days)));
        } else {
            fields.push(Field::new(*name, DataType::Utf8, false));
            arrays.push(Arc::new(StringArray::from(values)));
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, every other format, and the async route.
fn reports() -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture();
    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let mut files = vec![
        ("json".to_string(), json_fixture()),
        ("jsonl".to_string(), jsonl_fixture()),
    ];
    #[cfg(feature = "parquet")]
    {
        files.push(("parquet".to_string(), parquet_fixture(false)));
        files.push(("parquet date32".to_string(), parquet_fixture(true)));
    }
    for (label, file) in &files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.clone(), report));
    }

    #[cfg(feature = "async-streaming")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = runtime
            .block_on(Profiler::new().profile_file(csv.path()))
            .expect("async CSV");
        reports.push(("async csv".to_string(), report));
    }
    reports
}

/// What the in-memory column analysis reports over every value: the
/// database connectors' route, and the one that never sampled.
fn expected() -> Vec<Value> {
    (0..COLUMNS.len())
        .map(|index| {
            let profile = analyze_column(COLUMNS[index], &column_values(index));
            let ColumnStats::DateTime(stats) = profile.stats else {
                panic!(
                    "{} is not a date column: {:?}",
                    COLUMNS[index], profile.stats
                );
            };
            serde_json::to_value(stats).unwrap()
        })
        .collect()
}

#[test]
fn the_oracle_holds_the_values_past_the_sample() {
    let expected = expected();
    assert_eq!(expected[0]["max_datetime"], "2026-09-29");
    assert_eq!(expected[0]["min_datetime"], "2019-03-04");
    assert_eq!(expected[0]["year_distribution"]["2026"], 1);
    assert_eq!(expected[1]["slash_date_order"], "month_first");
    assert_eq!(expected[2]["hour_distribution"]["23"], 1);
    // The newest date opens the largest gap (#1300), so a route that read
    // the sample alone would report a shorter one.
    let last_cycle_day =
        chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap() + chrono::Duration::days(499);
    let newest = chrono::NaiveDate::from_ymd_opt(2026, 9, 29).unwrap();
    assert_eq!(
        expected[0]["largest_gap"],
        serde_json::json!({
            "days": (newest - last_cycle_day).num_days(),
            "from": last_cycle_day.to_string(),
            "to": "2026-09-29",
        })
    );
    assert_eq!(
        expected[1]["largest_gap"],
        serde_json::json!({ "days": 20, "from": "2024-01-12", "to": "2024-02-01" })
    );
}

#[test]
fn every_route_reports_the_date_statistics_of_every_value() {
    let expected = expected();
    let mut wrong = Vec::new();
    let mut routes = 0;
    for (label, report) in reports() {
        routes += 1;
        // The fixture is past the sample on every route, or it guards nothing.
        let sampled = report.column_profiles.iter().any(|column| {
            column
                .type_homogeneity
                .as_ref()
                .expect("classified")
                .classified_count()
                < ROWS
        });
        if !sampled {
            wrong.push(format!("[{label}] held every value in its sample"));
        }
        assert_eq!(
            report
                .metric_semantics
                .as_ref()
                .and_then(|semantics| semantics.date_statistics),
            Some(DateStatistics::EveryValue),
            "[{label}]"
        );
        for (index, name) in COLUMNS.iter().enumerate() {
            let Some(column) = report.column_profiles.iter().find(|c| c.name == *name) else {
                wrong.push(format!("[{label}] {name}: no profile"));
                continue;
            };
            let ColumnStats::DateTime(stats) = &column.stats else {
                wrong.push(format!("[{label}] {name}: not date statistics"));
                continue;
            };
            let got = serde_json::to_value(stats).unwrap();
            let fields = expected[index].as_object().unwrap().keys();
            for field in fields.chain(got.as_object().unwrap().keys()) {
                if got.get(field) != expected[index].get(field) {
                    wrong.push(format!(
                        "[{label}] {name}.{field}: {} != {}",
                        got.get(field).unwrap_or(&Value::Null),
                        expected[index].get(field).unwrap_or(&Value::Null)
                    ));
                }
            }
        }
    }
    assert!(routes >= 6, "only {routes} routes ran");
    wrong.dedup();
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}
