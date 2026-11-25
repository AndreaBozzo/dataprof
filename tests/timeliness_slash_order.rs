//! Timeliness reads a column's slash dates in the order its date statistics
//! record (#1305).
//!
//! The statistics decide the order over every value read (#1297). Timeliness
//! used to decide its own from the retained sample, so on a source past the
//! sample the one value that settles the order could be missing, and the
//! score, its bounds and the start/end comparison read every ambiguous date
//! the other way round from the report.
//!
//! Each fixture holds 40,000 ambiguous slash dates and, in its last row, past
//! the sample, the one value that only parses month-first.
//!
//! `python/tests/test_timeliness_slash_order.py` is the Python twin, which
//! also compares each file route with the same values held in memory.

use std::io::Write;

use dataprof::{
    ColumnStats, EngineType, FindingCode, MetricPack, ProfileReport, Profiler, QualityDimension,
    QualityPolicy,
};
use serde_json::Value;
use tempfile::NamedTempFile;

const ROWS: usize = 40_000;
const REFERENCE: &str = "2024-06-01";

fn day(text: &str) -> chrono::NaiveDate {
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

/// One fixture: its column names and the value of each column in every row
/// but the last, which holds `settling`.
struct Case {
    label: &'static str,
    columns: &'static [&'static str],
    ambiguous: &'static [&'static str],
    settling: &'static [&'static str],
}

const CASES: [Case; 3] = [
    // Month-first, 1 December 2024: after the reference date.
    Case {
        label: "future month-first",
        columns: &["event_date"],
        ambiguous: &["12/01/2024"],
        settling: &["12/31/2023"],
    },
    // Month-first, 12 January 2024: before it. Day-first it is 1 December.
    Case {
        label: "past month-first",
        columns: &["event_date"],
        ambiguous: &["01/12/2024"],
        settling: &["12/31/2023"],
    },
    // Month-first, 5 January then 3 February: every pair runs forwards.
    // Day-first, 1 May then 2 March: every pair is inverted.
    Case {
        label: "pair month-first",
        columns: &["start_date", "end_date"],
        ambiguous: &["01/05/2024", "02/03/2024"],
        settling: &["12/30/2023", "12/31/2023"],
    },
];

impl Case {
    fn rows(&self) -> Vec<Vec<&'static str>> {
        let mut rows = vec![self.ambiguous.to_vec(); ROWS - 1];
        rows.push(self.settling.to_vec());
        rows
    }
}

fn file(suffix: &str, text: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(text.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn records(case: &Case) -> Vec<String> {
    case.rows()
        .iter()
        .map(|row| {
            let fields: Vec<String> = case
                .columns
                .iter()
                .zip(row)
                .map(|(name, value)| format!(r#""{name}":"{value}""#))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_file(case: &Case) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let rows = case.rows();
    let fields: Vec<Field> = case
        .columns
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, false))
        .collect();
    let arrays: Vec<ArrayRef> = (0..case.columns.len())
        .map(|index| {
            let values: Vec<&str> = rows.iter().map(|row| row[index]).collect();
            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// The case through every engine over its CSV, every other file format and
/// the async route, all measured against [`REFERENCE`].
fn routes(case: &Case) -> Vec<(String, ProfileReport)> {
    let profiler = || Profiler::new().reference_date(day(REFERENCE));
    let mut csv_text = format!("{}\n", case.columns.join(","));
    for row in case.rows() {
        csv_text.push_str(&row.join(","));
        csv_text.push('\n');
    }
    let csv = file(".csv", &csv_text);
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("csv {engine:?}"), report));
    }
    let records = records(case);
    let jsonl: String = records.iter().map(|record| format!("{record}\n")).collect();
    let mut files = vec![
        ("json", file(".json", &format!("[{}]", records.join(",")))),
        ("jsonl", file(".jsonl", &jsonl)),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_file(case)));
    for (label, source) in files {
        let report = profiler()
            .analyze_file(source.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    // Without the statistics pack the report carries no date statistics, and
    // the quality block must not change with the packs (#1268).
    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let report = profiler()
            .engine(engine)
            .metric_packs(vec![MetricPack::Quality])
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?} quality only] {e}"));
        reports.push((format!("csv {engine:?} quality only"), report));
    }
    #[cfg(feature = "async-streaming")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = runtime
            .block_on(profiler().profile_file(csv.path()))
            .expect("async CSV");
        reports.push(("async csv".to_string(), report));
    }
    reports
}

/// A run without the statistics pack reports no statistics, although its
/// quality pass read the date statistics' slash order.
#[test]
fn a_quality_only_run_reports_no_date_statistics() {
    let mut leaked = Vec::new();
    for case in &CASES {
        for (route, report) in routes(case) {
            if !route.ends_with("quality only") {
                continue;
            }
            for column in &report.column_profiles {
                if !matches!(column.stats, ColumnStats::None) {
                    leaked.push(format!("{} [{route}] {}", case.label, column.name));
                }
            }
        }
    }
    assert!(
        leaked.is_empty(),
        "statistics reported:\n{}",
        leaked.join("\n")
    );
}

fn timeliness(report: &ProfileReport) -> Value {
    let document = serde_json::to_value(report).unwrap();
    serde_json::json!({
        "metrics": document["quality"]["metrics"]["timeliness"],
        "score": document["quality"]["scores"]["dimension_scores"]["timeliness"],
        "bounds": document["quality"]["score_bounds"]["dimension_scores"]["timeliness"],
    })
}

fn verdict(report: &ProfileReport, policy: QualityPolicy) -> String {
    let result = policy.evaluate(report).unwrap();
    serde_json::to_value(&result).unwrap()["verdict"]
        .as_str()
        .unwrap()
        .to_string()
}

/// The verdicts a route that reads the dates month-first gives: a minimum
/// timeliness score of 90, and `future_dates` forbidden.
fn verdicts(report: &ProfileReport) -> (String, String) {
    (
        verdict(
            report,
            QualityPolicy::new().min_dimension_score(QualityDimension::Timeliness, 90.0),
        ),
        verdict(
            report,
            QualityPolicy::new().forbid_findings([FindingCode::FutureDates]),
        ),
    )
}

#[test]
fn every_route_scores_timeliness_in_the_recorded_order() {
    let mut wrong = Vec::new();
    for case in &CASES {
        for (route, report) in routes(case) {
            let got = timeliness(&report);
            let metrics = &got["metrics"];
            let future = metrics["future_dates_count"].as_u64().unwrap();
            let violations = metrics["temporal_violations"].as_u64().unwrap();
            let pairs = metrics["temporal_pairs_checked"].as_u64().unwrap();
            let score = got["score"].as_f64().unwrap();
            let fine = match case.label {
                "future month-first" => future > 0 && score < 90.0,
                "past month-first" => future == 0 && score == 100.0,
                _ => future == 0 && pairs > 0 && violations == 0 && score == 100.0,
            };
            if !fine {
                wrong.push(format!("{} [{route}]: {got}", case.label));
            }
        }
    }
    assert!(wrong.is_empty(), "wrong timeliness:\n{}", wrong.join("\n"));
}

/// The gate decides each case as the dates read month-first decide it, and a
/// saved report decides as the run did. With exact timeliness over every value
/// read (#788 step 4), every route now decides `future_dates` cleanly instead
/// of returning inconclusive on a large file.
#[test]
fn every_route_and_its_saved_report_give_the_month_first_verdict() {
    let mut wrong = Vec::new();
    for case in &CASES {
        let expected = match case.label {
            "future month-first" => ("fail", "fail"),
            _ => ("pass", "pass"),
        };
        let expected = (expected.0.to_string(), expected.1.to_string());
        for (route, report) in routes(case) {
            let saved: ProfileReport =
                serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
            for (form, report) in [("run", &report), ("saved", &saved)] {
                let got = verdicts(report);
                if got != expected {
                    wrong.push(format!("{} [{route}, {form}]: {got:?}", case.label));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "wrong verdicts:\n{}", wrong.join("\n"));
}
