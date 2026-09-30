//! A column's slash dates are read in one day/month order (#811).
//!
//! Each value used to be parsed on its own: `12/31/2024` month-first because
//! day-first fails, `01/02/2024` day-first because that was tried first. A US
//! export came out with half its dates wrong by months. The order is now
//! resolved per column, recorded in `DateTimeStats::slash_date_order`, and
//! used by the statistics and the timeliness checks alike, on every engine and
//! input format.
//!
//! `python/tests/test_slash_date_order.py` is the Python twin.

use std::io::Write;

use dataprof::{
    ColumnStats, CsvParserConfig, EngineType, ProfileReport, Profiler, SlashDateOrder,
    analyze_column, analyze_csv_file,
};
use serde_json::json;
use tempfile::NamedTempFile;

/// A US export: every value is month-first. `start_date` and `end_date` run
/// forwards on every row when read that way.
const ROWS: [(&str, &str); 6] = [
    ("12/31/2024", "01/13/2025"),
    ("01/02/2024", "02/01/2024"),
    ("03/04/2024", "03/05/2024"),
    ("12/30/2024", "12/31/2024"),
    ("05/06/2024", "06/05/2024"),
    ("11/29/2024", "11/30/2024"),
];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "start_date,end_date").unwrap();
    for (start, end) in ROWS {
        writeln!(file, "{start},{end}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    ROWS.iter()
        .map(|(start, end)| format!(r#"{{"start_date":"{start}","end_date":"{end}"}}"#))
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

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("start_date", DataType::Utf8, false),
        Field::new("end_date", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ROWS.map(|row| row.0).to_vec())),
            Arc::new(StringArray::from(ROWS.map(|row| row.1).to_vec())),
        ],
    )
    .unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

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
    let mut files = vec![("json", json_fixture()), ("jsonl", jsonl_fixture())];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

#[test]
fn every_path_reads_a_us_column_month_first() {
    for (label, report) in reports() {
        let start = report
            .column_profiles
            .iter()
            .find(|column| column.name == "start_date")
            .unwrap_or_else(|| panic!("[{label}] start_date"));
        let ColumnStats::DateTime(stats) = &start.stats else {
            panic!(
                "[{label}] start_date is not a date column: {:?}",
                start.stats
            );
        };
        assert_eq!(
            stats.slash_date_order,
            Some(SlashDateOrder::MonthFirst),
            "[{label}]"
        );
        // Read value by value, the minimum was 2024-02-01 and three of the six
        // months were wrong.
        assert_eq!(stats.min_datetime, "2024-01-02", "[{label}]");
        assert_eq!(stats.max_datetime, "2024-12-31", "[{label}]");
        let mut months: Vec<(u32, usize)> = stats
            .month_distribution
            .iter()
            .map(|(month, count)| (*month, *count))
            .collect();
        months.sort_unstable();
        assert_eq!(
            months,
            [(1, 1), (3, 1), (5, 1), (11, 1), (12, 2)],
            "[{label}]"
        );

        // The pairs are compared in the same order: every row runs forwards.
        let timeliness = report
            .quality
            .as_ref()
            .and_then(|quality| quality.metrics.timeliness.as_ref())
            .unwrap_or_else(|| panic!("[{label}] timeliness assessed"));
        assert_eq!(timeliness.temporal_pairs_checked, 6, "[{label}]");
        assert_eq!(timeliness.temporal_violations, 0, "[{label}]");

        // Serialized, and read back.
        let document = serde_json::to_value(&report).expect("serializes");
        assert_eq!(
            document["column_profiles"][0]["stats"]["DateTime"]["slash_date_order"],
            json!("month_first"),
            "[{label}]"
        );
        let restored: ProfileReport = serde_json::from_value(document).expect("reads back");
        let ColumnStats::DateTime(restored) = &restored.column_profiles[0].stats else {
            panic!("[{label}] restored stats");
        };
        assert_eq!(
            restored.slash_date_order,
            Some(SlashDateOrder::MonthFirst),
            "[{label}]"
        );
    }
}

/// The database connectors and in-memory inputs profile through
/// `analyze_column`.
#[test]
fn column_analysis_reads_it_month_first_too() {
    let values: Vec<String> = ROWS.iter().map(|row| row.0.to_string()).collect();
    let profile = analyze_column("start_date", &values);
    let ColumnStats::DateTime(stats) = &profile.stats else {
        panic!("not a date column: {:?}", profile.stats);
    };
    assert_eq!(stats.slash_date_order, Some(SlashDateOrder::MonthFirst));
    assert_eq!(stats.min_datetime, "2024-01-02");
}

#[test]
fn a_column_that_contradicts_itself_is_reported_mixed() {
    let values: Vec<String> = ["12/31/2024", "31/12/2024", "01/02/2024"]
        .map(String::from)
        .to_vec();
    let profile = analyze_column("when", &values);
    let ColumnStats::DateTime(stats) = &profile.stats else {
        panic!("not a date column: {:?}", profile.stats);
    };
    assert_eq!(stats.slash_date_order, Some(SlashDateOrder::Mixed));
}

#[test]
fn an_undecided_column_says_the_order_was_assumed() {
    let values: Vec<String> = ["01/02/2024", "03/04/2024"].map(String::from).to_vec();
    let profile = analyze_column("when", &values);
    let ColumnStats::DateTime(stats) = &profile.stats else {
        panic!("not a date column: {:?}", profile.stats);
    };
    assert_eq!(
        stats.slash_date_order,
        Some(SlashDateOrder::AssumedDayFirst)
    );
    assert_eq!(stats.min_datetime, "2024-02-01");
}
