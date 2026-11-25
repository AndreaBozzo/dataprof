//! Timeliness counts over every scanned value using the same date reading as
//! the date statistics, with row-aligned start/end pairs (#788 step 4, #1305,
//! #787, #1422).
//!
//! `python/tests/test_exact_timeliness.py` is the Python twin.

use std::io::Write;

use dataprof::{
    AnalysisOptions, ColumnStats, CsvParserConfig, EngineType, MetricPack, NaiveDate,
    ProfileReport, Profiler, SemanticHints, StopCondition, ValueFacts,
    analyze_csv_file_with_options,
};
use serde_json::Value;
use tempfile::NamedTempFile;

/// Four times `COLUMN_SAMPLE_CAPACITY` (10,000).
const ROWS: usize = 40_000;
const REFERENCE: &str = "2024-06-01";

fn day(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

struct FixtureColumns {
    names: Vec<&'static str>,
    values: Vec<Vec<String>>,
    temporal_columns: Vec<String>,
    temporal_pairs: Vec<(String, String)>,
}

/// 40,000-row dataset whose critical dates all sit past row 39,000 (well past
/// the 10,000-value column sample):
///
/// - `event_date` (inferred Date):
///   - rows 0..39_700: `"2024-03-15"` (recent valid)
///   - rows 39_700..39_800: `"2025-01-10"` (100 future dates past sample)
///   - rows 39_800..39_900: `"2015-01-01"` (100 stale dates past sample, > 5y before 2024-06-01)
///   - rows 39_900..39_950: `" 2024-04-01 "` (50 whitespace-padded valid dates, accepted by unified reader)
///   - rows 39_950..39_980: `"2024-4-5"` (30 single-digit valid dates, accepted by unified reader)
///   - rows 39_980..40_000: `"not-a-date"` (20 invalid dates past sample)
/// - `start_date` / `end_date` (inferred Date role pair with nulls (#787) and
///   slash-order settling at row 39_999 (#1305)):
///   - row 0: `("", "02/03/2024")` -> null start skipped without shifting partner rows
///   - row 1: `("01/05/2024", "")` -> null end skipped without shifting partner rows
///   - rows 2..39_950: `("01/05/2024", "02/03/2024")` -> month-first: Jan 5 -> Feb 3 (clean); day-first would be inverted!
///   - rows 39_950..39_999: `("03/01/2024", "02/01/2024")` -> month-first: Mar 1 -> Feb 1 (49 inversions past sample!)
///   - row 39_999: `("12/30/2023", "12/31/2023")` -> settles BOTH columns to `MonthFirst` past sample!
/// - `mixed_hinted` (typed String, hinted via `temporal_columns`, no DateTimeStats):
///   - rows 0..39_900: `"text"` (non-date)
///   - rows 39_900..39_999: `"12/01/2024"` (99 ambiguous slash dates: month-first = Dec 1, 2024 = future!)
///   - row 39_999: `"12/31/2023"` (settles `mixed_hinted` to `MonthFirst` past sample!)
/// - `valid_from` / `valid_until` (typed String, declared via `temporal_pairs`, no DateTimeStats):
///   - rows 0..39_900: `("n/a", "n/a")` (Wait: "n/a" is null-like! Use `"pending"` so non-null.)
///   - rows 39_900..39_990: `("01/05/2024", "02/03/2024")` -> 90 clean pairs month-first
///   - rows 39_990..39_999: `("05/01/2024", "04/01/2024")` -> 9 inversions month-first
///   - row 39_999: `("12/30/2023", "12/31/2023")` -> settles both to `MonthFirst` past sample, 1 clean pair
fn fixture_columns(with_hints: bool) -> FixtureColumns {
    let mut event_date = vec!["2024-03-15".to_string(); ROWS];
    for slot in &mut event_date[39_700..39_800] {
        *slot = "2025-01-10".to_string();
    }
    for slot in &mut event_date[39_800..39_900] {
        *slot = "2015-01-01".to_string();
    }
    for slot in &mut event_date[39_900..39_950] {
        *slot = " 2024-04-01 ".to_string();
    }
    for slot in &mut event_date[39_950..39_980] {
        *slot = "2024-4-5".to_string();
    }
    for slot in &mut event_date[39_980..ROWS] {
        *slot = "not-a-date".to_string();
    }

    let mut start_date = vec!["01/05/2024".to_string(); ROWS];
    let mut end_date = vec!["02/03/2024".to_string(); ROWS];
    start_date[0] = String::new();
    end_date[1] = String::new();
    for row in 39_950..39_999 {
        start_date[row] = "03/01/2024".to_string();
        end_date[row] = "02/01/2024".to_string();
    }
    start_date[39_999] = "12/30/2023".to_string();
    end_date[39_999] = "12/31/2023".to_string();

    if !with_hints {
        return FixtureColumns {
            names: vec!["event_date", "start_date", "end_date"],
            values: vec![event_date, start_date, end_date],
            temporal_columns: vec![],
            temporal_pairs: vec![],
        };
    }

    let mut mixed_hinted = vec!["text".to_string(); ROWS];
    for slot in &mut mixed_hinted[39_900..39_999] {
        *slot = "12/01/2024".to_string();
    }
    mixed_hinted[39_999] = "12/31/2023".to_string();

    let mut valid_from = vec!["pending".to_string(); ROWS];
    let mut valid_until = vec!["pending".to_string(); ROWS];
    for row in 39_900..39_990 {
        valid_from[row] = "01/05/2024".to_string();
        valid_until[row] = "02/03/2024".to_string();
    }
    for row in 39_990..39_999 {
        valid_from[row] = "05/01/2024".to_string();
        valid_until[row] = "04/01/2024".to_string();
    }
    valid_from[39_999] = "12/30/2023".to_string();
    valid_until[39_999] = "12/31/2023".to_string();

    FixtureColumns {
        names: vec![
            "event_date",
            "start_date",
            "end_date",
            "mixed_hinted",
            "valid_from",
            "valid_until",
        ],
        values: vec![
            event_date,
            start_date,
            end_date,
            mixed_hinted,
            valid_from,
            valid_until,
        ],
        temporal_columns: vec!["mixed_hinted".to_string()],
        temporal_pairs: vec![("valid_from".to_string(), "valid_until".to_string())],
    }
}

fn csv_fixture(cols: &FixtureColumns) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", cols.names.join(",")).unwrap();
    for row in 0..ROWS {
        let cells: Vec<&str> = cols.values.iter().map(|c| c[row].as_str()).collect();
        writeln!(file, "{}", cells.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records(cols: &FixtureColumns) -> Vec<String> {
    (0..ROWS)
        .map(|row| {
            let fields: Vec<String> = cols
                .names
                .iter()
                .zip(&cols.values)
                .map(|(name, values)| format!(r#""{}":"{}""#, name, values[row]))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

fn json_fixture(cols: &FixtureColumns) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".json").unwrap();
    write!(file, "[{}]", json_records(cols).join(",")).unwrap();
    file.flush().unwrap();
    file
}

fn jsonl_fixture(cols: &FixtureColumns) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(file, "{}", json_records(cols).join("\n")).unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_text_fixture(cols: &FixtureColumns) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(
        cols.names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = cols
        .values
        .iter()
        .map(|values| {
            let opt: Vec<Option<&str>> = values
                .iter()
                .map(|v| (!v.is_empty()).then_some(v.as_str()))
                .collect();
            Arc::new(StringArray::from(opt)) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn all_route_reports(cols: &FixtureColumns) -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture(cols);
    let hints = SemanticHints::default()
        .with_temporal_columns(cols.temporal_columns.clone())
        .with_temporal_pairs(cols.temporal_pairs.clone());
    let options = AnalysisOptions::default()
        .with_reference_date(Some(day(REFERENCE)))
        .with_semantic_hints(hints.clone());

    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file_with_options(csv.path(), &CsvParserConfig::default(), &options)
            .expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .reference_date(day(REFERENCE))
            .temporal_columns(cols.temporal_columns.clone())
            .temporal_pairs(cols.temporal_pairs.clone())
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    // Quality-only run without statistics pack must match identically.
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .reference_date(day(REFERENCE))
        .temporal_columns(cols.temporal_columns.clone())
        .temporal_pairs(cols.temporal_pairs.clone())
        .metric_packs(vec![MetricPack::Quality])
        .analyze_file(csv.path())
        .expect("quality-only");
    reports.push(("quality_only".to_string(), report));

    let mut files = vec![("json", json_fixture(cols)), ("jsonl", jsonl_fixture(cols))];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_text_fixture(cols)));
    for (label, file) in files {
        let report = Profiler::new()
            .reference_date(day(REFERENCE))
            .temporal_columns(cols.temporal_columns.clone())
            .temporal_pairs(cols.temporal_pairs.clone())
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }

    #[cfg(feature = "parquet")]
    {
        let pq_file = parquet_text_fixture(cols);
        let pq_bytes = std::fs::read(pq_file.path()).unwrap();
        let report = dataprof::analyze_parquet_bytes_with_options(
            pq_bytes.into(),
            "data.parquet",
            &dataprof::ParquetConfig::default(),
            &options,
        )
        .expect("parquet bytes");
        reports.push(("parquet_bytes".to_string(), report));
    }

    #[cfg(feature = "async-streaming")]
    {
        let bytes = std::fs::read(csv.path()).unwrap();
        let info = dataprof::AsyncSourceInfo::new("data.csv", dataprof::FileFormat::Csv);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let report = rt
            .block_on(
                dataprof::AsyncStreamingProfiler::new()
                    .reference_date(day(REFERENCE))
                    .semantic_hints(hints.clone())
                    .analyze_stream(dataprof::BytesSource::new(bytes.into(), info)),
            )
            .expect("async CSV");
        reports.push(("async_csv".to_string(), report));
    }

    #[cfg(feature = "sqlite")]
    if hints.is_empty() {
        use sqlx::Connection;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let db_file = NamedTempFile::with_suffix(".db").unwrap();
        let db_path = db_file.path().to_string_lossy().replace('\\', "/");
        let conn_str = format!("sqlite://{db_path}?mode=rwc");
        let col_defs: String = cols
            .names
            .iter()
            .map(|name| format!("{name} TEXT"))
            .collect::<Vec<_>>()
            .join(", ");
        let col_list = cols.names.join(", ");
        let placeholders = vec!["?"; cols.names.len()].join(", ");
        let insert_sql = format!("INSERT INTO events ({col_list}) VALUES ({placeholders})");
        let select_sql = format!("SELECT {col_list} FROM events");

        let report = rt.block_on(async {
            let mut conn = sqlx::SqliteConnection::connect(&conn_str).await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE events ({col_defs})"
            )))
            .execute(&mut conn)
            .await
            .unwrap();
            let mut tx = conn.begin().await.unwrap();
            for row in 0..ROWS {
                let mut query = sqlx::query(sqlx::AssertSqlSafe(insert_sql.clone()));
                for col in &cols.values {
                    let val: Option<&str> = (!col[row].is_empty()).then_some(col[row].as_str());
                    query = query.bind(val);
                }
                query.execute(&mut *tx).await.unwrap();
            }
            tx.commit().await.unwrap();
            drop(conn);

            let db_config = dataprof::DatabaseConfig {
                connection_string: conn_str,
                ..Default::default()
            };
            dataprof::analyze_database_with_options(db_config, &select_sql, &options)
                .await
                .unwrap()
        });
        reports.push(("sqlite".to_string(), report));
    }

    reports
}

fn serialized_timeliness(report: &ProfileReport) -> Value {
    let doc = serde_json::to_value(report).unwrap();
    serde_json::json!({
        "metrics": doc["quality"]["metrics"]["timeliness"],
        "score": doc["quality"]["scores"]["dimension_scores"]["timeliness"],
    })
}

#[test]
fn every_route_counts_timeliness_and_hinted_slash_order_over_every_value() {
    let cols = fixture_columns(true);
    let reports = all_route_reports(&cols);
    assert!(reports.len() >= 7, "only {} routes ran", reports.len());

    // Expected counts across all 6 temporal columns:
    // - event_date: 40_000 checked, 39_980 valid (50 whitespace + 30 single-digit included),
    //   20 invalid, 100 future, 100 stale
    // - start_date: 39_999 checked, 39_999 valid, 0 invalid, 0 future, 0 stale
    // - end_date: 39_999 checked, 39_999 valid, 0 invalid, 0 future, 0 stale
    // - mixed_hinted: 40_000 checked, 100 valid (99 future month-first + 1 settling),
    //   39_900 invalid, 99 future, 0 stale
    // - valid_from: 40_000 checked, 100 valid, 39_900 invalid, 0 future, 0 stale
    // - valid_until: 40_000 checked, 100 valid, 39_900 invalid, 0 future, 0 stale
    // Totals:
    // - date_values_checked: 239_998
    // - invalid_date_values: 20 + 39_900 * 3 = 119_720
    // - future_dates_count: 100 + 99 = 199
    // - temporal_pairs_checked: (40_000 - 2) + 100 = 40_098
    // - temporal_violations: 49 + 9 = 58
    let reference_value = serialized_timeliness(&reports[0].1);
    let mut wrong = Vec::new();
    for (label, report) in &reports {
        let quality = report.quality.as_ref().unwrap();
        let t = quality.metrics.timeliness.as_ref().unwrap();
        let got = (
            t.date_values_checked,
            t.invalid_date_values,
            t.future_dates_count,
            t.temporal_pairs_checked,
            t.temporal_violations,
        );
        let expected = (239_998, 119_720, 199, 40_098, 58);
        if got != expected {
            wrong.push(format!("[{label}] timeliness={got:?} != {expected:?}"));
        }
        let exact = quality.exact_dimensions().unwrap_or_default();
        let sampled = quality.sampled_dimensions().unwrap_or_default();
        if !exact.contains(&"timeliness".to_string()) || sampled.contains(&"timeliness".to_string())
        {
            wrong.push(format!(
                "[{label}] timeliness provenance wrong: exact={exact:?} sampled={sampled:?}"
            ));
        }
        let semantics = report.metric_semantics.as_ref().and_then(|s| s.value_facts);
        if semantics != Some(ValueFacts::EveryValueConsistencyPrecisionAccuracyRulesTimeliness) {
            wrong.push(format!("[{label}] value_facts={semantics:?}"));
        }
        let serialized = serialized_timeliness(report);
        if serialized != reference_value {
            wrong.push(format!(
                "[{label}] serialized timeliness differs:\n  got: {serialized}\n  ref: {reference_value}"
            ));
        }
        // Quality-only must not leak DateTimeStats.
        if label == "quality_only" {
            for col in &report.column_profiles {
                if !matches!(col.stats, ColumnStats::None) {
                    wrong.push(format!("[{label}] {} leaked stats", col.name));
                }
            }
        } else {
            // `event_date`'s column invalid_count matches timeliness invalid dates (20),
            // confirming DateTimeStats, invalid_count, and timeliness share one date reading.
            let event_col = report
                .column_profiles
                .iter()
                .find(|c| c.name == "event_date")
                .unwrap();
            if event_col.invalid_count != Some(20) {
                wrong.push(format!(
                    "[{label}] event_date.invalid_count={:?} != Some(20)",
                    event_col.invalid_count
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn inferred_timeliness_matches_across_every_route_including_database() {
    // No semantic hints so SQLite joins the same 40,000-row parity matrix.
    let cols = fixture_columns(false);
    let reports = all_route_reports(&cols);
    #[cfg(feature = "sqlite")]
    assert!(
        reports.iter().any(|(label, _)| label == "sqlite"),
        "expected sqlite route in parity matrix"
    );

    let reference_value = serialized_timeliness(&reports[0].1);
    let expected = (119_998, 20, 100, 39_998, 49);
    let mut wrong = Vec::new();
    for (label, report) in &reports {
        let quality = report.quality.as_ref().unwrap();
        let t = quality.metrics.timeliness.as_ref().unwrap();
        let got = (
            t.date_values_checked,
            t.invalid_date_values,
            t.future_dates_count,
            t.temporal_pairs_checked,
            t.temporal_violations,
        );
        if got != expected {
            wrong.push(format!("[{label}] timeliness={got:?} != {expected:?}"));
        }
        let exact = quality.exact_dimensions().unwrap_or_default();
        let sampled = quality.sampled_dimensions().unwrap_or_default();
        if !exact.contains(&"timeliness".to_string()) || sampled.contains(&"timeliness".to_string())
        {
            wrong.push(format!("[{label}] exact={exact:?} sampled={sampled:?}"));
        }
        if serialized_timeliness(report) != reference_value {
            wrong.push(format!(
                "[{label}] serialized timeliness differs from standard"
            ));
        }
        let value_facts = report.metric_semantics.as_ref().and_then(|s| s.value_facts);
        if value_facts != Some(ValueFacts::EveryValueConsistencyPrecisionAccuracyRulesTimeliness) {
            wrong.push(format!("[{label}] value_facts={value_facts:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn issue_1422_timeliness_is_exact_on_every_route() {
    // #1422 fixture: 40,000 rows of `start_date,end_date` with month-first
    // settling on the last row. Every route places `timeliness` in
    // `exact_dimensions` and not in `sampled_dimensions`, and serializes
    // identical timeliness metrics and score.
    let mut start_date = vec!["01/05/2024".to_string(); ROWS];
    let mut end_date = vec!["02/03/2024".to_string(); ROWS];
    start_date[ROWS - 1] = "12/30/2023".to_string();
    end_date[ROWS - 1] = "12/31/2023".to_string();

    let cols = FixtureColumns {
        names: vec!["start_date", "end_date"],
        values: vec![start_date, end_date],
        temporal_columns: vec![],
        temporal_pairs: vec![],
    };
    let reports = all_route_reports(&cols);
    let first = serialized_timeliness(&reports[0].1);
    for (label, report) in &reports {
        let quality = report.quality.as_ref().unwrap();
        let exact = quality.exact_dimensions().unwrap_or_default();
        let sampled = quality.sampled_dimensions().unwrap_or_default();
        assert!(
            exact.contains(&"timeliness".to_string()),
            "[{label}] timeliness not in exact_dimensions: {exact:?}"
        );
        assert!(
            !sampled.contains(&"timeliness".to_string()),
            "[{label}] timeliness still in sampled_dimensions: {sampled:?}"
        );
        assert_eq!(serialized_timeliness(report), first, "[{label}]");
    }
}

#[test]
fn a_lone_pair_in_a_wide_table_is_never_reported_with_zero_rows() {
    // 17 `start_*` and 16 `end_*` names exceed the tracker's 256 cross-product
    // cap, so the unrelated `start_a` / `end_b` pair is not tracked row by
    // row. Only those two columns hold dates, so the calculator still selects
    // them as the lone pair: the report must count its rows, not list it as
    // compared with nothing checked.
    //
    // JSONL registers late keys one at a time, after `start_a` and `end_bx`,
    // so the pair is under the cap when it is registered; the tracked set
    // must still match the CSV header's, or past the 10,000-value sample the
    // two routes disagree on whether timeliness is exact, and on its counts.
    let starts: Vec<String> = ('a'..='q').map(|c| format!("start_{c}")).collect();
    let ends: Vec<String> = ('b'..='q').map(|c| format!("end_{c}x")).collect();
    let rows: Vec<(&str, &str)> = (0..12_000)
        .map(|row| {
            if (11_000..11_005).contains(&row) {
                ("2024-03-10", "2024-03-01")
            } else {
                ("2024-03-01", "2024-03-10")
            }
        })
        .collect();

    let mut csv = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(csv, "{},{}", starts.join(","), ends.join(",")).unwrap();
    for (start, end) in &rows {
        let mut cells = vec![start.to_string()];
        cells.extend(std::iter::repeat_n("x".to_string(), starts.len() - 1));
        cells.push(end.to_string());
        cells.extend(std::iter::repeat_n("x".to_string(), ends.len() - 1));
        writeln!(csv, "{}", cells.join(",")).unwrap();
    }
    csv.flush().unwrap();

    // The first record holds only the pair; every other key arrives in the
    // second, so the reader registers them one by one after the pair.
    let mut jsonl = NamedTempFile::with_suffix(".jsonl").unwrap();
    for (row, (start, end)) in rows.iter().enumerate() {
        let mut fields = vec![
            format!("\"start_a\":\"{start}\""),
            format!("\"end_bx\":\"{end}\""),
        ];
        if row > 0 {
            fields.extend(starts[1..].iter().map(|name| format!("\"{name}\":\"x\"")));
            fields.extend(ends[1..].iter().map(|name| format!("\"{name}\":\"x\"")));
        }
        writeln!(jsonl, "{{{}}}", fields.join(",")).unwrap();
    }
    jsonl.flush().unwrap();

    let mut seen = Vec::new();
    for path in [csv.path(), jsonl.path()] {
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .reference_date(day(REFERENCE))
            .analyze_file(path)
            .unwrap();
        let quality = report.quality.as_ref().unwrap();
        let t = quality.metrics.timeliness.as_ref().unwrap();
        assert!(t.temporal_pairs_checked > 0, "{path:?}: {t:?}");
        let exact = quality
            .exact_dimensions()
            .unwrap_or_default()
            .contains(&"timeliness".to_string());
        seen.push((exact, serialized_timeliness(&report)));
    }
    assert_eq!(seen[0], seen[1], "CSV vs JSONL timeliness");
}

#[test]
fn truncated_csv_reports_exact_timeliness_for_read_rows_only() {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "start_date,end_date").unwrap();
    for row in 0..20 {
        if row < 10 {
            writeln!(file, "2024-01-01,2024-01-05").unwrap();
        } else {
            writeln!(file, "2029-06-01,2024-01-01").unwrap();
        }
    }
    file.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .reference_date(day(REFERENCE))
        .stop_when(StopCondition::MaxRows(10))
        .analyze_file(file.path())
        .unwrap();
    let t = report
        .quality
        .as_ref()
        .unwrap()
        .metrics
        .timeliness
        .as_ref()
        .unwrap();
    assert_eq!(
        (
            t.date_values_checked,
            t.future_dates_count,
            t.temporal_pairs_checked,
            t.temporal_violations
        ),
        (20, 0, 10, 0)
    );
}
