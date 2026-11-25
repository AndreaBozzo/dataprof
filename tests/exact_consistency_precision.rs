//! Consistency and precision are counted over every scanned value (#788 step 2).
//!
//! File, Arrow, streaming and database routes all feed the same per-column
//! lexical/quality counters, so minority type violations, mixed date formats,
//! mixed decimal separators, encoding issues, date-named digit codes and
//! minority decimal scales placed past the 10,000-value sample produce
//! identical serialized consistency and precision blocks across every route,
//! and both dimensions move from `sampled_dimensions` to `exact_dimensions`.
//!
//! `python/tests/test_exact_consistency_precision.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CsvParserConfig, EngineType, ProfileReport, Profiler, QualityDimension, StopCondition,
    ValueFacts, analyze_csv_file,
};
use serde_json::Value;
use tempfile::NamedTempFile;

/// Four times `COLUMN_SAMPLE_CAPACITY` (10,000).
const ROWS: usize = 40_000;

struct FixtureColumns {
    names: Vec<&'static str>,
    values: Vec<Vec<String>>,
}

fn fixture_columns() -> FixtureColumns {
    // 1. Float column with 200 non-numeric junk tokens past row 35,000.
    //    39,800 / 40,000 = 99.5% numeric -> Float; 200 type-inconsistent values.
    let mut code: Vec<String> = (0..ROWS).map(|i| (1 + i % 900).to_string()).collect();
    for slot in &mut code[35_000..35_200] {
        *slot = "junk_token".to_string();
    }

    // 2. Float column with 500 scale-4 values past row 39,500 (rest scale-2).
    let mut amount: Vec<String> = (0..ROWS)
        .map(|i| format!("{}.{}{}", 10 + (i % 90), 1 + (i % 9), 1 + (i % 8)))
        .collect();
    for slot in &mut amount[39_500..40_000] {
        *slot = "12.3456".to_string();
    }

    // 3. Date column with 300 DD/MM/YYYY values past row 39,700 (rest YYYY-MM-DD).
    let mut event_date: Vec<String> = (0..ROWS)
        .map(|i| format!("2024-03-{:02}", 1 + (i % 28)))
        .collect();
    for slot in &mut event_date[39_700..40_000] {
        *slot = "15/03/2024".to_string();
    }

    // 4. Text column with 200 mojibake values past row 39,800.
    let mut note: Vec<String> = (0..ROWS).map(|i| format!("item_{i}")).collect();
    for slot in &mut note[39_800..40_000] {
        *slot = "Caf\u{00c3}\u{00a9}".to_string();
    }

    // 5. Text column with 39,800 single-dot, 100 single-comma, 100 both-separators.
    let mut sep: Vec<String> = (0..ROWS).map(|i| format!("v{}.5", i % 100)).collect();
    for slot in &mut sep[39_800..39_900] {
        *slot = "v1,5".to_string();
    }
    for slot in &mut sep[39_900..40_000] {
        *slot = "1,234.56".to_string();
    }

    // 6. Date-named string column holding digit codes (exact 1% zero-padded past
    //    row 39,600): must be scored by its dominant lexical class (40,000 numeric),
    //    not held to dates (0 dates).
    let mut departure_time: Vec<String> = vec!["1045".to_string(); ROWS];
    for slot in &mut departure_time[39_600..40_000] {
        *slot = "0930".to_string();
    }

    FixtureColumns {
        names: vec![
            "code",
            "amount",
            "event_date",
            "note",
            "sep",
            "departure_time",
        ],
        values: vec![code, amount, event_date, note, sep, departure_time],
    }
}

fn csv_fixture(cols: &FixtureColumns) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", cols.names.join(";")).unwrap();
    for row in 0..ROWS {
        let cells: Vec<&str> = cols.values.iter().map(|c| c[row].as_str()).collect();
        writeln!(file, "{}", cells.join(";")).unwrap();
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
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = cols
        .values
        .iter()
        .map(|values| Arc::new(StringArray::from(values.clone())) as ArrayRef)
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

    let mut files = vec![("json", json_fixture(cols)), ("jsonl", jsonl_fixture(cols))];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_text_fixture(cols)));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }

    #[cfg(feature = "async-streaming")]
    {
        let bytes = std::fs::read(csv.path()).unwrap();
        let info = dataprof::AsyncSourceInfo::new("data.csv", dataprof::FileFormat::Csv);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let report = rt
            .block_on(
                dataprof::AsyncStreamingProfiler::new()
                    .analyze_stream(dataprof::BytesSource::new(bytes.into(), info)),
            )
            .expect("async CSV");
        reports.push(("async_csv".to_string(), report));
    }

    #[cfg(feature = "sqlite")]
    {
        use sqlx::Connection;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let db_file = NamedTempFile::with_suffix(".db").unwrap();
        let db_path = db_file.path().to_string_lossy().replace('\\', "/");
        let conn_str = format!("sqlite://{db_path}?mode=rwc");
        let report = rt.block_on(async {
            let mut conn = sqlx::SqliteConnection::connect(&conn_str).await.unwrap();
            sqlx::query(
                "CREATE TABLE items (\
                 code TEXT NOT NULL, \
                 amount TEXT NOT NULL, \
                 event_date TEXT NOT NULL, \
                 note TEXT NOT NULL, \
                 sep TEXT NOT NULL, \
                 departure_time TEXT NOT NULL)",
            )
            .execute(&mut conn)
            .await
            .unwrap();
            let mut tx = conn.begin().await.unwrap();
            for row in 0..ROWS {
                sqlx::query(
                    "INSERT INTO items (code, amount, event_date, note, sep, departure_time) \
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(&cols.values[0][row])
                .bind(&cols.values[1][row])
                .bind(&cols.values[2][row])
                .bind(&cols.values[3][row])
                .bind(&cols.values[4][row])
                .bind(&cols.values[5][row])
                .execute(&mut *tx)
                .await
                .unwrap();
            }
            tx.commit().await.unwrap();
            drop(conn);

            let db_config = dataprof::DatabaseConfig {
                connection_string: conn_str,
                ..Default::default()
            };
            dataprof::analyze_database_with_options(
                db_config,
                "SELECT code, amount, event_date, note, sep, departure_time FROM items",
                &dataprof::AnalysisOptions::default(),
            )
            .await
            .unwrap()
        });
        reports.push(("sqlite".to_string(), report));
    }

    reports
}

#[test]
fn every_route_counts_consistency_and_precision_over_every_value() {
    let cols = fixture_columns();
    let reports = all_route_reports(&cols);
    let mut wrong = Vec::new();

    // Expected exact counts across all 6 columns * 40,000 rows:
    // - values_checked = 240,000
    // - inconsistent_values = 200 (`junk_token` in `code`)
    // - data_type_consistency = (239_800 / 240_000) * 100 = 99.9166... -> 99.92
    // - format_violations = 300 (mixed dates) + 100 (single comma) + 100 (both separators) = 500
    // - encoding_issues = 200 (mojibake in `note`)
    // - numeric_values_checked = 39,800 (`code` Float) + 40,000 (`amount` Float) = 79,800
    // - inconsistent_precision_values = 500 (scale-4 in `amount`)
    // - decimal_places_consistency = (79_300 / 79_800) * 100 = 99.3734... -> 99.37
    let mut baseline_consistency: Option<Value> = None;
    let mut baseline_precision: Option<Value> = None;
    let mut baseline_cons_score: Option<Value> = None;
    let mut baseline_prec_score: Option<Value> = None;

    for (label, report) in &reports {
        let doc: Value = serde_json::to_value(report).unwrap();
        let semantics = report.metric_semantics.as_ref().and_then(|s| s.value_facts);
        if semantics != Some(ValueFacts::EveryValueConsistencyPrecisionAccuracyRulesTimeliness) {
            wrong.push(format!("[{label}] value_facts={semantics:?}"));
        }

        let quality = report.quality.as_ref().expect("quality assessment");
        let exact = quality.exact_dimensions().unwrap_or_default();
        let sampled = quality.sampled_dimensions().unwrap_or_default();
        for dim in ["consistency", "precision"] {
            if !exact.iter().any(|d| d == dim) {
                wrong.push(format!(
                    "[{label}] {dim} missing from exact_dimensions={exact:?}"
                ));
            }
            if sampled.iter().any(|d| d == dim) {
                wrong.push(format!(
                    "[{label}] {dim} still in sampled_dimensions={sampled:?}"
                ));
            }
        }

        let q = &doc["quality"];
        let cons = &q["metrics"]["consistency"];
        let prec = &q["metrics"]["precision"];
        let cons_score = &q["scores"]["dimension_scores"]["consistency"];
        let prec_score = &q["scores"]["dimension_scores"]["precision"];

        if cons["values_checked"] != 240_000 {
            wrong.push(format!(
                "[{label}] values_checked={}",
                cons["values_checked"]
            ));
        }
        if cons["format_violations"] != 500 {
            wrong.push(format!(
                "[{label}] format_violations={}",
                cons["format_violations"]
            ));
        }
        if cons["encoding_issues"] != 200 {
            wrong.push(format!(
                "[{label}] encoding_issues={}",
                cons["encoding_issues"]
            ));
        }
        if cons["data_type_consistency"] != 99.92 {
            wrong.push(format!(
                "[{label}] data_type_consistency={}",
                cons["data_type_consistency"]
            ));
        }
        if prec["numeric_values_checked"] != 79_800 {
            wrong.push(format!(
                "[{label}] numeric_values_checked={}",
                prec["numeric_values_checked"]
            ));
        }
        if prec["inconsistent_precision_values"] != 500 {
            wrong.push(format!(
                "[{label}] inconsistent_precision_values={}",
                prec["inconsistent_precision_values"]
            ));
        }
        if prec["decimal_places_consistency"] != 99.37 {
            wrong.push(format!(
                "[{label}] decimal_places_consistency={}",
                prec["decimal_places_consistency"]
            ));
        }

        // Exact dimensions carry no estimate width: outward 2dp rounding around
        // the exact score leaves width <= 0.01 (identical to completeness).
        if let Some(bounds) = q.get("score_bounds").and_then(Value::as_object) {
            let dim_bounds = &bounds["dimension_scores"];
            for (dim, score) in [("consistency", cons_score), ("precision", prec_score)] {
                let interval = &dim_bounds[dim];
                let lower = interval["lower"].as_f64().unwrap();
                let upper = interval["upper"].as_f64().unwrap();
                let s = score.as_f64().unwrap();
                if lower > s || s > upper || (upper - lower) > 0.01 + 1e-9 {
                    wrong.push(format!(
                        "[{label}] score_bounds.{dim}={interval} not an exact 2dp interval for {score}"
                    ));
                }
            }
        }

        match (
            &baseline_consistency,
            &baseline_precision,
            &baseline_cons_score,
            &baseline_prec_score,
        ) {
            (None, _, _, _) => {
                baseline_consistency = Some(cons.clone());
                baseline_precision = Some(prec.clone());
                baseline_cons_score = Some(cons_score.clone());
                baseline_prec_score = Some(prec_score.clone());
            }
            (Some(bc), Some(bp), Some(bcs), Some(bps)) => {
                if cons != bc {
                    wrong.push(format!("[{label}] consistency {cons} != baseline {bc}"));
                }
                if prec != bp {
                    wrong.push(format!("[{label}] precision {prec} != baseline {bp}"));
                }
                if cons_score != bcs {
                    wrong.push(format!(
                        "[{label}] cons_score {cons_score} != baseline {bcs}"
                    ));
                }
                if prec_score != bps {
                    wrong.push(format!(
                        "[{label}] prec_score {prec_score} != baseline {bps}"
                    ));
                }
            }
            _ => unreachable!(),
        }
    }

    assert!(wrong.is_empty(), "mismatches:\n{}", wrong.join("\n"));
}

#[cfg(feature = "parquet")]
#[test]
fn typed_parquet_float64_matches_csv_consistency_and_precision_at_40k_rows() {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let ids: Vec<i64> = (0..ROWS as i64).collect();
    let prices: Vec<f64> = (0..ROWS)
        .map(|i| if i >= 39_500 { 12.3456 } else { 12.34 })
        .collect();

    let mut csv_file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(csv_file, "id,price").unwrap();
    for (&id, &price) in ids.iter().zip(&prices) {
        writeln!(csv_file, "{id},{price}").unwrap();
    }
    csv_file.flush().unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(Float64Array::from(prices)) as ArrayRef,
        ],
    )
    .unwrap();
    let pq_file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(pq_file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let csv_doc: Value =
        serde_json::to_value(Profiler::new().analyze_file(csv_file.path()).unwrap()).unwrap();
    let pq_doc: Value =
        serde_json::to_value(Profiler::new().analyze_file(pq_file.path()).unwrap()).unwrap();

    assert_eq!(
        pq_doc["quality"]["metrics"]["consistency"],
        csv_doc["quality"]["metrics"]["consistency"]
    );
    assert_eq!(
        pq_doc["quality"]["metrics"]["precision"],
        csv_doc["quality"]["metrics"]["precision"]
    );
    assert_eq!(
        pq_doc["quality"]["metrics"]["precision"]["numeric_values_checked"],
        ROWS
    );
    assert_eq!(
        pq_doc["quality"]["metrics"]["precision"]["inconsistent_precision_values"],
        500
    );
}

#[test]
fn requesting_only_exact_dimensions_emits_no_score_bounds_and_partial_read_stays_exact() {
    let cols = fixture_columns();
    let csv = csv_fixture(&cols);

    let exact_only = Profiler::new()
        .quality_dimensions(vec![
            QualityDimension::Completeness,
            QualityDimension::Consistency,
            QualityDimension::Precision,
        ])
        .analyze_file(csv.path())
        .unwrap();
    let quality = exact_only.quality.as_ref().unwrap();
    assert!(quality.score_bounds().is_none());
    assert_eq!(
        quality.sampled_dimensions().unwrap_or_default(),
        Vec::<String>::new()
    );
    let exact = quality.exact_dimensions().unwrap_or_default();
    assert!(exact.contains(&"completeness".to_string()));
    assert!(exact.contains(&"consistency".to_string()));
    assert!(exact.contains(&"precision".to_string()));

    // Partial read via row cap: consistency and precision follow completeness
    // and stay in `exact_dimensions` (exact over every scanned row).
    let capped = Profiler::new()
        .stop_when(StopCondition::MaxRows(15_000))
        .analyze_file(csv.path())
        .unwrap();
    let capped_q = capped.quality.as_ref().unwrap();
    let capped_exact = capped_q.exact_dimensions().unwrap_or_default();
    let capped_sampled = capped_q.sampled_dimensions().unwrap_or_default();
    assert!(capped_exact.contains(&"completeness".to_string()));
    assert!(capped_exact.contains(&"consistency".to_string()));
    assert!(capped_exact.contains(&"precision".to_string()));
    assert!(!capped_sampled.contains(&"consistency".to_string()));
    assert!(!capped_sampled.contains(&"precision".to_string()));
    assert_eq!(
        capped_q
            .metrics
            .consistency
            .as_ref()
            .unwrap()
            .values_checked,
        15_000 * 6
    );
}
