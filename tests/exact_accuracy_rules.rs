//! Accuracy rules and value-driven semantic-hint bindings are counted over
//! every scanned value (#788 step 3).
//!
//! File, Arrow/Parquet, async streaming and database routes all feed the same
//! per-column `LexicalTally` counters and derive both accuracy rule counts
//! (`rule_values_checked`, `rule_violating_values`, `range_violations`,
//! `negative_values_in_positive`, `numeric_values_checked`) and value-driven
//! `semantic_hint_bindings` from one shared implementation.
//!
//! `python/tests/test_exact_accuracy_rules.py` is the Python twin.

use std::io::Write;

use dataprof::{
    AnalysisOptions, CsvParserConfig, EngineType, ProfileReport, Profiler, SemanticHintBinding,
    SemanticHintKind, SemanticHints, StopCondition, ValueFacts, analyze_csv_file_with_options,
};
use serde_json::Value;
use tempfile::NamedTempFile;

/// Four times `COLUMN_SAMPLE_CAPACITY` (10,000).
const ROWS: usize = 40_000;

struct FixtureColumns {
    names: Vec<&'static str>,
    values: Vec<Vec<String>>,
    positive_columns: Vec<String>,
    temporal_columns: Vec<String>,
}

fn fixture_columns() -> FixtureColumns {
    // 1. Name-based age rule (0..=150): 200 violations past row 39,800.
    let mut customer_age: Vec<String> = (0..ROWS).map(|i| (20 + (i % 40)).to_string()).collect();
    for slot in &mut customer_age[39_800..39_900] {
        *slot = "-5".to_string();
    }
    for slot in &mut customer_age[39_900..ROWS] {
        *slot = "200".to_string();
    }

    // 2. Overlapping percent rule (0..=100) + positive_columns hint:
    //    100 negatives (-10 -> breaks both rules, 1 violating value) and
    //    100 values just above 100 (exact decimal comparison) past row 39,800.
    let mut discount_percent: Vec<String> =
        (0..ROWS).map(|i| (10 + (i % 80)).to_string()).collect();
    for slot in &mut discount_percent[39_800..39_900] {
        *slot = "-10".to_string();
    }
    for slot in &mut discount_percent[39_900..ROWS] {
        *slot = "100.000000000000000001".to_string();
    }

    // 3. Name-based count rule (>= 0): 150 negative values past row 39,850.
    let mut item_count: Vec<String> = (0..ROWS).map(|i| (i % 500).to_string()).collect();
    for slot in &mut item_count[39_850..ROWS] {
        *slot = "-1".to_string();
    }

    // 4. Name-based year rule (1900..=2100): 100 out-of-range values past 39,900.
    let mut birth_year: Vec<String> = (0..ROWS).map(|i| (1950 + (i % 70)).to_string()).collect();
    for slot in &mut birth_year[39_900..39_950] {
        *slot = "1850".to_string();
    }
    for slot in &mut birth_year[39_950..ROWS] {
        *slot = "2150".to_string();
    }

    // 5. Sign-only positive_columns hint: 50 subnormal negatives past 39,950.
    let mut balance: Vec<String> = (0..ROWS).map(|i| (100 + (i % 800)).to_string()).collect();
    for slot in &mut balance[39_950..ROWS] {
        *slot = "-1e-400".to_string();
    }

    // 6. Unruled numeric column: contributes to numeric_values_checked only.
    let price: Vec<String> = (0..ROWS).map(|i| (10 + (i % 90)).to_string()).collect();

    // 7. Mixed text column hinted positive_columns: 5 numeric values (negative)
    //    past row 39,995. Must bind (not raise inert) and add 5 checked / 5
    //    violating values across every route.
    let mut mixed_positive: Vec<String> = vec!["not_a_number".to_string(); ROWS];
    for slot in &mut mixed_positive[39_995..ROWS] {
        *slot = "-5".to_string();
    }

    // 8. Mixed text column hinted temporal_columns: 5 dates past row 39,995.
    let mut event_when: Vec<String> = vec!["not_a_date".to_string(); ROWS];
    for slot in &mut event_when[39_995..ROWS] {
        *slot = "2024-06-15".to_string();
    }

    FixtureColumns {
        names: vec![
            "customer_age",
            "discount_percent",
            "item_count",
            "birth_year",
            "balance",
            "price",
            "mixed_positive",
            "event_when",
        ],
        values: vec![
            customer_age,
            discount_percent,
            item_count,
            birth_year,
            balance,
            price,
            mixed_positive,
            event_when,
        ],
        positive_columns: vec![
            "discount_percent".to_string(),
            "balance".to_string(),
            "mixed_positive".to_string(),
        ],
        temporal_columns: vec!["event_when".to_string()],
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

fn hints_for(cols: &FixtureColumns) -> SemanticHints {
    SemanticHints::new(cols.positive_columns.clone(), vec![])
        .with_temporal_columns(cols.temporal_columns.clone())
}

fn all_route_reports(cols: &FixtureColumns) -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture(cols);
    let hints = hints_for(cols);
    let options = AnalysisOptions::default().with_semantic_hints(hints.clone());

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
            .positive_columns(cols.positive_columns.clone())
            .temporal_columns(cols.temporal_columns.clone())
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let mut files = vec![("json", json_fixture(cols)), ("jsonl", jsonl_fixture(cols))];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_text_fixture(cols)));
    for (label, file) in files {
        let report = Profiler::new()
            .positive_columns(cols.positive_columns.clone())
            .temporal_columns(cols.temporal_columns.clone())
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
            .map(|name| format!("{name} TEXT NOT NULL"))
            .collect::<Vec<_>>()
            .join(", ");
        let col_list = cols.names.join(", ");
        let placeholders = vec!["?"; cols.names.len()].join(", ");
        let insert_sql = format!("INSERT INTO items ({col_list}) VALUES ({placeholders})");
        let select_sql = format!("SELECT {col_list} FROM items");

        let report = rt.block_on(async {
            let mut conn = sqlx::SqliteConnection::connect(&conn_str).await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE items ({col_defs})"
            )))
            .execute(&mut conn)
            .await
            .unwrap();
            let mut tx = conn.begin().await.unwrap();
            for row in 0..ROWS {
                let mut query = sqlx::query(sqlx::AssertSqlSafe(insert_sql.clone()));
                for col in &cols.values {
                    query = query.bind(&col[row]);
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

#[test]
fn every_route_counts_accuracy_rules_and_hint_bindings_over_every_value() {
    let cols = fixture_columns();
    let expected_bindings = vec![
        SemanticHintBinding {
            column: "discount_percent".to_string(),
            kind: SemanticHintKind::Positive,
            checked_values: ROWS,
            matched_values: ROWS,
            exact: true,
        },
        SemanticHintBinding {
            column: "balance".to_string(),
            kind: SemanticHintKind::Positive,
            checked_values: ROWS,
            matched_values: ROWS,
            exact: true,
        },
        SemanticHintBinding {
            column: "mixed_positive".to_string(),
            kind: SemanticHintKind::Positive,
            checked_values: ROWS,
            matched_values: 5,
            exact: true,
        },
        SemanticHintBinding {
            column: "event_when".to_string(),
            kind: SemanticHintKind::Temporal,
            checked_values: ROWS,
            matched_values: 5,
            exact: true,
        },
    ];

    let mut wrong = Vec::new();
    for (label, report) in all_route_reports(&cols) {
        let semantics = report.metric_semantics.as_ref().and_then(|s| s.value_facts);
        if semantics != Some(ValueFacts::EveryValueConsistencyPrecisionAccuracyRules) {
            wrong.push(format!("[{label}] value_facts={semantics:?}"));
        }

        let quality = report.quality.as_ref().expect("quality assessment");
        let acc = quality.metrics.accuracy.as_ref().expect("accuracy metrics");
        let got = (
            acc.rule_values_checked,
            acc.rule_violating_values,
            acc.range_violations,
            acc.negative_values_in_positive,
            acc.numeric_values_checked,
        );
        let expected = (200_005, 705, 650, 155, 240_005);
        if got != expected {
            wrong.push(format!("[{label}] rules={got:?} != {expected:?}"));
        }

        if report.semantic_hint_bindings != expected_bindings {
            wrong.push(format!(
                "[{label}] bindings={:?} != {:?}",
                report.semantic_hint_bindings, expected_bindings
            ));
        }

        // Serialized JSON carries the exact same rule counts.
        let doc: Value = serde_json::to_value(&report).unwrap();
        let acc_json = &doc["quality"]["metrics"]["accuracy"];
        if acc_json["rule_values_checked"] != 200_005
            || acc_json["rule_violating_values"] != 705
            || acc_json["range_violations"] != 650
            || acc_json["negative_values_in_positive"] != 155
            || acc_json["numeric_values_checked"] != 240_005
        {
            wrong.push(format!("[{label}] serialized accuracy={acc_json}"));
        }
    }

    assert!(
        wrong.is_empty(),
        "route disagreements:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn ruled_only_non_numeric_columns_move_accuracy_to_exact_dimensions() {
    // Only a text column hinted `positive_columns`, with 100 numeric values
    // (50 valid, 50 negative) placed past row 39,900. No column is eligible for
    // the outlier test, so accuracy has no sampled component and leaves
    // `sampled_dimensions` across every route.
    let mut mixed_positive: Vec<String> = vec!["not_a_number".to_string(); ROWS];
    for slot in &mut mixed_positive[39_900..39_950] {
        *slot = "10".to_string();
    }
    for slot in &mut mixed_positive[39_950..ROWS] {
        *slot = "-5".to_string();
    }

    let cols = FixtureColumns {
        names: vec!["mixed_positive"],
        values: vec![mixed_positive],
        positive_columns: vec!["mixed_positive".to_string()],
        temporal_columns: vec![],
    };

    let mut wrong = Vec::new();
    for (label, report) in all_route_reports(&cols) {
        let quality = report.quality.as_ref().expect("quality assessment");
        let acc = quality.metrics.accuracy.as_ref().expect("accuracy metrics");
        if (acc.rule_values_checked, acc.rule_violating_values) != (100, 50) {
            wrong.push(format!(
                "[{label}] rules=({}, {}) != (100, 50)",
                acc.rule_values_checked, acc.rule_violating_values
            ));
        }
        let score = quality.scores().dimension_scores["accuracy"];
        if score != Some(50.0) {
            wrong.push(format!("[{label}] accuracy score={score:?} != Some(50.0)"));
        }
        let sampled = quality.sampled_dimensions().unwrap_or_default();
        let exact = quality.exact_dimensions().unwrap_or_default();
        if sampled.contains(&"accuracy".to_string()) {
            wrong.push(format!(
                "[{label}] accuracy still in sampled_dimensions={sampled:?}"
            ));
        }
        if !exact.contains(&"accuracy".to_string()) {
            wrong.push(format!(
                "[{label}] accuracy missing from exact_dimensions={exact:?}"
            ));
        }
    }

    assert!(
        wrong.is_empty(),
        "route disagreements:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn truncated_csv_reports_exact_rules_for_read_rows_and_non_exact_hint_bindings() {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "customer_age,balance").unwrap();
    for i in 0..20 {
        if i < 10 {
            writeln!(file, "30,not_a_number").unwrap();
        } else {
            writeln!(file, "-5,100").unwrap();
        }
    }
    file.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .stop_when(StopCondition::MaxRows(10))
        .positive_columns(vec!["balance".to_string()])
        .analyze_file(file.path())
        .expect("unread rows may hold a match, so balance hint is not proven inert");

    let acc = report
        .quality
        .as_ref()
        .unwrap()
        .metrics
        .accuracy
        .as_ref()
        .unwrap();
    assert_eq!(
        (acc.rule_values_checked, acc.rule_violating_values),
        (10, 0)
    );

    let binding = report
        .semantic_hint_bindings
        .iter()
        .find(|b| b.column == "balance")
        .expect("balance binding");
    assert_eq!((binding.checked_values, binding.matched_values), (10, 0));
    assert!(!binding.exact);
    assert!(!binding.is_proven_inert());
}

#[test]
fn name_based_accuracy_rules_match_across_every_route_including_database() {
    // No semantic hints so the database route (which rejects semantic hints)
    // joins the same 40,000-row parity matrix alongside CSV, JSON, Parquet
    // and async streams.
    let full = fixture_columns();
    let cols = FixtureColumns {
        names: vec![
            "customer_age",
            "discount_percent",
            "item_count",
            "birth_year",
            "price",
        ],
        values: vec![
            full.values[0].clone(),
            full.values[1].clone(),
            full.values[2].clone(),
            full.values[3].clone(),
            full.values[5].clone(),
        ],
        positive_columns: vec![],
        temporal_columns: vec![],
    };

    let reports = all_route_reports(&cols);
    #[cfg(feature = "sqlite")]
    assert!(
        reports.iter().any(|(label, _)| label == "sqlite"),
        "expected sqlite route in parity matrix"
    );

    let expected = (160_000, 650, 650, 0, 200_000);
    let mut wrong = Vec::new();
    for (label, report) in reports {
        let acc = report
            .quality
            .as_ref()
            .unwrap()
            .metrics
            .accuracy
            .as_ref()
            .unwrap();
        let got = (
            acc.rule_values_checked,
            acc.rule_violating_values,
            acc.range_violations,
            acc.negative_values_in_positive,
            acc.numeric_values_checked,
        );
        if got != expected {
            wrong.push(format!("[{label}] rules={got:?} != {expected:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "route disagreements:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn exact_rule_bounds_give_point_interval_when_quartiles_coincide() {
    // 40,000 rows of `customer_age`: 39,500 values of 30 and 500 values of -5
    // placed past row 39,500. Quartiles coincide on both the sample and the
    // full source (Q1 = Q3 = 30), so the outlier test does not run; accuracy
    // is still in `sampled_dimensions` because `customer_age` is numeric past
    // the sample, so `score_bounds` runs on it. With exact rule counts wired
    // into `bounds.rs`, the rule share is exact (500 / 40,000 = 1.25%), the
    // score is 98.75, and `score_bounds.dimension_scores["accuracy"]` is the
    // point interval [98.75, 98.75].
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "customer_age").unwrap();
    for i in 0..ROWS {
        writeln!(file, "{}", if i >= 39_500 { -5 } else { 30 }).unwrap();
    }
    file.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(file.path())
        .unwrap();
    let quality = report.quality.as_ref().unwrap();
    let acc = quality.metrics.accuracy.as_ref().unwrap();
    assert_eq!(
        (acc.rule_values_checked, acc.rule_violating_values),
        (40_000, 500)
    );
    assert_eq!(quality.scores().dimension_scores["accuracy"], Some(98.75));

    let bounds = quality.score_bounds().expect("score_bounds present");
    let interval = bounds.dimension_scores["accuracy"].expect("bounded accuracy interval");
    assert_eq!(
        (interval.lower, interval.upper),
        (98.75, 98.75),
        "exact rule share with untested coinciding quartiles must yield a point interval"
    );
}
