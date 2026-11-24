use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write as _;

use dataprof::{
    AnalysisOptions, CheckStatus, EngineType, MetricValue, NotEvaluated, ProfileReport, Profiler,
    QualityPolicy, analyze_csv_file_with_options, analyze_csv_from_reader_with_options,
    analyze_json_from_reader_with_options,
};
use tempfile::NamedTempFile;

const DISTINCT: usize = 96;
const REPEATED: usize = 5;
const LOW_LIMIT: usize = 32;

fn limits(limit: usize) -> BTreeMap<String, usize> {
    BTreeMap::from([("id".to_owned(), limit)])
}

fn file(suffix: &str, contents: impl AsRef<[u8]>) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents.as_ref()).unwrap();
    file.flush().unwrap();
    file
}

fn csv_text(distinct: usize, repeated: usize) -> String {
    let mut csv = String::from("id,label\n");
    for (row, id) in (0..distinct).chain(0..repeated).enumerate() {
        writeln!(&mut csv, "O{id},L{row}").unwrap();
    }
    csv
}

fn reports(
    limit: usize,
    repeated: usize,
) -> Vec<(String, Result<ProfileReport, dataprof::DataProfilerError>)> {
    let csv = csv_text(DISTINCT, repeated);
    let csv_file = file(".csv", &csv);
    let json = (0..DISTINCT)
        .chain(0..repeated)
        .enumerate()
        .map(|(row, id)| format!(r#"{{"label":"L{row}","id":"O{id}"}}"#))
        .collect::<Vec<_>>();
    let json_text = format!("[{}]", json.join(","));
    let jsonl_text = json.join("\n");
    let json_file = file(".json", &json_text);
    let jsonl_file = file(".jsonl", &jsonl_text);
    let options = AnalysisOptions::default().with_unique_count_exact_limits(limits(limit));
    let profiler = || Profiler::new().unique_count_exact_limits(limits(limit));
    let mut reports = vec![
        (
            "CSV standard".into(),
            analyze_csv_file_with_options(csv_file.path(), &Default::default(), &options),
        ),
        ("CSV auto".into(), profiler().analyze_file(csv_file.path())),
        (
            "CSV incremental".into(),
            profiler()
                .engine(EngineType::Incremental)
                .analyze_file(csv_file.path()),
        ),
        ("JSON".into(), profiler().analyze_file(json_file.path())),
        ("JSONL".into(), profiler().analyze_file(jsonl_file.path())),
    ];

    #[cfg(feature = "arrow")]
    reports.push((
        "CSV columnar".into(),
        profiler()
            .engine(EngineType::Columnar)
            .analyze_file(csv_file.path()),
    ));

    #[cfg(feature = "parquet")]
    {
        use std::sync::Arc;

        use arrow::array::StringArray;
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let id = StringArray::from_iter_values(
            (0..DISTINCT).chain(0..repeated).map(|i| format!("O{i}")),
        );
        let label =
            StringArray::from_iter_values((0..DISTINCT + repeated).map(|i| format!("L{i}")));
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(id) as arrow::array::ArrayRef),
            ("label", Arc::new(label) as arrow::array::ArrayRef),
        ])
        .unwrap();
        let parquet = NamedTempFile::with_suffix(".parquet").unwrap();
        let mut writer =
            ArrowWriter::try_new(parquet.reopen().unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        reports.push(("Parquet".into(), profiler().analyze_file(parquet.path())));
    }

    #[cfg(feature = "async-streaming")]
    {
        use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        reports.push((
            "async file".into(),
            rt.block_on(profiler().profile_file(csv_file.path())),
        ));
        for (name, format, data) in [
            ("async CSV", FileFormat::Csv, csv),
            ("async JSON", FileFormat::Json, json_text),
            ("async JSONL", FileFormat::Jsonl, jsonl_text),
        ] {
            let source =
                BytesSource::new(bytes::Bytes::from(data), AsyncSourceInfo::new(name, format));
            reports.push((name.into(), rt.block_on(profiler().profile_stream(source))));
        }
    }
    reports
}

fn verify_reports(
    reports: Vec<(String, Result<ProfileReport, dataprof::DataProfilerError>)>,
    limit: usize,
    repeated: usize,
) {
    let policy = QualityPolicy::new().unique_columns(["id"]);
    let expected_status = if limit < DISTINCT {
        CheckStatus::NotEvaluated(NotEvaluated::ApproximateCount)
    } else if repeated > 0 {
        CheckStatus::Failed
    } else {
        CheckStatus::Passed
    };
    let mut wrong = Vec::new();
    let mut standard_count = None;
    for (route, result) in reports {
        let report = match result {
            Ok(report) => report,
            Err(error) => {
                wrong.push(format!("{route}: {error}"));
                continue;
            }
        };
        let document = serde_json::to_value(&report).unwrap();
        let Some(id) = document["column_profiles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|column| column["name"] == "id")
        else {
            wrong.push(format!("{route}: missing id profile"));
            continue;
        };
        let count = id["unique_count"].as_u64().unwrap();
        let expected_count = standard_count.get_or_insert(count);
        if count != *expected_count || (limit >= DISTINCT && count != DISTINCT as u64) {
            wrong.push(format!(
                "{route}: unique_count={count}, expected {expected_count}"
            ));
        }
        if id["unique_count_is_approximate"] != (limit < DISTINCT) {
            wrong.push(format!("{route}: unique_count_is_approximate={id}"));
        }
        let gate = policy.evaluate(&report).unwrap();
        let check = &gate.checks[0];
        if check.status != expected_status {
            wrong.push(format!("{route}: status={:?}", check.status));
        }
        if limit >= DISTINCT && check.observed != Some(MetricValue::Count(repeated)) {
            wrong.push(format!("{route}: observed={:?}", check.observed));
        }
        let restored: ProfileReport = serde_json::from_value(document.clone()).unwrap();
        let recorded = &document["analysis_settings"]["unique_count_exact_limits"];
        let restored_limits = &restored
            .analysis_settings
            .as_ref()
            .unwrap()
            .unique_count_exact_limits;
        if recorded != &serde_json::json!({"id": limit}) || restored_limits != &limits(limit) {
            wrong.push(format!(
                "{route}: recorded={recorded}, restored={restored_limits:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn every_file_and_stream_route_honours_the_named_limit() {
    for (limit, repeated) in [(LOW_LIMIT, REPEATED), (DISTINCT, REPEATED), (DISTINCT, 0)] {
        verify_reports(reports(limit, repeated), limit, repeated);
    }
}

#[test]
fn direct_readers_honour_the_named_limit() {
    let options = AnalysisOptions::default().with_unique_count_exact_limits(limits(1));
    let (csv, ..) = analyze_csv_from_reader_with_options(
        &b"id,label\nx,1\ny,2\n"[..],
        &Default::default(),
        &options,
    )
    .unwrap();
    let (json, ..) = analyze_json_from_reader_with_options(
        &br#"[{"id":"x","label":1},{"id":"y","label":2}]"#[..],
        &Default::default(),
        &options,
    )
    .unwrap();
    let mut wrong = Vec::new();
    for (route, columns) in [("CSV reader", csv), ("JSON reader", json)] {
        for column in columns {
            let expected = column.name == "id";
            if column.unique_count != Some(2)
                || column.unique_count_is_approximate != Some(expected)
            {
                wrong.push(format!("{route}: {column:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn memory_pressure_spills_unnamed_columns_and_keeps_named_columns() {
    let csv = file(".csv", csv_text(90_000, REPEATED));
    let mut engines = vec![EngineType::Incremental];
    #[cfg(feature = "arrow")]
    engines.push(EngineType::Columnar);
    let mut wrong = Vec::new();
    for engine in engines {
        let run = |limits| {
            Profiler::new()
                .engine(engine)
                .memory_limit_mb(1)
                .duplicate_rows_exact_limit(100_000)
                .unique_count_exact_limits(limits)
                .analyze_file(csv.path())
                .unwrap()
        };
        let default = run(BTreeMap::new());
        let named = run(limits(90_000));
        for (report, named_id) in [(&default, false), (&named, true)] {
            for column in &report.column_profiles {
                let approximate = column.name != "id" || !named_id;
                if column.unique_count_is_approximate != Some(approximate)
                    || (!approximate && column.unique_count != Some(90_000))
                {
                    wrong.push(format!(
                        "{engine:?} named={named_id}: {} has {:?}, approximate {:?}",
                        column.name, column.unique_count, column.unique_count_is_approximate
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn limit_changes_preserve_baseline_comparability_for_exact_counts() {
    let baseline_file = file(".csv", "id\nx\ny\nz\n");
    let current_file = file(".csv", "id\nx\nx\nz\n");
    let run = |file: &NamedTempFile, limit| {
        Profiler::new()
            .unique_count_exact_limits(limits(limit))
            .analyze_file(file.path())
            .unwrap()
    };
    let baseline = run(&baseline_file, 3);
    let current = run(&current_file, 4);
    let estimated = run(&baseline_file, 1);

    let delta = dataprof::ReportDelta::between(&baseline, &current);
    let compared = delta.configuration.unique_count_exact_limits.unwrap();
    assert_eq!(compared.a, Some(limits(3)));
    assert_eq!(compared.b, Some(limits(4)));
    assert_eq!(compared.same, Some(false));

    let policy = QualityPolicy::new().max_distinct_drop("id", 0.0);
    let result = policy
        .evaluate_against(&current, &dataprof::ReportSide::from_report(&baseline))
        .unwrap();
    assert_eq!(result.checks[0].status, CheckStatus::Failed);
    let uncertain = policy
        .evaluate_against(&current, &dataprof::ReportSide::from_report(&estimated))
        .unwrap();
    assert_eq!(
        uncertain.checks[0].status,
        CheckStatus::NotEvaluated(NotEvaluated::ApproximateCount)
    );
}

#[test]
fn named_limit_validation_checks_the_profiled_schema() {
    let csv = file(".csv", csv_text(1, 0));
    let cases = [
        (
            Profiler::new().unique_count_exact_limits(limits(0)),
            "must be at least 1",
        ),
        (
            Profiler::new().unique_count_exact_limits(BTreeMap::from([("missing".into(), 1)])),
            "absent from the source",
        ),
        (
            Profiler::new()
                .columns(vec!["label".into()])
                .unique_count_exact_limits(limits(1)),
            "excluded from profiling",
        ),
    ];
    for (profiler, expected) in cases {
        let error = profiler.analyze_file(csv.path()).unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[cfg(feature = "sqlite")]
#[test]
fn a_database_query_honours_the_named_limit() {
    use dataprof::DatabaseConfig;
    use sqlx::sqlite::SqlitePoolOptions;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("keys.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();
    rt.block_on(async {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{db_path}"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id TEXT, label TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        let mut transaction = pool.begin().await.unwrap();
        for (row, id) in (0..DISTINCT).chain(0..REPEATED).enumerate() {
            sqlx::query("INSERT INTO t VALUES (?, ?)")
                .bind(format!("O{id}"))
                .bind(format!("L{row}"))
                .execute(&mut *transaction)
                .await
                .unwrap();
        }
        transaction.commit().await.unwrap();
        pool.close().await;
    });
    for limit in [LOW_LIMIT, DISTINCT] {
        let result = rt.block_on(
            Profiler::new()
                .unique_count_exact_limits(limits(limit))
                .database(DatabaseConfig {
                    connection_string: db_path.clone(),
                    load_credentials_from_env: false,
                    ..Default::default()
                })
                .analyze_query("SELECT * FROM t"),
        );
        verify_reports(vec![("SQLite".into(), result)], limit, REPEATED);
    }
}
