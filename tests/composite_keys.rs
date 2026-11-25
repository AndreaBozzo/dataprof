use std::io::Write as _;

use dataprof::{AnalysisOptions, EngineType, ProfileReport, Profiler};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

fn groups() -> Vec<Vec<String>> {
    vec![vec!["country".into(), "amount".into()], vec!["id".into()]]
}

fn fixture() -> Vec<(String, Option<String>, Option<&'static str>)> {
    let mut cells = Vec::new();
    for i in 0..6 {
        cells.push((Some(format!("C{i}")), Some("1.50")));
        cells.push((Some(format!("C{i}")), Some("1.5")));
    }
    cells.extend([
        (Some("C0".into()), Some("1.5")),
        (Some("A".into()), None),
        (Some("A".into()), None),
        (None, None),
        (None, None),
    ]);
    cells
        .into_iter()
        .enumerate()
        .map(|(i, (country, amount))| (format!("row{i}"), country, amount))
        .collect()
}

fn expected() -> Value {
    json!([
        {
            "columns": ["country", "amount"], "rows_counted": 15,
            "rows_skipped_all_null": 2, "distinct_keys": 7, "repeated_rows": 8,
            "distinct_repeated_keys": 7, "is_exact": true,
            "sample_repeated_keys": [["A", null], ["C0", "1.5"], ["C1", "1.5"], ["C2", "1.5"], ["C3", "1.5"]],
        },
        {
            "columns": ["id"], "rows_counted": 17, "rows_skipped_all_null": 0,
            "distinct_keys": 17, "repeated_rows": 0, "distinct_repeated_keys": 0,
            "is_exact": true, "sample_repeated_keys": [],
        }
    ])
}

fn file(suffix: &str, contents: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents).unwrap();
    file.flush().unwrap();
    file
}

fn compare_fields(route: &str, found: &Value, wanted: &Value, wrong: &mut Vec<String>) {
    match (found.as_array(), wanted.as_array()) {
        (Some(found), Some(wanted)) if found.len() == wanted.len() => {
            for (actual, group) in found.iter().zip(wanted) {
                for (field, value) in group.as_object().unwrap() {
                    if actual.get(field) != Some(value) {
                        wrong.push(format!(
                            "{route} {}.{field}: expected {value}, got {}",
                            group["columns"], actual[field]
                        ));
                    }
                }
                for field in actual.as_object().unwrap().keys() {
                    if group.get(field).is_none() {
                        wrong.push(format!(
                            "{route} {}.{field}: unexpected field",
                            group["columns"]
                        ));
                    }
                }
            }
        }
        _ => wrong.push(format!(
            "{route}: expected key groups {wanted}, got {found}"
        )),
    }
}

#[test]
fn composite_keys_agree_over_whole_rows() {
    use std::fmt::Write as _;

    let rows = fixture();
    let mut csv = String::from("id,country,amount\n");
    let json_rows: Vec<_> = rows
        .iter()
        .map(|(id, country, amount)| {
            writeln!(
                csv,
                "{id},{},{}",
                country.as_deref().unwrap_or(""),
                amount.unwrap_or("")
            )
            .unwrap();
            let amount: Value =
                amount.map_or(Value::Null, |value| serde_json::from_str(value).unwrap());
            json!({"id": id, "country": country, "amount": amount})
        })
        .collect();
    let json = serde_json::to_string(&json_rows).unwrap();
    let jsonl = json_rows
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let csv_path = file(".csv", csv.as_bytes());
    let json_path = file(".json", json.as_bytes());
    let jsonl_path = file(".jsonl", jsonl.as_bytes());
    let options = AnalysisOptions::default().with_unique_keys(groups());
    let profiler = || Profiler::new().unique_keys(groups());
    let mut reports = vec![
        (
            "CSV standard",
            dataprof::analyze_csv_file_with_options(csv_path.path(), &Default::default(), &options),
        ),
        ("CSV auto", profiler().analyze_file(csv_path.path())),
        (
            "CSV incremental",
            profiler()
                .engine(EngineType::Incremental)
                .analyze_file(csv_path.path()),
        ),
        ("JSON", profiler().analyze_file(json_path.path())),
        ("JSONL", profiler().analyze_file(jsonl_path.path())),
    ];

    #[cfg(feature = "arrow")]
    reports.push((
        "CSV columnar",
        profiler()
            .engine(EngineType::Columnar)
            .analyze_file(csv_path.path()),
    ));

    #[cfg(feature = "parquet")]
    {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, Decimal128Array, StringArray},
            record_batch::RecordBatch,
        };
        use parquet::arrow::ArrowWriter;

        let ids = StringArray::from_iter_values(rows.iter().map(|(id, _, _)| id));
        let countries =
            StringArray::from_iter(rows.iter().map(|(_, country, _)| country.as_deref()));
        let amounts =
            Decimal128Array::from_iter(rows.iter().map(|(_, _, amount)| amount.map(|_| 150)))
                .with_precision_and_scale(5, 2)
                .unwrap();
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(ids) as ArrayRef),
            ("country", Arc::new(countries) as ArrayRef),
            ("amount", Arc::new(amounts) as ArrayRef),
        ])
        .unwrap();
        let mut parquet_data = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet_data, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let path = file(".parquet", &parquet_data);
        reports.push(("Parquet file", profiler().analyze_file(path.path())));
        reports.push((
            "Parquet buffer",
            dataprof_parquet::analyze_parquet_bytes_with_options(
                parquet_data.into(),
                "keys.parquet",
                &Default::default(),
                &options,
            ),
        ));

        // The same amounts as a string column keep their two spellings.
        let ids = StringArray::from_iter_values(rows.iter().map(|(id, _, _)| id));
        let countries =
            StringArray::from_iter(rows.iter().map(|(_, country, _)| country.as_deref()));
        let amounts = StringArray::from_iter(rows.iter().map(|(_, _, amount)| *amount));
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(ids) as ArrayRef),
            ("country", Arc::new(countries) as ArrayRef),
            ("amount", Arc::new(amounts) as ArrayRef),
        ])
        .unwrap();
        let mut parquet_data = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet_data, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let path = file(".parquet", &parquet_data);
        reports.push((
            "Parquet string amounts",
            profiler().analyze_file(path.path()),
        ));
    }

    #[cfg(feature = "async-streaming")]
    {
        use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};

        let runtime = runtime();
        reports.push((
            "async CSV file",
            runtime.block_on(profiler().profile_file(csv_path.path())),
        ));
        for (route, format, contents) in [
            ("async CSV reader", FileFormat::Csv, csv),
            ("async JSON reader", FileFormat::Json, json),
            ("async JSONL reader", FileFormat::Jsonl, jsonl),
        ] {
            let source = BytesSource::new(
                bytes::Bytes::from(contents),
                AsyncSourceInfo::new(route, format),
            );
            reports.push((route, runtime.block_on(profiler().profile_stream(source))));
        }
    }

    #[cfg(feature = "sqlite")]
    {
        use sqlx::sqlite::SqlitePoolOptions;

        let db = file(".sqlite", b"");
        let path = db.path().display().to_string();
        let runtime = runtime();
        runtime.block_on(async {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&format!("sqlite://{path}"))
                .await
                .unwrap();
            sqlx::query("CREATE TABLE keys_fixture (id TEXT, country TEXT, amount NUMERIC)")
                .execute(&pool)
                .await
                .unwrap();
            for (id, country, amount) in &rows {
                sqlx::query("INSERT INTO keys_fixture VALUES (?, ?, ?)")
                    .bind(id)
                    .bind(country)
                    .bind(amount)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            pool.close().await;
        });
        let db_profiler = profiler().database(dataprof::DatabaseConfig {
            connection_string: path,
            load_credentials_from_env: false,
            ..Default::default()
        });
        reports.push((
            "SQLite",
            runtime.block_on(db_profiler.analyze_query("SELECT * FROM keys_fixture")),
        ));
    }

    let mut wrong = Vec::new();
    let wanted = expected();
    for (route, report) in reports {
        let report = match report {
            Ok(report) => report,
            Err(error) => {
                wrong.push(format!("{route}: {error}"));
                continue;
            }
        };
        let document = serde_json::to_value(&report).unwrap();
        let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
        let restored: ProfileReport = serde_json::from_value(document.clone()).unwrap();
        for (label, block) in [
            ("document", document["composite_keys"].clone()),
            ("summary", summary["composite_keys"].clone()),
            (
                "restored",
                serde_json::to_value(restored.composite_keys).unwrap(),
            ),
        ] {
            compare_fields(&format!("{route} {label}"), &block, &wanted, &mut wrong);
        }
        let id = report
            .column_profiles
            .iter()
            .find(|column| column.name == "id")
            .unwrap();
        if id.unique_count != Some(17) {
            wrong.push(format!(
                "{route}: legacy id distinct count is {:?}",
                id.unique_count
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[cfg(feature = "sqlite")]
#[test]
fn database_binary_key_columns_are_rejected() {
    use sqlx::sqlite::SqlitePoolOptions;

    let db = file(".sqlite", b"");
    let path = db.path().display().to_string();
    let runtime = runtime();
    runtime.block_on(async {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{path}"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE blobs (id TEXT, payload BLOB)")
            .execute(&pool)
            .await
            .unwrap();
        for (id, payload) in [("a", &b"\x01"[..]), ("a", &b"\x02"[..])] {
            sqlx::query("INSERT INTO blobs VALUES (?, ?)")
                .bind(id)
                .bind(payload)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
    });
    let profiler = Profiler::new()
        .unique_keys(vec![vec!["id".into(), "payload".into()]])
        .database(dataprof::DatabaseConfig {
            connection_string: path,
            load_credentials_from_env: false,
            ..Default::default()
        });
    // Both payloads read as null, so ("a", 0x01) and ("a", 0x02) would count
    // as one repeated key.
    let error = runtime
        .block_on(profiler.analyze_query("SELECT * FROM blobs"))
        .unwrap_err();
    assert!(error.to_string().contains("payload"), "{error}");
}

#[cfg(any(feature = "async-streaming", feature = "sqlite"))]
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn key_frames_limits_and_configuration() {
    // The backspace byte equals the text tag. Without lengths, the two keys
    // would have identical bytes despite the tag before each cell.
    let csv = file(
        ".csv",
        b"a,b\nab,\x08cd\nab\x08,cd\nab,\x08cd\nab\x08,cd\n,\n",
    );
    let plain = Profiler::new().analyze_file(csv.path()).unwrap();
    let run = |limit| {
        Profiler::new()
            .unique_keys(vec![vec!["a".into(), "b".into()]])
            .unique_keys_exact_limit(limit)
            .analyze_file(csv.path())
            .unwrap()
    };
    let exact = run(2);
    let lower_bound = run(1);
    let first = &exact.composite_keys.as_ref().unwrap()[0];
    let second = &lower_bound.composite_keys.as_ref().unwrap()[0];
    assert_eq!((first.rows_counted, first.rows_skipped_all_null), (4, 1));
    assert_eq!(
        (
            first.distinct_keys,
            first.repeated_rows,
            first.distinct_repeated_keys,
            first.is_exact
        ),
        (2, 2, 2, true)
    );
    assert_eq!(
        (
            second.distinct_keys,
            second.repeated_rows,
            second.distinct_repeated_keys,
            second.is_exact
        ),
        (1, 1, 1, false)
    );
    assert!(
        serde_json::to_value(&plain)
            .unwrap()
            .get("composite_keys")
            .is_none()
    );
    let groups_changed = dataprof::ReportDelta::between(&plain, &exact)
        .configuration
        .unique_keys
        .unwrap();
    assert_eq!(groups_changed.same, Some(false));
    let limits_changed = dataprof::ReportDelta::between(&exact, &lower_bound)
        .configuration
        .unique_keys
        .unwrap();
    assert_eq!(limits_changed.same, Some(false));
}
