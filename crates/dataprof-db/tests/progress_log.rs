//! Progress logging accounts for every row of a query (#887).
//!
//! Progress is logged every `batch_size` rows. The rows after the last full
//! interval must be logged too, or the last line stops short of the rows read.
//! The logger is process-wide, so this check lives in its own test binary.

#![cfg(feature = "sqlite")]

use std::sync::{Mutex, OnceLock};

use dataprof_core::AnalysisOptions;
use dataprof_db::{DatabaseConfig, analyze_database_with_options};
use sqlx::sqlite::SqlitePoolOptions;

/// Every message logged in this test binary.
struct Capture(Mutex<Vec<String>>);

impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }

    fn flush(&self) {}
}

fn capture() -> &'static Capture {
    static CAPTURE: OnceLock<&'static Capture> = OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture: &'static Capture = Box::leak(Box::new(Capture(Mutex::new(Vec::new()))));
        log::set_logger(capture).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        capture
    })
}

#[tokio::test]
async fn progress_is_logged_for_the_rows_after_the_last_full_interval() {
    let capture = capture();

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("progress.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{db_path}"))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    for id in 0..7 {
        sqlx::query("INSERT INTO t VALUES (?)")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    // 7 rows with a batch of 3: full intervals end at rows 3 and 6, and the
    // seventh row comes after them.
    let config = DatabaseConfig {
        connection_string: db_path,
        batch_size: 3,
        load_credentials_from_env: false,
        ..Default::default()
    };
    analyze_database_with_options(config, "SELECT * FROM t", &AnalysisOptions::default())
        .await
        .unwrap();

    let progress: Vec<String> = capture
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|message| message.starts_with("SQLite streaming progress"))
        .cloned()
        .collect();
    assert_eq!(
        progress.last().map(String::as_str),
        Some("SQLite streaming progress: 100.0% (7/7 rows)"),
        "progress lines: {progress:?}"
    );
}
