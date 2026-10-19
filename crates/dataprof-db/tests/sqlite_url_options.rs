//! #912: SQLite URI query parameters must not be stripped before sqlx sees
//! them. These are behavioral tests: they exercise actual read-only and
//! create-if-missing modes, not only string reconstruction.
#![cfg(feature = "sqlite")]

use dataprof_db::{DatabaseConfig, create_connector};
use sqlx::sqlite::SqlitePoolOptions;

fn config(url: String) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: url,
        load_credentials_from_env: false,
        ..Default::default()
    }
}

fn sqlite_url(path: &std::path::Path, mode: &str) -> String {
    // sqlx accepts sqlite:///abs/path on Unix and sqlite:///C:/path on
    // Windows. A slash is preserved rather than trimming URL path segments.
    let path = path.to_string_lossy().replace('\\', "/");
    let prefix = if cfg!(windows) {
        "sqlite:///"
    } else {
        "sqlite://"
    };
    format!("{prefix}{path}?mode={mode}")
}

#[tokio::test]
async fn mode_rwc_creates_a_missing_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("created-by-mode-rwc.db");
    assert!(!path.exists());

    let mut connector = create_connector(config(sqlite_url(&path, "rwc"))).unwrap();
    connector.connect().await.unwrap();
    assert!(connector.test_connection().await.unwrap());
    assert!(path.exists(), "SQLite must honor mode=rwc, not drop it");
    connector.disconnect().await.unwrap();
}

#[tokio::test]
async fn mode_rw_does_not_create_a_missing_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("must-not-be-created.db");
    assert!(!path.exists());

    let mut connector = create_connector(config(sqlite_url(&path, "rw"))).unwrap();
    assert!(
        connector.connect().await.is_err(),
        "mode=rw must reject a missing DB rather than silently create it"
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn mode_ro_refuses_writes_to_existing_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read-only.db");

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&sqlite_url(&path, "rwc"))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE rows (id INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let mut connector = create_connector(config(sqlite_url(&path, "ro"))).unwrap();
    connector.connect().await.unwrap();
    assert!(connector.test_connection().await.unwrap());
    let error = connector
        .profile_query("INSERT INTO rows (id) VALUES (1)")
        .await
        .expect_err("mode=ro must reject writes to the database");
    assert!(
        error.to_string().to_ascii_lowercase().contains("readonly")
            || error.to_string().to_ascii_lowercase().contains("read-only"),
        "unexpected SQLite error: {error}"
    );
    connector.disconnect().await.unwrap();
}

/// Execute inside a separate test process: DATABASE_URL is process-global and
/// must not affect parallel database tests.
#[tokio::test]
async fn sqlite_explicit_url_default_env_child() {
    if std::env::var_os("DATAPROF_SQLITE_ENV_CHILD").is_none() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("explicit-default-rwc.db");
    let mut connector = create_connector(DatabaseConfig {
        connection_string: sqlite_url(&path, "rwc"),
        ..Default::default()
    })
    .unwrap();
    connector.connect().await.unwrap();
    assert!(path.exists(), "the requested SQLite path must be created");
    assert!(connector.test_connection().await.unwrap());
    connector.disconnect().await.unwrap();
}

#[test]
fn explicit_sqlite_mode_survives_unrelated_database_url() {
    let executable = std::env::current_exe().expect("locate Rust test executable");
    let output = std::process::Command::new(executable)
        .args([
            "--exact",
            "sqlite_explicit_url_default_env_child",
            "--nocapture",
        ])
        .env("DATAPROF_SQLITE_ENV_CHILD", "1")
        .env(
            "DATABASE_URL",
            "postgresql://unrelated:unrelated@127.0.0.1:1/unrelated_database",
        )
        .output()
        .expect("execute isolated default-configuration regression");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "an unrelated DATABASE_URL must not hijack an explicit SQLite target:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
