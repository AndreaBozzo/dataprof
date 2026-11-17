//! An empty connection string keeps the TLS settings it was given.
//!
//! With an empty connection string the URL is built from the environment.
//! Built from the credential fields alone, it dropped the query of
//! `POSTGRES_URL`, `MYSQL_URL` and `DATABASE_URL`, so their `sslmode` and CA
//! never applied. The factory also replaced the caller's `ssl_config` with the
//! one read from the SSL environment variables, so `SslConfig::production()`
//! connected with `prefer`.
//!
//! This binary sets process environment variables, so it holds one test.
//! The live part needs `POSTGRES_TEST_URL` and `MYSQL_TEST_URL`; CI sets both.

#![cfg(all(feature = "postgres", feature = "mysql"))]

use dataprof_db::connection::ConnectionInfo;
use dataprof_db::{DatabaseConfig, SslConfig, create_connector, load_secure_database_config};

const URL_VARIABLES: [&str; 3] = ["POSTGRES_URL", "MYSQL_URL", "DATABASE_URL"];

#[allow(unsafe_code)]
fn only(name: &str, value: &str) {
    // SAFETY: the only test in this binary; no other thread reads the
    // environment while it runs.
    unsafe {
        for variable in URL_VARIABLES {
            std::env::remove_var(variable);
        }
        std::env::set_var(name, value);
    }
}

fn empty_config(ssl_config: SslConfig) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: String::new(),
        ssl_config: Some(ssl_config),
        ..Default::default()
    }
}

/// Whether connecting with `config` succeeds.
async fn connects(config: DatabaseConfig) -> bool {
    let mut connector = create_connector(config).unwrap();
    connector.connect().await.is_ok()
}

#[tokio::test]
async fn an_empty_connection_string_keeps_its_tls_settings() {
    // The URL built from each variable keeps the variable's TLS settings.
    for (name, url, database_type, key, mode) in [
        (
            "POSTGRES_URL",
            "postgresql://u:p@h:5432/db?sslmode=verify-full&sslrootcert=%2Fca%26x.crt",
            "postgresql",
            "sslmode",
            "verify-full",
        ),
        (
            "DATABASE_URL",
            "postgresql://u:p@h:5432/db?sslmode=verify-full&sslrootcert=%2Fca%26x.crt",
            "postgresql",
            "sslmode",
            "verify-full",
        ),
        (
            "MYSQL_URL",
            "mysql://u:p@h:3306/db?ssl-mode=VERIFY_IDENTITY&ssl-ca=%2Fca%26x.crt",
            "mysql",
            "ssl-mode",
            "VERIFY_IDENTITY",
        ),
    ] {
        only(name, url);
        let (built, _) = load_secure_database_config(database_type).unwrap();
        let info = ConnectionInfo::parse(&built).unwrap();
        assert_eq!(info.query_param(key), Some(mode), "{name}: {built}");
        let ca = info
            .query_param("sslrootcert")
            .or(info.query_param("ssl-ca"));
        assert_eq!(ca, Some("/ca&x.crt"), "{name}: {built}");
    }

    // Against the dev servers: a verifying mode in the variable, or the
    // production preset, refuses a server it cannot verify (PostgreSQL has no
    // TLS, MySQL's certificate is self-signed). The default still connects.
    // A MySQL URL in `DATABASE_URL` alone selects MySQL: it selected
    // PostgreSQL and failed to connect.
    let postgres = std::env::var("POSTGRES_TEST_URL").ok();
    let mysql = std::env::var("MYSQL_TEST_URL").ok();
    let mysql_verifying = "ssl-mode=VERIFY_CA&ssl-ca=no-such-dir%2Fca.crt";
    for (name, url, verifying) in [
        (
            "POSTGRES_URL",
            postgres.clone(),
            "sslmode=verify-full&sslrootcert=no-such-dir%2Fca.crt",
        ),
        (
            "DATABASE_URL",
            postgres,
            "sslmode=verify-full&sslrootcert=no-such-dir%2Fca.crt",
        ),
        ("MYSQL_URL", mysql.clone(), mysql_verifying),
        ("DATABASE_URL", mysql, mysql_verifying),
    ] {
        let Some(url) = url else {
            eprintln!("{name}: no test server; skipping the live part");
            continue;
        };
        let separator = if url.contains('?') { '&' } else { '?' };

        only(name, &url);
        assert!(
            connects(empty_config(SslConfig::default())).await,
            "{name} plain"
        );

        only(name, &format!("{url}{separator}{verifying}"));
        assert!(
            !connects(empty_config(SslConfig::default())).await,
            "{name} verifying URL"
        );

        only(name, &url);
        assert!(
            !connects(empty_config(SslConfig::production())).await,
            "{name} production"
        );
    }
}
