//! The URL built from the environment alone keeps each part one component.
//!
//! With an empty connection string the loader builds the URL from the
//! credentials it reads, which hold decoded values. A database name
//! `app%3Fsslmode=disable` in `POSTGRES_URL` decodes to `app?sslmode=disable`;
//! interpolated as text it became a query pair, and the configured TLS mode
//! counted it as the caller's own setting and left TLS disabled.
//!
//! This binary sets process environment variables, so it holds one test.

use dataprof_db::connection::ConnectionInfo;
use dataprof_db::load_secure_database_config;

#[test]
#[allow(unsafe_code)]
fn a_database_name_from_the_environment_adds_no_query() {
    // SAFETY: the only test in this binary, run before any other thread
    // reads the environment.
    unsafe {
        std::env::set_var(
            "POSTGRES_URL",
            "postgresql://u:p@localhost:5432/app%3Fsslmode=disable",
        );
        std::env::set_var("POSTGRES_SSL_MODE", "verify-full");
    }
    let (url, _) = load_secure_database_config("postgresql").unwrap();
    let info = ConnectionInfo::parse(&url).unwrap();
    assert_eq!(
        info.database.as_deref(),
        Some("app?sslmode=disable"),
        "{url}"
    );
    assert_eq!(info.query_param("sslmode"), Some("verify-full"), "{url}");
}
