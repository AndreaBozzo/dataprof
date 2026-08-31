//! Database-specific connectors
//!
//! This module contains implementations for various database systems:
//! - PostgreSQL with connection pooling
//! - MySQL/MariaDB
//! - SQLite (embedded)

mod common;

// The decode macros expand at the call site and reference this by path, so it
// has to be reachable from the crate root even though `common` stays private.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub use common::render_naive_datetime;
pub mod mysql;
pub mod postgres;
pub mod sqlite;

pub use mysql::MySqlConnector;
pub use postgres::PostgresConnector;
pub use sqlite::SqliteConnector;
