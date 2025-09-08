//! Database-specific connectors
//!
//! This module contains implementations for various database systems:
//! - PostgreSQL with connection pooling
//! - MySQL/MariaDB
//! - SQLite (embedded)
//! - DuckDB (analytical database)

pub mod duckdb;
pub mod mysql;
pub mod postgres;
pub mod sqlite;

pub use duckdb::DuckDbConnector;
pub use mysql::MySqlConnector;
pub use postgres::PostgresConnector;
pub use sqlite::SqliteConnector;
