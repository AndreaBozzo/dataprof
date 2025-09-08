//! Database connectivity module for DataProfiler
//!
//! This module provides connectors for various databases including:
//! - PostgreSQL (with connection pooling)
//! - MySQL/MariaDB
//! - SQLite
//! - DuckDB
//!
//! Supports streaming/chunked processing for large datasets and maintains
//! the same data profiling features as file-based sources.

use crate::types::{FileInfo, QualityReport, ScanInfo};
use anyhow::Result;
use std::collections::HashMap;

pub mod connection;
pub mod connectors;
pub mod streaming;

pub use connection::*;
pub use connectors::*;

/// Database configuration for connection strings and settings
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub connection_string: String,
    pub batch_size: usize,
    pub max_connections: Option<u32>,
    pub connection_timeout: Option<std::time::Duration>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            batch_size: 10000, // Default batch size for streaming
            max_connections: Some(10),
            connection_timeout: Some(std::time::Duration::from_secs(30)),
        }
    }
}

/// Trait that all database connectors must implement
#[async_trait::async_trait]
pub trait DatabaseConnector: Send + Sync {
    /// Connect to the database
    async fn connect(&mut self) -> Result<()>;

    /// Disconnect from the database
    async fn disconnect(&mut self) -> Result<()>;

    /// Execute a query and get column data for profiling
    async fn profile_query(&mut self, query: &str) -> Result<HashMap<String, Vec<String>>>;

    /// Execute a query with streaming for large result sets
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<HashMap<String, Vec<String>>>;

    /// Get table schema information
    async fn get_table_schema(&mut self, table_name: &str) -> Result<Vec<String>>;

    /// Count total rows in table (for progress tracking)
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64>;

    /// Test connection
    async fn test_connection(&mut self) -> Result<bool>;
}

/// Factory function to create appropriate database connector
pub fn create_connector(config: DatabaseConfig) -> Result<Box<dyn DatabaseConnector>> {
    let connection_str = config.connection_string.as_str();

    if connection_str.starts_with("postgresql://") || connection_str.starts_with("postgres://") {
        Ok(Box::new(connectors::postgres::PostgresConnector::new(
            config,
        )?))
    } else if connection_str.starts_with("mysql://") {
        Ok(Box::new(connectors::mysql::MySqlConnector::new(config)?))
    } else if connection_str.starts_with("sqlite://")
        || connection_str.ends_with(".db")
        || connection_str.ends_with(".sqlite")
        || connection_str == ":memory:"
    {
        Ok(Box::new(connectors::sqlite::SqliteConnector::new(config)?))
    } else if connection_str.ends_with(".duckdb") || connection_str.contains("duckdb") {
        Ok(Box::new(connectors::duckdb::DuckDbConnector::new(config)?))
    } else {
        Err(anyhow::anyhow!(
            "Unsupported database connection string: {}",
            connection_str
        ))
    }
}

/// High-level function to profile a database table or query
pub async fn profile_database(config: DatabaseConfig, query: &str) -> Result<QualityReport> {
    let mut connector = create_connector(config.clone())?;

    // Connect to database
    connector.connect().await?;

    let start = std::time::Instant::now();

    // Execute query and get data
    let columns = if query.trim().to_uppercase().starts_with("SELECT") {
        // Custom query
        connector
            .profile_query_streaming(query, config.batch_size)
            .await?
    } else {
        // Assume it's a table name, create SELECT * query
        let select_query = format!("SELECT * FROM {}", query);
        connector
            .profile_query_streaming(&select_query, config.batch_size)
            .await?
    };

    // Disconnect
    connector.disconnect().await?;

    if columns.is_empty() {
        return Ok(QualityReport {
            file_info: FileInfo {
                path: format!("Database: {}", query),
                total_rows: Some(0),
                total_columns: 0,
                file_size_mb: 0.0,
            },
            column_profiles: vec![],
            issues: vec![],
            scan_info: ScanInfo {
                rows_scanned: 0,
                sampling_ratio: 1.0,
                scan_time_ms: start.elapsed().as_millis(),
            },
        });
    }

    // Use existing column analysis from lib.rs
    let mut column_profiles = Vec::new();
    let total_rows = columns.values().next().map(|v| v.len()).unwrap_or(0);

    for (name, data) in &columns {
        let profile = crate::analyze_column(name, data);
        column_profiles.push(profile);
    }

    // Check quality issues using existing quality checker
    let issues = crate::utils::quality::QualityChecker::check_columns(&column_profiles, &columns);

    let scan_time_ms = start.elapsed().as_millis();

    Ok(QualityReport {
        file_info: FileInfo {
            path: format!("Database: {}", query),
            total_rows: Some(total_rows),
            total_columns: column_profiles.len(),
            file_size_mb: 0.0, // Not applicable for database queries
        },
        column_profiles,
        issues,
        scan_info: ScanInfo {
            rows_scanned: total_rows,
            sampling_ratio: 1.0,
            scan_time_ms,
        },
    })
}
