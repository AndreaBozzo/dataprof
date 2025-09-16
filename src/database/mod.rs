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

use crate::analysis::analyze_column;
use crate::types::{FileInfo, QualityReport, ScanInfo};
use anyhow::Result;
use std::collections::HashMap;

pub mod connection;
pub mod connectors;
pub mod ml_readiness_simple;
pub mod retry;
pub mod sampling;
pub mod security;
pub mod streaming;

pub use connection::*;
pub use connectors::*;
pub use ml_readiness_simple::*;
pub use retry::*;
pub use sampling::*;
pub use security::*;

/// Database configuration for connection strings and settings
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub connection_string: String,
    pub batch_size: usize,
    pub max_connections: Option<u32>,
    pub connection_timeout: Option<std::time::Duration>,
    pub retry_config: Option<RetryConfig>,
    pub sampling_config: Option<SamplingConfig>,
    pub enable_ml_readiness: bool,
    pub ssl_config: Option<SslConfig>,
    pub load_credentials_from_env: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            batch_size: 10000, // Default batch size for streaming
            max_connections: Some(10),
            connection_timeout: Some(std::time::Duration::from_secs(30)),
            retry_config: Some(RetryConfig::default()),
            sampling_config: None,     // No sampling by default
            enable_ml_readiness: true, // Enable ML readiness assessment by default
            ssl_config: Some(SslConfig::default()),
            load_credentials_from_env: true, // Load credentials from environment by default
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
pub fn create_connector(mut config: DatabaseConfig) -> Result<Box<dyn DatabaseConnector>> {
    // Apply environment variables and SSL configuration if enabled
    if config.load_credentials_from_env || config.connection_string.is_empty() {
        config = apply_environment_configuration(config)?;
    }

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

/// Apply environment configuration to database config
fn apply_environment_configuration(mut config: DatabaseConfig) -> Result<DatabaseConfig> {
    // Determine database type from connection string or auto-detect
    let database_type = if config.connection_string.is_empty() {
        // Try to auto-detect from environment
        if std::env::var("POSTGRES_URL").is_ok()
            || std::env::var("DATABASE_URL")
                .map(|url| url.starts_with("postgres"))
                .unwrap_or(false)
        {
            "postgresql".to_string()
        } else if std::env::var("MYSQL_URL").is_ok() {
            "mysql".to_string()
        } else {
            // Default to PostgreSQL if no clear indication
            "postgresql".to_string()
        }
    } else {
        let conn_info = ConnectionInfo::parse(&config.connection_string)?;
        conn_info.database_type().to_string()
    };
    let database_type = database_type.as_str();

    // Load secure configuration from environment
    if config.connection_string.is_empty() {
        let (secure_connection_string, ssl_config) = load_secure_database_config(database_type)?;
        config.connection_string = secure_connection_string;
        config.ssl_config = Some(ssl_config);
    } else {
        // Apply SSL configuration to existing connection string
        if let Some(ssl_config) = &config.ssl_config {
            config.connection_string = ssl_config
                .apply_to_connection_string(config.connection_string.clone(), database_type);
        }

        // Apply environment credentials if enabled
        if config.load_credentials_from_env {
            let credentials = DatabaseCredentials::from_environment(database_type);
            config.connection_string =
                credentials.apply_to_connection_string(&config.connection_string);
        }
    }

    Ok(config)
}

/// High-level function to profile a database table or query
pub async fn profile_database(config: DatabaseConfig, query: &str) -> Result<QualityReport> {
    let mut connector = create_connector(config.clone())?;

    // Connect to database
    connector.connect().await?;

    let start = std::time::Instant::now();

    // Determine the actual query to execute
    let (actual_query, is_table) = if query.trim().to_uppercase().starts_with("SELECT") {
        (query.to_string(), false)
    } else {
        (format!("SELECT * FROM {}", query), true)
    };

    // Get total row count for sampling decisions
    let total_rows = if is_table {
        connector.count_table_rows(query).await.unwrap_or(0)
    } else {
        // For complex queries, we can't easily count without executing
        0
    };

    // Determine if we need sampling
    let (final_query, sample_info) = if let Some(sampling_config) = &config.sampling_config {
        if total_rows > sampling_config.sample_size as u64 {
            let sampled_query = sampling_config.generate_sample_query(&actual_query, total_rows)?;
            let info = SampleInfo::new(
                total_rows,
                sampling_config.sample_size.min(total_rows as usize) as u64,
                sampling_config.strategy.clone(),
            );
            (sampled_query, Some(info))
        } else {
            (actual_query, None)
        }
    } else {
        (actual_query, None)
    };

    // Execute query and get data
    let columns = connector
        .profile_query_streaming(&final_query, config.batch_size)
        .await?;

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
                sampling_ratio: sample_info.map(|s| s.sampling_ratio).unwrap_or(1.0),
                scan_time_ms: start.elapsed().as_millis(),
            },
        });
    }

    // Use existing column analysis from lib.rs
    let mut column_profiles = Vec::new();
    let actual_rows_processed = columns.values().next().map(|v| v.len()).unwrap_or(0);
    let effective_total_rows = if total_rows > 0 {
        total_rows
    } else {
        actual_rows_processed as u64
    };

    for (name, data) in &columns {
        let profile = analyze_column(name, data);
        column_profiles.push(profile);
    }

    // Check quality issues using existing quality checker
    let issues = crate::utils::quality::QualityChecker::check_columns(&column_profiles, &columns);

    // Convert to Vec<String> for compatibility with existing QualityIssue system
    let mut string_issues: Vec<String> = issues
        .into_iter()
        .map(|issue| match issue {
            crate::types::QualityIssue::NullValues {
                column,
                count,
                percentage,
            } => {
                format!(
                    "NULL VALUES in '{}': {} ({:.1}%)",
                    column, count, percentage
                )
            }
            crate::types::QualityIssue::MixedDateFormats { column, formats } => {
                format!("MIXED DATE FORMATS in '{}': {:?}", column, formats)
            }
            crate::types::QualityIssue::Duplicates { column, count } => {
                format!("DUPLICATES in '{}': {} duplicates", column, count)
            }
            crate::types::QualityIssue::Outliers {
                column,
                values,
                threshold,
            } => {
                format!(
                    "OUTLIERS in '{}': {} values beyond threshold {}",
                    column,
                    values.len(),
                    threshold
                )
            }
            crate::types::QualityIssue::MixedTypes { column, types } => {
                format!("MIXED TYPES in '{}': {:?}", column, types)
            }
        })
        .collect();

    // Add sampling-related warnings if applicable
    if let Some(ref sample_info) = sample_info {
        if let Some(warning) = sample_info.get_warning() {
            string_issues.push(format!("SAMPLING WARNING: {}", warning));
        }
    }

    // Perform ML readiness assessment if enabled
    let ml_readiness = if config.enable_ml_readiness {
        Some(assess_ml_readiness(&column_profiles, effective_total_rows)?)
    } else {
        None
    };

    // Add ML readiness issues to the general issues list
    if let Some(ref ml_assessment) = ml_readiness {
        for ml_issue in &ml_assessment.issues {
            string_issues.push(format!("ML READINESS: {}", ml_issue));
        }
    }

    let scan_time_ms = start.elapsed().as_millis();
    let sampling_ratio = sample_info.map(|s| s.sampling_ratio).unwrap_or(1.0);

    Ok(QualityReport {
        file_info: FileInfo {
            path: format!(
                "Database: {} ({})",
                query,
                if sampling_ratio < 1.0 {
                    format!("sampled {:.1}%", sampling_ratio * 100.0)
                } else {
                    "complete scan".to_string()
                }
            ),
            total_rows: Some(effective_total_rows as usize),
            total_columns: column_profiles.len(),
            file_size_mb: 0.0, // Not applicable for database queries
        },
        column_profiles,
        issues: vec![], // Convert string issues back to QualityIssue enum if needed
        scan_info: ScanInfo {
            rows_scanned: actual_rows_processed,
            sampling_ratio,
            scan_time_ms,
        },
    })
}

/// Enhanced database profiling with ML readiness assessment
pub async fn profile_database_with_ml(
    config: DatabaseConfig,
    query: &str,
) -> Result<(QualityReport, Option<MLReadinessScore>)> {
    let quality_report = profile_database(config.clone(), query).await?;

    let ml_readiness = if config.enable_ml_readiness {
        let total_rows = quality_report.file_info.total_rows.unwrap_or(0);
        Some(assess_ml_readiness(
            &quality_report.column_profiles,
            total_rows as u64,
        )?)
    } else {
        None
    };

    Ok((quality_report, ml_readiness))
}
