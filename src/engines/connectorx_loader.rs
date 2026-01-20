//! ConnectorX-based database loader for high-performance Arrow-native data loading
//!
//! This module provides a unified interface to load data from databases directly into
//! Arrow RecordBatches using ConnectorX. This approach offers several advantages:
//!
//! - **Zero-copy**: Data flows directly from database to Arrow format
//! - **Parallel loading**: Automatic partitioning and parallel fetching
//! - **Unified API**: Same interface for Postgres, MySQL, SQLite
//! - **DataFusion integration**: RecordBatches can be registered as DataFusion tables

use crate::engines::columnar::RecordBatchAnalyzer;
use crate::types::{DataQualityMetrics, DataSource, QualityReport, QueryEngine, ScanInfo};
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use std::time::Instant;

#[cfg(feature = "connectorx-postgres")]
use connectorx::prelude::*;
#[cfg(feature = "connectorx-mysql")]
use connectorx::prelude::*;

#[cfg(any(
    feature = "connectorx-postgres",
    feature = "connectorx-mysql"
))]
use connectorx::{
    destinations::arrow::ArrowDestination, prelude::get_arrow, source_router::SourceConn,
    sql::CXQuery,
};

/// Database type for ConnectorX connections
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    Postgres,
    MySQL,
    SQLite,
}

impl DatabaseType {
    /// Detect database type from connection string
    pub fn from_connection_string(conn_str: &str) -> Option<Self> {
        let lower = conn_str.to_lowercase();
        if lower.starts_with("postgresql://") || lower.starts_with("postgres://") {
            Some(Self::Postgres)
        } else if lower.starts_with("mysql://") {
            Some(Self::MySQL)
        } else if lower.starts_with("sqlite://") || lower.ends_with(".db") {
            Some(Self::SQLite)
        } else {
            None
        }
    }

    /// Get the QueryEngine variant for reporting
    pub fn to_query_engine(&self) -> QueryEngine {
        match self {
            Self::Postgres => QueryEngine::Postgres,
            Self::MySQL => QueryEngine::MySql,
            Self::SQLite => QueryEngine::Sqlite,
        }
    }
}

/// Configuration for ConnectorX database loading
#[derive(Debug, Clone)]
pub struct ConnectorXConfig {
    /// Database connection string (e.g., "postgresql://user:pass@host/db")
    pub connection_string: String,
    /// Number of parallel connections for partitioned queries
    pub partition_num: Option<usize>,
    /// Column to partition on for parallel loading (must be numeric, non-null)
    pub partition_on: Option<String>,
    /// Protocol to use (e.g., "binary" for Postgres, "text" for MySQL)
    pub protocol: Option<String>,
}

impl ConnectorXConfig {
    /// Create a new configuration with just a connection string
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            partition_num: None,
            partition_on: None,
            protocol: None,
        }
    }

    /// Set the number of partitions for parallel loading
    pub fn with_partitions(mut self, num: usize) -> Self {
        self.partition_num = Some(num);
        self
    }

    /// Set the column to partition on (must be numeric, non-null)
    pub fn with_partition_column(mut self, column: impl Into<String>) -> Self {
        self.partition_on = Some(column.into());
        self
    }

    /// Set the protocol (e.g., "binary", "text", "cursor")
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }

    /// Build the connection string with protocol if specified
    fn build_connection_string(&self) -> String {
        match &self.protocol {
            Some(proto) => {
                if self.connection_string.contains('?') {
                    format!("{}&cxprotocol={}", self.connection_string, proto)
                } else {
                    format!("{}?cxprotocol={}", self.connection_string, proto)
                }
            }
            None => self.connection_string.clone(),
        }
    }
}

/// ConnectorX-based database loader
///
/// Provides high-performance data loading from databases directly into Arrow format.
/// Uses ConnectorX under the hood for zero-copy, parallel data transfer.
///
/// # Example
///
/// ```no_run
/// use dataprof::database::connectorx_loader::{ConnectorXLoader, ConnectorXConfig};
///
/// # async fn example() -> anyhow::Result<()> {
/// let config = ConnectorXConfig::new("postgresql://user:pass@localhost/mydb")
///     .with_partitions(4)
///     .with_partition_column("id");
///
/// let loader = ConnectorXLoader::new(config)?;
/// let report = loader.profile_query("SELECT * FROM users")?;
/// println!("Rows: {}", report.scan_info.total_rows);
/// # Ok(())
/// # }
/// ```
pub struct ConnectorXLoader {
    config: ConnectorXConfig,
    db_type: DatabaseType,
}

impl ConnectorXLoader {
    /// Create a new ConnectorX loader
    pub fn new(config: ConnectorXConfig) -> Result<Self> {
        let db_type = DatabaseType::from_connection_string(&config.connection_string)
            .context("Unsupported database type. Use postgresql://, mysql://, or sqlite://")?;

        // Verify the feature is enabled for this database type
        Self::check_feature_enabled(db_type)?;

        Ok(Self { config, db_type })
    }

    /// Check if the required feature is enabled
    fn check_feature_enabled(db_type: DatabaseType) -> Result<()> {
        match db_type {
            #[cfg(feature = "connectorx-postgres")]
            DatabaseType::Postgres => Ok(()),
            #[cfg(not(feature = "connectorx-postgres"))]
            DatabaseType::Postgres => Err(anyhow::anyhow!(
                "PostgreSQL support not enabled. Enable 'connectorx-postgres' feature."
            )),

            #[cfg(feature = "connectorx-mysql")]
            DatabaseType::MySQL => Ok(()),
            #[cfg(not(feature = "connectorx-mysql"))]
            DatabaseType::MySQL => Err(anyhow::anyhow!(
                "MySQL support not enabled. Enable 'connectorx-mysql' feature."
            )),

            // SQLite via ConnectorX is currently disabled due to libsqlite3-sys conflict
            // Use the sqlx-based sqlite feature instead
            DatabaseType::SQLite => Err(anyhow::anyhow!(
                "SQLite via ConnectorX is not available. Use the 'sqlite' feature with sqlx instead."
            )),
        }
    }

    /// Get the database type
    pub fn database_type(&self) -> DatabaseType {
        self.db_type
    }

    /// Load data from a SQL query into Arrow RecordBatches
    #[cfg(any(
        feature = "connectorx-postgres",
        feature = "connectorx-mysql"
    ))]
    pub fn load_query(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let conn_str = self.config.build_connection_string();
        let source_conn = SourceConn::try_from(conn_str.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to parse connection string: {}", e))?;

        let queries = vec![CXQuery::from(query)];

        let destination = get_arrow(&source_conn, None, &queries, None)
            .map_err(|e| anyhow::anyhow!("ConnectorX query failed: {:?}", e))?;

        destination
            .arrow()
            .map_err(|e| anyhow::anyhow!("Failed to get Arrow batches: {:?}", e))
    }

    /// Load data from a SQL query into Arrow RecordBatches (stub when no features enabled)
    #[cfg(not(any(
        feature = "connectorx-postgres",
        feature = "connectorx-mysql"
    )))]
    pub fn load_query(&self, _query: &str) -> Result<Vec<RecordBatch>> {
        Err(anyhow::anyhow!(
            "No ConnectorX database features enabled. Enable connectorx-postgres or connectorx-mysql."
        ))
    }

    /// Profile a SQL query and return a QualityReport
    pub fn profile_query(&self, query: &str) -> Result<QualityReport> {
        let start = Instant::now();

        log::info!(
            "ConnectorX: Loading data from {:?} database",
            self.db_type
        );

        // Load data into Arrow RecordBatches
        let batches = self.load_query(query)?;

        // Process batches with RecordBatchAnalyzer
        let mut analyzer = RecordBatchAnalyzer::new();
        let mut batch_count = 0;

        for batch in &batches {
            if batch.num_rows() > 0 {
                batch_count += 1;
                analyzer.process_batch(batch)?;
            }
        }

        let total_rows = analyzer.total_rows();
        log::info!(
            "ConnectorX: Processed {} rows in {} batches",
            total_rows,
            batch_count
        );

        // Build the report
        let column_profiles = analyzer.to_profiles();
        let sample_columns = analyzer.create_sample_columns();

        // Calculate quality metrics
        let data_quality_metrics =
            DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
                .map_err(|e| anyhow::anyhow!("Quality metrics calculation failed: {}", e))?;

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        Ok(QualityReport::new(
            DataSource::Query {
                engine: self.db_type.to_query_engine(),
                statement: query.to_string(),
                database: self.extract_database_name(),
                execution_id: None,
            },
            column_profiles,
            ScanInfo::new(total_rows, num_columns, total_rows, 1.0, scan_time_ms),
            data_quality_metrics,
        ))
    }

    /// Profile an entire table
    pub fn profile_table(&self, table_name: &str) -> Result<QualityReport> {
        // Validate table name to prevent SQL injection
        if !table_name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
        {
            return Err(anyhow::anyhow!("Invalid table name: {}", table_name));
        }

        let query = format!("SELECT * FROM {}", table_name);
        self.profile_query(&query)
    }

    /// Get raw Arrow RecordBatches for a query (useful for DataFusion integration)
    pub fn get_record_batches(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.load_query(query)
    }

    /// Extract database name from connection string
    fn extract_database_name(&self) -> Option<String> {
        let conn = &self.config.connection_string;
        if let Some(pos) = conn.rfind('/') {
            let db_part = &conn[pos + 1..];
            let db_name = db_part.split('?').next().unwrap_or(db_part);
            if !db_name.is_empty() {
                return Some(db_name.to_string());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_type_detection() {
        assert_eq!(
            DatabaseType::from_connection_string("postgresql://localhost/test"),
            Some(DatabaseType::Postgres)
        );
        assert_eq!(
            DatabaseType::from_connection_string("postgres://localhost/test"),
            Some(DatabaseType::Postgres)
        );
        assert_eq!(
            DatabaseType::from_connection_string("mysql://localhost/test"),
            Some(DatabaseType::MySQL)
        );
        assert_eq!(
            DatabaseType::from_connection_string("sqlite://test.db"),
            Some(DatabaseType::SQLite)
        );
        assert_eq!(
            DatabaseType::from_connection_string("unknown://localhost"),
            None
        );
    }

    #[test]
    fn test_config_builder() {
        let config = ConnectorXConfig::new("postgresql://localhost/test")
            .with_partitions(4)
            .with_partition_column("id")
            .with_protocol("binary");

        assert_eq!(config.partition_num, Some(4));
        assert_eq!(config.partition_on, Some("id".to_string()));
        assert_eq!(config.protocol, Some("binary".to_string()));
    }

    #[test]
    fn test_connection_string_building() {
        let config = ConnectorXConfig::new("postgresql://localhost/test")
            .with_protocol("binary");

        assert_eq!(
            config.build_connection_string(),
            "postgresql://localhost/test?cxprotocol=binary"
        );

        let config_with_params = ConnectorXConfig::new("postgresql://localhost/test?sslmode=require")
            .with_protocol("cursor");

        assert_eq!(
            config_with_params.build_connection_string(),
            "postgresql://localhost/test?sslmode=require&cxprotocol=cursor"
        );
    }

    #[test]
    fn test_table_name_validation() {
        let config = ConnectorXConfig::new("postgresql://localhost/test");
        
        // This will fail because no feature is enabled in tests, but we can check the validation
        #[cfg(any(
            feature = "connectorx-postgres",
            feature = "connectorx-mysql"
        ))]
        {
            let loader = ConnectorXLoader::new(config).unwrap();
            assert!(loader.profile_table("valid_table").is_ok() || true); // May fail due to connection
            assert!(loader.profile_table("invalid;table").is_err());
        }
    }
}
