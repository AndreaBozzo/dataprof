//! Database connectivity module for dataprof.
//!
//! This crate owns the database profiling surface, including connection
//! handling, secure configuration helpers, sampling, streaming, and the
//! feature-gated sqlx-based connectors.

pub(crate) use dataprof_core::DataProfilerError;
use dataprof_core::{DataSource, ExecutionMetadata, QualityDimension, QueryEngine};
use dataprof_metrics::analyze_column;
use dataprof_runtime::{ProfileReport, ReportAssembler};
use std::collections::HashMap;

pub mod connection;
pub mod connectors;
pub mod retry;
pub mod sampling;
pub mod security;
pub mod streaming;

pub use connection::*;
pub use connectors::*;
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
    pub ssl_config: Option<SslConfig>,
    pub load_credentials_from_env: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            batch_size: 10000,
            max_connections: Some(10),
            connection_timeout: Some(std::time::Duration::from_secs(30)),
            retry_config: Some(RetryConfig::default()),
            sampling_config: None,
            ssl_config: Some(SslConfig::default()),
            load_credentials_from_env: true,
        }
    }
}

/// Trait that all database connectors must implement
#[async_trait::async_trait]
pub trait DatabaseConnector: Send + Sync {
    /// Connect to the database
    async fn connect(&mut self) -> Result<(), DataProfilerError>;

    /// Disconnect from the database
    async fn disconnect(&mut self) -> Result<(), DataProfilerError>;

    /// Execute a query and get column data for profiling
    async fn profile_query(
        &mut self,
        query: &str,
    ) -> Result<HashMap<String, Vec<String>>, DataProfilerError>;

    /// Execute a query with streaming for large result sets
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<HashMap<String, Vec<String>>, DataProfilerError>;

    /// Get table schema information
    async fn get_table_schema(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<String>, DataProfilerError>;

    /// Count total rows in table (for progress tracking)
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64, DataProfilerError>;

    /// Test connection
    async fn test_connection(&mut self) -> Result<bool, DataProfilerError>;
}

/// Factory function to create appropriate database connector
pub fn create_connector(
    mut config: DatabaseConfig,
) -> Result<Box<dyn DatabaseConnector>, DataProfilerError> {
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
    } else {
        Err(DataProfilerError::DatabaseConfigError {
            message: format!(
                "Unsupported database connection string: {}. Supported: postgresql://, mysql://, sqlite://",
                connection_str
            ),
        })
    }
}

/// Apply environment configuration to database config
fn apply_environment_configuration(
    mut config: DatabaseConfig,
) -> Result<DatabaseConfig, DataProfilerError> {
    let database_type = if config.connection_string.is_empty() {
        if std::env::var("POSTGRES_URL").is_ok()
            || std::env::var("DATABASE_URL")
                .map(|url| url.starts_with("postgres"))
                .unwrap_or(false)
        {
            "postgresql".to_string()
        } else if std::env::var("MYSQL_URL").is_ok() {
            "mysql".to_string()
        } else {
            "postgresql".to_string()
        }
    } else {
        let conn_info = ConnectionInfo::parse(&config.connection_string)?;
        conn_info.database_type().to_string()
    };
    let database_type = database_type.as_str();

    if config.connection_string.is_empty() {
        let (secure_connection_string, ssl_config) = load_secure_database_config(database_type)?;
        config.connection_string = secure_connection_string;
        config.ssl_config = Some(ssl_config);
    } else {
        if let Some(ssl_config) = &config.ssl_config {
            config.connection_string = ssl_config
                .apply_to_connection_string(config.connection_string.clone(), database_type);
        }

        if config.load_credentials_from_env {
            let credentials = DatabaseCredentials::from_environment(database_type);
            config.connection_string =
                credentials.apply_to_connection_string(&config.connection_string);
        }
    }

    Ok(config)
}

/// High-level function to analyze a database table or query.
pub async fn analyze_database(
    config: DatabaseConfig,
    query: &str,
    calculate_quality: bool,
    quality_dimensions: Option<Vec<QualityDimension>>,
) -> Result<ProfileReport, DataProfilerError> {
    let mut connector = create_connector(config.clone())?;

    connector.connect().await?;

    let start = std::time::Instant::now();

    let (actual_query, is_table) = if query.trim().to_uppercase().starts_with("SELECT") {
        let validated_query = security::validate_base_query(query)?;
        (validated_query, false)
    } else {
        security::validate_sql_identifier(query)?;
        (format!("SELECT * FROM {}", query), true)
    };

    let total_rows = if is_table {
        connector.count_table_rows(query).await.unwrap_or(0)
    } else {
        0
    };

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

    let columns = connector
        .profile_query_streaming(&final_query, config.batch_size)
        .await?;

    connector.disconnect().await?;

    let query_engine = detect_query_engine(&config.connection_string);

    if columns.is_empty() {
        let mut exec = ExecutionMetadata::new(0, 0, start.elapsed().as_millis());
        if let Some(ref info) = sample_info
            && info.sampling_ratio < 1.0
        {
            exec = exec
                .with_sampling(info.sampling_ratio)
                .with_source_exhausted(false);
        }
        return Ok(ReportAssembler::new(
            DataSource::Query {
                engine: query_engine.clone(),
                statement: query.to_string(),
                database: extract_database_name(&config.connection_string),
                execution_id: None,
            },
            exec,
        )
        .skip_quality()
        .build());
    }

    let mut column_profiles = Vec::new();
    let actual_rows_processed = columns.values().next().map(|v| v.len()).unwrap_or(0);

    for (name, data) in &columns {
        let profile = analyze_column(name, data);
        column_profiles.push(profile);
    }

    let scan_time_ms = start.elapsed().as_millis();
    let sampling_ratio = sample_info.map(|s| s.sampling_ratio).unwrap_or(1.0);
    let num_columns = column_profiles.len();

    let mut execution = ExecutionMetadata::new(actual_rows_processed, num_columns, scan_time_ms);
    if sampling_ratio < 1.0 {
        execution = execution
            .with_sampling(sampling_ratio)
            .with_source_exhausted(false);
    }

    let mut assembler = ReportAssembler::new(
        DataSource::Query {
            engine: query_engine,
            statement: query.to_string(),
            database: extract_database_name(&config.connection_string),
            execution_id: None,
        },
        execution,
    )
    .columns(column_profiles);

    if calculate_quality {
        assembler = assembler.with_quality_data(columns);
        if let Some(dims) = quality_dimensions {
            assembler = assembler.with_requested_dimensions(dims);
        }
    } else {
        assembler = assembler.skip_quality();
    }

    Ok(assembler.build())
}

/// Detect query engine from connection string
fn detect_query_engine(connection_string: &str) -> QueryEngine {
    let conn = connection_string.to_lowercase();
    if conn.starts_with("postgres") || conn.starts_with("postgresql") {
        QueryEngine::Postgres
    } else if conn.starts_with("mysql") || conn.starts_with("mariadb") {
        QueryEngine::MySql
    } else if conn.starts_with("sqlite") {
        QueryEngine::Sqlite
    } else {
        QueryEngine::Custom("unknown".to_string())
    }
}

/// Extract database name from connection string
fn extract_database_name(connection_string: &str) -> Option<String> {
    if let Some(pos) = connection_string.rfind('/') {
        let db_part = &connection_string[pos + 1..];
        let db_name = db_part.split('?').next().unwrap_or(db_part);
        if !db_name.is_empty() {
            return Some(db_name.to_string());
        }
    }
    None
}
