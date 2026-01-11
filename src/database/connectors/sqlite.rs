//! SQLite database connector (embedded database)

#[cfg(not(feature = "sqlite"))]
use super::common::feature_not_enabled_error;
#[cfg(feature = "sqlite")]
use super::common::{build_count_query, not_connected_error};
use crate::database::connection::ConnectionInfo;
use crate::database::{DatabaseConfig, DatabaseConnector, validate_sql_identifier};
use crate::{process_rows_to_columns, streaming_profile_loop};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[cfg(feature = "sqlite")]
use {sqlx::sqlite::SqlitePool, sqlx::sqlite::SqlitePoolOptions};

/// SQLite embedded database connector
pub struct SqliteConnector {
    #[allow(dead_code)]
    config: DatabaseConfig,
    #[allow(dead_code)]
    connection_info: ConnectionInfo,
    #[cfg(feature = "sqlite")]
    pool: Option<SqlitePool>,
    #[cfg(not(feature = "sqlite"))]
    #[allow(dead_code)]
    pool: Option<()>,
}

impl SqliteConnector {
    /// Create a new SQLite connector
    pub fn new(config: DatabaseConfig) -> Result<Self> {
        let connection_info = ConnectionInfo::parse(&config.connection_string)?;

        if connection_info.database_type() != "sqlite" {
            return Err(anyhow::anyhow!(
                "Invalid connection string for SQLite: {}",
                config.connection_string
            ));
        }

        Ok(Self {
            config,
            connection_info,
            pool: None,
        })
    }

    /// Get the database file path
    #[allow(dead_code)]
    fn get_db_path(&self) -> Result<String> {
        if let Some(path) = &self.connection_info.path {
            Ok(path.clone())
        } else if let Some(db) = &self.connection_info.database {
            Ok(db.clone())
        } else {
            Err(anyhow::anyhow!("No database path specified for SQLite"))
        }
    }
}

#[async_trait]
impl DatabaseConnector for SqliteConnector {
    async fn connect(&mut self) -> Result<()> {
        #[cfg(feature = "sqlite")]
        {
            let db_path = self.get_db_path()?;
            let connection_string = if db_path == ":memory:" {
                "sqlite::memory:".to_string()
            } else {
                format!("sqlite://{}", db_path)
            };

            let pool = SqlitePoolOptions::new()
                .max_connections(1) // SQLite is typically single-connection
                .acquire_timeout(
                    self.config
                        .connection_timeout
                        .unwrap_or(std::time::Duration::from_secs(30)),
                )
                .connect(&connection_string)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to SQLite: {}", e))?;

            self.pool = Some(pool);
            Ok(())
        }

        #[cfg(not(feature = "sqlite"))]
        {
            Err(anyhow::anyhow!(
                "SQLite support not compiled. Enable 'sqlite' feature."
            ))
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        #[cfg(feature = "sqlite")]
        {
            if let Some(pool) = &self.pool {
                pool.close().await;
                self.pool = None;
            }
        }
        Ok(())
    }

    #[allow(unused_variables)]
    async fn profile_query(&mut self, query: &str) -> Result<HashMap<String, Vec<String>>> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            let rows = sqlx::query(query)
                .fetch_all(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Query execution failed: {}", e))?;

            Ok(process_rows_to_columns!(rows))
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    #[allow(unused_variables)]
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<HashMap<String, Vec<String>>> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            // Get total count for progress tracking
            let count_query = build_count_query(query)?;
            let total_rows: i64 = sqlx::query_scalar(&count_query)
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to count rows: {}", e))?;

            streaming_profile_loop!(pool, query, batch_size, total_rows, "SQLite")
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    #[allow(unused_variables)]
    async fn get_table_schema(&mut self, table_name: &str) -> Result<Vec<String>> {
        #[cfg(feature = "sqlite")]
        {
            use sqlx::Row;

            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            // SQLite uses PRAGMA instead of information_schema
            let query = format!("PRAGMA table_info({})", table_name);

            let rows = sqlx::query(&query)
                .fetch_all(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get table schema: {}", e))?;

            let mut columns = Vec::new();
            for row in rows {
                let column_name: String = row
                    .try_get(1) // PRAGMA table_info: name column is at index 1
                    .map_err(|e| anyhow::anyhow!("Failed to read column name: {}", e))?;
                columns.push(column_name);
            }

            Ok(columns)
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    #[allow(unused_variables)]
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            validate_sql_identifier(table_name)?;
            let query = format!("SELECT COUNT(*) FROM {}", table_name);
            let count: i64 = sqlx::query_scalar(&query)
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to count rows: {}", e))?;

            Ok(count as u64)
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    async fn test_connection(&mut self) -> Result<bool> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            let result: i32 = sqlx::query_scalar("SELECT 1")
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Connection test failed: {}", e))?;

            Ok(result == 1)
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }
}
