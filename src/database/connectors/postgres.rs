//! PostgreSQL database connector with connection pooling

#[cfg(not(feature = "postgres"))]
use super::common::feature_not_enabled_error;
#[cfg(feature = "postgres")]
use super::common::{build_count_query, not_connected_error};
use crate::database::connection::ConnectionInfo;
use crate::database::{DatabaseConfig, DatabaseConnector, validate_sql_identifier};
use crate::{process_rows_to_columns, streaming_profile_loop};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[cfg(feature = "postgres")]
use {sqlx::postgres::PgPool, sqlx::postgres::PgPoolOptions};

/// PostgreSQL connector with connection pooling support
pub struct PostgresConnector {
    #[allow(dead_code)]
    config: DatabaseConfig,
    #[allow(dead_code)]
    connection_info: ConnectionInfo,
    #[cfg(feature = "postgres")]
    pool: Option<PgPool>,
    #[cfg(not(feature = "postgres"))]
    #[allow(dead_code)]
    pool: Option<()>,
}

impl PostgresConnector {
    /// Create a new PostgreSQL connector
    pub fn new(config: DatabaseConfig) -> Result<Self> {
        let connection_info = ConnectionInfo::parse(&config.connection_string)?;

        if connection_info.database_type() != "postgresql" {
            return Err(anyhow::anyhow!(
                "Invalid connection string for PostgreSQL: {}",
                config.connection_string
            ));
        }

        Ok(Self {
            config,
            connection_info,
            pool: None,
        })
    }
}

#[async_trait]
impl DatabaseConnector for PostgresConnector {
    async fn connect(&mut self) -> Result<()> {
        #[cfg(feature = "postgres")]
        {
            let connection_string = self.connection_info.to_connection_string("sqlx");

            let pool = PgPoolOptions::new()
                .max_connections(self.config.max_connections.unwrap_or(10))
                .acquire_timeout(
                    self.config
                        .connection_timeout
                        .unwrap_or(std::time::Duration::from_secs(30)),
                )
                .connect(&connection_string)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;

            self.pool = Some(pool);
            Ok(())
        }

        #[cfg(not(feature = "postgres"))]
        {
            Err(anyhow::anyhow!(
                "PostgreSQL support not compiled. Enable 'postgres' feature."
            ))
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        #[cfg(feature = "postgres")]
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
        #[cfg(feature = "postgres")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            let rows = sqlx::query(query)
                .fetch_all(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Query execution failed: {}", e))?;

            Ok(process_rows_to_columns!(rows))
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    #[allow(unused_variables)]
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<HashMap<String, Vec<String>>> {
        #[cfg(feature = "postgres")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            // Get total count for progress tracking
            let count_query = build_count_query(query)?;
            let total_rows: i64 = sqlx::query_scalar(&count_query)
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to count rows: {}", e))?;

            streaming_profile_loop!(pool, query, batch_size, total_rows, "PostgreSQL")
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    #[allow(unused_variables)]
    async fn get_table_schema(&mut self, table_name: &str) -> Result<Vec<String>> {
        #[cfg(feature = "postgres")]
        {
            use sqlx::Row;

            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            let query = r#"
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            "#;

            let rows = sqlx::query(query)
                .bind(table_name)
                .fetch_all(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get table schema: {}", e))?;

            let mut columns = Vec::new();
            for row in rows {
                let column_name: String = row
                    .try_get(0)
                    .map_err(|e| anyhow::anyhow!("Failed to read column name: {}", e))?;
                columns.push(column_name);
            }

            Ok(columns)
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    #[allow(unused_variables)]
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64> {
        #[cfg(feature = "postgres")]
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

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    async fn test_connection(&mut self) -> Result<bool> {
        #[cfg(feature = "postgres")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            let result: i32 = sqlx::query_scalar("SELECT 1")
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Connection test failed: {}", e))?;

            Ok(result == 1)
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }
}
