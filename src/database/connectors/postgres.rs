//! PostgreSQL database connector with connection pooling

use crate::database::connection::ConnectionInfo;
#[cfg(feature = "postgres")]
use crate::database::streaming::{merge_column_batches, StreamingProgress};
use crate::database::{DatabaseConfig, DatabaseConnector};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[cfg(feature = "postgres")]
use {
    sqlx::postgres::PgPoolOptions,
    sqlx::{postgres::PgPool, Column, Row},
};

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
            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Not connected to database"))?;

            let rows = sqlx::query(query)
                .fetch_all(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Query execution failed: {}", e))?;

            if rows.is_empty() {
                return Ok(HashMap::new());
            }

            // Get column names from the first row
            let columns = rows[0].columns().to_vec();
            let mut result: HashMap<String, Vec<String>> = HashMap::new();

            // Initialize columns
            for col in &columns {
                result.insert(col.name().to_string(), Vec::new());
            }

            // Process rows
            for row in &rows {
                for (i, col) in columns.iter().enumerate() {
                    let value: Option<String> = row.try_get(i).unwrap_or(None);
                    let string_value = value.unwrap_or_default();

                    if let Some(column_data) = result.get_mut(col.name()) {
                        column_data.push(string_value);
                    }
                }
            }

            Ok(result)
        }

        #[cfg(not(feature = "postgres"))]
        {
            Err(anyhow::anyhow!(
                "PostgreSQL support not compiled. Enable 'postgres' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<HashMap<String, Vec<String>>> {
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Not connected to database"))?;

            // Get total count for progress tracking
            let count_query = if query.trim().to_uppercase().starts_with("SELECT") {
                format!("SELECT COUNT(*) FROM ({}) as count_subquery", query)
            } else {
                format!("SELECT COUNT(*) FROM {}", query)
            };

            let total_rows: i64 = sqlx::query_scalar(&count_query)
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to count rows: {}", e))?;

            let mut progress = StreamingProgress::new(Some(total_rows as u64));
            let mut all_batches = Vec::new();
            let mut offset = 0;

            loop {
                let batch_query = format!("{} LIMIT {} OFFSET {}", query, batch_size, offset);

                let rows = sqlx::query(&batch_query)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| anyhow::anyhow!("Batch query execution failed: {}", e))?;

                if rows.is_empty() {
                    break;
                }

                // Process batch
                let columns = rows[0].columns().to_vec();
                let mut batch_result: HashMap<String, Vec<String>> = HashMap::new();

                // Initialize columns for this batch
                for col in &columns {
                    batch_result.insert(col.name().to_string(), Vec::new());
                }

                // Process rows in this batch
                for row in &rows {
                    for (i, col) in columns.iter().enumerate() {
                        let value: Option<String> = row.try_get(i).unwrap_or(None);
                        let string_value = value.unwrap_or_default();

                        if let Some(column_data) = batch_result.get_mut(col.name()) {
                            column_data.push(string_value);
                        }
                    }
                }

                all_batches.push(batch_result);
                progress.update(rows.len() as u64);

                // Optional progress reporting
                if let Some(percentage) = progress.percentage() {
                    println!(
                        "PostgreSQL streaming progress: {:.1}% ({}/{} rows)",
                        percentage, progress.processed_rows, total_rows
                    );
                }

                offset += batch_size;

                // Check if we've processed all rows
                if rows.len() < batch_size {
                    break;
                }
            }

            // Merge all batches
            merge_column_batches(all_batches)
        }

        #[cfg(not(feature = "postgres"))]
        {
            Err(anyhow::anyhow!(
                "PostgreSQL support not compiled. Enable 'postgres' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn get_table_schema(&mut self, table_name: &str) -> Result<Vec<String>> {
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Not connected to database"))?;

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
        {
            Err(anyhow::anyhow!(
                "PostgreSQL support not compiled. Enable 'postgres' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64> {
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Not connected to database"))?;

            let query = format!("SELECT COUNT(*) FROM {}", table_name);
            let count: i64 = sqlx::query_scalar(&query)
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to count rows: {}", e))?;

            Ok(count as u64)
        }

        #[cfg(not(feature = "postgres"))]
        {
            Err(anyhow::anyhow!(
                "PostgreSQL support not compiled. Enable 'postgres' feature."
            ))
        }
    }

    async fn test_connection(&mut self) -> Result<bool> {
        #[cfg(feature = "postgres")]
        {
            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Not connected to database"))?;

            let result: i32 = sqlx::query_scalar("SELECT 1")
                .fetch_one(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Connection test failed: {}", e))?;

            Ok(result == 1)
        }

        #[cfg(not(feature = "postgres"))]
        {
            Err(anyhow::anyhow!(
                "PostgreSQL support not compiled. Enable 'postgres' feature."
            ))
        }
    }
}
