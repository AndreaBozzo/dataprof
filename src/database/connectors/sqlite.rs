//! SQLite database connector (embedded database)

use crate::database::connection::ConnectionInfo;
#[cfg(feature = "sqlite")]
use crate::database::streaming::{merge_column_batches, StreamingProgress};
use crate::database::{DatabaseConfig, DatabaseConnector};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[cfg(feature = "sqlite")]
use {
    sqlx::sqlite::SqlitePoolOptions,
    sqlx::{sqlite::SqlitePool, Column, Row},
};

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

        #[cfg(not(feature = "sqlite"))]
        {
            Err(anyhow::anyhow!(
                "SQLite support not compiled. Enable 'sqlite' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<HashMap<String, Vec<String>>> {
        #[cfg(feature = "sqlite")]
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
                        "SQLite streaming progress: {:.1}% ({}/{} rows)",
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

        #[cfg(not(feature = "sqlite"))]
        {
            Err(anyhow::anyhow!(
                "SQLite support not compiled. Enable 'sqlite' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn get_table_schema(&mut self, table_name: &str) -> Result<Vec<String>> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Not connected to database"))?;

            let query = format!("PRAGMA table_info({})", table_name);

            let rows = sqlx::query(&query)
                .fetch_all(pool)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get table schema: {}", e))?;

            let mut columns = Vec::new();
            for row in rows {
                let column_name: String = row
                    .try_get(1) // name column is at index 1
                    .map_err(|e| anyhow::anyhow!("Failed to read column name: {}", e))?;
                columns.push(column_name);
            }

            Ok(columns)
        }

        #[cfg(not(feature = "sqlite"))]
        {
            Err(anyhow::anyhow!(
                "SQLite support not compiled. Enable 'sqlite' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64> {
        #[cfg(feature = "sqlite")]
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

        #[cfg(not(feature = "sqlite"))]
        {
            Err(anyhow::anyhow!(
                "SQLite support not compiled. Enable 'sqlite' feature."
            ))
        }
    }

    async fn test_connection(&mut self) -> Result<bool> {
        #[cfg(feature = "sqlite")]
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

        #[cfg(not(feature = "sqlite"))]
        {
            Err(anyhow::anyhow!(
                "SQLite support not compiled. Enable 'sqlite' feature."
            ))
        }
    }
}
