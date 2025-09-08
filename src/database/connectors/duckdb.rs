//! DuckDB analytical database connector

use crate::database::connection::ConnectionInfo;
#[cfg(feature = "duckdb")]
use crate::database::streaming::{merge_column_batches, StreamingProgress};
use crate::database::{DatabaseConfig, DatabaseConnector};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[cfg(feature = "duckdb")]
use {duckdb::Connection, tokio::task};

/// DuckDB analytical database connector
pub struct DuckDbConnector {
    #[allow(dead_code)]
    config: DatabaseConfig,
    #[allow(dead_code)]
    connection_info: ConnectionInfo,
    #[cfg(feature = "duckdb")]
    db_path: String,
    #[cfg(not(feature = "duckdb"))]
    #[allow(dead_code)]
    db_path: String,
}

impl DuckDbConnector {
    /// Create a new DuckDB connector
    pub fn new(config: DatabaseConfig) -> Result<Self> {
        let connection_info = ConnectionInfo::parse(&config.connection_string)?;

        if connection_info.database_type() != "duckdb" {
            return Err(anyhow::anyhow!(
                "Invalid connection string for DuckDB: {}",
                config.connection_string
            ));
        }

        let db_path = if let Some(path) = &connection_info.path {
            path.clone()
        } else if let Some(db) = &connection_info.database {
            db.clone()
        } else {
            ":memory:".to_string()
        };

        Ok(Self {
            config,
            connection_info,
            db_path,
        })
    }
}

#[async_trait]
impl DatabaseConnector for DuckDbConnector {
    async fn connect(&mut self) -> Result<()> {
        #[cfg(feature = "duckdb")]
        {
            // Test the connection by opening and immediately closing it
            let db_path = self.db_path.clone();
            task::spawn_blocking(move || {
                if db_path == ":memory:" {
                    Connection::open_in_memory()
                } else {
                    Connection::open(&db_path)
                }
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {}", e))?
            .map_err(|e| anyhow::anyhow!("Failed to connect to DuckDB: {}", e))?;

            Ok(())
        }

        #[cfg(not(feature = "duckdb"))]
        {
            Err(anyhow::anyhow!(
                "DuckDB support not compiled. Enable 'duckdb' feature."
            ))
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Nothing to do for DuckDB - connections are per-operation
        Ok(())
    }

    #[allow(unused_variables)]
    async fn profile_query(&mut self, query: &str) -> Result<HashMap<String, Vec<String>>> {
        #[cfg(feature = "duckdb")]
        {
            let db_path = self.db_path.clone();
            let query = query.to_string();

            // Run DuckDB query in a separate thread since it's blocking
            let result = task::spawn_blocking(move || -> Result<HashMap<String, Vec<String>>> {
                let connection = if db_path == ":memory:" {
                    Connection::open_in_memory()
                } else {
                    Connection::open(&db_path)
                }
                .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;
                let mut stmt = connection
                    .prepare(&query)
                    .map_err(|e| anyhow::anyhow!("Failed to prepare query: {}", e))?;

                // Get column names first
                let column_names: Vec<String> =
                    stmt.column_names().iter().map(|s| s.to_string()).collect();

                let rows = stmt
                    .query_map([], |row| {
                        let mut row_data = Vec::new();
                        let column_count = row.as_ref().column_count();

                        for i in 0..column_count {
                            let value: Result<String, _> = row.get(i);
                            let string_value = match value {
                                Ok(v) => v,
                                Err(_) => {
                                    // Try to get as different types if string fails
                                    if let Ok(i_val) = row.get::<_, i64>(i) {
                                        i_val.to_string()
                                    } else if let Ok(f_val) = row.get::<_, f64>(i) {
                                        f_val.to_string()
                                    } else if let Ok(b_val) = row.get::<_, bool>(i) {
                                        b_val.to_string()
                                    } else {
                                        String::new() // NULL or unsupported type
                                    }
                                }
                            };
                            row_data.push(string_value);
                        }
                        Ok(row_data)
                    })
                    .map_err(|e| anyhow::anyhow!("Query execution failed: {}", e))?;

                let mut result: HashMap<String, Vec<String>> = HashMap::new();

                // Initialize columns
                for col_name in &column_names {
                    result.insert(col_name.clone(), Vec::new());
                }

                // Process rows
                for row_result in rows {
                    let row_data =
                        row_result.map_err(|e| anyhow::anyhow!("Failed to process row: {}", e))?;

                    for (i, value) in row_data.iter().enumerate() {
                        if let Some(col_name) = column_names.get(i) {
                            if let Some(column_data) = result.get_mut(col_name) {
                                column_data.push(value.clone());
                            }
                        }
                    }
                }

                Ok(result)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {}", e))??;

            Ok(result)
        }

        #[cfg(not(feature = "duckdb"))]
        {
            Err(anyhow::anyhow!(
                "DuckDB support not compiled. Enable 'duckdb' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<HashMap<String, Vec<String>>> {
        #[cfg(feature = "duckdb")]
        {
            // For DuckDB, we'll implement streaming by using LIMIT/OFFSET
            let db_path = self.db_path.clone();
            let query = query.to_string();
            let batch_size_copy = batch_size;

            // Get total count first
            let total_rows = task::spawn_blocking({
                let db_path = db_path.clone();
                let query = query.clone();
                move || -> Result<u64> {
                    let connection = if db_path == ":memory:" {
                        Connection::open_in_memory()
                    } else {
                        Connection::open(&db_path)
                    }
                    .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;
                    let count_query = if query.trim().to_uppercase().starts_with("SELECT") {
                        format!("SELECT COUNT(*) FROM ({}) as count_subquery", query)
                    } else {
                        format!("SELECT COUNT(*) FROM {}", query)
                    };

                    let mut stmt = connection
                        .prepare(&count_query)
                        .map_err(|e| anyhow::anyhow!("Failed to prepare count query: {}", e))?;

                    let count: i64 = stmt
                        .query_row([], |row| row.get(0))
                        .map_err(|e| anyhow::anyhow!("Failed to count rows: {}", e))?;

                    Ok(count as u64)
                }
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {}", e))??;

            let mut progress = StreamingProgress::new(Some(total_rows));
            let mut all_batches = Vec::new();
            let mut offset = 0;

            loop {
                let batch_query = format!("{} LIMIT {} OFFSET {}", query, batch_size_copy, offset);

                let batch_result = task::spawn_blocking({
                    let db_path = db_path.clone();
                    let batch_query = batch_query.clone();
                    move || -> Result<HashMap<String, Vec<String>>> {
                        let connection = if db_path == ":memory:" {
                            Connection::open_in_memory()
                        } else {
                            Connection::open(&db_path)
                        }
                        .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;
                        let mut stmt = connection
                            .prepare(&batch_query)
                            .map_err(|e| anyhow::anyhow!("Failed to prepare batch query: {}", e))?;

                        // Get column names first
                        let column_names: Vec<String> =
                            stmt.column_names().iter().map(|s| s.to_string()).collect();

                        let rows = stmt
                            .query_map([], |row| {
                                let mut row_data = Vec::new();
                                let column_count = row.as_ref().column_count();

                                for i in 0..column_count {
                                    let value: Result<String, _> = row.get(i);
                                    let string_value = match value {
                                        Ok(v) => v,
                                        Err(_) => {
                                            // Try different types
                                            if let Ok(i_val) = row.get::<_, i64>(i) {
                                                i_val.to_string()
                                            } else if let Ok(f_val) = row.get::<_, f64>(i) {
                                                f_val.to_string()
                                            } else if let Ok(b_val) = row.get::<_, bool>(i) {
                                                b_val.to_string()
                                            } else {
                                                String::new()
                                            }
                                        }
                                    };
                                    row_data.push(string_value);
                                }
                                Ok(row_data)
                            })
                            .map_err(|e| anyhow::anyhow!("Batch query execution failed: {}", e))?;

                        let mut batch_data: HashMap<String, Vec<String>> = HashMap::new();

                        // Initialize columns
                        for col_name in &column_names {
                            batch_data.insert(col_name.clone(), Vec::new());
                        }

                        // Process rows
                        let mut row_count = 0;
                        for row_result in rows {
                            let row_data = row_result
                                .map_err(|e| anyhow::anyhow!("Failed to process row: {}", e))?;

                            for (i, value) in row_data.iter().enumerate() {
                                if let Some(col_name) = column_names.get(i) {
                                    if let Some(column_data) = batch_data.get_mut(col_name) {
                                        column_data.push(value.clone());
                                    }
                                }
                            }
                            row_count += 1;
                        }

                        // If no rows, return empty result to signal end
                        if row_count == 0 {
                            return Ok(HashMap::new());
                        }

                        Ok(batch_data)
                    }
                })
                .await
                .map_err(|e| anyhow::anyhow!("Task execution failed: {}", e))??;

                if batch_result.is_empty() {
                    break;
                }

                let batch_rows = batch_result.values().next().map(|v| v.len()).unwrap_or(0);
                progress.update(batch_rows as u64);

                // Optional progress reporting
                if let Some(percentage) = progress.percentage() {
                    println!(
                        "DuckDB streaming progress: {:.1}% ({}/{} rows)",
                        percentage, progress.processed_rows, total_rows
                    );
                }

                all_batches.push(batch_result);
                offset += batch_size_copy;

                // Check if we've processed all rows
                if batch_rows < batch_size_copy {
                    break;
                }
            }

            // Merge all batches
            merge_column_batches(all_batches)
        }

        #[cfg(not(feature = "duckdb"))]
        {
            Err(anyhow::anyhow!(
                "DuckDB support not compiled. Enable 'duckdb' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn get_table_schema(&mut self, table_name: &str) -> Result<Vec<String>> {
        #[cfg(feature = "duckdb")]
        {
            let db_path = self.db_path.clone();
            let table_name = table_name.to_string();

            let columns = task::spawn_blocking(move || -> Result<Vec<String>> {
                let connection = if db_path == ":memory:" {
                    Connection::open_in_memory()
                } else {
                    Connection::open(&db_path)
                }
                .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;
                let query = format!("DESCRIBE {}", table_name);

                let mut stmt = connection
                    .prepare(&query)
                    .map_err(|e| anyhow::anyhow!("Failed to describe table: {}", e))?;

                let rows = stmt
                    .query_map([], |row| {
                        let column_name: String = row.get(0)?; // column_name is usually first
                        Ok(column_name)
                    })
                    .map_err(|e| anyhow::anyhow!("Failed to execute describe: {}", e))?;

                let mut columns = Vec::new();
                for row_result in rows {
                    let column_name = row_result
                        .map_err(|e| anyhow::anyhow!("Failed to read column name: {}", e))?;
                    columns.push(column_name);
                }

                Ok(columns)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {}", e))??;

            Ok(columns)
        }

        #[cfg(not(feature = "duckdb"))]
        {
            Err(anyhow::anyhow!(
                "DuckDB support not compiled. Enable 'duckdb' feature."
            ))
        }
    }

    #[allow(unused_variables)]
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64> {
        #[cfg(feature = "duckdb")]
        {
            let db_path = self.db_path.clone();
            let table_name = table_name.to_string();

            let count = task::spawn_blocking(move || -> Result<u64> {
                let connection = if db_path == ":memory:" {
                    Connection::open_in_memory()
                } else {
                    Connection::open(&db_path)
                }
                .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;
                let query = format!("SELECT COUNT(*) FROM {}", table_name);

                let mut stmt = connection
                    .prepare(&query)
                    .map_err(|e| anyhow::anyhow!("Failed to prepare count query: {}", e))?;

                let count: i64 = stmt
                    .query_row([], |row| row.get(0))
                    .map_err(|e| anyhow::anyhow!("Failed to count rows: {}", e))?;

                Ok(count as u64)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {}", e))??;

            Ok(count)
        }

        #[cfg(not(feature = "duckdb"))]
        {
            Err(anyhow::anyhow!(
                "DuckDB support not compiled. Enable 'duckdb' feature."
            ))
        }
    }

    async fn test_connection(&mut self) -> Result<bool> {
        #[cfg(feature = "duckdb")]
        {
            let db_path = self.db_path.clone();
            let test_result = task::spawn_blocking(move || -> Result<bool> {
                let connection = if db_path == ":memory:" {
                    Connection::open_in_memory()
                } else {
                    Connection::open(&db_path)
                }
                .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;
                let mut stmt = connection
                    .prepare("SELECT 1")
                    .map_err(|e| anyhow::anyhow!("Failed to prepare test query: {}", e))?;

                let result: i32 = stmt
                    .query_row([], |row| row.get(0))
                    .map_err(|e| anyhow::anyhow!("Connection test failed: {}", e))?;

                Ok(result == 1)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {}", e))??;

            Ok(test_result)
        }

        #[cfg(not(feature = "duckdb"))]
        {
            Err(anyhow::anyhow!(
                "DuckDB support not compiled. Enable 'duckdb' feature."
            ))
        }
    }
}
