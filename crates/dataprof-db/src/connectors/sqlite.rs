//! SQLite database connector (embedded database)

#[cfg(not(feature = "sqlite"))]
use super::common::feature_not_enabled_error;
#[cfg(feature = "sqlite")]
use super::common::not_connected_error;
use crate::connection::ConnectionInfo;
#[cfg(feature = "sqlite")]
use crate::security::validate_sql_identifier;
use crate::{DataProfilerError, DatabaseConfig, DatabaseConnector, QueryColumns};
#[cfg(feature = "sqlite")]
use crate::{process_rows_to_columns, streaming_profile_loop};
use async_trait::async_trait;

#[cfg(feature = "sqlite")]
use {sqlx::sqlite::SqlitePool, sqlx::sqlite::SqlitePoolOptions};

/// SQLite embedded database connector
#[derive(Debug)]
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
    pub fn new(config: DatabaseConfig) -> Result<Self, DataProfilerError> {
        let connection_info = ConnectionInfo::parse(&config.connection_string)?;

        if connection_info.database_type() != "sqlite" {
            return Err(DataProfilerError::DatabaseConfigError {
                message: format!(
                    "Invalid connection string for SQLite: {}",
                    config.connection_string
                ),
            });
        }

        // SQLite has no network endpoint or credentials. Reject user, password, or port.
        // A host by itself is allowed (e.g. sqlite://relative/path.db).
        if connection_info.username.is_some()
            || connection_info.password.is_some()
            || connection_info.port.is_some()
        {
            let mut found = Vec::new();
            if connection_info.username.is_some() {
                found.push("user");
            }
            if connection_info.password.is_some() {
                found.push("password");
            }
            if connection_info.port.is_some() {
                found.push("port");
            }
            let found_str = found.join(", ");
            return Err(DataProfilerError::DatabaseConfigError {
                message: format!(
                    "SQLite URLs do not support {found_str}. Supported forms: sqlite:///absolute/path.db, sqlite://relative/path.db, sqlite::memory:, or a bare file path"
                ),
            });
        }

        Ok(Self {
            config,
            connection_info,
            pool: None,
        })
    }

    /// Get the database file path
    #[allow(dead_code)]
    fn get_db_path(&self) -> Result<String, DataProfilerError> {
        if let Some(path) = &self.connection_info.path {
            Ok(path.clone())
        } else if let Some(db) = &self.connection_info.database {
            Ok(db.clone())
        } else {
            Err(DataProfilerError::DatabaseConfigError {
                message: "No database path specified for SQLite".to_string(),
            })
        }
    }
}

#[async_trait]
impl DatabaseConnector for SqliteConnector {
    async fn connect(&mut self) -> Result<(), DataProfilerError> {
        #[cfg(feature = "sqlite")]
        {
            let db_path = self.get_db_path()?;
            let connection_string = if db_path == ":memory:" {
                "sqlite::memory:".to_string()
            } else {
                format!("sqlite://{}", db_path)
            };

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .acquire_timeout(
                    self.config
                        .connection_timeout
                        .unwrap_or(std::time::Duration::from_secs(30)),
                )
                .connect(&connection_string)
                .await
                .map_err(|e| {
                    DataProfilerError::database_connection(&format!(
                        "Failed to connect to SQLite: {}",
                        e
                    ))
                })?;

            self.pool = Some(pool);
            Ok(())
        }

        #[cfg(not(feature = "sqlite"))]
        {
            Err(DataProfilerError::database_feature_disabled(
                "SQLite", "sqlite",
            ))
        }
    }

    async fn disconnect(&mut self) -> Result<(), DataProfilerError> {
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
    async fn profile_query(&mut self, query: &str) -> Result<QueryColumns, DataProfilerError> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            let rows = sqlx::query(query).fetch_all(pool).await.map_err(|e| {
                DataProfilerError::database_query(&format!("Query execution failed: {}", e))
            })?;

            process_rows_to_columns!(rows)
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    #[allow(unused_variables)]
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<QueryColumns, DataProfilerError> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            streaming_profile_loop!(pool, query, batch_size, "SQLite")
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    #[allow(unused_variables)]
    async fn get_table_schema(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<String>, DataProfilerError> {
        #[cfg(feature = "sqlite")]
        {
            use sqlx::Row;

            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;
            let query = format!("PRAGMA table_info({table_name})");

            let rows = sqlx::query(&query).fetch_all(pool).await.map_err(|e| {
                DataProfilerError::database_query(&format!("Failed to get table schema: {}", e))
            })?;

            let mut columns = Vec::new();
            for row in rows {
                let column_name: String = row.try_get(1).map_err(|e| {
                    DataProfilerError::database_query(&format!("Failed to read column name: {}", e))
                })?;
                columns.push(column_name);
            }

            Ok(columns)
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    #[allow(unused_variables)]
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64, DataProfilerError> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            validate_sql_identifier(table_name)?;
            let query = format!("SELECT COUNT(*) FROM {table_name}");
            let count: i64 = sqlx::query_scalar(&query)
                .fetch_one(pool)
                .await
                .map_err(|e| {
                    DataProfilerError::database_query(&format!("Failed to count rows: {}", e))
                })?;
            Ok(count as u64)
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }

    async fn test_connection(&mut self) -> Result<bool, DataProfilerError> {
        #[cfg(feature = "sqlite")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;
            let result: i32 = sqlx::query_scalar("SELECT 1")
                .fetch_one(pool)
                .await
                .map_err(|e| {
                    DataProfilerError::database_query(&format!("Connection test failed: {}", e))
                })?;

            Ok(result == 1)
        }

        #[cfg(not(feature = "sqlite"))]
        Err(feature_not_enabled_error("SQLite", "sqlite"))
    }
}

/// SQLite has no JSON or array type: JSON is stored as TEXT, and the `String`
/// arm decodes it as text.
#[cfg(feature = "sqlite")]
impl crate::DecodeStructured for sqlx::sqlite::SqliteRow {
    fn decode_structured(&self, _index: usize) -> Option<crate::StructuredValue> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatabaseConfig;

    fn config(url: &str) -> DatabaseConfig {
        DatabaseConfig {
            connection_string: url.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn rejects_user() {
        let err = SqliteConnector::new(config("sqlite://user@host/data.db")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("user"), "{msg}");
        assert!(msg.contains("Supported forms"), "{msg}");
    }

    #[test]
    fn rejects_password() {
        let err = SqliteConnector::new(config("sqlite://:pw@host/data.db")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("password"), "{msg}");
    }

    #[test]
    fn rejects_port() {
        let err = SqliteConnector::new(config("sqlite://host:5432/data.db")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("port"), "{msg}");
    }

    #[test]
    fn rejects_user_password_and_port() {
        let err = SqliteConnector::new(config("sqlite://user:pw@host:5432/data.db")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("user"), "{msg}");
        assert!(msg.contains("password"), "{msg}");
        assert!(msg.contains("port"), "{msg}");
        // Must not include the actual password.
        assert!(!msg.contains("pw"), "{msg}");
    }

    #[test]
    fn accepts_supported_forms() {
        for url in [
            "sqlite:///absolute/path.db",
            "sqlite://relative/path.db",
            "sqlite::memory:",
            "/absolute/path.db",
            "relative.db",
        ] {
            SqliteConnector::new(config(url)).unwrap_or_else(|e| panic!("should accept {url}: {e}"));
        }
    }
}
