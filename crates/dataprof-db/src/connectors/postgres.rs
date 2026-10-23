//! PostgreSQL database connector with connection pooling

#[cfg(not(feature = "postgres"))]
use super::common::feature_not_enabled_error;
#[cfg(feature = "postgres")]
use super::common::not_connected_error;
use crate::connection::ConnectionInfo;
#[cfg(feature = "postgres")]
use crate::security::validate_sql_identifier;
use crate::{DataProfilerError, DatabaseConfig, DatabaseConnector, QueryColumns};
#[cfg(feature = "postgres")]
use crate::{process_rows_to_columns, streaming_profile_loop};
use async_trait::async_trait;

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
    pub fn new(config: DatabaseConfig) -> Result<Self, DataProfilerError> {
        let connection_info = ConnectionInfo::parse(&config.connection_string)?;

        if connection_info.database_type() != "postgresql" {
            return Err(DataProfilerError::DatabaseConfigError {
                message: format!(
                    "Invalid connection string for PostgreSQL: expected a postgresql:// URL, got a {} URL",
                    connection_info.database_type()
                ),
            });
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
    async fn connect(&mut self) -> Result<(), DataProfilerError> {
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
                .map_err(|e| {
                    DataProfilerError::database_connection(&format!(
                        "Failed to connect to PostgreSQL: {}",
                        e
                    ))
                })?;

            self.pool = Some(pool);
            Ok(())
        }

        #[cfg(not(feature = "postgres"))]
        {
            Err(DataProfilerError::database_feature_disabled(
                "PostgreSQL",
                "postgres",
            ))
        }
    }

    async fn disconnect(&mut self) -> Result<(), DataProfilerError> {
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
    async fn profile_query(&mut self, query: &str) -> Result<QueryColumns, DataProfilerError> {
        #[cfg(feature = "postgres")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            let rows = sqlx::query(query).fetch_all(pool).await.map_err(|e| {
                DataProfilerError::database_query(&format!("Query execution failed: {}", e))
            })?;

            // NUMERIC has no `Type<Sqlite>` impl, so it cannot sit in the
            // shared chain. Without this arm it falls past every arm and
            // records as null (#365).
            process_rows_to_columns!(rows, [::sqlx::types::BigDecimal])
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    #[allow(unused_variables)]
    async fn profile_query_streaming(
        &mut self,
        query: &str,
        batch_size: usize,
    ) -> Result<QueryColumns, DataProfilerError> {
        #[cfg(feature = "postgres")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            streaming_profile_loop!(
                pool,
                query,
                batch_size,
                "PostgreSQL",
                [::sqlx::types::BigDecimal]
            )
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    #[allow(unused_variables)]
    async fn get_table_schema(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<String>, DataProfilerError> {
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
                .map_err(|e| {
                    DataProfilerError::database_query(&format!("Failed to get table schema: {}", e))
                })?;

            let mut columns = Vec::new();
            for row in rows {
                let column_name: String = row.try_get(0).map_err(|e| {
                    DataProfilerError::database_query(&format!("Failed to read column name: {}", e))
                })?;
                columns.push(column_name);
            }

            Ok(columns)
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    #[allow(unused_variables)]
    async fn count_table_rows(&mut self, table_name: &str) -> Result<u64, DataProfilerError> {
        #[cfg(feature = "postgres")]
        {
            let pool = self.pool.as_ref().ok_or_else(not_connected_error)?;

            validate_sql_identifier(table_name)?;
            let query = format!("SELECT COUNT(*) FROM {}", table_name);
            let count: i64 = sqlx::query_scalar(&query)
                .fetch_one(pool)
                .await
                .map_err(|e| {
                    DataProfilerError::database_query(&format!("Failed to count rows: {}", e))
                })?;

            Ok(count as u64)
        }

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }

    async fn test_connection(&mut self) -> Result<bool, DataProfilerError> {
        #[cfg(feature = "postgres")]
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

        #[cfg(not(feature = "postgres"))]
        Err(feature_not_enabled_error("PostgreSQL", "postgres"))
    }
}

/// JSON and JSONB through serde_json, arrays of any element type, and enums,
/// whose value is their label (#952).
///
/// An array renders as its binary wire encoding in hex, which is canonical: two
/// equal arrays of one column encode alike, so duplicate rows still compare by
/// value. Nothing reads the text otherwise. Every non-null value of an array
/// column is an array, so the column is reported as nested, counts only, and
/// its values are withheld from every value-level check. Rendering it as JSON
/// instead would need a decode per element type, and an element type without
/// one would fall back to null, the failure this decoder exists to remove.
///
/// A JSON document serde_json cannot hold keeps the database's own text, see
/// [`raw_json_structured_value`](super::common::raw_json_structured_value).
#[cfg(feature = "postgres")]
impl crate::DecodeStructured for sqlx::postgres::PgRow {
    fn decode_structured(&self, index: usize) -> Option<crate::StructuredValue> {
        use sqlx::postgres::{PgTypeKind, PgValueFormat};
        use sqlx::{Column, Row, TypeInfo};
        use std::fmt::Write;

        let column_type = self.column(index).type_info();
        if matches!(column_type.kind(), PgTypeKind::Enum(_)) {
            // An enum's value is its label, as text in both wire formats, but
            // `try_get::<String>` refuses it for the type mismatch (#952).
            // Decoding skips that check.
            // decode-audit: unknown — a label that is not text falls through
            // to the chain's undecodable-value error, never to a default.
            let raw = self.try_get_raw(index).ok()?;
            let text = <String as sqlx::Decode<sqlx::Postgres>>::decode(raw).ok()?;
            return Some(crate::StructuredValue {
                text,
                container: false,
            });
        }
        let is_jsonb = column_type.name() == "JSONB";
        let is_json = is_jsonb || column_type.name() == "JSON";
        if !is_json && !matches!(column_type.kind(), PgTypeKind::Array(_)) {
            return None;
        }
        if is_json && let Ok(value) = self.try_get::<sqlx::types::JsonValue, _>(index) {
            return Some(super::common::json_structured_value(&value));
        }

        // Both calls fail only on NULL, and the `Option<String>` arm took
        // every NULL first.
        let raw = self.try_get_raw(index).expect("a decoded column index");
        let mut bytes = raw.as_bytes().expect("a non-null value has bytes");
        if is_json {
            // Binary `jsonb` is a format version byte, then the document text.
            if is_jsonb && raw.format() == PgValueFormat::Binary {
                bytes = &bytes[1..];
            }
            return Some(super::common::raw_json_structured_value(
                &String::from_utf8_lossy(bytes),
            ));
        }

        let mut text = String::with_capacity(2 + 2 * bytes.len());
        text.push_str("\\x");
        for byte in bytes {
            // Writing to a String cannot fail.
            let _ = write!(text, "{byte:02x}");
        }
        Some(crate::StructuredValue {
            text,
            container: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_mismatch_error_does_not_leak_the_connection_string() {
        // A mysql:// URL handed to the Postgres connector is a config error, but
        // it must report the detected scheme — never echo the raw string, which
        // carries the password.
        let config = DatabaseConfig {
            connection_string: "mysql://admin:s3cret@db.internal/app".to_string(),
            ..Default::default()
        };
        let msg = match PostgresConnector::new(config) {
            Ok(_) => panic!("scheme mismatch must fail"),
            Err(err) => err.to_string(),
        };
        assert!(!msg.contains("s3cret"), "password must not leak: {msg}");
        assert!(!msg.contains("admin"), "username must not leak: {msg}");
        assert!(
            msg.contains("mysql"),
            "should name the detected scheme: {msg}"
        );
    }
}
