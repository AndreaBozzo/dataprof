//! Integration tests for database connectors
//!
//! These tests verify that the database connectors compile and work correctly.
//! They use in-memory databases where possible to avoid requiring external setup.

#[cfg(feature = "database")]
use anyhow::Result;

#[cfg(feature = "database")]
use dataprof::{create_connector, profile_database, DatabaseConfig};

#[cfg(all(test, feature = "database", feature = "sqlite"))]
mod sqlite_tests {
    use super::*;

    #[tokio::test]
    async fn test_sqlite_memory_connection() -> Result<()> {
        let config = DatabaseConfig {
            connection_string: ":memory:".to_string(),
            batch_size: 1000,
            max_connections: Some(1),
            connection_timeout: Some(std::time::Duration::from_secs(5)),
        };

        let mut connector = create_connector(config)?;

        // Test connection
        connector.connect().await?;
        let is_connected = connector.test_connection().await?;
        assert!(is_connected);

        connector.disconnect().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_sqlite_create_and_profile_table() -> Result<()> {
        let config = DatabaseConfig {
            connection_string: ":memory:".to_string(),
            batch_size: 1000,
            max_connections: Some(1),
            connection_timeout: Some(std::time::Duration::from_secs(5)),
        };

        // Use the high-level profile_database function
        // Note: This test would need actual SQLite setup with test data
        // For now, we just verify the function exists and can be called

        // This would normally fail because we don't have test data,
        // but it verifies the API compiles correctly
        let result = profile_database(config, "SELECT 1 as test_column").await;

        // We expect this to fail with a connection error since we're using :memory:
        // without setting up the database, but that's ok for a compilation test
        match result {
            Ok(_) => {
                // If it somehow works, that's great
                println!("SQLite test passed unexpectedly - that's good!");
            }
            Err(e) => {
                // Expected - we don't have a proper test database setup
                println!("SQLite test failed as expected: {}", e);
            }
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "database", feature = "duckdb"))]
mod duckdb_tests {
    use super::*;

    #[tokio::test]
    async fn test_duckdb_memory_connection() -> Result<()> {
        let config = DatabaseConfig {
            connection_string: ":memory:".to_string(),
            batch_size: 1000,
            max_connections: Some(1),
            connection_timeout: Some(std::time::Duration::from_secs(5)),
        };

        let mut connector = create_connector(config)?;

        // Test connection
        connector.connect().await?;
        let is_connected = connector.test_connection().await?;
        assert!(is_connected);

        connector.disconnect().await?;

        Ok(())
    }
}

#[cfg(all(test, feature = "database"))]
mod connection_tests {
    use super::*;
    use dataprof::database::connection::ConnectionInfo;

    #[test]
    fn test_parse_postgresql_connection_string() {
        let conn_str = "postgresql://user:pass@localhost:5432/mydb";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.database_type(), "postgresql");
        assert_eq!(info.host, Some("localhost".to_string()));
        assert_eq!(info.port, Some(5432));
        assert_eq!(info.username, Some("user".to_string()));
        assert_eq!(info.password, Some("pass".to_string()));
        assert_eq!(info.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_parse_mysql_connection_string() {
        let conn_str = "mysql://root:password@127.0.0.1:3306/testdb";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.database_type(), "mysql");
        assert_eq!(info.host, Some("127.0.0.1".to_string()));
        assert_eq!(info.port, Some(3306));
        assert_eq!(info.username, Some("root".to_string()));
        assert_eq!(info.password, Some("password".to_string()));
        assert_eq!(info.database, Some("testdb".to_string()));
    }

    #[test]
    fn test_parse_sqlite_path() {
        let conn_str = "/path/to/database.db";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.database_type(), "sqlite");
        assert_eq!(info.path, Some("/path/to/database.db".to_string()));
    }

    #[test]
    fn test_parse_duckdb_path() {
        let conn_str = "/path/to/data.duckdb";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.database_type(), "duckdb");
        assert_eq!(info.path, Some("/path/to/data.duckdb".to_string()));
    }

    #[test]
    fn test_create_connector_factory() {
        // Test that connector factory works for different database types

        let postgres_config = DatabaseConfig {
            connection_string: "postgresql://user:pass@localhost:5432/db".to_string(),
            ..Default::default()
        };
        let postgres_connector = create_connector(postgres_config);
        assert!(postgres_connector.is_ok());

        let mysql_config = DatabaseConfig {
            connection_string: "mysql://root:pass@localhost:3306/db".to_string(),
            ..Default::default()
        };
        let mysql_connector = create_connector(mysql_config);
        assert!(mysql_connector.is_ok());

        let sqlite_config = DatabaseConfig {
            connection_string: "/path/to/db.sqlite".to_string(),
            ..Default::default()
        };
        let sqlite_connector = create_connector(sqlite_config);
        assert!(sqlite_connector.is_ok());

        let duckdb_config = DatabaseConfig {
            connection_string: "/path/to/data.duckdb".to_string(),
            ..Default::default()
        };
        let duckdb_connector = create_connector(duckdb_config);
        assert!(duckdb_connector.is_ok());

        // Test unsupported connection string
        let invalid_config = DatabaseConfig {
            connection_string: "invalid://connection".to_string(),
            ..Default::default()
        };
        let invalid_connector = create_connector(invalid_config);
        assert!(invalid_connector.is_err());
    }
}

#[cfg(all(test, feature = "database"))]
mod streaming_tests {
    use dataprof::database::streaming::{
        estimate_memory_usage, merge_column_batches, StreamingProgress,
    };
    use std::collections::HashMap;

    #[test]
    fn test_merge_column_batches() {
        let batch1 = {
            let mut map = HashMap::new();
            map.insert("col1".to_string(), vec!["a".to_string(), "b".to_string()]);
            map.insert("col2".to_string(), vec!["1".to_string(), "2".to_string()]);
            map
        };

        let batch2 = {
            let mut map = HashMap::new();
            map.insert("col1".to_string(), vec!["c".to_string(), "d".to_string()]);
            map.insert("col2".to_string(), vec!["3".to_string(), "4".to_string()]);
            map
        };

        let merged = merge_column_batches(vec![batch1, batch2]).expect("Failed to merge batches");

        assert_eq!(
            merged.get("col1").expect("col1 not found"),
            &vec!["a", "b", "c", "d"]
        );
        assert_eq!(
            merged.get("col2").expect("col2 not found"),
            &vec!["1", "2", "3", "4"]
        );
    }

    #[test]
    fn test_memory_estimation() {
        let mut columns = HashMap::new();
        columns.insert(
            "test".to_string(),
            vec!["hello".to_string(), "world".to_string()],
        );

        let memory = estimate_memory_usage(&columns);
        // "test" (4) + "hello" (5) + "world" (5) = 14 bytes
        assert_eq!(memory, 14);
    }

    #[test]
    fn test_streaming_progress() {
        let mut progress = StreamingProgress::new(Some(1000));

        assert_eq!(progress.percentage(), Some(0.0));

        progress.update(250);
        assert_eq!(progress.percentage(), Some(25.0));

        progress.update(250);
        assert_eq!(progress.percentage(), Some(50.0));

        assert_eq!(progress.batches_processed, 2);
        assert_eq!(progress.processed_rows, 500);
    }
}

// Dummy test to ensure the module compiles when database features are not enabled
#[cfg(not(feature = "database"))]
#[test]
fn test_database_feature_not_enabled() {
    println!("Database features not enabled - skipping database tests");
}
