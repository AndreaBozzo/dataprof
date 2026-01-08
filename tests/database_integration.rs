//! Integration tests for database connectors
//!
//! These tests verify that the database connectors compile and work correctly.
//! They use in-memory databases where possible to avoid requiring external setup.
//!
//! ## Heavy Memory Tests
//! Some tests are marked with `#[ignore]` due to high memory consumption (>8GB RAM).
//! To run these tests separately: `cargo test --test database_integration -- --ignored`
//! For heavy integration tests, see `tests/database_heavy.rs`

#[cfg(feature = "database")]
use anyhow::Result;

#[cfg(feature = "database")]
use dataprof::{DatabaseConfig, create_connector};

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
            retry_config: Some(dataprof::database::RetryConfig::default()),
            sampling_config: None,
            ssl_config: Some(dataprof::database::SslConfig::default()),
            load_credentials_from_env: false,
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
        StreamingProgress, estimate_memory_usage, merge_column_batches,
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

/// Tests for new enhanced database features
#[cfg(all(test, feature = "database"))]
mod enhanced_features_tests {
    use super::*;
    use dataprof::database::{SamplingConfig, SamplingStrategy, SslConfig};

    #[test]
    fn test_database_config_defaults() {
        let config = DatabaseConfig::default();

        assert!(config.load_credentials_from_env);
        assert!(config.retry_config.is_some());
        assert!(config.ssl_config.is_some());
        assert_eq!(config.batch_size, 10000);
    }

    #[test]
    fn test_sampling_config_creation() {
        let quick_sample = SamplingConfig::quick_sample(5000);
        assert_eq!(quick_sample.sample_size, 5000);
        assert!(matches!(quick_sample.strategy, SamplingStrategy::Random));
        assert!(quick_sample.seed.is_some());

        let representative_sample =
            SamplingConfig::representative_sample(3000, Some("category".to_string()));
        assert_eq!(representative_sample.sample_size, 3000);
        assert!(matches!(
            representative_sample.strategy,
            SamplingStrategy::Stratified
        ));
        assert_eq!(
            representative_sample.stratify_column,
            Some("category".to_string())
        );
    }

    #[test]
    fn test_ssl_config_validation() {
        let ssl_config = SslConfig::default();
        assert!(ssl_config.validate().is_ok());

        let production_ssl = SslConfig::production();
        assert!(production_ssl.require_ssl);
        assert!(production_ssl.verify_server_cert);

        let dev_ssl = SslConfig::development();
        assert!(!dev_ssl.require_ssl);
        assert!(!dev_ssl.verify_server_cert);
    }

    #[test]
    fn test_sampling_query_generation() {
        let sampling_config = SamplingConfig::quick_sample(1000);

        // Test with table name
        let query = sampling_config
            .generate_sample_query("users", 10000)
            .unwrap();
        assert!(query.contains("RANDOM"));
        assert!(query.contains("LIMIT 1000"));

        // Test with complex query
        let complex_query = "SELECT u.*, p.name FROM users u JOIN profiles p ON u.id = p.user_id";
        let sampled_query = sampling_config
            .generate_sample_query(complex_query, 50000)
            .unwrap();
        assert!(sampled_query.contains("sample_subquery"));
        assert!(sampled_query.contains("LIMIT 1000"));
    }

    #[test]
    fn test_ssl_config_apply_to_connection_string() {
        let ssl_config = SslConfig {
            require_ssl: true,
            ssl_mode: Some("require".to_string()),
            ..Default::default()
        };

        let connection_string = "postgresql://user@localhost/db".to_string();
        let secure_string = ssl_config.apply_to_connection_string(connection_string, "postgresql");

        assert!(secure_string.contains("sslmode=require"));
    }

    #[test]
    fn test_sample_info_calculations() {
        let sample_info =
            dataprof::database::SampleInfo::new(10000, 1000, SamplingStrategy::Random);

        assert_eq!(sample_info.total_rows, 10000);
        assert_eq!(sample_info.sampled_rows, 1000);
        assert_eq!(sample_info.sampling_ratio, 0.1);
        assert!(sample_info.is_representative);

        // Test with smaller sample that should generate recommendations
        let small_sample_info =
            dataprof::database::SampleInfo::new(100000, 100, SamplingStrategy::Random);

        let recommendations = small_sample_info.get_recommendations();
        assert!(!recommendations.is_empty());
    }

    #[test]
    fn test_connection_string_security_validation() {
        use dataprof::database::validate_connection_security;

        let insecure_conn = "postgresql://user:password@localhost:5432/db";
        let ssl_config = SslConfig::default();

        let warnings =
            validate_connection_security(insecure_conn, &ssl_config, "postgresql").unwrap();
        assert!(!warnings.is_empty());
        assert!(warnings.iter().any(|w| w.contains("Password embedded")));
        assert!(warnings.iter().any(|w| w.contains("localhost")));
    }
}

// Dummy test to ensure the module compiles when database features are not enabled
#[cfg(not(feature = "database"))]
#[test]
fn test_database_feature_not_enabled() {
    println!("Database features not enabled - skipping database tests");
}
