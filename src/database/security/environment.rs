//! Environment configuration utilities for database security

use super::ssl_config::SslConfig;
use std::env;

/// Load SSL configuration from environment variables
pub fn load_ssl_config_from_environment(database_type: &str) -> SslConfig {
    let prefix = match database_type {
        "postgresql" => "POSTGRES",
        "mysql" => "MYSQL",
        _ => "DATABASE",
    };

    let mut ssl_config = SslConfig::default();

    // Load SSL mode
    if let Ok(ssl_mode) =
        env::var(format!("{}_SSL_MODE", prefix)).or_else(|_| env::var("DATABASE_SSL_MODE"))
    {
        ssl_config.ssl_mode = Some(ssl_mode);
        ssl_config.require_ssl = true;
    }

    // Load certificate paths
    ssl_config.ca_cert_path = env::var(format!("{}_SSL_CA", prefix))
        .or_else(|_| env::var("DATABASE_SSL_CA"))
        .ok();

    ssl_config.client_cert_path = env::var(format!("{}_SSL_CERT", prefix))
        .or_else(|_| env::var("DATABASE_SSL_CERT"))
        .ok();

    ssl_config.client_key_path = env::var(format!("{}_SSL_KEY", prefix))
        .or_else(|_| env::var("DATABASE_SSL_KEY"))
        .ok();

    // Load SSL verification setting
    if let Ok(verify_str) =
        env::var(format!("{}_SSL_VERIFY", prefix)).or_else(|_| env::var("DATABASE_SSL_VERIFY"))
    {
        ssl_config.verify_server_cert = verify_str.parse().unwrap_or(true);
    }

    // Auto-detect production environment
    if env::var("ENVIRONMENT").unwrap_or_default() == "production"
        || env::var("NODE_ENV").unwrap_or_default() == "production"
    {
        ssl_config.require_ssl = true;
        ssl_config.verify_server_cert = true;
    }

    ssl_config
}
