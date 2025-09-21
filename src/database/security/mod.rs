//! Database security utilities for SSL/TLS validation and credential management

pub mod connection_security;
pub mod credentials;
pub mod environment;
pub mod sql_validation;
pub mod ssl_config;
pub mod utils;

// Re-export main types and functions for convenience
pub use connection_security::{load_secure_database_config, validate_connection_security};
pub use credentials::DatabaseCredentials;
pub use environment::load_ssl_config_from_environment;
pub use sql_validation::{validate_base_query, validate_sql_identifier};
pub use ssl_config::SslConfig;
pub use utils::{add_query_param, sanitize_error_message};
