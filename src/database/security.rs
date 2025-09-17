//! Database security utilities for SSL/TLS validation and credential management

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::Path;

/// SSL/TLS configuration for database connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SslConfig {
    /// Whether to require SSL/TLS
    pub require_ssl: bool,
    /// Path to CA certificate file
    pub ca_cert_path: Option<String>,
    /// Path to client certificate file
    pub client_cert_path: Option<String>,
    /// Path to client private key file
    pub client_key_path: Option<String>,
    /// Whether to verify the server certificate
    pub verify_server_cert: bool,
    /// SSL mode (for PostgreSQL: disable, allow, prefer, require, verify-ca, verify-full)
    pub ssl_mode: Option<String>,
}

impl Default for SslConfig {
    fn default() -> Self {
        Self {
            require_ssl: false,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            verify_server_cert: true,
            ssl_mode: Some("prefer".to_string()),
        }
    }
}

impl SslConfig {
    /// Create SSL config for production environments (strict security)
    pub fn production() -> Self {
        Self {
            require_ssl: true,
            ca_cert_path: None, // Use system CA bundle
            client_cert_path: None,
            client_key_path: None,
            verify_server_cert: true,
            ssl_mode: Some("require".to_string()),
        }
    }

    /// Create SSL config for development environments (relaxed security)
    pub fn development() -> Self {
        Self {
            require_ssl: false,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            verify_server_cert: false,
            ssl_mode: Some("prefer".to_string()),
        }
    }

    /// Validate SSL configuration
    pub fn validate(&self) -> Result<()> {
        // Check if certificate files exist
        if let Some(ca_cert_path) = &self.ca_cert_path {
            if !Path::new(ca_cert_path).exists() {
                return Err(anyhow::anyhow!(
                    "CA certificate file not found: {}",
                    ca_cert_path
                ));
            }
        }

        if let Some(client_cert_path) = &self.client_cert_path {
            if !Path::new(client_cert_path).exists() {
                return Err(anyhow::anyhow!(
                    "Client certificate file not found: {}",
                    client_cert_path
                ));
            }
        }

        if let Some(client_key_path) = &self.client_key_path {
            if !Path::new(client_key_path).exists() {
                return Err(anyhow::anyhow!(
                    "Client private key file not found: {}",
                    client_key_path
                ));
            }
        }

        // Validate SSL mode
        if let Some(ssl_mode) = &self.ssl_mode {
            let valid_modes = [
                "disable",
                "allow",
                "prefer",
                "require",
                "verify-ca",
                "verify-full",
            ];
            if !valid_modes.contains(&ssl_mode.as_str()) {
                return Err(anyhow::anyhow!(
                    "Invalid SSL mode '{}'. Valid modes: {}",
                    ssl_mode,
                    valid_modes.join(", ")
                ));
            }
        }

        // Warn about security implications
        if self.require_ssl && !self.verify_server_cert {
            eprintln!("WARNING: SSL required but server certificate verification is disabled. This may be insecure.");
        }

        if !self.require_ssl {
            eprintln!("WARNING: SSL not required. Database connections may be unencrypted.");
        }

        Ok(())
    }

    /// Apply SSL configuration to connection string
    pub fn apply_to_connection_string(
        &self,
        mut connection_string: String,
        database_type: &str,
    ) -> String {
        match database_type {
            "postgresql" => {
                if let Some(ssl_mode) = &self.ssl_mode {
                    connection_string = add_query_param(connection_string, "sslmode", ssl_mode);
                }

                if let Some(ca_cert) = &self.ca_cert_path {
                    connection_string = add_query_param(connection_string, "sslrootcert", ca_cert);
                }

                if let Some(client_cert) = &self.client_cert_path {
                    connection_string = add_query_param(connection_string, "sslcert", client_cert);
                }

                if let Some(client_key) = &self.client_key_path {
                    connection_string = add_query_param(connection_string, "sslkey", client_key);
                }
            }
            "mysql" => {
                if self.require_ssl {
                    connection_string = add_query_param(connection_string, "tls", "true");
                }

                if !self.verify_server_cert {
                    connection_string =
                        add_query_param(connection_string, "tls-skip-verify", "true");
                }

                if let Some(ca_cert) = &self.ca_cert_path {
                    connection_string = add_query_param(connection_string, "tls-ca", ca_cert);
                }
            }
            "sqlite" => {
                // SQLite doesn't support SSL as it's an embedded database
                if self.require_ssl {
                    eprintln!("WARNING: SSL configuration ignored for SQLite (embedded database)");
                }
            }
            _ => {
                eprintln!(
                    "WARNING: SSL configuration for database type '{}' not implemented",
                    database_type
                );
            }
        }

        connection_string
    }
}

/// Database credentials management with environment variable support
#[derive(Debug, Clone, Default)]
pub struct DatabaseCredentials {
    /// Database username
    pub username: Option<String>,
    /// Database password
    pub password: Option<String>,
    /// Database host
    pub host: Option<String>,
    /// Database port
    pub port: Option<u16>,
    /// Database name
    pub database: Option<String>,
    /// Additional connection parameters
    pub extra_params: HashMap<String, String>,
}

impl DatabaseCredentials {
    /// Create empty credentials
    pub fn new() -> Self {
        Self::default()
    }

    /// Load credentials from environment variables
    pub fn from_environment(database_type: &str) -> Self {
        let prefix = match database_type {
            "postgresql" => "POSTGRES",
            "mysql" => "MYSQL",
            "sqlite" => "SQLITE",
            _ => "DATABASE",
        };

        let mut creds = Self::new();

        // Load standard environment variables
        creds.username = env::var(format!("{}_USER", prefix))
            .ok()
            .or_else(|| env::var(format!("{}_USERNAME", prefix)).ok())
            .or_else(|| env::var("DATABASE_USER").ok())
            .or_else(|| env::var("DB_USER").ok());

        creds.password = env::var(format!("{}_PASSWORD", prefix))
            .ok()
            .or_else(|| env::var("DATABASE_PASSWORD").ok())
            .or_else(|| env::var("DB_PASSWORD").ok());

        creds.host = env::var(format!("{}_HOST", prefix))
            .ok()
            .or_else(|| env::var("DATABASE_HOST").ok())
            .or_else(|| env::var("DB_HOST").ok());

        if let Ok(port_str) = env::var(format!("{}_PORT", prefix))
            .or_else(|_| env::var("DATABASE_PORT"))
            .or_else(|_| env::var("DB_PORT"))
        {
            creds.port = port_str.parse().ok();
        }

        creds.database = env::var(format!("{}_DATABASE", prefix))
            .ok()
            .or_else(|| env::var(format!("{}_DB", prefix)).ok())
            .or_else(|| env::var("DATABASE_NAME").ok())
            .or_else(|| env::var("DB_NAME").ok());

        // Load URL-style environment variable
        if let Ok(database_url) = env::var("DATABASE_URL") {
            if let Ok(parsed) = crate::database::connection::ConnectionInfo::parse(&database_url) {
                creds.username = creds.username.or(parsed.username);
                creds.password = creds.password.or(parsed.password);
                creds.host = creds.host.or(parsed.host);
                creds.port = creds.port.or(parsed.port);
                creds.database = creds.database.or(parsed.database);
            }
        }

        // Load database-specific URL variables
        let url_var = format!("{}_URL", prefix);
        if let Ok(url) = env::var(&url_var) {
            if let Ok(parsed) = crate::database::connection::ConnectionInfo::parse(&url) {
                creds.username = creds.username.or(parsed.username);
                creds.password = creds.password.or(parsed.password);
                creds.host = creds.host.or(parsed.host);
                creds.port = creds.port.or(parsed.port);
                creds.database = creds.database.or(parsed.database);
            }
        }

        creds
    }

    /// Apply credentials to connection string
    pub fn apply_to_connection_string(&self, connection_string: &str) -> String {
        if let Ok(mut conn_info) =
            crate::database::connection::ConnectionInfo::parse(connection_string)
        {
            // Override with environment variables if they exist
            if let Some(username) = &self.username {
                conn_info.username = Some(username.clone());
            }
            if let Some(password) = &self.password {
                conn_info.password = Some(password.clone());
            }
            if let Some(host) = &self.host {
                conn_info.host = Some(host.clone());
            }
            if let Some(port) = self.port {
                conn_info.port = Some(port);
            }
            if let Some(database) = &self.database {
                conn_info.database = Some(database.clone());
            }

            // Add extra parameters
            for (key, value) in &self.extra_params {
                conn_info.query_params.insert(key.clone(), value.clone());
            }

            conn_info.to_original_string()
        } else {
            connection_string.to_string()
        }
    }

    /// Validate that required credentials are present
    pub fn validate(&self, database_type: &str) -> Result<()> {
        match database_type {
            "postgresql" | "mysql" => {
                if self.host.is_none() {
                    return Err(anyhow::anyhow!(
                        "Database host is required for {}",
                        database_type
                    ));
                }
                if self.username.is_none() {
                    return Err(anyhow::anyhow!(
                        "Database username is required for {}",
                        database_type
                    ));
                }
                // Password might be optional for some authentication methods
            }
            "sqlite" => {
                // SQLite doesn't need network credentials
                if self.database.is_none() {
                    eprintln!("WARNING: No database file path specified for SQLite");
                }
            }
            _ => {
                eprintln!(
                    "WARNING: Credential validation for database type '{}' not implemented",
                    database_type
                );
            }
        }

        Ok(())
    }

    /// Get a masked version for logging (passwords hidden)
    pub fn to_masked_string(&self) -> String {
        format!(
            "DatabaseCredentials {{ username: {:?}, password: {}, host: {:?}, port: {:?}, database: {:?} }}",
            self.username,
            if self.password.is_some() { "***" } else { "None" },
            self.host,
            self.port,
            self.database
        )
    }
}

/// Add query parameter to connection string
fn add_query_param(connection_string: String, key: &str, value: &str) -> String {
    if connection_string.contains('?') {
        format!("{}&{}={}", connection_string, key, value)
    } else {
        format!("{}?{}={}", connection_string, key, value)
    }
}

/// Validate database connection security
pub fn validate_connection_security(
    connection_string: &str,
    ssl_config: &SslConfig,
    database_type: &str,
) -> Result<Vec<String>> {
    let mut warnings = Vec::new();

    // Parse connection string to check for security issues
    if let Ok(conn_info) = crate::database::connection::ConnectionInfo::parse(connection_string) {
        // Check for plaintext passwords in connection string
        if conn_info.password.is_some() {
            warnings.push(
                "Password embedded in connection string. Consider using environment variables."
                    .to_string(),
            );
        }

        // Check SSL configuration
        if database_type != "sqlite" {
            let has_ssl_params = conn_info.query_params.contains_key("sslmode")
                || conn_info.query_params.contains_key("tls")
                || conn_info.query_params.contains_key("ssl");

            if !has_ssl_params && !ssl_config.require_ssl {
                warnings.push(
                    "No SSL/TLS configuration detected. Database traffic may be unencrypted."
                        .to_string(),
                );
            }
        }

        // Check for development/localhost connections in production
        if let Some(host) = &conn_info.host {
            if host == "localhost" || host == "127.0.0.1" || host == "::1" {
                warnings.push(
                    "Connecting to localhost. Ensure this is intentional in production."
                        .to_string(),
                );
            }
        }

        // Check for default ports
        let default_ports = HashMap::from([("postgresql", 5432), ("mysql", 3306)]);

        if let Some(default_port) = default_ports.get(database_type) {
            if conn_info.port == Some(*default_port) {
                warnings.push(format!(
                    "Using default port for {}. Consider using a non-standard port for security.",
                    database_type
                ));
            }
        }
    }

    // Validate SSL config
    if let Err(e) = ssl_config.validate() {
        warnings.push(format!("SSL configuration issue: {}", e));
    }

    Ok(warnings)
}

/// Load database configuration from environment with security best practices
pub fn load_secure_database_config(database_type: &str) -> Result<(String, SslConfig)> {
    let credentials = DatabaseCredentials::from_environment(database_type);

    // Build base connection string
    let base_connection_string = match database_type {
        "postgresql" => {
            let host = credentials.host.as_deref().unwrap_or("localhost");
            let port = credentials.port.unwrap_or(5432);
            let database = credentials.database.as_deref().unwrap_or("postgres");
            format!("postgresql://{}:{}/{}", host, port, database)
        }
        "mysql" => {
            let host = credentials.host.as_deref().unwrap_or("localhost");
            let port = credentials.port.unwrap_or(3306);
            let database = credentials.database.as_deref().unwrap_or("mysql");
            format!("mysql://{}:{}/{}", host, port, database)
        }
        "sqlite" => credentials
            .database
            .clone()
            .unwrap_or_else(|| ":memory:".to_string()),
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported database type: {}",
                database_type
            ))
        }
    };

    // Apply credentials
    let connection_string = credentials.apply_to_connection_string(&base_connection_string);

    // Load SSL config from environment
    let ssl_config = load_ssl_config_from_environment(database_type);

    // Apply SSL to connection string
    let secure_connection_string =
        ssl_config.apply_to_connection_string(connection_string, database_type);

    // Validate credentials
    credentials.validate(database_type)?;

    // Validate security
    let warnings =
        validate_connection_security(&secure_connection_string, &ssl_config, database_type)?;
    for warning in warnings {
        eprintln!("SECURITY WARNING: {}", warning);
    }

    Ok((secure_connection_string, ssl_config))
}

/// Load SSL configuration from environment variables
fn load_ssl_config_from_environment(database_type: &str) -> SslConfig {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_config_validation() {
        let mut config = SslConfig::default();
        assert!(config.validate().is_ok());

        config.ca_cert_path = Some("/nonexistent/path".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_add_query_param() {
        let base = "postgresql://user@host/db".to_string();
        let result = add_query_param(base, "sslmode", "require");
        assert_eq!(result, "postgresql://user@host/db?sslmode=require");

        let with_existing = "postgresql://user@host/db?existing=value".to_string();
        let result = add_query_param(with_existing, "sslmode", "require");
        assert_eq!(
            result,
            "postgresql://user@host/db?existing=value&sslmode=require"
        );
    }

    #[test]
    fn test_credentials_masking() {
        let mut creds = DatabaseCredentials::new();
        creds.username = Some("testuser".to_string());
        creds.password = Some("secret123".to_string());

        let masked = creds.to_masked_string();
        assert!(masked.contains("testuser"));
        assert!(masked.contains("***"));
        assert!(!masked.contains("secret123"));
    }

    #[test]
    fn test_connection_security_validation() {
        let connection_string = "postgresql://user:pass@localhost:5432/db";
        let ssl_config = SslConfig::default();

        let warnings = validate_connection_security(connection_string, &ssl_config, "postgresql")
            .expect("Failed to validate connection security");

        assert!(!warnings.is_empty());
        assert!(warnings.iter().any(|w| w.contains("Password embedded")));
        assert!(warnings.iter().any(|w| w.contains("localhost")));
    }
}
