//! SSL/TLS configuration for secure database connections

use super::utils::add_query_param;
use anyhow::Result;
use serde::{Deserialize, Serialize};
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
}
