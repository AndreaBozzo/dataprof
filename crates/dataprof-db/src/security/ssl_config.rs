//! SSL/TLS configuration for secure database connections

use super::utils::add_query_param_if_absent;
use crate::DataProfilerError;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// PostgreSQL `sslmode` values under which a plaintext connection can happen.
const PG_TLS_OPTIONAL: [&str; 3] = ["disable", "allow", "prefer"];

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
    /// Create SSL config for production environments: TLS required, and the
    /// server certificate and host name verified (`verify-full`).
    pub fn production() -> Self {
        Self {
            require_ssl: true,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            verify_server_cert: true,
            ssl_mode: Some("verify-full".to_string()),
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
    pub fn validate(&self) -> Result<(), DataProfilerError> {
        if let Some(ca_cert_path) = &self.ca_cert_path
            && !Path::new(ca_cert_path).exists()
        {
            return Err(DataProfilerError::database_ssl(&format!(
                "CA certificate file not found: {}",
                ca_cert_path
            )));
        }

        if let Some(client_cert_path) = &self.client_cert_path
            && !Path::new(client_cert_path).exists()
        {
            return Err(DataProfilerError::database_ssl(&format!(
                "Client certificate file not found: {}",
                client_cert_path
            )));
        }

        if let Some(client_key_path) = &self.client_key_path
            && !Path::new(client_key_path).exists()
        {
            return Err(DataProfilerError::database_ssl(&format!(
                "Client private key file not found: {}",
                client_key_path
            )));
        }

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
                return Err(DataProfilerError::database_ssl(&format!(
                    "Invalid SSL mode '{}'. Valid modes: {}",
                    ssl_mode,
                    valid_modes.join(", ")
                )));
            }
        }

        if self.require_ssl && !self.verify_server_cert {
            log::warn!(
                "SSL required but server certificate verification is disabled. This may be insecure."
            );
        }

        if !self.require_ssl {
            log::warn!("SSL not required. Database connections may be unencrypted.");
        }

        Ok(())
    }

    /// The PostgreSQL `sslmode` this configuration asks for.
    ///
    /// An explicit mode is kept as given, except that `require_ssl` raises a
    /// mode that leaves TLS optional (`disable`, `allow`, `prefer`, or none):
    /// to `verify-full` when `verify_server_cert` is set, else to `require`.
    /// sqlx verifies the server certificate only under `verify-ca` and
    /// `verify-full`.
    fn postgres_ssl_mode(&self) -> Option<String> {
        let requested = self.ssl_mode.as_deref();
        let tls_optional = requested.is_none_or(|mode| PG_TLS_OPTIONAL.contains(&mode));
        if self.require_ssl && tls_optional {
            let mode = if self.verify_server_cert {
                "verify-full"
            } else {
                "require"
            };
            return Some(mode.to_string());
        }
        requested.map(str::to_string)
    }

    /// The MySQL `ssl-mode` this configuration asks for, under the same rule
    /// as [`Self::postgres_ssl_mode`]. `ssl_mode` uses the PostgreSQL names,
    /// which map onto MySQL's; any other value is passed through for sqlx to
    /// reject.
    fn mysql_ssl_mode(&self) -> Option<String> {
        let requested = self.ssl_mode.as_deref().map(|mode| match mode {
            "disable" => "DISABLED",
            "allow" | "prefer" => "PREFERRED",
            "require" => "REQUIRED",
            "verify-ca" => "VERIFY_CA",
            "verify-full" => "VERIFY_IDENTITY",
            other => other,
        });
        let tls_optional = matches!(requested, None | Some("DISABLED" | "PREFERRED"));
        if self.require_ssl && tls_optional {
            let mode = if self.verify_server_cert {
                "VERIFY_IDENTITY"
            } else {
                "REQUIRED"
            };
            return Some(mode.to_string());
        }
        requested.map(str::to_string)
    }

    /// Apply SSL configuration to connection string.
    ///
    /// A TLS setting already present in the connection string wins over the
    /// configuration: sqlx reads query pairs in order, so appending after it
    /// would silently replace the caller's choice.
    pub fn apply_to_connection_string(
        &self,
        mut connection_string: String,
        database_type: &str,
    ) -> String {
        match database_type {
            "postgresql" => {
                if let Some(ssl_mode) = self.postgres_ssl_mode() {
                    connection_string = add_query_param_if_absent(
                        connection_string,
                        &["sslmode", "ssl-mode"],
                        &ssl_mode,
                    );
                }

                if let Some(ca_cert) = &self.ca_cert_path {
                    connection_string = add_query_param_if_absent(
                        connection_string,
                        &["sslrootcert", "ssl-root-cert", "ssl-ca"],
                        ca_cert,
                    );
                }

                if let Some(client_cert) = &self.client_cert_path {
                    connection_string = add_query_param_if_absent(
                        connection_string,
                        &["sslcert", "ssl-cert"],
                        client_cert,
                    );
                }

                if let Some(client_key) = &self.client_key_path {
                    connection_string = add_query_param_if_absent(
                        connection_string,
                        &["sslkey", "ssl-key"],
                        client_key,
                    );
                }
            }
            "mysql" => {
                if let Some(ssl_mode) = self.mysql_ssl_mode() {
                    connection_string = add_query_param_if_absent(
                        connection_string,
                        &["ssl-mode", "sslmode"],
                        &ssl_mode,
                    );
                }

                if let Some(ca_cert) = &self.ca_cert_path {
                    connection_string =
                        add_query_param_if_absent(connection_string, &["ssl-ca", "sslca"], ca_cert);
                }

                if let Some(client_cert) = &self.client_cert_path {
                    connection_string = add_query_param_if_absent(
                        connection_string,
                        &["ssl-cert", "sslcert"],
                        client_cert,
                    );
                }

                if let Some(client_key) = &self.client_key_path {
                    connection_string = add_query_param_if_absent(
                        connection_string,
                        &["ssl-key", "sslkey"],
                        client_key,
                    );
                }
            }
            "sqlite" => {
                if self.require_ssl {
                    log::warn!("SSL configuration ignored for SQLite (embedded database)");
                }
            }
            _ => {
                log::warn!(
                    "SSL configuration validation for '{}' not fully implemented",
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

    /// The modes below are read back through sqlx's own URL parser, so they
    /// are the modes the driver connects with, not just the text appended.
    #[cfg(feature = "postgres")]
    mod postgres {
        use super::super::SslConfig;
        use sqlx::postgres::{PgConnectOptions, PgSslMode};
        use std::str::FromStr;

        fn mode(config: &SslConfig, url: &str) -> PgSslMode {
            let url = config.apply_to_connection_string(url.to_string(), "postgresql");
            PgConnectOptions::from_str(&url).unwrap().get_ssl_mode()
        }

        #[test]
        fn production_verifies_the_server_certificate() {
            let got = mode(&SslConfig::production(), "postgresql://u@h/db");
            assert!(matches!(got, PgSslMode::VerifyFull), "{got:?}");
        }

        #[test]
        fn a_mode_in_the_url_is_not_overridden_by_the_default_config() {
            let got = mode(
                &SslConfig::default(),
                "postgresql://u@h/db?sslmode=verify-full",
            );
            assert!(matches!(got, PgSslMode::VerifyFull), "{got:?}");
        }

        #[test]
        fn require_ssl_raises_an_optional_mode() {
            // ENVIRONMENT=production sets these two flags over the default
            // `prefer` mode.
            let config = SslConfig {
                require_ssl: true,
                verify_server_cert: true,
                ..SslConfig::default()
            };
            let got = mode(&config, "postgresql://u@h/db");
            assert!(matches!(got, PgSslMode::VerifyFull), "{got:?}");

            let config = SslConfig {
                require_ssl: true,
                verify_server_cert: false,
                ..SslConfig::default()
            };
            let got = mode(&config, "postgresql://u@h/db");
            assert!(matches!(got, PgSslMode::Require), "{got:?}");
        }

        #[test]
        fn an_explicit_tls_mode_is_kept() {
            let config = SslConfig {
                require_ssl: true,
                verify_server_cert: true,
                ssl_mode: Some("require".to_string()),
                ..SslConfig::default()
            };
            let got = mode(&config, "postgresql://u@h/db");
            assert!(matches!(got, PgSslMode::Require), "{got:?}");
        }
    }

    #[cfg(feature = "mysql")]
    mod mysql {
        use super::super::SslConfig;
        use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
        use std::str::FromStr;

        fn mode(config: &SslConfig, url: &str) -> MySqlSslMode {
            let url = config.apply_to_connection_string(url.to_string(), "mysql");
            MySqlConnectOptions::from_str(&url).unwrap().get_ssl_mode()
        }

        #[test]
        fn production_verifies_the_server_identity() {
            let got = mode(&SslConfig::production(), "mysql://u@h/db");
            assert!(matches!(got, MySqlSslMode::VerifyIdentity), "{got:?}");
        }

        #[test]
        fn require_ssl_without_verification_requires_tls() {
            let config = SslConfig {
                require_ssl: true,
                verify_server_cert: false,
                ..SslConfig::default()
            };
            let got = mode(&config, "mysql://u@h/db");
            assert!(matches!(got, MySqlSslMode::Required), "{got:?}");
        }

        #[test]
        fn a_mode_in_the_url_is_not_overridden() {
            let got = mode(&SslConfig::production(), "mysql://u@h/db?ssl-mode=DISABLED");
            assert!(matches!(got, MySqlSslMode::Disabled), "{got:?}");
        }
    }
}
