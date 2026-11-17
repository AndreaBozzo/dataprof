//! Database connection security validation and configuration

use super::{
    credentials::DatabaseCredentials, environment::load_ssl_config_from_environment,
    ssl_config::SslConfig,
};
use crate::DataProfilerError;
use std::collections::HashMap;

/// Validate database connection security
pub fn validate_connection_security(
    connection_string: &str,
    ssl_config: &SslConfig,
    database_type: &str,
) -> Result<Vec<String>, DataProfilerError> {
    let mut warnings = Vec::new();

    if let Ok(conn_info) = crate::connection::ConnectionInfo::parse(connection_string) {
        if conn_info.password.is_some() {
            warnings.push(
                "Password embedded in connection string. Consider using environment variables."
                    .to_string(),
            );
        }

        if database_type != "sqlite" {
            let has_ssl_params = ["sslmode", "tls", "ssl"]
                .iter()
                .any(|key| conn_info.query_param(key).is_some());

            if !has_ssl_params && !ssl_config.require_ssl {
                warnings.push(
                    "No SSL/TLS configuration detected. Database traffic may be unencrypted."
                        .to_string(),
                );
            }
        }

        if let Some(host) = &conn_info.host
            && (host == "localhost" || host == "127.0.0.1" || host == "::1")
        {
            warnings.push(
                "Connecting to localhost. Ensure this is intentional in production.".to_string(),
            );
        }

        let default_ports = HashMap::from([("postgresql", 5432), ("mysql", 3306)]);

        if let Some(default_port) = default_ports.get(database_type)
            && conn_info.port == Some(*default_port)
        {
            warnings.push(format!(
                "Using default port for {}. Consider using a non-standard port for security.",
                database_type
            ));
        }
    }

    if let Err(e) = ssl_config.validate() {
        warnings.push(format!("SSL configuration issue: {}", e));
    }

    Ok(warnings)
}

/// Load database configuration from environment with security best practices
pub fn load_secure_database_config(
    database_type: &str,
) -> Result<(String, SslConfig), DataProfilerError> {
    load_secure_database_config_with_ssl(database_type, None)
}

/// The query pairs of the URL-valued variables, `{PREFIX}_URL` then
/// `DATABASE_URL`. The driver applies the last of two settings, and the
/// credentials take `DATABASE_URL` first, so it comes last here too.
fn environment_url_query(database_type: &str) -> Vec<(String, String)> {
    let prefix = match database_type {
        "postgresql" => "POSTGRES",
        "mysql" => "MYSQL",
        _ => return Vec::new(),
    };
    [format!("{prefix}_URL"), "DATABASE_URL".to_string()]
        .iter()
        .filter_map(|name| std::env::var(name).ok())
        .filter_map(|url| crate::connection::ConnectionInfo::parse(&url).ok())
        .filter(|info| info.database_type() == database_type)
        .flat_map(|info| info.query_params)
        .collect()
}

/// [`load_secure_database_config`], with the caller's TLS configuration in
/// place of the SSL environment variables when `ssl_config` is given.
///
/// The TLS settings written in `{PREFIX}_URL` or `DATABASE_URL` are kept, and
/// as in an explicit URL they win over either configuration. Building the URL
/// from the credentials alone dropped them, and a verifying `sslmode` there
/// connected with `prefer`.
pub fn load_secure_database_config_with_ssl(
    database_type: &str,
    ssl_config: Option<SslConfig>,
) -> Result<(String, SslConfig), DataProfilerError> {
    let credentials = DatabaseCredentials::from_environment(database_type);

    // The credentials hold literal values, so the base URL is serialized with
    // each part encoded: a database name `app?sslmode=disable` interpolated
    // as text became a query pair that overrode the TLS configuration.
    let base_url = |scheme: &str, port: u16, database: &str| {
        crate::connection::ConnectionInfo {
            scheme: scheme.to_string(),
            host: Some(
                credentials
                    .host
                    .clone()
                    .unwrap_or_else(|| "localhost".to_string()),
            ),
            port: Some(credentials.port.unwrap_or(port)),
            username: None,
            password: None,
            database: Some(
                credentials
                    .database
                    .clone()
                    .unwrap_or_else(|| database.to_string()),
            ),
            path: None,
            query_params: environment_url_query(database_type),
        }
        .to_original_string()
    };
    let base_connection_string = match database_type {
        "postgresql" => base_url("postgresql", 5432, "postgres"),
        "mysql" => base_url("mysql", 3306, "mysql"),
        "sqlite" => credentials
            .database
            .clone()
            .unwrap_or_else(|| ":memory:".to_string()),
        _ => {
            return Err(DataProfilerError::database_config(&format!(
                "Unsupported database type: {}",
                database_type
            )));
        }
    };

    let connection_string = credentials.apply_to_connection_string(&base_connection_string);
    let ssl_config = ssl_config.unwrap_or_else(|| load_ssl_config_from_environment(database_type));
    let secure_connection_string =
        ssl_config.apply_to_connection_string(connection_string, database_type);

    credentials.validate(database_type)?;

    let warnings =
        validate_connection_security(&secure_connection_string, &ssl_config, database_type)?;
    for warning in warnings {
        log::warn!("SECURITY WARNING: {}", warning);
    }

    Ok((secure_connection_string, ssl_config))
}

#[cfg(test)]
mod tests {
    use super::*;

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
