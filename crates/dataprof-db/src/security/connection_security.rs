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
    let credentials = DatabaseCredentials::from_environment(database_type);

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
            return Err(DataProfilerError::database_config(&format!(
                "Unsupported database type: {}",
                database_type
            )));
        }
    };

    let connection_string = credentials.apply_to_connection_string(&base_connection_string);
    let ssl_config = load_ssl_config_from_environment(database_type);
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
