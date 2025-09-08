//! Connection management and utilities for database connectors

use anyhow::Result;
use url::Url;

/// Parse database connection string and extract components
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub scheme: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub path: Option<String>,
    pub query_params: std::collections::HashMap<String, String>,
}

impl ConnectionInfo {
    /// Parse a connection string into its components
    pub fn parse(connection_string: &str) -> Result<Self> {
        // Handle file paths (for SQLite/DuckDB)
        if !connection_string.contains("://") {
            return Ok(ConnectionInfo {
                scheme: "file".to_string(),
                host: None,
                port: None,
                username: None,
                password: None,
                database: None,
                path: Some(connection_string.to_string()),
                query_params: std::collections::HashMap::new(),
            });
        }

        let url = Url::parse(connection_string)
            .map_err(|e| anyhow::anyhow!("Invalid connection string: {}", e))?;

        let mut query_params = std::collections::HashMap::new();
        for (key, value) in url.query_pairs() {
            query_params.insert(key.to_string(), value.to_string());
        }

        Ok(ConnectionInfo {
            scheme: url.scheme().to_string(),
            host: url.host_str().map(|s| s.to_string()),
            port: url.port(),
            username: if url.username().is_empty() {
                None
            } else {
                Some(url.username().to_string())
            },
            password: url.password().map(|s| s.to_string()),
            database: if url.path().len() > 1 {
                Some(url.path().trim_start_matches('/').to_string())
            } else {
                None
            },
            path: if url.scheme() == "file" {
                Some(url.path().to_string())
            } else {
                None
            },
            query_params,
        })
    }

    /// Get the database type from the scheme
    pub fn database_type(&self) -> &str {
        match self.scheme.as_str() {
            "postgresql" | "postgres" => "postgresql",
            "mysql" => "mysql",
            "sqlite" => "sqlite",
            "file" => {
                if let Some(path) = &self.path {
                    if path.ends_with(".duckdb") {
                        "duckdb"
                    } else {
                        "sqlite"
                    }
                } else {
                    "sqlite"
                }
            }
            _ => "unknown",
        }
    }

    /// Build a connection string for specific database libraries
    pub fn to_connection_string(&self, target_format: &str) -> String {
        match target_format {
            "sqlx" => match self.scheme.as_str() {
                "postgresql" | "postgres" => {
                    let mut parts = vec![format!("{}://", self.scheme)];

                    if let (Some(user), Some(pass)) = (&self.username, &self.password) {
                        parts.push(format!("{}:{}@", user, pass));
                    } else if let Some(user) = &self.username {
                        parts.push(format!("{}@", user));
                    }

                    if let Some(host) = &self.host {
                        parts.push(host.clone());
                        if let Some(port) = self.port {
                            parts.push(format!(":{}", port));
                        }
                    }

                    if let Some(db) = &self.database {
                        parts.push(format!("/{}", db));
                    }

                    parts.join("")
                }
                "mysql" => {
                    let mut parts = vec!["mysql://".to_string()];

                    if let (Some(user), Some(pass)) = (&self.username, &self.password) {
                        parts.push(format!("{}:{}@", user, pass));
                    } else if let Some(user) = &self.username {
                        parts.push(format!("{}@", user));
                    }

                    if let Some(host) = &self.host {
                        parts.push(host.clone());
                        if let Some(port) = self.port {
                            parts.push(format!(":{}", port));
                        }
                    }

                    if let Some(db) = &self.database {
                        parts.push(format!("/{}", db));
                    }

                    parts.join("")
                }
                "sqlite" | "file" => {
                    if let Some(path) = &self.path {
                        format!("sqlite://{}", path)
                    } else {
                        "sqlite://memory:".to_string()
                    }
                }
                _ => self.to_original_string(),
            },
            _ => self.to_original_string(),
        }
    }

    /// Reconstruct the original connection string
    pub fn to_original_string(&self) -> String {
        if let Some(path) = &self.path {
            return path.clone();
        }

        let mut parts = vec![format!("{}://", self.scheme)];

        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            parts.push(format!("{}:{}@", user, pass));
        } else if let Some(user) = &self.username {
            parts.push(format!("{}@", user));
        }

        if let Some(host) = &self.host {
            parts.push(host.clone());
            if let Some(port) = self.port {
                parts.push(format!(":{}", port));
            }
        }

        if let Some(db) = &self.database {
            parts.push(format!("/{}", db));
        }

        if !self.query_params.is_empty() {
            let query: Vec<String> = self
                .query_params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            parts.push(format!("?{}", query.join("&")));
        }

        parts.join("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_postgresql_connection() {
        let conn_str = "postgresql://user:pass@localhost:5432/mydb";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.scheme, "postgresql");
        assert_eq!(info.host, Some("localhost".to_string()));
        assert_eq!(info.port, Some(5432));
        assert_eq!(info.username, Some("user".to_string()));
        assert_eq!(info.password, Some("pass".to_string()));
        assert_eq!(info.database, Some("mydb".to_string()));
        assert_eq!(info.database_type(), "postgresql");
    }

    #[test]
    fn test_parse_mysql_connection() {
        let conn_str = "mysql://root:password@127.0.0.1:3306/testdb";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.scheme, "mysql");
        assert_eq!(info.host, Some("127.0.0.1".to_string()));
        assert_eq!(info.port, Some(3306));
        assert_eq!(info.username, Some("root".to_string()));
        assert_eq!(info.password, Some("password".to_string()));
        assert_eq!(info.database, Some("testdb".to_string()));
        assert_eq!(info.database_type(), "mysql");
    }

    #[test]
    fn test_parse_sqlite_connection() {
        let conn_str = "sqlite:///path/to/db.sqlite";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.scheme, "sqlite");
        assert_eq!(info.database_type(), "sqlite");
    }

    #[test]
    fn test_parse_file_path() {
        let conn_str = "/path/to/database.db";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.scheme, "file");
        assert_eq!(info.path, Some("/path/to/database.db".to_string()));
        assert_eq!(info.database_type(), "sqlite");
    }

    #[test]
    fn test_parse_duckdb_file() {
        let conn_str = "/path/to/data.duckdb";
        let info = ConnectionInfo::parse(conn_str).expect("Failed to parse connection string");

        assert_eq!(info.scheme, "file");
        assert_eq!(info.path, Some("/path/to/data.duckdb".to_string()));
        assert_eq!(info.database_type(), "duckdb");
    }
}
