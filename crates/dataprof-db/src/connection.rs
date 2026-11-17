//! Connection management and utilities for database connectors

use crate::DataProfilerError;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use url::Url;

/// Characters a user, password or database name keeps unescaped in a URL:
/// RFC 3986's unreserved set. Everything else is percent-encoded, `%`
/// included, so the driver decodes back the literal value.
const URL_COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// The host keeps the brackets and colons of an IPv6 literal. Any other
/// delimiter is encoded, so a host from the environment such as
/// `h/db?sslmode=disable` cannot add a path or query; it fails to resolve.
const URL_HOST: &AsciiSet = &URL_COMPONENT.remove(b'[').remove(b']').remove(b':');

/// Parse database connection string and extract components.
///
/// `username`, `password`, `database` and `query_params` hold decoded, literal
/// values; the URLs built from them encode each one again.
#[derive(Clone)]
pub struct ConnectionInfo {
    pub scheme: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub path: Option<String>,
    /// Query pairs in the order written. The driver applies them in order, so
    /// a later pair overrides an earlier one for the same setting (`sslmode`
    /// and `ssl-mode` included): the order is part of the meaning.
    pub query_params: Vec<(String, String)>,
}

impl std::fmt::Debug for ConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted_password: Option<&'static str> = match self.password {
            Some(_) => Some("<REDACTED>"),
            None => None,
        };
        f.debug_struct("ConnectionInfo")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &redacted_password)
            .field("database", &self.database)
            .field("path", &self.path)
            .field("query_params", &self.query_params)
            .finish()
    }
}

fn decode_component(component: &str) -> Result<String, DataProfilerError> {
    percent_decode_str(component)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|e| DataProfilerError::DatabaseConfigError {
            message: format!("Invalid connection string: {}", e),
        })
}

impl ConnectionInfo {
    /// Parse a connection string into its components
    pub fn parse(connection_string: &str) -> Result<Self, DataProfilerError> {
        if !connection_string.contains("://") {
            return Ok(ConnectionInfo {
                scheme: "file".to_string(),
                host: None,
                port: None,
                username: None,
                password: None,
                database: None,
                path: Some(connection_string.to_string()),
                query_params: Vec::new(),
            });
        }

        let url =
            Url::parse(connection_string).map_err(|e| DataProfilerError::DatabaseConfigError {
                message: format!("Invalid connection string: {}", e),
            })?;

        let query_params = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();

        Ok(ConnectionInfo {
            scheme: url.scheme().to_string(),
            host: url.host_str().map(|s| s.to_string()),
            port: url.port(),
            username: if url.username().is_empty() {
                None
            } else {
                Some(decode_component(url.username())?)
            },
            password: url.password().map(decode_component).transpose()?,
            database: if url.path().len() > 1 {
                Some(decode_component(url.path().trim_start_matches('/'))?)
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

    /// The value the driver applies for `key`: the last pair with that name.
    pub fn query_param(&self, key: &str) -> Option<&str> {
        self.query_params
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    /// Set `key` to `value`, replacing every pair already named `key`.
    pub fn set_query_param(&mut self, key: &str, value: &str) {
        self.query_params.retain(|(name, _)| name != key);
        self.query_params.push((key.to_string(), value.to_string()));
    }

    /// Get the database type from the scheme
    pub fn database_type(&self) -> &str {
        match self.scheme.as_str() {
            "postgresql" | "postgres" => "postgresql",
            "mysql" => "mysql",
            "sqlite" | "file" => "sqlite",
            _ => "unknown",
        }
    }

    /// Build a connection string for specific database libraries
    pub fn to_connection_string(&self, target_format: &str) -> String {
        match (target_format, self.scheme.as_str()) {
            ("sqlx", "sqlite" | "file") => {
                if let Some(path) = &self.path {
                    format!("sqlite://{}", path)
                } else {
                    "sqlite://memory:".to_string()
                }
            }
            _ => self.to_original_string(),
        }
    }

    /// The URL with every component percent-encoded and the query pairs in
    /// their original order. The query carries the TLS settings (`sslmode`,
    /// `ssl-mode`, certificate paths), so a URL handed to a driver without
    /// it, or with its pairs reordered, can connect unverified.
    fn to_url(&self) -> String {
        let mut url = format!("{}://", self.scheme);

        if let Some(user) = &self.username {
            url.push_str(&utf8_percent_encode(user, URL_COMPONENT).to_string());
            if let Some(pass) = &self.password {
                url.push(':');
                url.push_str(&utf8_percent_encode(pass, URL_COMPONENT).to_string());
            }
            url.push('@');
        }

        if let Some(host) = &self.host {
            url.push_str(&utf8_percent_encode(host, URL_HOST).to_string());
            if let Some(port) = self.port {
                url.push_str(&format!(":{}", port));
            }
        }

        if let Some(db) = &self.database {
            url.push('/');
            url.push_str(&utf8_percent_encode(db, URL_COMPONENT).to_string());
        }

        if !self.query_params.is_empty() {
            let query = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(&self.query_params)
                .finish();
            url.push('?');
            url.push_str(&query);
        }

        url
    }

    /// Reconstruct the original connection string
    pub fn to_original_string(&self) -> String {
        if let Some(path) = &self.path {
            return path.clone();
        }
        self.to_url()
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

    /// The query carries the TLS settings, so the URL built for sqlx keeps
    /// it: dropping it connected without verifying the server.
    #[test]
    fn the_sqlx_url_keeps_the_tls_settings() {
        let postgres = ConnectionInfo::parse(
            "postgresql://u:p@db.example:5432/app?sslmode=verify-full&sslrootcert=/certs/my%20ca.crt",
        )
        .unwrap()
        .to_connection_string("sqlx");
        assert_eq!(
            postgres,
            "postgresql://u:p@db.example:5432/app?sslmode=verify-full&sslrootcert=%2Fcerts%2Fmy+ca.crt"
        );
        let reparsed = ConnectionInfo::parse(&postgres).unwrap();
        assert_eq!(
            reparsed.query_param("sslrootcert").unwrap(),
            "/certs/my ca.crt"
        );

        let mysql =
            ConnectionInfo::parse("mysql://u:p@db.example:3306/app?ssl-mode=VERIFY_IDENTITY")
                .unwrap()
                .to_connection_string("sqlx");
        assert_eq!(
            mysql,
            "mysql://u:p@db.example:3306/app?ssl-mode=VERIFY_IDENTITY"
        );
    }

    /// sqlx applies query pairs in order, so the later alias decides the TLS
    /// mode. Serializing must not reorder them: sorted, or in hash order, the
    /// disabling mode could land last.
    #[test]
    fn the_sqlx_url_keeps_the_query_order() {
        for url in [
            "postgresql://u@h:5432/db?sslmode=disable&ssl-mode=verify-full",
            "mysql://u@h:3306/db?sslmode=DISABLED&ssl-mode=VERIFY_IDENTITY",
        ] {
            let info = ConnectionInfo::parse(url).unwrap();
            assert_eq!(info.to_connection_string("sqlx"), url);
            assert_eq!(info.to_original_string(), url);
        }
    }

    /// Credentials are held decoded and written back encoded, so a password
    /// with URL delimiters in it reaches the driver intact.
    #[test]
    fn credentials_round_trip_through_the_url() {
        let url = "postgresql://user%40corp:p%40%3A%2F%20%25@h:5432/my%20db";
        let info = ConnectionInfo::parse(url).unwrap();
        assert_eq!(info.username.as_deref(), Some("user@corp"));
        assert_eq!(info.password.as_deref(), Some("p@:/ %"));
        assert_eq!(info.database.as_deref(), Some("my db"));
        assert_eq!(info.to_original_string(), url);
    }

    /// A database name or host holding URL delimiters stays one component:
    /// written as text, `?sslmode=disable` in it became a TLS setting.
    #[test]
    fn delimiters_in_a_database_or_host_add_no_query() {
        let info = ConnectionInfo {
            scheme: "postgresql".to_string(),
            host: Some("h/db?sslmode=disable".to_string()),
            port: Some(5432),
            username: None,
            password: None,
            database: Some("app?sslmode=disable".to_string()),
            path: None,
            query_params: Vec::new(),
        };
        let reparsed = ConnectionInfo::parse(&info.to_original_string()).unwrap();
        assert!(reparsed.query_params.is_empty(), "{reparsed:?}");
        assert_eq!(reparsed.database.as_deref(), Some("app?sslmode=disable"));

        let ipv6 = ConnectionInfo::parse("postgresql://u@[::1]:5432/db").unwrap();
        assert_eq!(ipv6.to_original_string(), "postgresql://u@[::1]:5432/db");
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
}
