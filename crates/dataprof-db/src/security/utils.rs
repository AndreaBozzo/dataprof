//! Security utility functions

/// Sanitize error messages to prevent information disclosure
pub fn sanitize_error_message(error_message: &str) -> String {
    let mut sanitized = error_message.to_string();

    let patterns_to_remove = [
        r"postgresql://[^\s]+",
        r"mysql://[^\s]+",
        r"sqlite://[^\s]+",
        r"password[=:]\s*[^\s;]+",
        r"pass[=:]\s*[^\s;]+",
        r"pwd[=:]\s*[^\s;]+",
        r"Password [^:]*: ['\x22][^'\x22]+['\x22]",
        r"verification failed: ['\x22][^'\x22]+['\x22]",
        r"[A-Za-z]:\\[^\s]+",
        r"/[^\s]*/.env[^\s]*",
        r"/[^\s]*/config[^\s]*",
        r"/home/[^\s/]+/[^\s]*",
        r"/etc/[^\s]*",
        r"\.[^\s]*conf[^\s]*",
        r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b",
        r"user[\s=:]['\x22]?[^'\x22\s;]+['\x22]?",
        r"username[\s=:]['\x22]?[^'\x22\s;]+['\x22]?",
        r"for user: [a-zA-Z0-9_]+",
        r"failed for user: [a-zA-Z0-9_]+",
        r"for username: ['\x22][^'\x22]+['\x22]",
        r"denied for username: ['\x22][^'\x22]+['\x22]",
        r"permission denied: ['\x22][^'\x22]+['\x22]",
        r"User [^:]*: ['\x22][^'\x22]+['\x22]",
    ];

    for pattern in &patterns_to_remove {
        if let Ok(regex) = regex::Regex::new(pattern) {
            sanitized = regex.replace_all(&sanitized, "[REDACTED]").to_string();
        }
    }

    if sanitized.len() > 500 {
        sanitized = format!("{}... [truncated for security]", &sanitized[..500]);
    }

    sanitized
}

/// Add query parameter to connection string
pub fn add_query_param(connection_string: String, key: &str, value: &str) -> String {
    if connection_string.contains('?') {
        format!("{}&{}={}", connection_string, key, value)
    } else {
        format!("{}?{}={}", connection_string, key, value)
    }
}

/// Add a query parameter unless the connection string already sets one of
/// `aliases` (the names the driver accepts for the same setting).
///
/// sqlx applies query pairs in order, so a value appended after the caller's
/// own would override it. A setting the caller wrote in the URL wins.
pub fn add_query_param_if_absent(
    connection_string: String,
    aliases: &[&str],
    value: &str,
) -> String {
    let already_set = connection_string.split_once('?').is_some_and(|(_, query)| {
        query.split('&').any(|pair| {
            let key = pair.split_once('=').map_or(pair, |(key, _)| key);
            aliases.contains(&key)
        })
    });
    if already_set {
        connection_string
    } else {
        add_query_param(connection_string, aliases[0], value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_query_param_if_absent_keeps_the_callers_value() {
        let url = "postgresql://h/db?ssl-mode=verify-full".to_string();
        let result = add_query_param_if_absent(url, &["sslmode", "ssl-mode"], "prefer");
        assert_eq!(result, "postgresql://h/db?ssl-mode=verify-full");

        let url = "postgresql://h/db?application_name=x".to_string();
        let result = add_query_param_if_absent(url, &["sslmode", "ssl-mode"], "prefer");
        assert_eq!(
            result,
            "postgresql://h/db?application_name=x&sslmode=prefer"
        );
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
    fn test_error_message_sanitization() {
        let error_with_password =
            "Connection failed: postgresql://user:secret123@localhost:5432/db";
        let sanitized = sanitize_error_message(error_with_password);
        assert!(!sanitized.contains("secret123"));
        assert!(sanitized.contains("[REDACTED]"));

        let error_with_path = "Config file not found: C:\\Users\\admin\\.env";
        let sanitized = sanitize_error_message(error_with_path);
        assert!(sanitized.contains("[REDACTED]"));

        let normal_error = "Table 'products' doesn't exist";
        let sanitized = sanitize_error_message(normal_error);
        assert_eq!(sanitized, normal_error);
    }
}
