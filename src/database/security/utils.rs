//! Security utility functions

use regex;

/// Sanitize error messages to prevent information disclosure
pub fn sanitize_error_message(error_message: &str) -> String {
    let mut sanitized = error_message.to_string();

    // Remove common patterns that might contain sensitive information
    let patterns_to_remove = [
        // Connection strings and URLs
        r"postgresql://[^\s]+",
        r"mysql://[^\s]+",
        r"sqlite://[^\s]+",
        // Password patterns
        r"password[=:]\s*[^\s;]+",
        r"pass[=:]\s*[^\s;]+",
        r"pwd[=:]\s*[^\s;]+",
        r"Password [^:]*: ['\x22][^'\x22]+['\x22]",
        r"verification failed: ['\x22][^'\x22]+['\x22]",
        // File paths that might contain sensitive info
        r"[A-Za-z]:\\[^\s]+",
        r"/[^\s]*/.env[^\s]*",
        r"/[^\s]*/config[^\s]*",
        r"/home/[^\s/]+/[^\s]*",
        r"/etc/[^\s]*",
        r"\.[^\s]*conf[^\s]*",
        // IP addresses (be conservative)
        r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b",
        // Usernames in connection errors
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

    // Truncate very long error messages that might contain sensitive data
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(sanitized, normal_error); // Should be unchanged
    }
}
