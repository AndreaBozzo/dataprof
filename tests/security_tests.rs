//! Security tests for DataProfiler database module
//!
//! These tests verify that SQL injection attacks are properly prevented
//! and that sensitive information is not disclosed through error messages.

#![cfg(feature = "database")]

use dataprof::database::security::{
    sanitize_error_message, validate_base_query, validate_sql_identifier,
};

#[cfg(test)]
mod sql_injection_tests {
    use super::*;

    #[test]
    fn test_prevent_sql_injection_in_table_names() {
        // Valid table names should pass
        assert!(validate_sql_identifier("users").is_ok());
        assert!(validate_sql_identifier("user_accounts").is_ok());
        assert!(validate_sql_identifier("schema.table").is_ok());
        assert!(validate_sql_identifier("\"quoted table\"").is_ok());

        // SQL injection attempts should be blocked
        assert!(validate_sql_identifier("users; DROP TABLE users; --").is_err());
        assert!(validate_sql_identifier("users UNION SELECT * FROM passwords").is_err());
        assert!(validate_sql_identifier("users/* comment */").is_err());
        assert!(validate_sql_identifier("users--").is_err());
        assert!(validate_sql_identifier("'; DROP TABLE users; --").is_err());
        assert!(validate_sql_identifier("users OR 1=1").is_err());
        assert!(validate_sql_identifier("users; INSERT INTO").is_err());
        assert!(validate_sql_identifier("users; UPDATE SET").is_err());
        assert!(validate_sql_identifier("users; DELETE FROM").is_err());
    }

    #[test]
    fn test_prevent_sql_injection_in_queries() {
        // Valid SELECT queries should pass
        assert!(validate_base_query("SELECT * FROM users").is_ok());
        assert!(validate_base_query("SELECT id, name FROM products WHERE active = 1").is_ok());
        assert!(validate_base_query("  SELECT COUNT(*) FROM orders  ").is_ok());

        // Non-SELECT queries should be blocked
        assert!(validate_base_query("DROP TABLE users").is_err());
        assert!(validate_base_query("INSERT INTO users VALUES (1, 'admin')").is_err());
        assert!(validate_base_query("UPDATE users SET password = 'hacked'").is_err());
        assert!(validate_base_query("DELETE FROM users").is_err());
        assert!(validate_base_query("TRUNCATE TABLE users").is_err());
        assert!(validate_base_query("ALTER TABLE users ADD COLUMN backdoor TEXT").is_err());

        // SQL injection attempts in SELECT queries should be blocked
        assert!(validate_base_query("SELECT * FROM users; DROP TABLE users; --").is_err());
        assert!(validate_base_query("SELECT * FROM users UNION SELECT * FROM passwords").is_err());
        assert!(validate_base_query("SELECT * FROM users/* malicious comment */").is_err());
        assert!(validate_base_query("SELECT * FROM users -- comment").is_err());
        assert!(validate_base_query("SELECT * FROM users; EXEC sp_configure").is_err());
    }

    #[test]
    fn test_prevent_dangerous_sql_keywords() {
        let dangerous_inputs = vec![
            "INFORMATION_SCHEMA.tables",
            "pg_tables",
            "mysql.user",
            "sys.tables",
            "master..sysdatabases",
            "users; GRANT ALL",
            "users; REVOKE ALL",
            "users; EXECUTE sp_",
        ];

        for input in dangerous_inputs {
            assert!(
                validate_sql_identifier(input).is_err(),
                "Should block dangerous input: {}",
                input
            );
        }
    }

    #[test]
    fn test_prevent_special_characters_injection() {
        let malicious_inputs = vec![
            "users';--",
            "users\"; DROP TABLE",
            "users` OR 1=1 #",
            "users] UNION SELECT",
            "users\x00",     // null byte
            "users\n; DROP", // newline injection
            "users\r; DROP", // carriage return injection
            "users\t; DROP", // tab injection
        ];

        for input in malicious_inputs {
            assert!(
                validate_sql_identifier(input).is_err(),
                "Should block malicious input: {}",
                input
            );
        }
    }

    #[test]
    fn test_length_limits() {
        // Very long identifier should be rejected
        let long_identifier = "a".repeat(200);
        assert!(validate_sql_identifier(&long_identifier).is_err());

        // Very long query should be rejected
        let long_query = format!("SELECT * FROM {}", "a".repeat(10000));
        assert!(validate_base_query(&long_query).is_err());
    }

    #[test]
    fn test_empty_and_whitespace_inputs() {
        // Empty inputs should be rejected
        assert!(validate_sql_identifier("").is_err());
        assert!(validate_sql_identifier("   ").is_err());
        assert!(validate_base_query("").is_err());
        assert!(validate_base_query("   ").is_err());
    }
}

#[cfg(test)]
mod error_sanitization_tests {
    use super::*;

    #[test]
    fn test_sanitize_connection_strings() {
        let error_with_credentials =
            "Connection failed: postgresql://admin:supersecret@db.example.com:5432/mydb";
        let sanitized = sanitize_error_message(error_with_credentials);

        assert!(!sanitized.contains("supersecret"));
        assert!(!sanitized.contains("admin"));
        assert!(sanitized.contains("[REDACTED]"));
    }

    #[test]
    fn test_sanitize_password_patterns() {
        let errors_with_passwords = vec![
            "Authentication failed: password=mypassword123",
            "Login error: pass=secret123",
            "Connection denied: pwd=topsecret",
            "Auth failed with password: 'admin123'",
            "Password verification failed: \"secret456\"",
        ];

        for error in errors_with_passwords {
            let sanitized = sanitize_error_message(error);
            assert!(
                sanitized.contains("[REDACTED]"),
                "Should redact password in: {}",
                error
            );
            assert!(
                !sanitized.contains("secret")
                    && !sanitized.contains("admin")
                    && !sanitized.contains("password"),
                "Should not contain password text in: {}",
                sanitized
            );
        }
    }

    #[test]
    fn test_sanitize_file_paths() {
        let errors_with_paths = vec![
            "Config file not found: C:\\Users\\admin\\.env",
            "Cannot read: /home/user/.config/database.conf",
            "File error: C:\\secrets\\database_password.txt",
            "Path not found: /etc/mysql/mysql.conf.d/secret.cnf",
        ];

        for error in errors_with_paths {
            let sanitized = sanitize_error_message(error);
            assert!(
                sanitized.contains("[REDACTED]"),
                "Should redact file path in: {}",
                error
            );
        }
    }

    #[test]
    fn test_sanitize_ip_addresses() {
        let errors_with_ips = vec![
            "Connection timeout to 192.168.1.100",
            "Cannot reach database at 10.0.0.5:5432",
            "Host unreachable: 172.16.254.1",
        ];

        for error in errors_with_ips {
            let sanitized = sanitize_error_message(error);
            assert!(
                sanitized.contains("[REDACTED]"),
                "Should redact IP address in: {}",
                error
            );
        }
    }

    #[test]
    fn test_sanitize_usernames() {
        let errors_with_usernames = vec![
            "Authentication failed for user: dbadmin",
            "Login denied for username: 'service_account'",
            "User permission denied: \"backup_user\"",
        ];

        for error in errors_with_usernames {
            let sanitized = sanitize_error_message(error);
            assert!(
                sanitized.contains("[REDACTED]"),
                "Should redact username in: {}",
                error
            );
        }
    }

    #[test]
    fn test_preserve_safe_error_messages() {
        let safe_errors = vec![
            "Table 'products' doesn't exist",
            "Column 'price' not found",
            "Syntax error near 'FROM'",
            "Connection timeout",
            "Database locked",
            "Too many connections",
        ];

        for error in safe_errors {
            let sanitized = sanitize_error_message(error);
            assert_eq!(
                sanitized, error,
                "Safe error message should not be modified: {}",
                error
            );
        }
    }

    #[test]
    fn test_truncate_very_long_errors() {
        let very_long_error = "Error: ".to_string() + &"x".repeat(600);
        let sanitized = sanitize_error_message(&very_long_error);

        assert!(sanitized.len() <= 550); // Should be truncated
        assert!(sanitized.contains("[truncated for security]"));
    }
}

#[cfg(test)]
mod integration_security_tests {
    use super::*;

    #[test]
    fn test_comprehensive_sql_injection_scenarios() {
        // Test various real-world SQL injection attack patterns
        let attack_patterns = vec![
            // Union-based attacks
            "users' UNION SELECT username, password FROM admin_users --",
            "products' UNION SELECT 1, @@version, 3 --",

            // Boolean-based blind attacks
            "users' AND 1=1 --",
            "users' AND 1=2 --",
            "users' AND (SELECT COUNT(*) FROM information_schema.tables) > 0 --",

            // Time-based blind attacks
            "users'; WAITFOR DELAY '00:00:05'; --",
            "users' AND (SELECT COUNT(*) FROM SLEEP(5)) --",

            // Error-based attacks
            "users' AND EXTRACTVALUE(1, CONCAT(0x7e, VERSION(), 0x7e)) --",
            "users' AND (SELECT * FROM (SELECT COUNT(*), CONCAT(VERSION(), FLOOR(RAND(0)*2)) x FROM information_schema.tables GROUP BY x) a) --",

            // Stacked queries
            "users'; DROP TABLE users; CREATE TABLE users (id INT); --",
            "users'; INSERT INTO admin_users VALUES ('hacker', 'password'); --",

            // Comment injection
            "users /* comment */ --",
            "users -- comment",
            "users # comment",

            // Encoding attacks
            "users%27; DROP TABLE users; --",
            "users\'; DROP TABLE users; --",
        ];

        for pattern in attack_patterns {
            assert!(
                validate_sql_identifier(pattern).is_err(),
                "Should block SQL injection pattern: {}",
                pattern
            );
        }
    }

    #[test]
    fn test_edge_case_injections() {
        let edge_cases = vec![
            // Case variations
            "users; drop table users;",
            "users; DROP table USERS;",
            "users; Drop Table Users;",
            // Whitespace variations
            "users;  DROP   TABLE  users;",
            "users;\tDROP\tTABLE\tusers;",
            "users;\nDROP\nTABLE\nusers;",
            // Unicode and special characters
            "users\u{0000}; DROP TABLE users;", // null byte
            "users\u{00A0}; DROP TABLE users;", // non-breaking space
            // Nested quotes
            "users'; SELECT 'nested''quote'; --",
            "users\"; SELECT \"nested\"\"quote\"; --",
        ];

        for case in edge_cases {
            assert!(
                validate_sql_identifier(case).is_err(),
                "Should block edge case injection: {}",
                case
            );
        }
    }

    #[test]
    fn test_database_specific_attacks() {
        let db_specific_attacks = vec![
            // PostgreSQL specific
            "users'; SELECT version(); --",
            "users'; COPY (SELECT '') TO PROGRAM 'rm -rf /'; --",
            // MySQL specific
            "users'; SELECT @@version; --",
            "users'; SELECT LOAD_FILE('/etc/passwd'); --",
            // SQLite specific
            "users'; ATTACH DATABASE '/tmp/evil.db' AS evil; --",
            // SQL Server specific
            "users'; EXEC xp_cmdshell('dir'); --",
            "users'; SELECT @@servername; --",
        ];

        for attack in db_specific_attacks {
            assert!(
                validate_sql_identifier(attack).is_err(),
                "Should block database-specific attack: {}",
                attack
            );
        }
    }
}
