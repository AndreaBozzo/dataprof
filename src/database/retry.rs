//! Connection retry logic with exponential backoff

use crate::database::security::sanitize_error_message;
use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;

/// Retry configuration for database operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial backoff delay
    pub initial_delay: Duration,
    /// Maximum backoff delay
    pub max_delay: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f32,
    /// Whether to use jitter to avoid thundering herd
    pub use_jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            use_jitter: true,
        }
    }
}

/// Retry a database operation with exponential backoff
pub async fn retry_database_operation<T, F, Fut, E>(
    config: &RetryConfig,
    operation: F,
    operation_name: &str,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display + Send + Sync + 'static,
{
    let mut last_error = None;
    let mut delay = config.initial_delay;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                last_error = Some(anyhow::anyhow!("{}", error));

                if attempt < config.max_retries {
                    let actual_delay = if config.use_jitter {
                        add_jitter(delay)
                    } else {
                        delay
                    };

                    let sanitized_error = sanitize_error_message(&error.to_string());
                    log::warn!(
                        "Database operation '{}' failed on attempt {}/{}, retrying in {:?}: {}",
                        operation_name,
                        attempt + 1,
                        config.max_retries + 1,
                        actual_delay,
                        sanitized_error
                    );

                    sleep(actual_delay).await;
                    delay = std::cmp::min(
                        Duration::from_millis(
                            (delay.as_millis() as f32 * config.backoff_multiplier) as u64,
                        ),
                        config.max_delay,
                    );
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "Database operation '{}' failed after {} attempts",
            operation_name,
            config.max_retries + 1
        )
    }))
}

/// Add jitter to delay to avoid thundering herd problem
fn add_jitter(delay: Duration) -> Duration {
    use rand::Rng;
    let mut rng = rand::rng();
    let jitter_factor = rng.random_range(0.5..1.5);
    Duration::from_millis((delay.as_millis() as f64 * jitter_factor) as u64)
}

/// Check if an error is retryable (connection-related)
pub fn is_retryable_error(error: &str) -> bool {
    let error_lower = error.to_lowercase();

    // Common retryable database errors
    error_lower.contains("connection") ||
    error_lower.contains("timeout") ||
    error_lower.contains("network") ||
    error_lower.contains("temporary") ||
    error_lower.contains("unavailable") ||
    error_lower.contains("broken pipe") ||
    error_lower.contains("connection reset") ||
    error_lower.contains("connection refused") ||
    error_lower.contains("host unreachable") ||
    error_lower.contains("too many connections") ||
    error_lower.contains("database is locked") ||  // SQLite specific
    error_lower.contains("server has gone away") || // MySQL specific
    error_lower.contains("connection timed out") // General timeout
}

/// Enhanced retry logic that only retries on connection errors
pub async fn retry_on_connection_error<T, F, Fut, E>(
    config: &RetryConfig,
    operation: F,
    operation_name: &str,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display + Send + Sync + 'static,
{
    let mut last_error = None;
    let mut delay = config.initial_delay;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                let error_str = error.to_string();

                // Only retry if it's a connection-related error
                if !is_retryable_error(&error_str) {
                    return Err(anyhow::anyhow!("{}", error));
                }

                last_error = Some(anyhow::anyhow!("{}", error));

                if attempt < config.max_retries {
                    let actual_delay = if config.use_jitter {
                        add_jitter(delay)
                    } else {
                        delay
                    };

                    let sanitized_error = sanitize_error_message(&error.to_string());
                    log::warn!(
                        "Retryable database error in '{}' (attempt {}/{}), retrying in {:?}: {}",
                        operation_name,
                        attempt + 1,
                        config.max_retries + 1,
                        actual_delay,
                        sanitized_error
                    );

                    sleep(actual_delay).await;
                    delay = std::cmp::min(
                        Duration::from_millis(
                            (delay.as_millis() as f32 * config.backoff_multiplier) as u64,
                        ),
                        config.max_delay,
                    );
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "Database operation '{}' failed after {} attempts",
            operation_name,
            config.max_retries + 1
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn test_retry_success_after_failure() {
        let config = RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            backoff_multiplier: 2.0,
            use_jitter: false,
        };

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_database_operation(
            &config,
            || {
                let c = counter_clone.clone();
                async move {
                    let count = c.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        Err("Connection failed")
                    } else {
                        Ok("Success")
                    }
                }
            },
            "test_operation",
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.expect("Expected successful result"), "Success");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_is_retryable_error() {
        assert!(is_retryable_error("Connection refused"));
        assert!(is_retryable_error("Database timeout"));
        assert!(is_retryable_error("Network error"));
        assert!(is_retryable_error("Too many connections"));
        assert!(is_retryable_error("database is locked"));

        assert!(!is_retryable_error("Syntax error"));
        assert!(!is_retryable_error("Permission denied"));
        assert!(!is_retryable_error("Table not found"));
    }
}
