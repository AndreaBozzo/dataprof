/// Shared engine configuration for memory limits.
///
/// Provides a consistent default across all engines that enforce memory bounds.
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    /// Maximum memory budget in megabytes.
    pub limit_mb: usize,
}

impl MemoryConfig {
    pub fn new(limit_mb: usize) -> Self {
        Self { limit_mb }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self { limit_mb: 256 }
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryConfig;

    #[test]
    fn default_limit_matches_engine_expectation() {
        assert_eq!(MemoryConfig::default().limit_mb, 256);
    }

    #[test]
    fn new_sets_limit() {
        assert_eq!(MemoryConfig::new(1024).limit_mb, 1024);
    }
}
