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
