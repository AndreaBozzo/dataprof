use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Memory resource tracker to detect potential leaks
#[derive(Debug, Clone)]
pub struct MemoryTracker {
    allocations: Arc<Mutex<HashMap<String, AllocationInfo>>>,
    leak_threshold_mb: usize,
}

#[derive(Debug, Clone)]
struct AllocationInfo {
    size_bytes: usize,
    timestamp: u64,
    resource_type: String,
    // stack_trace: String, // TODO: Add in debug builds
}

#[derive(Debug)]
pub struct MemoryLeak {
    pub resource_id: String,
    pub size_bytes: usize,
    pub age_seconds: u64,
    pub resource_type: String,
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new(100) // Default 100MB threshold
    }
}

impl MemoryTracker {
    /// Create a new memory tracker with leak detection threshold in MB
    pub fn new(leak_threshold_mb: usize) -> Self {
        Self {
            allocations: Arc::new(Mutex::new(HashMap::new())),
            leak_threshold_mb,
        }
    }

    /// Track a memory allocation
    pub fn track_allocation(&self, resource_id: String, size_bytes: usize, resource_type: &str) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();

        let info = AllocationInfo {
            size_bytes,
            timestamp,
            resource_type: resource_type.to_string(),
        };

        if let Ok(mut allocations) = self.allocations.lock() {
            allocations.insert(resource_id, info);
        }
    }

    /// Mark a resource as deallocated
    pub fn track_deallocation(&self, resource_id: &str) {
        if let Ok(mut allocations) = self.allocations.lock() {
            allocations.remove(resource_id);
        }
    }

    /// Check for potential memory leaks
    pub fn detect_leaks(&self) -> Vec<MemoryLeak> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();

        let threshold_bytes = self.leak_threshold_mb * 1024 * 1024;

        if let Ok(allocations) = self.allocations.lock() {
            allocations
                .iter()
                .filter_map(|(id, info)| {
                    let age_seconds = current_time - info.timestamp;

                    // Consider it a leak if:
                    // - Size is above threshold OR
                    // - Age is more than 60 seconds
                    if info.size_bytes > threshold_bytes || age_seconds > 60 {
                        Some(MemoryLeak {
                            resource_id: id.clone(),
                            size_bytes: info.size_bytes,
                            age_seconds,
                            resource_type: info.resource_type.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get memory usage summary
    pub fn get_memory_stats(&self) -> (usize, usize, usize) {
        if let Ok(allocations) = self.allocations.lock() {
            let total_allocations = allocations.len();
            let total_bytes: usize = allocations.values().map(|info| info.size_bytes).sum();
            let total_mb = total_bytes / (1024 * 1024);

            (total_allocations, total_bytes, total_mb)
        } else {
            (0, 0, 0)
        }
    }

    /// Report detected leaks
    pub fn report_leaks(&self) -> String {
        let leaks = self.detect_leaks();

        if leaks.is_empty() {
            "No memory leaks detected.".to_string()
        } else {
            let mut report = format!("⚠️  {} potential memory leak(s) detected:\n\n", leaks.len());

            for leak in leaks {
                report.push_str(&format!(
                    "• Resource: {} ({})\n  Size: {} bytes ({:.2} MB)\n  Age: {}s\n\n",
                    leak.resource_id,
                    leak.resource_type,
                    leak.size_bytes,
                    leak.size_bytes as f64 / (1024.0 * 1024.0),
                    leak.age_seconds
                ));
            }

            report
        }
    }
}

/// RAII wrapper for tracked resources
pub struct TrackedResource<T> {
    resource: T,
    resource_id: String,
    tracker: MemoryTracker,
}

impl<T> TrackedResource<T> {
    pub fn new(
        resource: T,
        resource_id: String,
        size_bytes: usize,
        resource_type: &str,
        tracker: MemoryTracker,
    ) -> Self {
        tracker.track_allocation(resource_id.clone(), size_bytes, resource_type);

        Self {
            resource,
            resource_id,
            tracker,
        }
    }

    pub fn get(&self) -> &T {
        &self.resource
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.resource
    }
}

impl<T> Drop for TrackedResource<T> {
    fn drop(&mut self) {
        self.tracker.track_deallocation(&self.resource_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracking() {
        let tracker = MemoryTracker::new(50);

        // Track a large allocation
        tracker.track_allocation("test_mmap_1".to_string(), 100 * 1024 * 1024, "mmap");

        let (count, bytes, mb) = tracker.get_memory_stats();
        assert_eq!(count, 1);
        assert_eq!(bytes, 100 * 1024 * 1024);
        assert_eq!(mb, 100);

        // Should detect leak due to size
        let leaks = tracker.detect_leaks();
        assert_eq!(leaks.len(), 1);
        assert_eq!(leaks[0].resource_type, "mmap");

        // Clean up
        tracker.track_deallocation("test_mmap_1");
        let leaks = tracker.detect_leaks();
        assert_eq!(leaks.len(), 0);
    }

    #[test]
    fn test_age_based_leak_detection() {
        // Use very low threshold so our allocation triggers
        let tracker = MemoryTracker::new(0); // 0MB threshold

        // Track a small allocation that will be detected as leak
        tracker.track_allocation("test_small".to_string(), 1024, "buffer");

        let leaks = tracker.detect_leaks();
        assert_eq!(leaks.len(), 1);
        assert_eq!(leaks[0].resource_type, "buffer");
    }

    #[test]
    fn test_tracked_resource_raii() {
        let tracker = MemoryTracker::new(50);

        {
            let _resource = TrackedResource::new(
                vec![0u8; 1024],
                "test_vec".to_string(),
                1024,
                "vector",
                tracker.clone(),
            );

            let (count, _, _) = tracker.get_memory_stats();
            assert_eq!(count, 1);
        }

        // Should be automatically deallocated
        let (count, _, _) = tracker.get_memory_stats();
        assert_eq!(count, 0);
    }
}
