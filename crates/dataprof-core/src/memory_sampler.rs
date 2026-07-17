use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Tracks the peak resident set size (RSS) of the current process across a
/// profiling run.
///
/// Engines call [`sample`](Self::sample) at chunk/batch boundaries; the peak
/// observed feeds `ExecutionMetadata::memory_peak_mb`. The reading is the
/// whole-process RSS, so on embedded surfaces (e.g. the Python extension) it
/// includes host-process memory — it is an upper bound on what profiling used,
/// suitable for verifying the bounded-memory claim, not an exact attribution.
pub struct PeakMemorySampler {
    system: System,
    pid: Option<Pid>,
    peak_bytes: u64,
}

impl PeakMemorySampler {
    /// Create a sampler and take an initial reading.
    pub fn new() -> Self {
        let mut sampler = Self {
            system: System::new(),
            pid: sysinfo::get_current_pid().ok(),
            peak_bytes: 0,
        };
        sampler.sample();
        sampler
    }

    /// Refresh the process RSS and fold it into the running peak.
    pub fn sample(&mut self) {
        let Some(pid) = self.pid else { return };
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing().with_memory(),
        );
        if let Some(process) = self.system.process(pid) {
            self.peak_bytes = self.peak_bytes.max(process.memory());
        }
    }

    /// Peak RSS observed so far, in megabytes.
    ///
    /// `None` when the platform provided no reading — callers should leave
    /// `memory_peak_mb` absent (not analyzed) rather than report zero.
    pub fn peak_mb(&self) -> Option<f64> {
        (self.peak_bytes > 0).then(|| self.peak_bytes as f64 / (1024.0 * 1024.0))
    }
}

impl Default for PeakMemorySampler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sampler_reports_positive_peak() {
        let sampler = PeakMemorySampler::new();
        let peak = sampler
            .peak_mb()
            .expect("current process RSS should be readable on supported platforms");
        assert!(peak > 0.0, "peak RSS must be positive, got {peak}");
    }

    #[test]
    fn test_peak_is_monotonic_across_samples() {
        let mut sampler = PeakMemorySampler::new();
        let first = sampler.peak_mb().expect("initial reading");
        // Allocate enough that RSS cannot legitimately shrink below the first peak.
        let ballast = vec![1u8; 4 * 1024 * 1024];
        sampler.sample();
        let second = sampler.peak_mb().expect("second reading");
        assert!(
            second >= first,
            "peak must never decrease: {first} -> {second}"
        );
        drop(ballast);
    }
}
