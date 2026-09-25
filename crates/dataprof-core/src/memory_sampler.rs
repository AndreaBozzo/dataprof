use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Minimum spacing between two throttled samples.
///
/// One refresh costs about 14 ms on Windows, where sysinfo walks the process
/// table even when asked for a single process, and the streaming engines
/// sample once per chunk. Small chunks paid that cost thousands of times: a
/// 300,000-row CSV took 345 s with 1 KB chunks against 1.6 s with 1 MB ones.
/// Resident memory moves slowly next to that, so sampling at most four times
/// a second loses nothing a peak reading needs, and callers take one
/// unthrottled sample at the end.
const MIN_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// Tracks the peak resident set size (RSS) of the current process across a
/// profiling run.
///
/// Engines call [`sample`](Self::sample) at chunk/batch boundaries and
/// [`sample_now`](Self::sample_now) once at the end; the peak observed feeds
/// `ExecutionMetadata::memory_peak_mb`. The reading is the
/// whole-process RSS, so on embedded surfaces (e.g. the Python extension) it
/// includes host-process memory — it is an upper bound on what profiling used,
/// suitable for verifying the bounded-memory claim, not an exact attribution.
pub struct PeakMemorySampler {
    system: System,
    pid: Option<Pid>,
    peak_bytes: u64,
    last_sample: Option<Instant>,
    refreshes: u64,
}

impl PeakMemorySampler {
    /// Create a sampler and take an initial reading.
    pub fn new() -> Self {
        let mut sampler = Self {
            system: System::new(),
            pid: sysinfo::get_current_pid().ok(),
            peak_bytes: 0,
            last_sample: None,
            refreshes: 0,
        };
        sampler.sample_now();
        sampler
    }

    /// Sample unless the previous sample is less than [`MIN_SAMPLE_INTERVAL`]
    /// old. For per-chunk calls inside a loop.
    pub fn sample(&mut self) {
        if self
            .last_sample
            .is_some_and(|last| last.elapsed() < MIN_SAMPLE_INTERVAL)
        {
            return;
        }
        self.sample_now();
    }

    /// Refresh the process RSS and fold it into the running peak, whatever
    /// the interval. For the final reading of a run, so the reported peak
    /// covers the work done after the last throttled sample.
    pub fn sample_now(&mut self) {
        self.last_sample = Some(Instant::now());
        let Some(pid) = self.pid else { return };
        self.refreshes += 1;
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
    fn repeated_samples_within_the_interval_do_not_refresh() {
        let mut sampler = PeakMemorySampler::new();
        let before = sampler.refreshes;
        for _ in 0..1_000 {
            sampler.sample();
        }
        assert_eq!(sampler.refreshes, before, "a tight loop must not refresh");
        sampler.sample_now();
        assert_eq!(sampler.refreshes, before + 1, "sample_now always refreshes");
    }

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
        sampler.sample_now();
        let second = sampler.peak_mb().expect("second reading");
        assert!(
            second >= first,
            "peak must never decrease: {first} -> {second}"
        );
        drop(ballast);
    }
}
