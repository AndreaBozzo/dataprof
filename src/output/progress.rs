use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use is_terminal::IsTerminal;
use std::fmt::Write;
use std::io::{stderr, stdout};
use std::time::{Duration, Instant};

use crate::core::memory_tracker::MemoryTracker;

/// Enhanced progress indicators for CLI operations
pub struct ProgressManager {
    pub show_progress: bool,
    pub verbosity: u8,
    pub memory_tracker: Option<MemoryTracker>,
    pub is_stdout_terminal: bool,
    pub is_stderr_terminal: bool,
}

impl ProgressManager {
    pub fn new(show_progress: bool, verbosity: u8) -> Self {
        Self {
            show_progress,
            verbosity,
            memory_tracker: None,
            is_stdout_terminal: stdout().is_terminal(),
            is_stderr_terminal: stderr().is_terminal(),
        }
    }

    /// Create a new ProgressManager with memory tracking enabled
    pub fn with_memory_tracking(
        show_progress: bool,
        verbosity: u8,
        leak_threshold_mb: usize,
    ) -> Self {
        Self {
            show_progress,
            verbosity,
            memory_tracker: Some(MemoryTracker::new(leak_threshold_mb)),
            is_stdout_terminal: stdout().is_terminal(),
            is_stderr_terminal: stderr().is_terminal(),
        }
    }

    /// Check if output is going to a terminal (interactive) vs pipe/redirect
    pub fn is_interactive(&self) -> bool {
        self.is_stdout_terminal && self.is_stderr_terminal
    }

    /// Check if output should be machine-readable (for pipes/CI)
    pub fn should_use_machine_readable_format(&self) -> bool {
        !self.is_interactive()
    }

    /// Get current memory usage from tracker
    pub fn get_memory_usage(&self) -> Option<(usize, usize, usize)> {
        self.memory_tracker
            .as_ref()
            .map(|tracker| tracker.get_memory_stats())
    }

    /// Track a memory allocation
    pub fn track_allocation(&self, resource_id: String, size_bytes: usize, resource_type: &str) {
        if let Some(ref tracker) = self.memory_tracker {
            tracker.track_allocation(resource_id, size_bytes, resource_type);
        }
    }

    /// Track a memory deallocation
    pub fn track_deallocation(&self, resource_id: &str) {
        if let Some(ref tracker) = self.memory_tracker {
            tracker.track_deallocation(resource_id);
        }
    }

    /// Create a progress bar for file processing
    pub fn create_file_progress(&self, file_size: u64, file_name: &str) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(file_size);
        if let Ok(style) = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}",
        ) {
            pb.set_style(
                style
                    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                        let _ = write!(w, "{:.1}s", state.eta().as_secs_f64());
                    })
                    .progress_chars("🔄🔵⚪"),
            );
        }

        pb.set_message(format!("Processing {}", file_name));
        Some(pb)
    }

    /// Create a progress bar for batch processing
    pub fn create_batch_progress(&self, total_files: u64) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(total_files);
        if let Ok(style) = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.yellow/orange}] {pos:>3}/{len:3} files ({eta}) {msg}",
        ) {
            pb.set_style(
                style
                    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                        let _ = write!(w, "{:.1}s", state.eta().as_secs_f64());
                    })
                    .progress_chars("📁📂⚪"),
            );
        }

        pb.set_message("Processing files...");
        Some(pb)
    }

    /// Create a progress bar for ML analysis
    pub fn create_ml_progress(&self, total_steps: u64) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(total_steps);
        if let Ok(style) = ProgressStyle::with_template(
            "{spinner:.magenta} [{elapsed_precise}] [{wide_bar:.magenta/pink}] {pos:>2}/{len:2} {msg}",
        ) {
            pb.set_style(style.progress_chars("🤖🔮⚪"));
        }

        pb.set_message("ML Readiness Analysis...");
        Some(pb)
    }

    /// Create a progress bar for quality checking
    pub fn create_quality_progress(&self, total_columns: u64) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(total_columns);
        if let Ok(style) = ProgressStyle::with_template(
            "{spinner:.blue} [{elapsed_precise}] [{wide_bar:.blue/cyan}] {pos:>3}/{len:3} columns {msg}",
        ) {
            pb.set_style(style.progress_chars("🔍🔎⚪"));
        }

        pb.set_message("Quality checking...");
        Some(pb)
    }

    /// Create an indeterminate spinner for unknown-duration tasks
    pub fn create_spinner(&self, message: &str) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new_spinner();
        if let Ok(style) = ProgressStyle::with_template("{spinner:.green} {msg}") {
            pb.set_style(style.tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]));
        }
        pb.set_message(message.to_string());
        pb.enable_steady_tick(Duration::from_millis(80));
        Some(pb)
    }

    /// Create enhanced progress bar with memory tracking and performance metrics
    pub fn create_enhanced_file_progress(
        &self,
        file_size: u64,
        file_name: &str,
    ) -> Option<EnhancedProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = if self.is_interactive() {
            // Rich interactive progress bar with emojis and colors
            let pb = ProgressBar::new(file_size);
            if let Ok(style) = ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) | {msg}",
            ) {
                pb.set_style(
                    style
                        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                            let _ = write!(w, "{:.1}s", state.eta().as_secs_f64());
                        })
                        .progress_chars("🔄🔵⚪"),
                );
            }
            pb
        } else {
            // Simple machine-readable progress for pipes/CI
            let pb = ProgressBar::new(file_size);
            if let Ok(style) = ProgressStyle::with_template(
                "[{elapsed_precise}] {bytes}/{total_bytes} {bytes_per_sec} {msg}",
            ) {
                pb.set_style(style.progress_chars("=>-"));
            }
            pb
        };

        pb.set_message(format!("Processing {}", file_name));
        Some(EnhancedProgressBar::new(pb, self.memory_tracker.clone()))
    }

    /// Create adaptive progress bar based on terminal context
    pub fn create_adaptive_progress(&self, total: u64, operation: &str) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(total);

        let template = if self.is_interactive() {
            // Rich template for interactive terminals
            "{spinner:.blue} [{elapsed_precise}] [{wide_bar:.blue/cyan}] {pos:>7}/{len:7} {per_sec} | {msg}"
        } else {
            // Simple template for non-interactive (pipes, CI)
            "[{elapsed_precise}] {pos}/{len} {per_sec} {msg}"
        };

        if let Ok(style) = ProgressStyle::with_template(template) {
            if self.is_interactive() {
                pb.set_style(style.progress_chars("🔷🔹⚪"));
            } else {
                pb.set_style(style.progress_chars("=>-"));
            }
        }

        pb.set_message(operation.to_string());
        Some(pb)
    }

    /// Create a multi-progress for complex operations
    pub fn create_multi_progress(&self) -> Option<indicatif::MultiProgress> {
        if !self.show_progress {
            return None;
        }

        Some(indicatif::MultiProgress::new())
    }

    /// Display verbose information
    pub fn verbose(&self, level: u8, message: &str) {
        if self.verbosity >= level {
            match level {
                1 => println!("ℹ️  {}", message),
                2 => println!("🔧 [DEBUG] {}", message),
                3 => println!("🔬 [TRACE] {}", message),
                _ => println!("{}", message),
            }
        }
    }

    /// Display success message
    pub fn success(&self, message: &str) {
        if self.show_progress || self.verbosity > 0 {
            println!("✅ {}", message);
        }
    }

    /// Display warning message
    pub fn warning(&self, message: &str) {
        if self.show_progress || self.verbosity > 0 {
            println!("⚠️  {}", message);
        }
    }

    /// Display error message
    pub fn error(&self, message: &str) {
        println!("❌ {}", message);
    }

    /// Display info message
    pub fn info(&self, message: &str) {
        if self.verbosity > 0 {
            println!("ℹ️  {}", message);
        }
    }
}

/// Progress tracker for streaming operations
pub struct StreamingProgress {
    pub progress_bar: Option<ProgressBar>,
    pub start_time: Instant,
    pub last_update: Instant,
    pub update_interval: Duration,
}

impl StreamingProgress {
    pub fn new(progress_bar: Option<ProgressBar>) -> Self {
        Self {
            progress_bar,
            start_time: Instant::now(),
            last_update: Instant::now(),
            update_interval: Duration::from_millis(100), // Update every 100ms
        }
    }

    /// Update progress with current position
    pub fn update(&mut self, position: u64, message: Option<&str>) {
        if let Some(pb) = &self.progress_bar {
            let now = Instant::now();
            if now.duration_since(self.last_update) >= self.update_interval {
                pb.set_position(position);
                if let Some(msg) = message {
                    pb.set_message(msg.to_string());
                }
                self.last_update = now;
            }
        }
    }

    /// Update progress with increment
    pub fn increment(&mut self, delta: u64, message: Option<&str>) {
        if let Some(pb) = &self.progress_bar {
            let now = Instant::now();
            if now.duration_since(self.last_update) >= self.update_interval {
                pb.inc(delta);
                if let Some(msg) = message {
                    pb.set_message(msg.to_string());
                }
                self.last_update = now;
            }
        }
    }

    /// Finish progress with final message
    pub fn finish(&self, message: &str) {
        if let Some(pb) = &self.progress_bar {
            pb.finish_with_message(message.to_string());
        }
    }

    /// Finish progress and clear
    pub fn finish_and_clear(&self) {
        if let Some(pb) = &self.progress_bar {
            pb.finish_and_clear();
        }
    }

    /// Get elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Enhanced progress bar with memory tracking and performance metrics
pub struct EnhancedProgressBar {
    pub progress_bar: ProgressBar,
    pub memory_tracker: Option<MemoryTracker>,
    pub start_time: Instant,
    pub last_update: Instant,
    pub update_interval: Duration,
    pub bytes_processed: u64,
    pub rows_processed: u64,
    pub columns_processed: u64,
}

impl EnhancedProgressBar {
    pub fn new(progress_bar: ProgressBar, memory_tracker: Option<MemoryTracker>) -> Self {
        Self {
            progress_bar,
            memory_tracker,
            start_time: Instant::now(),
            last_update: Instant::now(),
            update_interval: Duration::from_millis(100),
            bytes_processed: 0,
            rows_processed: 0,
            columns_processed: 0,
        }
    }

    /// Update progress with enhanced metrics
    pub fn update(&mut self, position: u64, rows: u64, columns: u64, message: Option<&str>) {
        self.bytes_processed = position;
        self.rows_processed = rows;
        self.columns_processed = columns;

        let now = Instant::now();
        if now.duration_since(self.last_update) >= self.update_interval {
            self.progress_bar.set_position(position);

            // Calculate performance metrics
            let elapsed = now.duration_since(self.start_time).as_secs_f64();
            let mb_per_sec = if elapsed > 0.0 {
                (position as f64 / 1_048_576.0) / elapsed
            } else {
                0.0
            };
            let rows_per_sec = if elapsed > 0.0 {
                rows as f64 / elapsed
            } else {
                0.0
            };
            let cols_per_sec = if elapsed > 0.0 {
                columns as f64 / elapsed
            } else {
                0.0
            };

            // Get memory info if available
            let memory_info = self
                .memory_tracker
                .as_ref()
                .map(|tracker| tracker.get_memory_stats())
                .map(|(_, _, mb)| format!(" | Mem: {}MB", mb))
                .unwrap_or_default();

            let enhanced_message = format!(
                "{} | {:.1} MB/s | {:.0} rows/s | {:.0} cols/s{}",
                message.unwrap_or("Processing"),
                mb_per_sec,
                rows_per_sec,
                cols_per_sec,
                memory_info
            );

            self.progress_bar.set_message(enhanced_message);
            self.last_update = now;
        }
    }

    /// Update with performance hints
    pub fn update_with_hints(&mut self, position: u64, rows: u64, columns: u64, hints: Vec<&str>) {
        let hint_text = if !hints.is_empty() {
            format!(" 💡 {}", hints.join(" | "))
        } else {
            String::new()
        };

        let message = format!("Processing{}", hint_text);
        self.update(position, rows, columns, Some(&message));
    }

    /// Finish with summary
    pub fn finish_with_summary(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let final_mb_per_sec = if elapsed > 0.0 {
            (self.bytes_processed as f64 / 1_048_576.0) / elapsed
        } else {
            0.0
        };

        let summary = format!(
            "✅ Completed: {} rows, {} columns, {:.1} MB/s avg",
            self.rows_processed, self.columns_processed, final_mb_per_sec
        );

        self.progress_bar.finish_with_message(summary);
    }

    /// Check for memory leaks and report
    pub fn check_memory_leaks(&self) -> Option<String> {
        self.memory_tracker
            .as_ref()
            .map(|tracker| tracker.report_leaks())
    }
}

/// Generate performance hints based on current processing state
pub fn generate_performance_hints(
    file_size_mb: f64,
    rows_processed: u64,
    processing_speed: f64,
    memory_usage_mb: Option<usize>,
) -> Vec<String> {
    let mut hints = Vec::new();

    // File size based hints
    if file_size_mb > 100.0 && processing_speed < 1000.0 {
        hints.push("Consider --streaming for large files".to_string());
    }

    if file_size_mb > 500.0 {
        hints.push("Use --chunk-size for memory efficiency".to_string());
    }

    // Memory based hints
    if let Some(mem_mb) = memory_usage_mb {
        if mem_mb > 1000 {
            hints.push("High memory usage detected".to_string());
        }

        // Suggest streaming if memory usage is growing too fast
        if mem_mb as f64 > file_size_mb * 2.0 {
            hints.push("Consider streaming mode".to_string());
        }
    }

    // Performance based hints
    if processing_speed > 10000.0 {
        hints.push("Excellent processing speed".to_string());
    } else if processing_speed < 100.0 && rows_processed > 1000 {
        hints.push("Slow processing detected".to_string());
    }

    hints
}

/// Create a fancy startup banner
pub fn display_startup_banner(version: &str, colored: bool) {
    if colored {
        println!("🚀 DataProfiler CLI v{}", version);
        println!("   Fast CSV data profiling with ML readiness scoring");
    } else {
        println!("DataProfiler CLI v{}", version);
        println!("Fast CSV data profiling with ML readiness scoring");
    }
}

/// Display completion summary
pub fn display_completion_summary(files_processed: usize, total_time: Duration, colored: bool) {
    let time_str = if total_time.as_secs() > 60 {
        format!(
            "{}m {:.1}s",
            total_time.as_secs() / 60,
            total_time.as_secs_f64() % 60.0
        )
    } else {
        format!("{:.2}s", total_time.as_secs_f64())
    };

    if colored {
        println!(
            "\n🎉 ✨ Analysis complete! Processed {} files in {}",
            files_processed, time_str
        );
    } else {
        println!(
            "\nAnalysis complete! Processed {} files in {}",
            files_processed, time_str
        );
    }
}
