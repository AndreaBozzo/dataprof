use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::fmt::Write;
use std::time::{Duration, Instant};

/// Enhanced progress indicators for CLI operations
pub struct ProgressManager {
    pub show_progress: bool,
    pub verbosity: u8,
}

impl ProgressManager {
    pub fn new(show_progress: bool, verbosity: u8) -> Self {
        Self {
            show_progress,
            verbosity,
        }
    }

    /// Create a progress bar for file processing
    pub fn create_file_progress(&self, file_size: u64, file_name: &str) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}"
            )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("🔄🔵⚪"),
        );

        pb.set_message(format!("Processing {}", file_name));
        Some(pb)
    }

    /// Create a progress bar for batch processing
    pub fn create_batch_progress(&self, total_files: u64) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(total_files);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.yellow/orange}] {pos:>3}/{len:3} files ({eta}) {msg}"
            )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("📁📂⚪"),
        );

        pb.set_message("Processing files...");
        Some(pb)
    }

    /// Create a progress bar for ML analysis
    pub fn create_ml_progress(&self, total_steps: u64) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(total_steps);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.magenta} [{elapsed_precise}] [{wide_bar:.magenta/pink}] {pos:>2}/{len:2} {msg}"
            )
            .unwrap()
            .progress_chars("🤖🔮⚪"),
        );

        pb.set_message("ML Readiness Analysis...");
        Some(pb)
    }

    /// Create a progress bar for quality checking
    pub fn create_quality_progress(&self, total_columns: u64) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new(total_columns);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.blue} [{elapsed_precise}] [{wide_bar:.blue/cyan}] {pos:>3}/{len:3} columns {msg}"
            )
            .unwrap()
            .progress_chars("🔍🔎⚪"),
        );

        pb.set_message("Quality checking...");
        Some(pb)
    }

    /// Create an indeterminate spinner for unknown-duration tasks
    pub fn create_spinner(&self, message: &str) -> Option<ProgressBar> {
        if !self.show_progress {
            return None;
        }

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner:.green} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        pb.set_message(message.to_string());
        pb.enable_steady_tick(Duration::from_millis(80));
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
