use std::time::{Duration, Instant};
use sysinfo::System;

use crate::core::memory_tracker::MemoryTracker;

/// Real-time performance intelligence system
pub struct PerformanceIntelligence {
    system: System,
    start_time: Instant,
    last_analysis: Instant,
    analysis_interval: Duration,
    recommendations: Vec<PerformanceRecommendation>,
}

/// Performance recommendation with priority and context
#[derive(Debug, Clone)]
pub struct PerformanceRecommendation {
    pub category: RecommendationCategory,
    pub priority: RecommendationPriority,
    pub message: String,
    pub technical_details: String,
    pub estimated_impact: ImpactLevel,
    pub auto_applicable: bool,
}

/// Categories of performance recommendations
#[derive(Debug, Clone, PartialEq)]
pub enum RecommendationCategory {
    Memory,
    Processing,
    FileSize,
    Algorithm,
    System,
    Configuration,
}

/// Priority levels for recommendations
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecommendationPriority {
    Critical,
    High,
    Medium,
    Low,
    Info,
}

/// Expected performance impact
#[derive(Debug, Clone, PartialEq)]
pub enum ImpactLevel {
    High,    // >50% improvement expected
    Medium,  // 20-50% improvement
    Low,     // 5-20% improvement
    Minimal, // <5% improvement
}

/// Current performance metrics
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub processing_speed: f64,    // rows/sec
    pub throughput_mbps: f64,     // MB/s
    pub memory_usage_mb: usize,   // Current memory usage
    pub cpu_usage_percent: f64,   // Current CPU usage
    pub available_memory_mb: u64, // Available system memory
    pub file_size_mb: f64,        // File being processed
    pub rows_processed: u64,      // Total rows processed
    pub columns_processed: u64,   // Total columns processed
    pub elapsed_time: Duration,   // Time since processing started
}

impl PerformanceIntelligence {
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();

        Self {
            system,
            start_time: Instant::now(),
            last_analysis: Instant::now(),
            analysis_interval: Duration::from_secs(5), // Analyze every 5 seconds
            recommendations: Vec::new(),
        }
    }

    /// Analyze current performance and generate recommendations
    pub fn analyze_performance(
        &mut self,
        file_size_mb: f64,
        rows_processed: u64,
        columns_processed: u64,
        memory_tracker: Option<&MemoryTracker>,
    ) -> PerformanceMetrics {
        let now = Instant::now();
        let elapsed = now.duration_since(self.start_time);

        // Refresh system information
        self.system.refresh_memory();
        self.system.refresh_cpu_all();

        let processing_speed = if elapsed.as_secs_f64() > 0.0 {
            rows_processed as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let throughput_mbps = if elapsed.as_secs_f64() > 0.0 {
            file_size_mb / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let memory_usage_mb = memory_tracker
            .map(|tracker| tracker.get_memory_stats().2)
            .unwrap_or(0);

        let available_memory_mb = self.system.available_memory() / 1_048_576; // Convert to MB
        let cpu_usage_percent = self.system.global_cpu_usage() as f64;

        let metrics = PerformanceMetrics {
            processing_speed,
            throughput_mbps,
            memory_usage_mb,
            cpu_usage_percent,
            available_memory_mb,
            file_size_mb,
            rows_processed,
            columns_processed,
            elapsed_time: elapsed,
        };

        // Generate recommendations if enough time has passed
        if now.duration_since(self.last_analysis) >= self.analysis_interval {
            self.generate_recommendations(&metrics);
            self.last_analysis = now;
        }

        metrics
    }

    /// Generate intelligent performance recommendations
    fn generate_recommendations(&mut self, metrics: &PerformanceMetrics) {
        self.recommendations.clear();

        // Memory-based recommendations
        self.analyze_memory_performance(metrics);

        // Processing speed recommendations
        self.analyze_processing_performance(metrics);

        // File size optimization recommendations
        self.analyze_file_size_performance(metrics);

        // System resource recommendations
        self.analyze_system_performance(metrics);

        // Algorithm selection recommendations
        self.analyze_algorithm_performance(metrics);

        // Sort recommendations by priority
        self.recommendations
            .sort_by(|a, b| a.priority.cmp(&b.priority));
    }

    fn analyze_memory_performance(&mut self, metrics: &PerformanceMetrics) {
        let memory_ratio = metrics.memory_usage_mb as f64 / metrics.available_memory_mb as f64;

        if memory_ratio > 0.8 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Memory,
                priority: RecommendationPriority::Critical,
                message: "Memory usage critical - enable streaming mode".to_string(),
                technical_details: format!(
                    "Using {:.1}% of available memory ({} MB / {} MB)",
                    memory_ratio * 100.0,
                    metrics.memory_usage_mb,
                    metrics.available_memory_mb
                ),
                estimated_impact: ImpactLevel::High,
                auto_applicable: true,
            });
        } else if memory_ratio > 0.6 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Memory,
                priority: RecommendationPriority::High,
                message: "High memory usage - consider streaming or chunking".to_string(),
                technical_details: format!(
                    "Using {:.1}% of available memory",
                    memory_ratio * 100.0
                ),
                estimated_impact: ImpactLevel::Medium,
                auto_applicable: false,
            });
        }

        if metrics.file_size_mb > 500.0 && memory_ratio > 0.4 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Memory,
                priority: RecommendationPriority::Medium,
                message: "Large file detected - streaming recommended".to_string(),
                technical_details: format!(
                    "File size: {:.1} MB, Memory usage: {:.1}%",
                    metrics.file_size_mb,
                    memory_ratio * 100.0
                ),
                estimated_impact: ImpactLevel::Medium,
                auto_applicable: false,
            });
        }
    }

    fn analyze_processing_performance(&mut self, metrics: &PerformanceMetrics) {
        if metrics.processing_speed < 100.0 && metrics.rows_processed > 1000 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Processing,
                priority: RecommendationPriority::High,
                message: "Slow processing detected - consider optimization".to_string(),
                technical_details: format!(
                    "Current speed: {:.1} rows/sec",
                    metrics.processing_speed
                ),
                estimated_impact: ImpactLevel::High,
                auto_applicable: false,
            });
        }

        if metrics.throughput_mbps < 1.0 && metrics.file_size_mb > 10.0 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Processing,
                priority: RecommendationPriority::Medium,
                message: "Low throughput - check I/O performance".to_string(),
                technical_details: format!(
                    "Current throughput: {:.2} MB/s",
                    metrics.throughput_mbps
                ),
                estimated_impact: ImpactLevel::Medium,
                auto_applicable: false,
            });
        }

        if metrics.processing_speed > 10000.0 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Processing,
                priority: RecommendationPriority::Info,
                message: "Excellent processing speed".to_string(),
                technical_details: format!(
                    "Processing at {:.1} rows/sec",
                    metrics.processing_speed
                ),
                estimated_impact: ImpactLevel::Minimal,
                auto_applicable: false,
            });
        }
    }

    fn analyze_file_size_performance(&mut self, metrics: &PerformanceMetrics) {
        if metrics.file_size_mb > 1000.0 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::FileSize,
                priority: RecommendationPriority::Medium,
                message: "Very large file - consider preprocessing".to_string(),
                technical_details: format!(
                    "File size: {:.1} MB - consider splitting or sampling",
                    metrics.file_size_mb
                ),
                estimated_impact: ImpactLevel::High,
                auto_applicable: false,
            });
        }

        let estimated_completion = if metrics.processing_speed > 0.0 {
            let remaining_mb = metrics.file_size_mb
                - (metrics.throughput_mbps * metrics.elapsed_time.as_secs_f64());
            if remaining_mb > 0.0 {
                Some(Duration::from_secs_f64(
                    remaining_mb / metrics.throughput_mbps,
                ))
            } else {
                None
            }
        } else {
            None
        };

        if let Some(completion_time) = estimated_completion {
            if completion_time > Duration::from_secs(300) {
                // More than 5 minutes
                self.recommendations.push(PerformanceRecommendation {
                    category: RecommendationCategory::FileSize,
                    priority: RecommendationPriority::High,
                    message: "Long processing time expected".to_string(),
                    technical_details: format!(
                        "Estimated completion: {:.1} minutes",
                        completion_time.as_secs_f64() / 60.0
                    ),
                    estimated_impact: ImpactLevel::Medium,
                    auto_applicable: false,
                });
            }
        }
    }

    fn analyze_system_performance(&mut self, metrics: &PerformanceMetrics) {
        if metrics.cpu_usage_percent > 90.0 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::System,
                priority: RecommendationPriority::High,
                message: "High CPU usage - system may be overloaded".to_string(),
                technical_details: format!("CPU usage: {:.1}%", metrics.cpu_usage_percent),
                estimated_impact: ImpactLevel::Medium,
                auto_applicable: false,
            });
        }

        if metrics.available_memory_mb < 1024 {
            // Less than 1GB available
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::System,
                priority: RecommendationPriority::Critical,
                message: "Low system memory - immediate action required".to_string(),
                technical_details: format!("Available memory: {} MB", metrics.available_memory_mb),
                estimated_impact: ImpactLevel::High,
                auto_applicable: false,
            });
        }
    }

    fn analyze_algorithm_performance(&mut self, metrics: &PerformanceMetrics) {
        // Suggest streaming for large files
        if metrics.file_size_mb > 100.0
            && metrics.memory_usage_mb as f64 > metrics.file_size_mb * 0.5
        {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Algorithm,
                priority: RecommendationPriority::Medium,
                message: "Consider streaming algorithm for memory efficiency".to_string(),
                technical_details: "Memory usage exceeds 50% of file size".to_string(),
                estimated_impact: ImpactLevel::High,
                auto_applicable: true,
            });
        }

        // Suggest chunking for very wide datasets
        if metrics.columns_processed > 1000 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Algorithm,
                priority: RecommendationPriority::Medium,
                message: "Wide dataset detected - consider column chunking".to_string(),
                technical_details: format!("Processing {} columns", metrics.columns_processed),
                estimated_impact: ImpactLevel::Medium,
                auto_applicable: false,
            });
        }

        // Suggest sampling for exploratory analysis
        if metrics.rows_processed > 1_000_000 && metrics.processing_speed < 1000.0 {
            self.recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Algorithm,
                priority: RecommendationPriority::Low,
                message: "Large dataset - consider sampling for exploration".to_string(),
                technical_details: format!(
                    "Processing {} rows at {:.1} rows/sec",
                    metrics.rows_processed, metrics.processing_speed
                ),
                estimated_impact: ImpactLevel::High,
                auto_applicable: false,
            });
        }
    }

    /// Get current recommendations
    pub fn get_recommendations(&self) -> &[PerformanceRecommendation] {
        &self.recommendations
    }

    /// Get high-priority recommendations as hint strings
    pub fn get_performance_hints(&self) -> Vec<String> {
        self.recommendations
            .iter()
            .filter(|rec| {
                matches!(
                    rec.priority,
                    RecommendationPriority::Critical | RecommendationPriority::High
                )
            })
            .map(|rec| rec.message.clone())
            .collect()
    }

    /// Get a performance summary report
    pub fn generate_performance_report(&self, metrics: &PerformanceMetrics) -> String {
        let mut report = String::new();

        report.push_str("📊 Performance Analysis Report\n");
        report.push_str(&format!(
            "Processing Speed: {:.1} rows/sec\n",
            metrics.processing_speed
        ));
        report.push_str(&format!(
            "Throughput: {:.2} MB/s\n",
            metrics.throughput_mbps
        ));
        report.push_str(&format!(
            "Memory Usage: {} MB ({:.1}% of available)\n",
            metrics.memory_usage_mb,
            metrics.memory_usage_mb as f64 / metrics.available_memory_mb as f64 * 100.0
        ));
        report.push_str(&format!("CPU Usage: {:.1}%\n", metrics.cpu_usage_percent));
        report.push_str(&format!(
            "Elapsed Time: {:.1}s\n",
            metrics.elapsed_time.as_secs_f64()
        ));

        if !self.recommendations.is_empty() {
            report.push_str(&format!(
                "\n💡 Recommendations ({}):\n",
                self.recommendations.len()
            ));
            for (i, rec) in self.recommendations.iter().enumerate() {
                report.push_str(&format!(
                    "{}. [{}] {}\n",
                    i + 1,
                    format!("{:?}", rec.priority).to_uppercase(),
                    rec.message
                ));
            }
        }

        report
    }
}

impl Default for PerformanceIntelligence {
    fn default() -> Self {
        Self::new()
    }
}
