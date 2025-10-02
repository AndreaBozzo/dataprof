use crate::types::{ColumnProfile, ColumnStats, DataQualityMetrics, OutputFormat, QualityReport};
use anyhow::Result;
use is_terminal::IsTerminal;
use serde::Serialize;
use std::io::{stderr, stdout};

/// Trait for output formatting - enables modular output systems
pub trait OutputFormatter {
    fn format_report(&self, report: &QualityReport) -> Result<String>;
    fn format_profiles(&self, profiles: &[ColumnProfile]) -> Result<String>;
    fn format_simple_summary(&self, profiles: &[ColumnProfile]) -> Result<String>;
}

/// JSON formatter with enhanced structure
pub struct JsonFormatter;

/// CSV formatter for data processing pipelines
pub struct CsvFormatter;

/// Plain text formatter for scripting (no colors/formatting)
pub struct PlainFormatter;

/// Adaptive formatter that selects appropriate format based on terminal context
pub struct AdaptiveFormatter {
    pub is_interactive: bool,
    pub force_format: Option<OutputFormat>,
}

/// Context information for adaptive formatting
#[derive(Debug, Clone)]
pub struct OutputContext {
    pub is_terminal: bool,
    pub is_interactive: bool,
    pub is_ci_environment: bool,
    pub supports_color: bool,
    pub supports_unicode: bool,
}

impl OutputContext {
    /// Detect the current output context
    pub fn detect() -> Self {
        let is_terminal = stdout().is_terminal() && stderr().is_terminal();
        let is_ci_environment = std::env::var("CI").is_ok()
            || std::env::var("GITHUB_ACTIONS").is_ok()
            || std::env::var("JENKINS_URL").is_ok()
            || std::env::var("GITLAB_CI").is_ok();

        let supports_color = is_terminal
            && !is_ci_environment
            && std::env::var("NO_COLOR").is_err()
            && std::env::var("TERM").map_or(true, |term| term != "dumb");

        let supports_unicode = is_terminal
            && std::env::var("LC_ALL").unwrap_or_default().contains("UTF")
            || std::env::var("LANG").unwrap_or_default().contains("UTF");

        Self {
            is_terminal,
            is_interactive: is_terminal && !is_ci_environment,
            is_ci_environment,
            supports_color,
            supports_unicode,
        }
    }

    /// Determine the best output format for this context
    pub fn preferred_format(&self) -> OutputFormat {
        if self.is_ci_environment {
            OutputFormat::Json // Machine-readable for CI/CD
        } else if self.is_interactive {
            OutputFormat::Text // Rich text for interactive terminals
        } else {
            OutputFormat::Plain // Simple text for pipes/redirects
        }
    }
}

impl Default for AdaptiveFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveFormatter {
    pub fn new() -> Self {
        Self {
            is_interactive: OutputContext::detect().is_interactive,
            force_format: None,
        }
    }

    pub fn with_forced_format(format: OutputFormat) -> Self {
        Self {
            is_interactive: OutputContext::detect().is_interactive,
            force_format: Some(format),
        }
    }

    /// Get the appropriate formatter based on context
    fn get_formatter(&self, context: &OutputContext) -> Box<dyn OutputFormatter> {
        let default_format = context.preferred_format();
        let format = self.force_format.as_ref().unwrap_or(&default_format);

        match format {
            OutputFormat::Json => Box::new(JsonFormatter),
            OutputFormat::Csv => Box::new(CsvFormatter),
            OutputFormat::Plain => Box::new(PlainFormatter),
            OutputFormat::Text => {
                if context.is_interactive {
                    Box::new(InteractiveFormatter::new(context.clone()))
                } else {
                    Box::new(PlainFormatter)
                }
            }
        }
    }
}

/// Enhanced interactive formatter with colors and emojis
pub struct InteractiveFormatter {
    pub context: OutputContext,
}

impl InteractiveFormatter {
    pub fn new(context: OutputContext) -> Self {
        Self { context }
    }
}

/// Enhanced JSON structure for reports
#[derive(Serialize)]
pub struct JsonReport {
    pub metadata: JsonMetadata,
    pub quality: JsonQuality,
    pub columns: Vec<JsonColumn>,
    pub summary: JsonSummary,
    pub data_quality_metrics: Option<JsonDataQualityMetrics>,
}

/// Comprehensive data quality metrics following industry standards
#[derive(Serialize)]
pub struct JsonDataQualityMetrics {
    pub completeness: JsonCompletenessMetrics,
    pub consistency: JsonConsistencyMetrics,
    pub uniqueness: JsonUniquenessMetrics,
    pub accuracy: JsonAccuracyMetrics,
}

#[derive(Serialize)]
pub struct JsonCompletenessMetrics {
    pub missing_values_ratio: f64,
    pub complete_records_ratio: f64,
    pub null_columns: Vec<String>,
}

#[derive(Serialize)]
pub struct JsonConsistencyMetrics {
    pub data_type_consistency: f64,
    pub format_violations: usize,
    pub encoding_issues: usize,
}

#[derive(Serialize)]
pub struct JsonUniquenessMetrics {
    pub duplicate_rows: usize,
    pub key_uniqueness: f64,
    pub high_cardinality_warning: bool,
}

#[derive(Serialize)]
pub struct JsonAccuracyMetrics {
    pub outlier_ratio: f64,
    pub range_violations: usize,
    pub negative_values_in_positive: usize,
}

#[derive(Serialize)]
pub struct JsonMetadata {
    pub file_path: String,
    pub file_size_mb: f64,
    pub total_rows: Option<usize>,
    pub total_columns: usize,
    pub scan_time_ms: u128,
    pub sampling_info: Option<JsonSampling>,
}

#[derive(Serialize)]
pub struct JsonSampling {
    pub rows_scanned: usize,
    pub sampling_ratio: f64,
    pub was_sampled: bool,
}

#[derive(Serialize)]
pub struct JsonQuality {
    pub quality_score: f64,
}

#[derive(Serialize)]
pub struct JsonColumn {
    pub name: String,
    pub data_type: String,
    pub total_count: usize,
    pub null_count: usize,
    pub null_percentage: f64,
    pub stats: serde_json::Value,
    pub patterns: Vec<JsonPattern>,
}

#[derive(Serialize)]
pub struct JsonPattern {
    pub name: String,
    pub match_count: usize,
    pub match_percentage: f64,
}

#[derive(Serialize)]
pub struct JsonSummary {
    pub data_quality_score: f64,
    pub completeness_score: f64,
}

impl OutputFormatter for JsonFormatter {
    fn format_report(&self, report: &QualityReport) -> Result<String> {
        let json_report = JsonReport {
            metadata: JsonMetadata {
                file_path: report.file_info.path.clone(),
                file_size_mb: report.file_info.file_size_mb,
                total_rows: report.file_info.total_rows,
                total_columns: report.file_info.total_columns,
                scan_time_ms: report.scan_info.scan_time_ms,
                sampling_info: if report.scan_info.sampling_ratio < 1.0 {
                    Some(JsonSampling {
                        rows_scanned: report.scan_info.rows_scanned,
                        sampling_ratio: report.scan_info.sampling_ratio,
                        was_sampled: true,
                    })
                } else {
                    None
                },
            },
            quality: JsonQuality {
                quality_score: report.quality_score(),
            },
            columns: report
                .column_profiles
                .iter()
                .map(|p| self.format_column(p))
                .collect(),
            summary: JsonSummary {
                data_quality_score: report.quality_score(),
                completeness_score: self.calculate_completeness_score(&report.column_profiles),
            },
            data_quality_metrics: Some(
                self.format_data_quality_metrics(&report.data_quality_metrics),
            ),
        };

        Ok(serde_json::to_string_pretty(&json_report)?)
    }

    fn format_profiles(&self, profiles: &[ColumnProfile]) -> Result<String> {
        let columns: Vec<JsonColumn> = profiles.iter().map(|p| self.format_column(p)).collect();
        Ok(serde_json::to_string_pretty(&columns)?)
    }

    fn format_simple_summary(&self, profiles: &[ColumnProfile]) -> Result<String> {
        let summary = serde_json::json!({
            "columns": profiles.len(),
            "total_records": profiles.first().map_or(0, |p| p.total_count),
            "column_details": profiles.iter().map(|p| {
                serde_json::json!({
                    "name": p.name,
                    "type": format!("{:?}", p.data_type),
                    "null_percentage": (p.null_count as f64 / p.total_count as f64) * 100.0
                })
            }).collect::<Vec<_>>()
        });
        Ok(serde_json::to_string_pretty(&summary)?)
    }
}

impl JsonFormatter {
    fn format_column(&self, profile: &ColumnProfile) -> JsonColumn {
        let stats_json = match &profile.stats {
            ColumnStats::Numeric { min, max, mean } => {
                serde_json::json!({
                    "type": "numeric",
                    "min": min,
                    "max": max,
                    "mean": mean
                })
            }
            ColumnStats::Text {
                min_length,
                max_length,
                avg_length,
            } => {
                serde_json::json!({
                    "type": "text",
                    "min_length": min_length,
                    "max_length": max_length,
                    "avg_length": avg_length
                })
            }
        };

        JsonColumn {
            name: profile.name.clone(),
            data_type: format!("{:?}", profile.data_type),
            total_count: profile.total_count,
            null_count: profile.null_count,
            null_percentage: (profile.null_count as f64 / profile.total_count as f64) * 100.0,
            stats: stats_json,
            patterns: profile
                .patterns
                .iter()
                .map(|p| JsonPattern {
                    name: p.name.clone(),
                    match_count: p.match_count,
                    match_percentage: p.match_percentage,
                })
                .collect(),
        }
    }

    fn calculate_completeness_score(&self, profiles: &[ColumnProfile]) -> f64 {
        if profiles.is_empty() {
            return 100.0;
        }

        let total_cells: usize = profiles.iter().map(|p| p.total_count).sum();
        let null_cells: usize = profiles.iter().map(|p| p.null_count).sum();

        if total_cells == 0 {
            100.0
        } else {
            ((total_cells - null_cells) as f64 / total_cells as f64) * 100.0
        }
    }

    /// Format comprehensive data quality metrics for JSON output
    fn format_data_quality_metrics(&self, metrics: &DataQualityMetrics) -> JsonDataQualityMetrics {
        JsonDataQualityMetrics {
            completeness: JsonCompletenessMetrics {
                missing_values_ratio: metrics.missing_values_ratio,
                complete_records_ratio: metrics.complete_records_ratio,
                null_columns: metrics.null_columns.clone(),
            },
            consistency: JsonConsistencyMetrics {
                data_type_consistency: metrics.data_type_consistency,
                format_violations: metrics.format_violations,
                encoding_issues: metrics.encoding_issues,
            },
            uniqueness: JsonUniquenessMetrics {
                duplicate_rows: metrics.duplicate_rows,
                key_uniqueness: metrics.key_uniqueness,
                high_cardinality_warning: metrics.high_cardinality_warning,
            },
            accuracy: JsonAccuracyMetrics {
                outlier_ratio: metrics.outlier_ratio,
                range_violations: metrics.range_violations,
                negative_values_in_positive: metrics.negative_values_in_positive,
            },
        }
    }
}

impl OutputFormatter for CsvFormatter {
    fn format_report(&self, report: &QualityReport) -> Result<String> {
        let mut output = String::new();

        // Header
        output.push_str("column_name,data_type,total_count,null_count,null_percentage\n");

        // Data rows
        for profile in &report.column_profiles {
            let null_percentage = (profile.null_count as f64 / profile.total_count as f64) * 100.0;

            output.push_str(&format!(
                "{},{:?},{},{},{:.2}\n",
                profile.name,
                profile.data_type,
                profile.total_count,
                profile.null_count,
                null_percentage
            ));
        }

        Ok(output)
    }

    fn format_profiles(&self, profiles: &[ColumnProfile]) -> Result<String> {
        let mut output = String::new();
        output.push_str("column_name,data_type,total_count,null_count,null_percentage\n");

        for profile in profiles {
            let null_percentage = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
            output.push_str(&format!(
                "{},{:?},{},{},{:.2}\n",
                profile.name,
                profile.data_type,
                profile.total_count,
                profile.null_count,
                null_percentage
            ));
        }

        Ok(output)
    }

    fn format_simple_summary(&self, profiles: &[ColumnProfile]) -> Result<String> {
        self.format_profiles(profiles)
    }
}

impl OutputFormatter for PlainFormatter {
    fn format_report(&self, report: &QualityReport) -> Result<String> {
        let mut output = String::new();

        // File info
        output.push_str(&format!("File: {}\n", report.file_info.path));
        output.push_str(&format!("Size: {:.1} MB\n", report.file_info.file_size_mb));
        output.push_str(&format!("Columns: {}\n", report.file_info.total_columns));
        if let Some(rows) = report.file_info.total_rows {
            output.push_str(&format!("Rows: {}\n", rows));
        }
        output.push_str(&format!(
            "Scan time: {} ms\n\n",
            report.scan_info.scan_time_ms
        ));

        // Data Quality Metrics (ISO 8000/25012)
        let metrics = &report.data_quality_metrics;
        {
            output.push_str("Data Quality Metrics\n");
            output.push_str(&format!(
                "  Completeness: {:.1}% missing, {:.1}% complete\n",
                metrics.missing_values_ratio, metrics.complete_records_ratio
            ));
            output.push_str(&format!(
                "  Consistency: {:.1}% type consistency\n",
                metrics.data_type_consistency
            ));
            output.push_str(&format!(
                "  Uniqueness: {:.1}% key uniqueness\n",
                metrics.key_uniqueness
            ));
            output.push_str(&format!(
                "  Accuracy: {:.1}% outlier ratio\n",
                metrics.outlier_ratio
            ));

            if metrics.duplicate_rows > 0 {
                output.push_str(&format!("  Duplicate Rows: {}\n", metrics.duplicate_rows));
            }
            if metrics.format_violations > 0 {
                output.push_str(&format!(
                    "  Format Violations: {}\n",
                    metrics.format_violations
                ));
            }
            if metrics.encoding_issues > 0 {
                output.push_str(&format!("  Encoding Issues: {}\n", metrics.encoding_issues));
            }
            if !metrics.null_columns.is_empty() {
                output.push_str(&format!(
                    "  Null Columns: {}\n",
                    metrics.null_columns.join(", ")
                ));
            }
            output.push('\n');
        }

        // Column profiles
        output.push_str("Column Profiles\n");
        for profile in &report.column_profiles {
            output.push_str(&format!("Column: {}\n", profile.name));
            output.push_str(&format!("  Type: {:?}\n", profile.data_type));
            output.push_str(&format!("  Records: {}\n", profile.total_count));
            output.push_str(&format!("  Nulls: {}\n", profile.null_count));

            match &profile.stats {
                ColumnStats::Numeric { min, max, mean } => {
                    output.push_str(&format!("  Min: {:.2}\n", min));
                    output.push_str(&format!("  Max: {:.2}\n", max));
                    output.push_str(&format!("  Mean: {:.2}\n", mean));
                }
                ColumnStats::Text {
                    min_length,
                    max_length,
                    avg_length,
                } => {
                    output.push_str(&format!("  Min Length: {}\n", min_length));
                    output.push_str(&format!("  Max Length: {}\n", max_length));
                    output.push_str(&format!("  Avg Length: {:.1}\n", avg_length));
                }
            }
            output.push('\n');
        }

        Ok(output)
    }

    fn format_profiles(&self, profiles: &[ColumnProfile]) -> Result<String> {
        let mut output = String::new();

        for profile in profiles {
            output.push_str(&format!("Column: {}\n", profile.name));
            output.push_str(&format!("  Type: {:?}\n", profile.data_type));
            output.push_str(&format!("  Records: {}\n", profile.total_count));
            output.push_str(&format!("  Nulls: {}\n", profile.null_count));
            output.push('\n');
        }

        Ok(output)
    }

    fn format_simple_summary(&self, profiles: &[ColumnProfile]) -> Result<String> {
        self.format_profiles(profiles)
    }
}

impl OutputFormatter for InteractiveFormatter {
    fn format_report(&self, report: &QualityReport) -> Result<String> {
        use colored::*;
        let mut output = String::new();

        // Enhanced file info section with colors and emojis
        if self.context.supports_unicode {
            output.push_str(&format!(
                "📁 {} {}\n",
                "File:".bright_blue().bold(),
                report.file_info.path
            ));
            output.push_str(&format!(
                "📏 {} {:.1} MB\n",
                "Size:".bright_blue().bold(),
                report.file_info.file_size_mb
            ));
            output.push_str(&format!(
                "📊 {} {}\n",
                "Columns:".bright_blue().bold(),
                report.file_info.total_columns
            ));
        } else {
            output.push_str(&format!("File: {}\n", report.file_info.path));
            output.push_str(&format!("Size: {:.1} MB\n", report.file_info.file_size_mb));
            output.push_str(&format!("Columns: {}\n", report.file_info.total_columns));
        }

        if let Some(rows) = report.file_info.total_rows {
            if self.context.supports_unicode {
                output.push_str(&format!("📈 {} {}\n", "Rows:".bright_blue().bold(), rows));
            } else {
                output.push_str(&format!("Rows: {}\n", rows));
            }
        }

        // Performance info
        if self.context.supports_unicode {
            output.push_str(&format!(
                "⏱️  {} {} ms\n\n",
                "Scan time:".bright_blue().bold(),
                report.scan_info.scan_time_ms
            ));
        } else {
            output.push_str(&format!(
                "Scan time: {} ms\n\n",
                report.scan_info.scan_time_ms
            ));
        }

        // Column profiles with enhanced formatting
        if self.context.supports_unicode {
            output.push_str(&format!("📋 {}\n", "Column Profiles".bright_green().bold()));
        } else {
            output.push_str("Column Profiles\n");
        }

        for profile in &report.column_profiles {
            output.push_str(&format!("Column: {}\n", profile.name.bright_white().bold()));
            output.push_str(&format!("  Type: {:?}\n", profile.data_type));
            output.push_str(&format!("  Records: {}\n", profile.total_count));

            let null_percentage = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
            if null_percentage > 0.0 {
                output.push_str(&format!(
                    "  Nulls: {} ({:.1}%)\n",
                    profile.null_count, null_percentage
                ));
            } else {
                output.push_str(&format!("  Nulls: {}\n", profile.null_count));
            }

            match &profile.stats {
                ColumnStats::Numeric { min, max, mean } => {
                    output.push_str(&format!("  Min: {:.2}\n", min));
                    output.push_str(&format!("  Max: {:.2}\n", max));
                    output.push_str(&format!("  Mean: {:.2}\n", mean));
                }
                ColumnStats::Text {
                    min_length,
                    max_length,
                    avg_length,
                } => {
                    output.push_str(&format!("  Min Length: {}\n", min_length));
                    output.push_str(&format!("  Max Length: {}\n", max_length));
                    output.push_str(&format!("  Avg Length: {:.1}\n", avg_length));
                }
            }
            output.push('\n');
        }

        Ok(output)
    }

    fn format_profiles(&self, profiles: &[ColumnProfile]) -> Result<String> {
        use colored::*;
        let mut output = String::new();

        for profile in profiles {
            output.push_str(&format!("Column: {}\n", profile.name.bright_white().bold()));
            output.push_str(&format!("  Type: {:?}\n", profile.data_type));
            output.push_str(&format!("  Records: {}\n", profile.total_count));
            output.push_str(&format!("  Nulls: {}\n", profile.null_count));
            output.push('\n');
        }

        Ok(output)
    }

    fn format_simple_summary(&self, profiles: &[ColumnProfile]) -> Result<String> {
        self.format_profiles(profiles)
    }
}

impl OutputFormatter for AdaptiveFormatter {
    fn format_report(&self, report: &QualityReport) -> Result<String> {
        let context = OutputContext::detect();
        let formatter = self.get_formatter(&context);
        formatter.format_report(report)
    }

    fn format_profiles(&self, profiles: &[ColumnProfile]) -> Result<String> {
        let context = OutputContext::detect();
        let formatter = self.get_formatter(&context);
        formatter.format_profiles(profiles)
    }

    fn format_simple_summary(&self, profiles: &[ColumnProfile]) -> Result<String> {
        let context = OutputContext::detect();
        let formatter = self.get_formatter(&context);
        formatter.format_simple_summary(profiles)
    }
}

/// Factory function to create formatters
pub fn create_formatter(format: &str) -> Box<dyn OutputFormatter> {
    match format.to_lowercase().as_str() {
        "json" => Box::new(JsonFormatter),
        "csv" => Box::new(CsvFormatter),
        "plain" => Box::new(PlainFormatter),
        "adaptive" => Box::new(AdaptiveFormatter::new()),
        "interactive" => Box::new(InteractiveFormatter::new(OutputContext::detect())),
        _ => Box::new(JsonFormatter), // Default fallback
    }
}

/// Create adaptive formatter that auto-selects based on context
pub fn create_adaptive_formatter() -> Box<dyn OutputFormatter> {
    Box::new(AdaptiveFormatter::new())
}

/// Create adaptive formatter with forced format
pub fn create_adaptive_formatter_with_format(format: OutputFormat) -> Box<dyn OutputFormatter> {
    Box::new(AdaptiveFormatter::with_forced_format(format))
}

/// Output report with specified formatter
pub fn output_with_formatter(report: &QualityReport, format: &OutputFormat) -> Result<()> {
    let format_str = match format {
        OutputFormat::Json => "json",
        OutputFormat::Csv => "csv",
        OutputFormat::Plain => "plain",
        OutputFormat::Text => "text",
    };

    let formatter = create_formatter(format_str);
    let output = formatter.format_report(report)?;
    println!("{}", output);
    Ok(())
}

/// Adaptive output that automatically selects best format based on terminal context
pub fn output_with_adaptive_formatter(
    report: &QualityReport,
    force_format: Option<OutputFormat>,
) -> Result<()> {
    let formatter = if let Some(format) = force_format {
        create_adaptive_formatter_with_format(format)
    } else {
        create_adaptive_formatter()
    };

    let output = formatter.format_report(report)?;
    println!("{}", output);
    Ok(())
}

/// Check if current environment supports enhanced terminal features
pub fn supports_enhanced_output() -> bool {
    let context = OutputContext::detect();
    context.is_interactive && context.supports_color && context.supports_unicode
}

/// Get a hint about the best output format for current context
pub fn suggest_output_format() -> String {
    let context = OutputContext::detect();
    match context.preferred_format() {
        OutputFormat::Json => "JSON (machine-readable for CI/automated processing)".to_string(),
        OutputFormat::Text => "Interactive text (rich formatting for terminals)".to_string(),
        OutputFormat::Plain => "Plain text (simple format for pipes/scripts)".to_string(),
        OutputFormat::Csv => "CSV (tabular data format)".to_string(),
    }
}
