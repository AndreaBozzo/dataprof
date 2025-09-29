use crate::analysis::MlReadinessScore;
use crate::types::{
    ColumnProfile, ColumnStats, DataQualityMetrics, OutputFormat, QualityIssue, QualityReport,
};
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
    pub quality_score: Option<f64>,
    pub issues: Vec<JsonIssue>,
    pub issue_summary: JsonIssueSummary,
}

#[derive(Serialize)]
pub struct JsonIssue {
    pub severity: String,
    pub category: String,
    pub column: Option<String>,
    pub description: String,
    pub details: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct JsonIssueSummary {
    pub total: usize,
    pub critical: usize,
    pub warning: usize,
    pub info: usize,
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
    pub data_quality_score: Option<f64>,
    pub completeness_score: f64,
    pub consistency_score: f64,
    pub ml_readiness: Option<JsonMlReadiness>,
}

#[derive(Serialize)]
pub struct JsonMlReadiness {
    pub score: f64,
    pub recommendations: Vec<String>,
    pub blocking_issues: Vec<String>,
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
            quality: self.format_quality_section(&report.issues, report.quality_score().ok()),
            columns: report
                .column_profiles
                .iter()
                .map(|p| self.format_column(p))
                .collect(),
            summary: JsonSummary {
                data_quality_score: report.quality_score().ok(),
                completeness_score: self.calculate_completeness_score(&report.column_profiles),
                consistency_score: self.calculate_consistency_score(&report.issues),
                ml_readiness: None, // Will be populated by ML engine
            },
            data_quality_metrics: report
                .data_quality_metrics
                .as_ref()
                .map(|metrics| self.format_data_quality_metrics(metrics)),
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
    fn format_quality_section(
        &self,
        issues: &[QualityIssue],
        quality_score: Option<f64>,
    ) -> JsonQuality {
        let mut critical = 0;
        let mut warning = 0;
        let mut info = 0;

        let json_issues: Vec<JsonIssue> = issues.iter().map(|issue| {
            let (severity, category, column, description, details) = match issue {
                QualityIssue::NullValues { column, count, percentage } => {
                    warning += 1;
                    ("warning", "completeness", Some(column.clone()),
                     format!("Null values detected: {} ({:.1}%)", count, percentage),
                     Some(serde_json::json!({"count": count, "percentage": percentage})))
                },
                QualityIssue::MixedDateFormats { column, formats } => {
                    critical += 1;
                    ("critical", "consistency", Some(column.clone()),
                     "Mixed date formats detected".to_string(),
                     Some(serde_json::json!({"formats": formats})))
                },
                QualityIssue::Duplicates { column, count } => {
                    info += 1;
                    ("info", "uniqueness", Some(column.clone()),
                     format!("Duplicate values: {}", count),
                     Some(serde_json::json!({"count": count})))
                },
                QualityIssue::Outliers { column, values, threshold } => {
                    warning += 1;
                    ("warning", "validity", Some(column.clone()),
                     format!("Outliers detected: {} values (>{}σ)", values.len(), threshold),
                     Some(serde_json::json!({"count": values.len(), "threshold": threshold, "sample_values": values.iter().take(5).collect::<Vec<_>>()})))
                },
                QualityIssue::MixedTypes { column, types } => {
                    critical += 1;
                    ("critical", "consistency", Some(column.clone()),
                     "Mixed data types detected".to_string(),
                     Some(serde_json::json!({"types": types})))
                },
            };

            JsonIssue {
                severity: severity.to_string(),
                category: category.to_string(),
                column,
                description,
                details,
            }
        }).collect();

        JsonQuality {
            quality_score,
            issues: json_issues,
            issue_summary: JsonIssueSummary {
                total: issues.len(),
                critical,
                warning,
                info,
            },
        }
    }

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

    fn calculate_consistency_score(&self, issues: &[QualityIssue]) -> f64 {
        let critical_issues = issues
            .iter()
            .filter(|issue| {
                matches!(
                    issue,
                    QualityIssue::MixedDateFormats { .. } | QualityIssue::MixedTypes { .. }
                )
            })
            .count();

        if critical_issues == 0 {
            100.0
        } else {
            std::cmp::max(0, 100 - (critical_issues * 20)) as f64
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
        output.push_str(
            "column_name,data_type,total_count,null_count,null_percentage,has_quality_issues\n",
        );

        // Create issue map for quick lookup
        let issue_columns: std::collections::HashSet<String> = report
            .issues
            .iter()
            .map(|issue| match issue {
                QualityIssue::NullValues { column, .. }
                | QualityIssue::MixedDateFormats { column, .. }
                | QualityIssue::Duplicates { column, .. }
                | QualityIssue::Outliers { column, .. }
                | QualityIssue::MixedTypes { column, .. } => column.clone(),
            })
            .collect();

        // Data rows
        for profile in &report.column_profiles {
            let null_percentage = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
            let has_issues = issue_columns.contains(&profile.name);

            output.push_str(&format!(
                "{},{:?},{},{},{:.2},{}\n",
                profile.name,
                profile.data_type,
                profile.total_count,
                profile.null_count,
                null_percentage,
                has_issues
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

        // Quality issues
        if !report.issues.is_empty() {
            output.push_str(&format!("Quality Issues ({})\n", report.issues.len()));
            for (i, issue) in report.issues.iter().enumerate() {
                output.push_str(&format!("{}. ", i + 1));
                match issue {
                    QualityIssue::NullValues {
                        column,
                        count,
                        percentage,
                    } => {
                        output.push_str(&format!(
                            "[{}] {} null values ({:.1}%)\n",
                            column, count, percentage
                        ));
                    }
                    QualityIssue::MixedDateFormats { column, .. } => {
                        output.push_str(&format!("[{}] Mixed date formats\n", column));
                    }
                    QualityIssue::Duplicates { column, count } => {
                        output.push_str(&format!("[{}] {} duplicates\n", column, count));
                    }
                    QualityIssue::Outliers {
                        column,
                        values,
                        threshold,
                    } => {
                        output.push_str(&format!(
                            "[{}] {} outliers (>{}σ)\n",
                            column,
                            values.len(),
                            threshold
                        ));
                    }
                    QualityIssue::MixedTypes { column, .. } => {
                        output.push_str(&format!("[{}] Mixed data types\n", column));
                    }
                }
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

        // Quality issues with enhanced formatting
        if !report.issues.is_empty() {
            if self.context.supports_unicode {
                output.push_str(&format!(
                    "⚠️  {} ({})\n",
                    "Quality Issues".bright_yellow().bold(),
                    report.issues.len()
                ));
            } else {
                output.push_str(&format!("Quality Issues ({})\n", report.issues.len()));
            }

            for (i, issue) in report.issues.iter().enumerate() {
                let severity_indicator = if self.context.supports_unicode {
                    match issue {
                        QualityIssue::MixedDateFormats { .. } | QualityIssue::MixedTypes { .. } => {
                            "🚨"
                        }
                        QualityIssue::NullValues { .. } | QualityIssue::Outliers { .. } => "⚠️ ",
                        QualityIssue::Duplicates { .. } => "ℹ️ ",
                    }
                } else {
                    ""
                };

                output.push_str(&format!("{}{}. ", severity_indicator, i + 1));

                match issue {
                    QualityIssue::NullValues {
                        column,
                        count,
                        percentage,
                    } => {
                        output.push_str(&format!(
                            "[{}] {} null values ({:.1}%)\n",
                            column.bright_cyan(),
                            count,
                            percentage
                        ));
                    }
                    QualityIssue::MixedDateFormats { column, .. } => {
                        output
                            .push_str(&format!("[{}] Mixed date formats\n", column.bright_cyan()));
                    }
                    QualityIssue::Duplicates { column, count } => {
                        output.push_str(&format!(
                            "[{}] {} duplicates\n",
                            column.bright_cyan(),
                            count
                        ));
                    }
                    QualityIssue::Outliers {
                        column,
                        values,
                        threshold,
                    } => {
                        output.push_str(&format!(
                            "[{}] {} outliers (>{}σ)\n",
                            column.bright_cyan(),
                            values.len(),
                            threshold
                        ));
                    }
                    QualityIssue::MixedTypes { column, .. } => {
                        output.push_str(&format!("[{}] Mixed data types\n", column.bright_cyan()));
                    }
                }
            }
            output.push('\n');
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

pub fn output_with_formatter(
    report: &QualityReport,
    format: &OutputFormat,
    ml_score: Option<&MlReadinessScore>,
) -> Result<()> {
    let format_str = match format {
        OutputFormat::Json => "json",
        OutputFormat::Csv => "csv",
        OutputFormat::Plain => "plain",
        OutputFormat::Text => "text", // Fallback
    };

    let formatter = create_formatter(format_str);
    let mut output = formatter.format_report(report)?;

    // Add ML score to JSON output if available
    if matches!(format, OutputFormat::Json) && ml_score.is_some() {
        let mut json_value: serde_json::Value = serde_json::from_str(&output)?;
        if let Some(summary) = json_value.get_mut("summary") {
            if let Some(score) = ml_score {
                summary["ml_readiness"] = serde_json::json!({
                    "score": score.overall_score,
                    "level": score.readiness_level,
                    "recommendations": score.recommendations,
                    "feature_analysis": score.feature_analysis
                });
            }
        }
        output = serde_json::to_string_pretty(&json_value)?;
    }

    println!("{}", output);
    Ok(())
}

/// Adaptive output that automatically selects best format based on terminal context
pub fn output_with_adaptive_formatter(
    report: &QualityReport,
    ml_score: Option<&MlReadinessScore>,
    force_format: Option<OutputFormat>,
) -> Result<()> {
    let effective_format = force_format
        .clone()
        .unwrap_or_else(|| OutputContext::detect().preferred_format());

    let formatter = if let Some(format) = force_format {
        create_adaptive_formatter_with_format(format)
    } else {
        create_adaptive_formatter()
    };

    let mut output = formatter.format_report(report)?;

    if matches!(effective_format, OutputFormat::Json) && ml_score.is_some() {
        let mut json_value: serde_json::Value = serde_json::from_str(&output)?;
        if let Some(summary) = json_value.get_mut("summary") {
            if let Some(score) = ml_score {
                summary["ml_readiness"] = serde_json::json!({
                    "score": score.overall_score,
                    "level": score.readiness_level,
                    "recommendations": score.recommendations,
                    "feature_analysis": score.feature_analysis
                });
            }
        }
        output = serde_json::to_string_pretty(&json_value)?;
    }

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
