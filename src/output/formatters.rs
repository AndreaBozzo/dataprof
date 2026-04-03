use crate::types::{
    ColumnProfile, ColumnStats, DataSource, OutputFormat, ProfileReport, QualityMetrics,
};
use anyhow::Result;
use is_terminal::IsTerminal;
use serde::Serialize;
use std::io::{stderr, stdout};

/// Compute null percentage, returning 0.0 for empty datasets (avoids NaN).
fn null_pct(profile: &ColumnProfile) -> f64 {
    if profile.total_count == 0 {
        0.0
    } else {
        (profile.null_count as f64 / profile.total_count as f64) * 100.0
    }
}

/// Trait for output formatting - enables modular output systems
pub trait OutputFormatter {
    fn format_report(&self, report: &ProfileReport) -> Result<String>;
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

    pub fn format(format: OutputFormat) -> Self {
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

/// Enhanced JSON structure for reports following separation of concerns
/// Quality metrics are optional (ISO 8000/25012)
#[derive(Serialize)]
pub struct JsonReport {
    pub metadata: JsonMetadata,
    pub columns: Vec<JsonColumn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overall_quality_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_quality_metrics: Option<JsonDataQualityMetrics>,
}

/// Comprehensive data quality metrics following industry standards (ISO 8000/25012)
/// Only computed dimensions are serialized; skipped dimensions are omitted.
#[derive(Serialize)]
pub struct JsonDataQualityMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completeness: Option<JsonCompletenessMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistency: Option<JsonConsistencyMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniqueness: Option<JsonUniquenessMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accuracy: Option<JsonAccuracyMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeliness: Option<JsonTimelinessMetrics>,
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
pub struct JsonTimelinessMetrics {
    pub future_dates_count: usize,
    pub stale_data_ratio: f64,
    pub temporal_violations: usize,
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

impl OutputFormatter for JsonFormatter {
    fn format_report(&self, report: &ProfileReport) -> Result<String> {
        let json_report = JsonReport {
            metadata: JsonMetadata {
                file_path: report.data_source.identifier(),
                file_size_mb: report.data_source.size_mb().unwrap_or(0.0),
                total_rows: Some(report.execution.rows_processed),
                total_columns: report.execution.columns_detected,
                scan_time_ms: report.execution.scan_time_ms,
                sampling_info: if report.execution.sampling_applied {
                    Some(JsonSampling {
                        rows_scanned: report.execution.rows_processed,
                        sampling_ratio: report.execution.sampling_ratio.unwrap_or(1.0),
                        was_sampled: true,
                    })
                } else {
                    None
                },
            },
            columns: report
                .column_profiles
                .iter()
                .map(|p| self.format_column(p))
                .collect(),
            overall_quality_score: report.quality_score(),
            data_quality_metrics: report
                .quality
                .as_ref()
                .map(|q| self.format_data_quality_metrics(&q.metrics)),
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
                    "null_percentage": null_pct(p)
                })
            }).collect::<Vec<_>>()
        });
        Ok(serde_json::to_string_pretty(&summary)?)
    }
}

impl JsonFormatter {
    fn format_column(&self, profile: &ColumnProfile) -> JsonColumn {
        // Serialize the stats using serde to respect custom serializers
        let serialized =
            serde_json::to_value(&profile.stats).unwrap_or_else(|_| serde_json::json!({}));

        // Extract the inner object from the enum variant
        let mut stats_json = match serialized {
            serde_json::Value::Object(mut map) => {
                // serde serializes enums as {"Variant": {fields}}
                // Extract the inner object
                map.values_mut()
                    .next()
                    .and_then(|v| v.as_object_mut())
                    .map(|inner| inner.clone())
                    .map(serde_json::Value::Object)
                    .unwrap_or_else(|| serde_json::json!({}))
            }
            _ => serde_json::json!({}),
        };

        // Add type field
        let type_name = match &profile.stats {
            ColumnStats::Numeric(..) => "numeric",
            ColumnStats::Text(..) => "text",
            ColumnStats::DateTime(..) => "datetime",
            ColumnStats::Boolean(..) => "boolean",
        };

        if let serde_json::Value::Object(ref mut map) = stats_json {
            map.insert(
                "type".to_string(),
                serde_json::Value::String(type_name.to_string()),
            );
        }

        JsonColumn {
            name: profile.name.clone(),
            data_type: format!("{:?}", profile.data_type),
            total_count: profile.total_count,
            null_count: profile.null_count,
            null_percentage: null_pct(profile),
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

    /// Format comprehensive data quality metrics for JSON output (ISO 8000/25012).
    /// Only dimensions that were actually computed are included.
    fn format_data_quality_metrics(&self, metrics: &QualityMetrics) -> JsonDataQualityMetrics {
        JsonDataQualityMetrics {
            completeness: metrics
                .completeness
                .as_ref()
                .map(|c| JsonCompletenessMetrics {
                    missing_values_ratio: c.missing_values_ratio,
                    complete_records_ratio: c.complete_records_ratio,
                    null_columns: c.null_columns.clone(),
                }),
            consistency: metrics
                .consistency
                .as_ref()
                .map(|c| JsonConsistencyMetrics {
                    data_type_consistency: c.data_type_consistency,
                    format_violations: c.format_violations,
                    encoding_issues: c.encoding_issues,
                }),
            uniqueness: metrics.uniqueness.as_ref().map(|u| JsonUniquenessMetrics {
                duplicate_rows: u.duplicate_rows,
                key_uniqueness: u.key_uniqueness,
                high_cardinality_warning: u.high_cardinality_warning,
            }),
            accuracy: metrics.accuracy.as_ref().map(|a| JsonAccuracyMetrics {
                outlier_ratio: a.outlier_ratio,
                range_violations: a.range_violations,
                negative_values_in_positive: a.negative_values_in_positive,
            }),
            timeliness: metrics.timeliness.as_ref().map(|t| JsonTimelinessMetrics {
                future_dates_count: t.future_dates_count,
                stale_data_ratio: t.stale_data_ratio,
                temporal_violations: t.temporal_violations,
            }),
        }
    }
}

const CSV_HEADER: &str = "column_name,data_type,total_count,null_count,null_percentage,\
unique_count,min,max,mean,std_dev,median,\
min_length,max_length,avg_length,\
true_count,false_count,true_ratio\n";

fn csv_opt_f64(v: Option<f64>) -> String {
    match v {
        Some(f) if f.is_finite() => format!("{:.4}", f),
        _ => String::new(),
    }
}

fn csv_opt_usize(v: Option<usize>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => String::new(),
    }
}

fn format_csv_row(profile: &ColumnProfile) -> String {
    let (min, max, mean, std_dev, median) = match &profile.stats {
        ColumnStats::Numeric(n) => (
            Some(n.min),
            Some(n.max),
            Some(n.mean),
            Some(n.std_dev),
            n.median,
        ),
        _ => (None, None, None, None, None),
    };
    let (min_length, max_length, avg_length) = match &profile.stats {
        ColumnStats::Text(t) => (Some(t.min_length), Some(t.max_length), Some(t.avg_length)),
        _ => (None, None, None),
    };
    let (true_count, false_count, true_ratio) = match &profile.stats {
        ColumnStats::Boolean(b) => (Some(b.true_count), Some(b.false_count), Some(b.true_ratio)),
        _ => (None, None, None),
    };

    format!(
        "{},{:?},{},{},{:.2},{},{},{},{},{},{},{},{},{},{},{},{}\n",
        profile.name,
        profile.data_type,
        profile.total_count,
        profile.null_count,
        null_pct(profile),
        csv_opt_usize(profile.unique_count),
        csv_opt_f64(min),
        csv_opt_f64(max),
        csv_opt_f64(mean),
        csv_opt_f64(std_dev),
        csv_opt_f64(median),
        csv_opt_usize(min_length),
        csv_opt_usize(max_length),
        csv_opt_f64(avg_length),
        csv_opt_usize(true_count),
        csv_opt_usize(false_count),
        csv_opt_f64(true_ratio),
    )
}

impl OutputFormatter for CsvFormatter {
    fn format_report(&self, report: &ProfileReport) -> Result<String> {
        let mut output = String::from(CSV_HEADER);
        for profile in &report.column_profiles {
            output.push_str(&format_csv_row(profile));
        }
        Ok(output)
    }

    fn format_profiles(&self, profiles: &[ColumnProfile]) -> Result<String> {
        let mut output = String::from(CSV_HEADER);
        for profile in profiles {
            output.push_str(&format_csv_row(profile));
        }
        Ok(output)
    }

    fn format_simple_summary(&self, profiles: &[ColumnProfile]) -> Result<String> {
        self.format_profiles(profiles)
    }
}

impl OutputFormatter for PlainFormatter {
    fn format_report(&self, report: &ProfileReport) -> Result<String> {
        let mut output = String::new();

        // Source info
        output.push_str(&format!("Source: {}\n", report.data_source.identifier()));
        if let Some(size_mb) = report.data_source.size_mb() {
            output.push_str(&format!("Size: {:.1} MB\n", size_mb));
        }
        output.push_str(&format!("Columns: {}\n", report.execution.columns_detected));
        output.push_str(&format!("Rows: {}\n", report.execution.rows_processed));
        output.push_str(&format!(
            "Scan time: {} ms\n\n",
            report.execution.scan_time_ms
        ));

        // Data Quality Metrics (ISO 8000/25012) — only computed dimensions
        if let Some(ref quality) = report.quality {
            let metrics = &quality.metrics;
            output.push_str("Data Quality Metrics\n");

            if let Some(ref c) = metrics.completeness {
                output.push_str(&format!(
                    "  Completeness: {:.1}% missing, {:.1}% complete\n",
                    c.missing_values_ratio, c.complete_records_ratio
                ));
                if !c.null_columns.is_empty() {
                    output.push_str(&format!("  Null Columns: {}\n", c.null_columns.join(", ")));
                }
            }
            if let Some(ref c) = metrics.consistency {
                output.push_str(&format!(
                    "  Consistency: {:.1}% type consistency\n",
                    c.data_type_consistency
                ));
                if c.format_violations > 0 {
                    output.push_str(&format!("  Format Violations: {}\n", c.format_violations));
                }
                if c.encoding_issues > 0 {
                    output.push_str(&format!("  Encoding Issues: {}\n", c.encoding_issues));
                }
            }
            if let Some(ref u) = metrics.uniqueness {
                output.push_str(&format!(
                    "  Uniqueness: {:.1}% key uniqueness\n",
                    u.key_uniqueness
                ));
                if u.duplicate_rows > 0 {
                    output.push_str(&format!("  Duplicate Rows: {}\n", u.duplicate_rows));
                }
            }
            if let Some(ref a) = metrics.accuracy {
                output.push_str(&format!(
                    "  Accuracy: {:.1}% outlier ratio\n",
                    a.outlier_ratio
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
                ColumnStats::Numeric(n) => {
                    output.push_str(&format!("  Min: {:.2}\n", n.min));
                    output.push_str(&format!("  Max: {:.2}\n", n.max));
                    output.push_str(&format!("  Mean: {:.2}\n", n.mean));
                    output.push_str(&format!("  Std Dev: {:.2}\n", n.std_dev));
                    if let Some(med) = n.median {
                        output.push_str(&format!("  Median: {:.2}\n", med));
                    }
                }
                ColumnStats::Text(t) => {
                    output.push_str(&format!("  Min Length: {}\n", t.min_length));
                    output.push_str(&format!("  Max Length: {}\n", t.max_length));
                    output.push_str(&format!("  Avg Length: {:.1}\n", t.avg_length));
                }
                ColumnStats::DateTime(d) => {
                    output.push_str(&format!("  Min Date: {}\n", d.min_datetime));
                    output.push_str(&format!("  Max Date: {}\n", d.max_datetime));
                    output.push_str(&format!("  Duration: {:.0} days\n", d.duration_days));
                }
                ColumnStats::Boolean(b) => {
                    output.push_str(&format!("  True: {}\n", b.true_count));
                    output.push_str(&format!("  False: {}\n", b.false_count));
                    output.push_str(&format!("  True ratio: {:.4}\n", b.true_ratio));
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
    fn format_report(&self, report: &ProfileReport) -> Result<String> {
        use colored::*;
        let mut output = String::new();

        // Determine source label based on data source type
        let source_label = match &report.data_source {
            DataSource::File { .. } => "File:",
            DataSource::Query { .. } => "Query:",
            DataSource::DataFrame { .. } => "DataFrame:",
            DataSource::Stream { .. } => "Stream:",
        };

        let size_mb = report.data_source.size_mb().unwrap_or(0.0);
        if self.context.supports_unicode {
            output.push_str(&format!(
                "{} {}\n",
                source_label.bright_blue().bold(),
                report.data_source.identifier()
            ));
            if size_mb > 0.0 {
                output.push_str(&format!(
                    "{} {:.1} MB\n",
                    "Size:".bright_blue().bold(),
                    size_mb
                ));
            }
            output.push_str(&format!(
                "{} {}\n",
                "Columns:".bright_blue().bold(),
                report.execution.columns_detected
            ));
        } else {
            output.push_str(&format!(
                "{} {}\n",
                source_label,
                report.data_source.identifier()
            ));
            if size_mb > 0.0 {
                output.push_str(&format!("Size: {:.1} MB\n", size_mb));
            }
            output.push_str(&format!("Columns: {}\n", report.execution.columns_detected));
        }

        if self.context.supports_unicode {
            output.push_str(&format!(
                "{} {}\n",
                "Rows:".bright_blue().bold(),
                report.execution.rows_processed
            ));
        } else {
            output.push_str(&format!("Rows: {}\n", report.execution.rows_processed));
        }

        // Performance info
        if self.context.supports_unicode {
            output.push_str(&format!(
                "{} {} ms\n\n",
                "Scan time:".bright_blue().bold(),
                report.execution.scan_time_ms
            ));
        } else {
            output.push_str(&format!(
                "Scan time: {} ms\n\n",
                report.execution.scan_time_ms
            ));
        }

        // Column profiles with enhanced formatting
        if self.context.supports_unicode {
            output.push_str(&format!("{}\n", "Column Profiles".bright_green().bold()));
        } else {
            output.push_str("Column Profiles\n");
        }

        for profile in &report.column_profiles {
            output.push_str(&format!("Column: {}\n", profile.name.bright_white().bold()));
            output.push_str(&format!("  Type: {:?}\n", profile.data_type));
            output.push_str(&format!("  Records: {}\n", profile.total_count));

            let null_percentage = null_pct(profile);
            if null_percentage > 0.0 {
                output.push_str(&format!(
                    "  Nulls: {} ({:.1}%)\n",
                    profile.null_count, null_percentage
                ));
            } else {
                output.push_str(&format!("  Nulls: {}\n", profile.null_count));
            }

            match &profile.stats {
                ColumnStats::Numeric(n) => {
                    output.push_str(&format!("  Min: {:.2}\n", n.min));
                    output.push_str(&format!("  Max: {:.2}\n", n.max));
                    output.push_str(&format!("  Mean: {:.2}\n", n.mean));
                    output.push_str(&format!("  Std Dev: {:.2}\n", n.std_dev));
                    if let Some(med) = n.median {
                        output.push_str(&format!("  Median: {:.2}\n", med));
                    }
                }
                ColumnStats::Text(t) => {
                    output.push_str(&format!("  Min Length: {}\n", t.min_length));
                    output.push_str(&format!("  Max Length: {}\n", t.max_length));
                    output.push_str(&format!("  Avg Length: {:.1}\n", t.avg_length));
                }
                ColumnStats::DateTime(d) => {
                    output.push_str(&format!("  Min Date: {}\n", d.min_datetime));
                    output.push_str(&format!("  Max Date: {}\n", d.max_datetime));
                    output.push_str(&format!("  Duration: {:.0} days\n", d.duration_days));
                }
                ColumnStats::Boolean(b) => {
                    output.push_str(&format!("  True: {}\n", b.true_count));
                    output.push_str(&format!("  False: {}\n", b.false_count));
                    output.push_str(&format!("  True ratio: {:.4}\n", b.true_ratio));
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
    fn format_report(&self, report: &ProfileReport) -> Result<String> {
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

/// Create adaptive formatter that auto-selects based on context
pub fn create_adaptive_formatter() -> Box<dyn OutputFormatter> {
    Box::new(AdaptiveFormatter::new())
}

/// Create adaptive formatter with forced format
pub fn create_adaptive_formatter_with_format(format: OutputFormat) -> Box<dyn OutputFormatter> {
    Box::new(AdaptiveFormatter::format(format))
}

/// Adaptive output that automatically selects best format based on terminal context
pub fn output_with_adaptive_formatter(
    report: &ProfileReport,
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
