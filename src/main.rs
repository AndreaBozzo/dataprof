use anyhow::Result;
use clap::Parser;
use colored::*;
use dataprof::{
    analyze_csv, analyze_csv_with_sampling, ColumnProfile, ColumnStats, DataType, QualityIssue,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(about = "Fast CSV data profiler with quality checking")]
struct Cli {
    /// CSV file to analyze
    file: PathBuf,

    /// Enable quality checking (shows data issues)
    #[arg(short, long)]
    quality: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    println!(
        "{}",
        "📊 DataProfiler - Analyzing CSV...".bright_blue().bold()
    );
    println!();

    if cli.quality {
        // Use advanced analysis with quality checking
        let report = analyze_csv_with_sampling(&cli.file)?;

        // Show basic file info
        println!(
            "📁 {} | {:.1} MB | {} columns",
            cli.file.display(),
            report.file_info.file_size_mb,
            report.file_info.total_columns
        );

        if report.scan_info.sampling_ratio < 1.0 {
            println!(
                "📊 Sampled {} rows ({:.1}%)",
                report.scan_info.rows_scanned,
                report.scan_info.sampling_ratio * 100.0
            );
        }
        println!();

        // Show quality issues first
        display_quality_issues(&report.issues);

        // Then show column profiles
        for profile in report.column_profiles {
            display_profile(&profile);
            println!();
        }
    } else {
        // Use simple analysis (backwards compatible)
        let profiles = analyze_csv(&cli.file)?;

        // Display results
        for profile in profiles {
            display_profile(&profile);
            println!();
        }
    }

    Ok(())
}

fn display_profile(profile: &ColumnProfile) {
    println!(
        "{} {}",
        "Column:".bright_yellow(),
        profile.name.bright_white().bold()
    );

    let type_str = match profile.data_type {
        DataType::String => "String".green(),
        DataType::Integer => "Integer".blue(),
        DataType::Float => "Float".cyan(),
        DataType::Date => "Date".magenta(),
    };
    println!("  Type: {}", type_str);

    println!("  Records: {}", profile.total_count);

    if profile.null_count > 0 {
        let pct = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
        println!(
            "  Nulls: {} ({:.1}%)",
            profile.null_count.to_string().red(),
            pct
        );
    } else {
        println!("  Nulls: {}", "0".green());
    }

    match &profile.stats {
        ColumnStats::Numeric { min, max, mean } => {
            println!("  Min: {:.2}", min);
            println!("  Max: {:.2}", max);
            println!("  Mean: {:.2}", mean);
        }
        ColumnStats::Text {
            min_length,
            max_length,
            avg_length,
        } => {
            println!("  Min Length: {}", min_length);
            println!("  Max Length: {}", max_length);
            println!("  Avg Length: {:.1}", avg_length);
        }
    }

    // Show detected patterns
    if !profile.patterns.is_empty() {
        println!("  {}", "Patterns:".bright_cyan());
        for pattern in &profile.patterns {
            println!(
                "    {} - {} matches ({:.1}%)",
                pattern.name.bright_white(),
                pattern.match_count,
                pattern.match_percentage
            );
        }
    }
}

fn display_quality_issues(issues: &[QualityIssue]) {
    if issues.is_empty() {
        println!("✨ {}", "No quality issues found!".green().bold());
        println!();
        return;
    }

    println!(
        "⚠️  {} {}",
        "QUALITY ISSUES FOUND:".red().bold(),
        format!("({})", issues.len()).red()
    );
    println!();

    let mut critical_count = 0;
    let mut warning_count = 0;
    let mut info_count = 0;

    for (i, issue) in issues.iter().enumerate() {
        let (icon, severity_text) = match issue.severity() {
            dataprof::types::Severity::High => {
                critical_count += 1;
                ("🔴", "CRITICAL".red().bold())
            }
            dataprof::types::Severity::Medium => {
                warning_count += 1;
                ("🟡", "WARNING".yellow().bold())
            }
            dataprof::types::Severity::Low => {
                info_count += 1;
                ("🔵", "INFO".blue().bold())
            }
        };

        print!("{}. {} {} ", i + 1, icon, severity_text);

        match issue {
            QualityIssue::NullValues {
                column,
                count,
                percentage,
            } => {
                println!(
                    "[{}]: {} null values ({:.1}%)",
                    column.yellow(),
                    count.to_string().red(),
                    percentage
                );
            }
            QualityIssue::MixedDateFormats { column, formats } => {
                println!("[{}]: Mixed date formats", column.yellow());
                for (format, count) in formats {
                    println!("     - {}: {} rows", format, count);
                }
            }
            QualityIssue::Duplicates { column, count } => {
                println!(
                    "[{}]: {} duplicate values",
                    column.yellow(),
                    count.to_string().red()
                );
            }
            QualityIssue::Outliers {
                column,
                values,
                threshold,
            } => {
                println!(
                    "[{}]: {} outliers detected (>{}σ)",
                    column.yellow(),
                    values.len().to_string().red(),
                    threshold
                );
                for val in values.iter().take(3) {
                    println!("     - {}", val);
                }
                if values.len() > 3 {
                    println!("     ... and {} more", values.len() - 3);
                }
            }
            QualityIssue::MixedTypes { column, types } => {
                println!("[{}]: Mixed data types", column.yellow());
                for (dtype, count) in types {
                    println!("     - {}: {} rows", dtype, count);
                }
            }
        }
    }

    // Summary
    println!();
    print!("📊 Summary: ");
    if critical_count > 0 {
        print!("{} critical ", critical_count.to_string().red());
    }
    if warning_count > 0 {
        print!("{} warnings ", warning_count.to_string().yellow());
    }
    if info_count > 0 {
        print!("{} info", info_count.to_string().blue());
    }
    println!();
    println!();
}
