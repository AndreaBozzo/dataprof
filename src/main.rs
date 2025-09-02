use anyhow::Result;
use clap::Parser;
use colored::*;
use dataprof::{analyze_csv, ColumnProfile, ColumnStats, DataType};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(about = "A simple CSV data profiler")]
struct Cli {
    /// CSV file to analyze
    file: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    println!(
        "{}",
        "📊 DataProfiler - Analyzing CSV...".bright_blue().bold()
    );
    println!();

    let profiles = analyze_csv(&cli.file)?;

    // Display results
    for profile in profiles {
        display_profile(&profile);
        println!();
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
