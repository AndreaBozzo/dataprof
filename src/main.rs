use clap::{Parser, Subcommand};
use colored::*;
use std::path::PathBuf;
use std::time::Instant;
use anyhow::Result;

mod types;
mod sampler;
mod analyzer;
mod quality;
mod reporter;

use crate::types::*;
use crate::sampler::Sampler;
use crate::analyzer::analyze_dataframe;
use crate::quality::QualityChecker;
use crate::reporter::TerminalReporter;

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(about = "Fast data profiling and quality checking for large datasets")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Quick quality check with smart sampling
    Check {
        /// Input file path
        file: PathBuf,
        
        /// Use fast sampling (default: true)
        #[arg(long, default_value = "true")]
        fast: bool,
        
        /// Maximum rows to scan
        #[arg(long)]
        max_rows: Option<usize>,
    },
    
    /// Deep analysis of the dataset
    Analyze {
        /// Input file path
        file: PathBuf,
        
        /// Output format (terminal, json, html)
        #[arg(long, default_value = "terminal")]
        output: String,
    },
    
    /// Compare two datasets
    Diff {
        /// First file
        file1: PathBuf,
        /// Second file
        file2: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Check { file, fast, max_rows } => {
            check_command(file, fast, max_rows)?;
        }
        Commands::Analyze { file, output } => {
            analyze_command(file, output)?;
        }
        Commands::Diff { file1: _, file2: _ } => {
            println!("Diff command coming in week 3!");
        }
    }
    
    Ok(())
}

fn check_command(file: PathBuf, _fast: bool, max_rows: Option<usize>) -> Result<()> {
    let start = Instant::now();
    
    // Header
    println!("\n{} {}", 
        "⚡".yellow(), 
        "DataProfiler Quick Check".bold()
    );
    
    // Get file info
    let metadata = std::fs::metadata(&file)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;
    
    // Create sampler
    let mut sampler = Sampler::new(file_size_mb);
    if let Some(max) = max_rows {
        // Override con max_rows se specificato
        sampler = Sampler { target_rows: max };
    }
    
    // Sample and analyze
    let pb = reporter::create_progress_bar(100, "Reading file...");
    let (df, sample_info) = sampler.sample_csv(&file)?;
    pb.finish_and_clear();
    
    // Analyze columns
    let pb = reporter::create_progress_bar(df.width() as u64, "Analyzing columns...");
    let profiles = analyze_dataframe(&df);
    pb.finish_and_clear();
    
    // Check quality
    let issues = QualityChecker::check_dataframe(&df, &profiles);
    
    // Create report
    let report = QualityReport {
        file_info: FileInfo {
            path: file.display().to_string(),
            total_rows: sample_info.total_rows,
            total_columns: df.width(),
            file_size_mb,
        },
        scan_info: ScanInfo {
            rows_scanned: sample_info.sampled_rows,
            sampling_ratio: sample_info.sampling_ratio,
            scan_time_ms: start.elapsed().as_millis(),
        },
        column_profiles: profiles,
        issues,
    };
    
    // Print report
    TerminalReporter::report(&report);
    
    Ok(())
}

fn analyze_command(file: PathBuf, output: String) -> Result<()> {
    match output.as_str() {
        "terminal" => check_command(file, false, None),
        "json" => {
            println!("JSON output coming soon!");
            Ok(())
        }
        "html" => {
            println!("HTML output coming soon!");
            Ok(())
        }
        _ => {
            eprintln!("Unknown output format: {}", output);
            Ok(())
        }
    }
}

