use anyhow::Result;
use clap::Parser;

// Local modules
mod cli;
mod error;

use cli::{route_command, Command};
use dataprof::core::exit_codes;

fn main() -> Result<()> {
    // Custom parsing to handle engine-info flag
    let args: Vec<String> = std::env::args().collect();
    if args.contains(&"--engine-info".to_string()) {
        return show_engine_info();
    }

    // Parse subcommand CLI
    #[derive(Parser)]
    #[command(name = "dataprof")]
    #[command(
        version,
        about = "Fast data profiler with ISO 8000/25012 quality metrics"
    )]
    struct SubcommandCli {
        #[command(subcommand)]
        command: Command,
    }

    let cli = SubcommandCli::parse();

    // Route to appropriate command handler
    match route_command(cli.command) {
        Ok(_) => std::process::exit(exit_codes::SUCCESS),
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            std::process::exit(exit_codes::GENERAL_ERROR);
        }
    }
}

/// Show engine information
fn show_engine_info() -> Result<()> {
    use colored::*;
    use sysinfo::System;

    println!(
        "{}",
        "🔧 DataProfiler Engine Information".bright_blue().bold()
    );
    println!();

    // System information
    println!("{}", "System Resources:".bright_yellow());
    let mut sys = System::new_all();
    sys.refresh_all();

    let total_memory_gb = sys.total_memory() as f64 / 1_073_741_824.0;
    let available_memory_gb = sys.available_memory() as f64 / 1_073_741_824.0;
    let cpu_cores = num_cpus::get();

    println!("  CPU Cores: {}", cpu_cores);
    println!("  Total Memory: {:.1} GB", total_memory_gb);
    println!("  Available Memory: {:.1} GB", available_memory_gb);
    println!(
        "  Memory Usage: {:.1}%",
        ((total_memory_gb - available_memory_gb) / total_memory_gb) * 100.0
    );
    println!();

    // Engine availability
    println!("{}", "Available Engines:".bright_yellow());

    println!(
        "  ✅ {} - Basic streaming for small files (<100MB)",
        "Streaming".green()
    );
    println!(
        "  ✅ {} - Memory-efficient for medium files (50-200MB)",
        "MemoryEfficient".green()
    );
    println!(
        "  ✅ {} - True streaming for large files (>200MB)",
        "TrueStreaming".green()
    );

    #[cfg(feature = "arrow")]
    {
        println!(
            "  ✅ {} - High-performance columnar processing (>500MB)",
            "Arrow".green()
        );
    }
    #[cfg(not(feature = "arrow"))]
    {
        println!(
            "  ❌ {} - Not available (compile with --features arrow)",
            "Arrow".red()
        );
    }

    println!(
        "  🚀 {} - Intelligent automatic selection",
        "Auto".bright_green().bold()
    );
    println!();

    // Recommendations
    println!("{}", "Recommendations:".bright_yellow());
    println!(
        "  • Use {} for best performance",
        "--engine auto".bright_green()
    );

    #[cfg(not(feature = "arrow"))]
    {
        println!(
            "  • Compile with {} for better large file performance",
            "--features arrow".bright_yellow()
        );
    }

    if available_memory_gb < 2.0 {
        println!(
            "  ⚠️ {} Low memory detected - streaming engines recommended",
            "Warning:".yellow()
        );
    }

    Ok(())
}
