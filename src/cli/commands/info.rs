use anyhow::Result;
use clap::Args;

/// Show engine information and system capabilities
#[derive(Debug, Args)]
pub struct InfoArgs {
    /// Show detailed system information
    #[arg(long)]
    pub detailed: bool,
}

/// Execute the info command - displays engine information and system capabilities
pub fn execute(args: &InfoArgs) -> Result<()> {
    show_engine_info(args.detailed)
}

/// Show engine information
fn show_engine_info(detailed: bool) -> Result<()> {
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

    if detailed {
        println!();
        println!("{}", "Detailed Information:".bright_yellow());
        println!("  Cargo version: {}", env!("CARGO_PKG_VERSION"));
        println!("  Features enabled:");

        #[cfg(feature = "arrow")]
        println!("    - arrow");
        #[cfg(feature = "parquet")]
        println!("    - parquet");
        #[cfg(feature = "python")]
        println!("    - python");
        #[cfg(feature = "database")]
        println!("    - database");

        #[cfg(not(any(
            feature = "arrow",
            feature = "parquet",
            feature = "python",
            feature = "database"
        )))]
        println!("    - minimal (no optional features)");
    }

    Ok(())
}
