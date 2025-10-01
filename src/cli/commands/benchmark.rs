use anyhow::Result;
use colored::*;
use sysinfo::System;

use dataprof::AdaptiveProfiler;

pub fn show_engine_info() -> Result<()> {
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
    println!(
        "  • Use {} to compare engines on your data",
        "--benchmark".bright_cyan()
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

// TODO: Port to new CLI architecture or remove
/*
pub fn run_benchmark_analysis(cli: &Cli) -> Result<()> {
    println!(
        "{}",
        "🏁 DataProfiler Engine Benchmark".bright_blue().bold()
    );
    println!("File: {}", cli.file.display());
    println!();

    let profiler = AdaptiveProfiler::new()
        .with_logging(true)
        .with_performance_logging(true);

    let performances = profiler.benchmark_engines(&cli.file)?;

    println!("\n📊 Benchmark Results:");
    println!("{}", "=".repeat(60));

    // Sort by execution time
    let mut sorted = performances.clone();
    sorted.sort_by(|a, b| {
        if a.success != b.success {
            b.success.cmp(&a.success) // Success first
        } else if a.success {
            a.execution_time_ms.cmp(&b.execution_time_ms) // Faster first
        } else {
            std::cmp::Ordering::Equal
        }
    });

    for (i, perf) in sorted.iter().enumerate() {
        let rank = if perf.success {
            match i {
                0 => "🥇".to_string(),
                1 => "🥈".to_string(),
                2 => "🥉".to_string(),
                _ => format!("#{}", i + 1),
            }
        } else {
            "❌".to_string()
        };

        println!("{} {:?}", rank, perf.engine_type);

        if perf.success {
            println!("   Time: {:.2}s", perf.execution_time_ms as f64 / 1000.0);
            if perf.rows_per_second > 0.0 {
                println!("   Speed: {:.0} rows/sec", perf.rows_per_second);
            }
        } else {
            println!("   Status: Failed");
            if let Some(error) = &perf.error_message {
                println!("   Error: {}", error);
            }
        }
        println!();
    }

    // Recommendations
    if let Some(fastest) = sorted.first() {
        if fastest.success {
            println!(
                "🎯 {} Recommendation: Use {:?} for optimal performance on this file type",
                "Best:".green().bold(),
                fastest.engine_type
            );
        }
    }

    Ok(())
}
*/
