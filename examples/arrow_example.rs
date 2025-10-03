/// Example demonstrating Apache Arrow integration for high-performance data profiling
///
/// This example shows:
/// - When to use Arrow vs standard engines
/// - How to benchmark different engines on your data
/// - Performance optimization with Arrow
///
/// Run with:
///   cargo run --example arrow_example --features arrow
use anyhow::Result;
use dataprof::{AdaptiveProfiler, DataProfiler};
use std::path::Path;

fn main() -> Result<()> {
    println!("🚀 DataProf Arrow Integration Example\n");

    // Example 1: Adaptive profiler with automatic engine selection (RECOMMENDED)
    println!("=== Example 1: Automatic Engine Selection ===");
    let profiler = DataProfiler::auto();

    // The adaptive profiler will automatically choose:
    // - Arrow engine for large files (>100MB) with many columns
    // - Streaming engine for very large files that don't fit in memory
    // - Memory-efficient engine for medium files
    let report = profiler.analyze_file(Path::new("data/sample.csv"))?;

    println!(
        "✅ Analyzed {} rows with {} columns",
        report.scan_info.rows_scanned,
        report.column_profiles.len()
    );
    println!("   Time: {}ms\n", report.scan_info.scan_time_ms);

    // Example 2: Benchmark all engines to find the best one
    #[cfg(feature = "arrow")]
    {
        println!("=== Example 2: Engine Benchmarking ===");
        let profiler = AdaptiveProfiler::new()
            .with_logging(true)
            .with_performance_logging(true);

        let performances = profiler.benchmark_engines(Path::new("data/sample.csv"))?;

        println!("📊 Performance Comparison:");
        for perf in &performances {
            println!(
                "  {:20} {:8.2}s  ({:>10.0} rows/sec)  {}",
                format!("{:?}", perf.engine_type),
                perf.execution_time_ms as f64 / 1000.0,
                perf.rows_per_second,
                if perf.success { "✅" } else { "❌" }
            );
        }
        println!();
    }

    // Example 3: Explicit Arrow engine usage for large files
    #[cfg(feature = "arrow")]
    {
        println!("=== Example 3: Explicit Arrow Engine ===");
        let arrow_profiler = DataProfiler::columnar()
            .batch_size(8192) // Optimize batch size for your data
            .memory_limit_mb(512); // Set memory constraints

        let report = arrow_profiler.analyze_csv_file(Path::new("data/large_dataset.csv"))?;

        println!("✅ Arrow engine processed:");
        println!("   Rows: {}", report.scan_info.rows_scanned);
        println!("   Time: {}ms", report.scan_info.scan_time_ms);
        println!(
            "   Throughput: {:.0} rows/sec\n",
            report.scan_info.rows_scanned as f64 / (report.scan_info.scan_time_ms as f64 / 1000.0)
        );
    }

    #[cfg(not(feature = "arrow"))]
    {
        println!("⚠️  Arrow feature not enabled!");
        println!("   Rebuild with: cargo run --example arrow_example --features arrow\n");
    }

    // Example 4: Performance comparison for decision making
    println!("=== Example 4: When to Use Arrow ===");
    println!("✅ Use Arrow when:");
    println!("   • Files > 100MB with many columns (>20)");
    println!("   • Uniform columnar data (consistent types)");
    println!("   • Production pipelines needing max throughput");
    println!("   • Memory is available for batch processing");
    println!();
    println!("❌ Use Standard Engine when:");
    println!("   • Small files (<10MB)");
    println!("   • Mixed/messy data with type inconsistencies");
    println!("   • Memory constrained environments");
    println!("   • Quick exploratory analysis");

    Ok(())
}
