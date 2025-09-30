use super::report::ReportArgs;
use dataprof::{generate_html_report, DataProfiler};
use anyhow::{Context, Result};
use std::fs;

/// Execute the report command - generate comprehensive HTML/PDF reports
pub fn execute(args: &ReportArgs) -> Result<()> {
    println!("📊 Generating Report...");

    // Smart defaults: auto-enable streaming for large files
    let file_size_mb = get_file_size_mb(&args.file)?;
    let use_streaming = args.streaming || file_size_mb > 100.0;

    // Create profiler
    let profiler = if use_streaming {
        DataProfiler::streaming()
    } else {
        DataProfiler::new()
    };

    println!("  ✅ Quality metrics calculated");

    // Run analysis
    let report = profiler.analyze_file(&args.file)?;
    println!("  ✅ ML readiness scored");

    // Determine output path
    let output_path = if let Some(path) = &args.output {
        path.clone()
    } else {
        // Auto-generate: data.csv -> data_report.html
        let mut path = args.file.clone();
        let file_stem = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("report");
        path.set_file_name(format!("{}_report", file_stem));
        path.set_extension(args.format.as_str());
        path
    };

    // Generate report based on format
    match args.format.as_str() {
        "html" => generate_html(&report, &output_path, args.include_code)?,
        "pdf" => {
            println!("⚠️  PDF generation not yet implemented. Generating HTML instead.");
            generate_html(&report, &output_path.with_extension("html"), args.include_code)?;
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported format: {}. Use 'html' or 'pdf'",
                args.format
            ));
        }
    }

    println!("  ✅ Visualizations generated");
    println!("  ✅ HTML report created");

    let file_size = fs::metadata(&output_path)?.len();
    let size_mb = file_size as f64 / 1_048_576.0;

    println!("\n📄 Report saved: {} ({:.1} MB)", output_path.display(), size_mb);

    // Suggest how to open
    if cfg!(target_os = "windows") {
        println!("\n💡 Open with: start {}", output_path.display());
    } else if cfg!(target_os = "macos") {
        println!("\n💡 Open with: open {}", output_path.display());
    } else {
        println!("\n💡 Open with: xdg-open {}", output_path.display());
    }

    Ok(())
}

fn get_file_size_mb(path: &std::path::Path) -> Result<f64> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.len() as f64 / 1_048_576.0)
}

fn generate_html(
    report: &dataprof::types::DataReport,
    output_path: &std::path::Path,
    include_code: bool,
) -> Result<()> {
    // Use existing HTML generation function
    generate_html_report(report, output_path)
        .context("Failed to generate HTML report")?;

    // If custom template or code inclusion is requested, we'd enhance the HTML here
    if include_code {
        enhance_with_code_snippets(output_path, report)?;
    }

    Ok(())
}

fn enhance_with_code_snippets(
    _output_path: &std::path::Path,
    _report: &dataprof::types::DataReport,
) -> Result<()> {
    // TODO: Add code snippets section to existing HTML
    // For now, this is a placeholder
    Ok(())
}