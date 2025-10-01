use clap::Args;
use std::path::PathBuf;

/// ML readiness arguments
#[derive(Debug, Args)]
pub struct MlArgs {
    /// Input file to analyze
    pub file: PathBuf,

    /// Show inline code snippets
    #[arg(long)]
    pub code: bool,

    /// Generate complete preprocessing script
    #[arg(long)]
    pub script: Option<PathBuf>,

    /// Target framework (sklearn, pandas, polars)
    #[arg(long, default_value = "sklearn")]
    pub framework: String,

    /// Show detailed per-feature analysis
    #[arg(long)]
    pub detailed: bool,

    /// Output format (text, json)
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Output file path for analysis results
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Use streaming for large files
    #[arg(long)]
    pub streaming: bool,

    /// Common analysis options (progress, chunk-size, config)
    #[command(flatten)]
    pub common: super::CommonAnalysisOptions,
}
use anyhow::Result;
use dataprof::MlReadinessEngine;
use std::fs;

use crate::cli::{analyze_file_with_options, AnalysisOptions};

pub fn execute(args: &MlArgs) -> Result<()> {
    // Build analysis options from command arguments
    let options = AnalysisOptions {
        progress: args.common.progress,
        chunk_size: args.common.chunk_size,
        config: args.common.config.clone(),
        streaming: args.streaming,
        sample: None,
    };

    // Use shared core logic that handles all improvements
    let report = analyze_file_with_options(&args.file, options)?;

    let ml_engine = MlReadinessEngine::new();
    let ml_score = ml_engine.calculate_ml_score(&report)?;

    match args.format.as_str() {
        "json" => print_json_output(&ml_score)?,
        _ => print_text_output(&ml_score, args.detailed, args.code)?,
    }

    if let Some(script_path) = &args.script {
        generate_script(&ml_score, script_path, &args.framework)?;
    }

    if let Some(output_path) = &args.output {
        save_output(output_path, &ml_score, &args.format)?;
    }

    Ok(())
}

fn print_text_output(
    ml_score: &dataprof::analysis::MlReadinessScore,
    _detailed: bool,
    show_code: bool,
) -> Result<()> {
    println!("\n🤖 ML Readiness Score: {:.0}%\n", ml_score.overall_score);

    if !ml_score.blocking_issues.is_empty() {
        println!("❌ Blocking Issues ({}):", ml_score.blocking_issues.len());
        for issue in &ml_score.blocking_issues {
            println!("  • {}", issue.description);
        }
    }

    if !ml_score.recommendations.is_empty() {
        println!("\n🔧 Recommendations ({}):", ml_score.recommendations.len());
        for (i, rec) in ml_score.recommendations.iter().enumerate() {
            println!("  {}. {}", i + 1, rec.description);
        }
    }

    if show_code {
        println!("\n💻 Code Snippets:");
        for (i, rec) in ml_score.recommendations.iter().take(3).enumerate() {
            if let Some(code) = &rec.code_snippet {
                println!(
                    "\n{}. {}:\n```python\n{}\n```",
                    i + 1,
                    rec.description,
                    code
                );
            }
        }
    }

    Ok(())
}

fn print_json_output(ml_score: &dataprof::analysis::MlReadinessScore) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(ml_score)?);
    Ok(())
}

fn generate_script(
    ml_score: &dataprof::analysis::MlReadinessScore,
    path: &std::path::Path,
    framework: &str,
) -> Result<()> {
    let mut script = format!("# ML Preprocessing Script\n# Framework: {}\n\nimport pandas as pd\n\ndef preprocess_data(df):\n", framework);

    for rec in &ml_score.recommendations {
        if let Some(code) = &rec.code_snippet {
            script.push_str(&format!("    # {}\n", rec.description));
            for line in code.lines() {
                script.push_str(&format!("    {}\n", line));
            }
        }
    }

    script.push_str("    return df\n");

    fs::write(path, script)?;
    println!("✅ Script saved: {}", path.display());
    Ok(())
}

fn save_output(
    path: &std::path::Path,
    ml_score: &dataprof::analysis::MlReadinessScore,
    format: &str,
) -> Result<()> {
    let content = if format == "json" {
        serde_json::to_string_pretty(ml_score)?
    } else {
        format!("{:#?}", ml_score)
    };
    fs::write(path, content)?;
    Ok(())
}
