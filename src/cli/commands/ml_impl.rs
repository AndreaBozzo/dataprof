use super::ml::MlArgs;
use dataprof::{DataProfiler, MlReadinessEngine};
use anyhow::Result;
use std::fs;

/// Execute the ml command - ML readiness analysis and code generation
pub fn execute(args: &MlArgs) -> Result<()> {
    // Smart defaults: auto-enable streaming for large files
    let file_size_mb = get_file_size_mb(&args.file)?;
    let use_streaming = args.streaming || file_size_mb > 100.0;

    // Create profiler
    let profiler = if use_streaming {
        DataProfiler::streaming()
    } else {
        DataProfiler::new()
    };

    // Run analysis
    let report = profiler.analyze_file(&args.file)?;

    // Get ML insights
    if let Some(ml_insights) = &report.ml_readiness_insights {
        // Output based on format
        match args.format.as_str() {
            "json" => print_json_output(ml_insights)?,
            _ => print_text_output(ml_insights, args.detailed, args.code)?,
        }

        // Generate script if requested
        if let Some(script_path) = &args.script {
            generate_script(ml_insights, script_path, &args.framework)?;
        }

        // Save analysis to file if requested
        if let Some(output_path) = &args.output {
            save_output(output_path, ml_insights, &args.format)?;
        }
    } else {
        println!("❌ No ML insights available. Run with --quality flag first.");
    }

    Ok(())
}

fn get_file_size_mb(path: &std::path::Path) -> Result<f64> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.len() as f64 / 1_048_576.0)
}

fn print_text_output(
    insights: &dataprof::types::MlReadinessInsights,
    detailed: bool,
    show_code: bool,
) -> Result<()> {
    println!("\n🤖 ML Readiness Score: {}% - {}\n", insights.score, insights.readiness_status);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Feature analysis
    println!("\n📊 Feature Analysis:");
    println!("  ✅ {} features suitable for ML", count_suitable_features(insights));

    if !insights.blocking_issues.is_empty() {
        println!("  ❌ {} blocking issues", insights.blocking_issues.len());
    }

    // Blocking issues
    if !insights.blocking_issues.is_empty() {
        println!("\n❌ Blocking Issues:");
        for (i, issue) in insights.blocking_issues.iter().enumerate() {
            println!("  {}. {}", i + 1, issue);
        }
    }

    // Recommendations
    if !insights.recommendations.is_empty() {
        println!("\n🔧 Preprocessing Steps ({}):", insights.recommendations.len());
        for (i, rec) in insights.recommendations.iter().enumerate() {
            println!("  {}. {}", i + 1, rec);
        }
    }

    // Code snippets if requested
    if show_code && !insights.code_snippets.is_empty() {
        println!("\n💻 Code Snippets:\n");
        for (i, snippet) in insights.code_snippets.iter().take(3).enumerate() {
            println!("{}. {}:", i + 1, snippet.description);
            println!("\n```python\n{}\n```\n", snippet.code);
        }
    }

    // Suggestions
    if !show_code && !insights.code_snippets.is_empty() {
        println!("\n💡 Run with --code to see Python snippets");
    }

    if insights.code_snippets.len() > 3 && show_code {
        println!("💡 {} more snippets available. Run with --script to generate full preprocessing script",
                 insights.code_snippets.len() - 3);
    }

    if detailed {
        print_detailed_analysis(insights);
    }

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    Ok(())
}

fn print_detailed_analysis(insights: &dataprof::types::MlReadinessInsights) {
    println!("\n📋 Detailed Feature Analysis:");

    // Feature type breakdown
    let mut numeric_count = 0;
    let mut categorical_count = 0;
    let mut datetime_count = 0;

    for snippet in &insights.code_snippets {
        if snippet.description.contains("Scale") || snippet.description.contains("numeric") {
            numeric_count += 1;
        } else if snippet.description.contains("Encode") || snippet.description.contains("categorical") {
            categorical_count += 1;
        } else if snippet.description.contains("date") || snippet.description.contains("time") {
            datetime_count += 1;
        }
    }

    println!("  • Numeric features: {}", numeric_count);
    println!("  • Categorical features: {}", categorical_count);
    println!("  • Datetime features: {}", datetime_count);
}

fn count_suitable_features(insights: &dataprof::types::MlReadinessInsights) -> usize {
    // Rough estimate based on recommendations
    insights.code_snippets.len()
}

fn print_json_output(insights: &dataprof::types::MlReadinessInsights) -> Result<()> {
    let json = serde_json::to_string_pretty(insights)?;
    println!("{}", json);
    Ok(())
}

fn generate_script(
    insights: &dataprof::types::MlReadinessInsights,
    path: &std::path::Path,
    framework: &str,
) -> Result<()> {
    let mut script = String::new();

    // Header
    script.push_str(&format!(
        "# ML Preprocessing Script - Generated by DataProf\n\
         # Framework: {}\n\
         # ML Readiness Score: {}%\n\n",
        framework, insights.score
    ));

    // Imports
    script.push_str("import pandas as pd\n");
    script.push_str("import numpy as np\n");

    match framework {
        "sklearn" => {
            script.push_str("from sklearn.preprocessing import StandardScaler, LabelEncoder\n");
            script.push_str("from sklearn.impute import SimpleImputer\n\n");
        }
        "polars" => {
            script.push_str("import polars as pl\n\n");
        }
        _ => {
            script.push_str("\n");
        }
    }

    // Main function
    script.push_str("def preprocess_data(df):\n");
    script.push_str("    \"\"\"Apply all preprocessing steps\"\"\"\n");
    script.push_str("    df = df.copy()\n\n");

    // Add all code snippets
    for snippet in &insights.code_snippets {
        script.push_str(&format!("    # {}\n", snippet.description));

        // Indent the code
        for line in snippet.code.lines() {
            if !line.trim().is_empty() {
                script.push_str(&format!("    {}\n", line));
            }
        }
        script.push_str("\n");
    }

    script.push_str("    return df\n\n");

    // Example usage
    script.push_str("# Example usage:\n");
    script.push_str("# df = pd.read_csv('your_data.csv')\n");
    script.push_str("# df_processed = preprocess_data(df)\n");

    fs::write(path, script)?;
    println!("✅ Preprocessing script saved: {}", path.display());
    println!("💡 Run with: python {}", path.display());

    Ok(())
}

fn save_output(
    path: &std::path::Path,
    insights: &dataprof::types::MlReadinessInsights,
    format: &str,
) -> Result<()> {
    let content = match format {
        "json" => serde_json::to_string_pretty(insights)?,
        _ => format!("{:#?}", insights),
    };

    fs::write(path, content)?;
    println!("✅ ML analysis saved to: {}", path.display());
    Ok(())
}