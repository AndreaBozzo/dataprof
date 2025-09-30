use super::ml::MlArgs;
use dataprof::{DataProfiler, MlReadinessEngine};
use anyhow::Result;
use std::fs;

pub fn execute(args: &MlArgs) -> Result<()> {
    let mut profiler = DataProfiler::streaming();
    let report = profiler.analyze_file(&args.file)?;

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

fn print_text_output(ml_score: &dataprof::analysis::MlReadinessScore, _detailed: bool, show_code: bool) -> Result<()> {
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
                println!("\n{}. {}:\n```python\n{}\n```", i + 1, rec.description, code);
            }
        }
    }

    Ok(())
}

fn print_json_output(ml_score: &dataprof::analysis::MlReadinessScore) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(ml_score)?);
    Ok(())
}

fn generate_script(ml_score: &dataprof::analysis::MlReadinessScore, path: &std::path::Path, framework: &str) -> Result<()> {
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

fn save_output(path: &std::path::Path, ml_score: &dataprof::analysis::MlReadinessScore, format: &str) -> Result<()> {
    let content = if format == "json" {
        serde_json::to_string_pretty(ml_score)?
    } else {
        format!("{:#?}", ml_score)
    };
    fs::write(path, content)?;
    Ok(())
}
