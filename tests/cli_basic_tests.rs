use anyhow::Result;
use std::fs;
use std::io::Write;
use std::process::Command;
use tempfile::{tempdir, NamedTempFile};

/// Helper function to create test CSV data
fn create_test_csv() -> Result<NamedTempFile> {
    let mut temp_file = NamedTempFile::with_suffix(".csv")?;
    writeln!(temp_file, "name,age,score,email")?;
    writeln!(temp_file, "Alice,25,95.5,alice@example.com")?;
    writeln!(temp_file, "Bob,30,87.2,bob@test.org")?;
    writeln!(temp_file, "Charlie,,92.1,charlie@invalid")?;
    writeln!(temp_file, "Diana,28,99.9,diana@company.com")?;
    writeln!(temp_file, "Eve,35,88.7,")?;
    Ok(temp_file)
}

/// Helper function to run CLI command and get output
fn run_cli_command(args: &[&str]) -> Result<(bool, String, String)> {
    // Use cargo run instead of building binary separately for more reliable testing
    let mut cargo_args = vec!["run", "--release", "--bin", "dataprof-cli", "--"];
    cargo_args.extend_from_slice(args);

    let output = Command::new("cargo").args(&cargo_args).output()?;

    let success = output.status.success();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Debug output if test fails
    if !success
        && !stderr.contains("File not found")
        && !stderr.contains("cannot be zero")
        && !stderr.contains("requires streaming")
    {
        eprintln!("Command failed: cargo {}", cargo_args.join(" "));
        eprintln!("Exit code: {:?}", output.status.code());
        eprintln!("Stdout: {}", stdout);
        eprintln!("Stderr: {}", stderr);
    }

    Ok((success, stdout, stderr))
}

#[test]
fn test_cli_help_command() -> Result<()> {
    let (success, stdout, _stderr) = run_cli_command(&["--help"])?;

    assert!(success, "Help command should succeed");
    assert!(
        stdout.contains("DataProfiler CLI"),
        "Should contain CLI name"
    );
    assert!(
        stdout.contains("EXAMPLES:"),
        "Should contain examples section"
    );
    assert!(
        stdout.contains("--quality"),
        "Should mention quality option"
    );
    assert!(
        stdout.contains("--ml-score"),
        "Should mention ML score option"
    );

    Ok(())
}

#[test]
fn test_cli_short_help() -> Result<()> {
    let (success, stdout, _stderr) = run_cli_command(&["-h"])?;

    assert!(success, "Short help command should succeed");
    assert!(
        stdout.contains("Fast CSV data profiler"),
        "Should contain description"
    );
    assert!(
        stdout.contains("Usage:"),
        "Should contain usage information"
    );

    Ok(())
}

#[test]
fn test_cli_version_info() -> Result<()> {
    let (success, stdout, _stderr) = run_cli_command(&["--engine-info"])?;

    assert!(success, "Engine info command should succeed");
    assert!(stdout.contains("DataProfiler"), "Should contain tool name");
    assert!(
        stdout.contains("Engine Information"),
        "Should contain engine info"
    );

    Ok(())
}

#[test]
fn test_cli_basic_file_analysis() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path])?;

    assert!(success, "Basic file analysis should succeed");
    assert!(
        stdout.contains("Column:") || stdout.contains("DataProfiler"),
        "Should contain analysis output"
    );
    assert!(stdout.contains("name"), "Should analyze name column");
    assert!(stdout.contains("age"), "Should analyze age column");
    assert!(stdout.contains("score"), "Should analyze score column");

    Ok(())
}

#[test]
fn test_cli_json_output_format() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--format", "json"])?;

    assert!(success, "JSON format should succeed");
    assert!(stdout.contains("{"), "Should contain JSON opening brace");
    assert!(
        stdout.contains("\"rows\"") || stdout.contains("\"columns\""),
        "Should contain JSON structure keys"
    );

    Ok(())
}

#[test]
fn test_cli_csv_output_format() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--format", "csv"])?;

    assert!(success, "CSV format should succeed");
    assert!(stdout.contains("Column:"), "Should contain column analysis");
    assert!(
        stdout.contains("name") && stdout.contains("String"),
        "Should analyze name column as String type"
    );

    Ok(())
}

#[test]
fn test_cli_quality_assessment() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--quality"])?;

    assert!(success, "Quality assessment should succeed");
    assert!(
        stdout.to_lowercase().contains("quality"),
        "Should mention quality"
    );

    Ok(())
}

#[test]
fn test_cli_ml_score_analysis() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--ml-score"])?;

    assert!(success, "ML score analysis should succeed");
    assert!(
        stdout.contains("Column:") || stdout.contains("Type:"),
        "Should contain column analysis output"
    );

    Ok(())
}

#[test]
fn test_cli_html_report_generation() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let temp_dir = tempdir()?;
    let html_path = temp_dir.path().join("test_report.html");
    let html_path_str = html_path.to_str().unwrap();

    let (success, _stdout, _stderr) =
        run_cli_command(&[file_path, "--html", html_path_str, "--quality"])?;

    assert!(success, "HTML report generation should succeed");
    assert!(html_path.exists(), "HTML file should be created");

    let html_content = fs::read_to_string(&html_path)?;
    assert!(
        html_content.contains("<!DOCTYPE html>"),
        "Should be valid HTML"
    );
    assert!(
        html_content.contains("DataProfiler Report"),
        "Should contain report title"
    );

    Ok(())
}

#[test]
fn test_cli_streaming_mode() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--streaming"])?;

    assert!(success, "Streaming mode should succeed");
    assert!(
        stdout.contains("Column:") || stdout.contains("DataProfiler"),
        "Should show analysis output"
    );

    Ok(())
}

#[test]
fn test_cli_verbosity_levels() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    // Test single -v
    let (success, _stdout, _stderr) = run_cli_command(&[file_path, "-v"])?;
    assert!(success, "Single verbosity should succeed");

    // Test double -vv
    let (success, _stdout, _stderr) = run_cli_command(&[file_path, "-vv"])?;
    assert!(success, "Double verbosity should succeed");

    Ok(())
}

#[test]
fn test_cli_no_color_option() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--no-color"])?;

    assert!(success, "No color option should succeed");
    assert!(
        stdout.contains("Column:") || stdout.contains("DataProfiler"),
        "Should still show analysis output"
    );

    Ok(())
}

#[test]
fn test_cli_error_file_not_found() -> Result<()> {
    let (success, _stdout, stderr) = run_cli_command(&["nonexistent_file.csv"])?;

    assert!(!success, "Should fail for non-existent file");
    assert!(
        stderr.contains("File not found") || stderr.contains("not found"),
        "Should show file not found error"
    );

    Ok(())
}

#[test]
fn test_cli_error_invalid_chunk_size() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, _stdout, stderr) = run_cli_command(&[file_path, "--chunk-size", "0"])?;

    assert!(!success, "Should fail for zero chunk size");
    assert!(
        stderr.contains("Chunk size cannot be zero"),
        "Should show appropriate error"
    );

    Ok(())
}

#[test]
fn test_cli_error_invalid_sample_size() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, _stdout, stderr) = run_cli_command(&[file_path, "--sample", "0"])?;

    assert!(!success, "Should fail for zero sample size");
    assert!(
        stderr.contains("Sample size cannot be zero"),
        "Should show appropriate error"
    );

    Ok(())
}

#[test]
fn test_cli_error_progress_without_streaming() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, _stdout, stderr) = run_cli_command(&[file_path, "--progress"])?;

    assert!(!success, "Should fail when progress used without streaming");
    assert!(
        stderr.contains("Progress display requires streaming mode"),
        "Should show appropriate error"
    );

    Ok(())
}

#[test]
fn test_cli_benchmark_mode() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--benchmark"])?;

    assert!(success, "Benchmark mode should succeed");
    assert!(
        stdout.contains("Benchmark") || stdout.contains("Performance"),
        "Should show benchmark results"
    );

    Ok(())
}

#[test]
fn test_cli_configuration_file() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let temp_dir = tempdir()?;
    let config_path = temp_dir.path().join("test_config.toml");

    let config_content = r#"
[output]
default_format = "text"
colored = false
verbosity = 1
show_progress = false

[output.html]
auto_generate = false
include_detailed_stats = true

[ml]
auto_score = false
warning_threshold = 70.0
include_recommendations = true
calculate_feature_importance = false
suggest_preprocessing = true

[quality]
enabled = true
null_threshold = 10.0
outlier_threshold = 3.0
detect_duplicates = true
detect_mixed_types = true
check_date_formats = true

[engine]
default_engine = "auto"
parallel = true
max_concurrent = 4

[engine.memory]
max_usage_mb = 0
monitor = true
auto_streaming_threshold_mb = 100.0
"#;
    fs::write(&config_path, config_content)?;
    let config_path_str = config_path.to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[file_path, "--config", config_path_str])?;

    assert!(success, "Configuration file should be accepted");
    assert!(
        stdout.contains("Column:") || stdout.contains("DataProfiler"),
        "Should still process file"
    );

    Ok(())
}

#[test]
fn test_cli_comprehensive_analysis() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let temp_dir = tempdir()?;
    let html_path = temp_dir.path().join("comprehensive_report.html");
    let html_path_str = html_path.to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&[
        file_path,
        "--quality",
        "--ml-score",
        "--html",
        html_path_str,
        "--format",
        "json",
        "-vv",
    ])?;

    assert!(success, "Comprehensive analysis should succeed");
    assert!(
        stdout.contains("\"rows\"") || stdout.contains("\"columns\""),
        "Should output JSON"
    );
    assert!(html_path.exists(), "Should create HTML report");

    Ok(())
}
