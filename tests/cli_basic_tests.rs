use anyhow::Result;
use std::fs;
use std::io::Write;
use std::process::Command;
use tempfile::{NamedTempFile, tempdir};

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
    let mut cargo_args = vec!["run", "--bin", "dataprof-cli", "--"];
    cargo_args.extend_from_slice(args);

    let output = Command::new("cargo").args(&cargo_args).output()?;

    let success = output.status.success();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !success
        && !stderr.contains("File not found")
        && !stderr.contains("cannot be zero")
        && !stderr.contains("requires streaming")
        && !stderr.contains("No such file")
    {
        eprintln!("Command failed: cargo {}", cargo_args.join(" "));
        eprintln!("Exit code: {:?}", output.status.code());
        eprintln!("Stdout: {}", stdout);
        eprintln!("Stderr: {}", stderr);
    }

    Ok((success, stdout, stderr))
}

// ============================================================================
// General CLI Tests
// ============================================================================

#[test]
fn test_cli_help_command() -> Result<()> {
    let (success, stdout, _stderr) = run_cli_command(&["--help"])?;

    assert!(success, "Help command should succeed");
    assert!(stdout.contains("analyze"), "Should mention analyze command");
    assert!(stdout.contains("report"), "Should mention report command");
    assert!(stdout.contains("batch"), "Should mention batch command");
    assert!(
        stdout.contains("benchmark"),
        "Should mention benchmark command"
    );

    Ok(())
}

#[test]
fn test_cli_short_help() -> Result<()> {
    let (success, stdout, _stderr) = run_cli_command(&["-h"])?;

    assert!(success, "Short help command should succeed");
    assert!(
        stdout.contains("Usage:"),
        "Should contain usage information"
    );

    Ok(())
}

#[test]
fn test_cli_version_info() -> Result<()> {
    let (success, stdout, _stderr) = run_cli_command(&["--version"])?;

    assert!(success, "Version command should succeed");
    assert!(
        stdout.contains("dataprof") || stdout.contains("0."),
        "Should contain version info"
    );

    Ok(())
}

// ============================================================================
// Analyze Subcommand Tests
// ============================================================================

#[test]
fn test_analyze_basic_file() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&["analyze", file_path])?;

    assert!(success, "Basic file analysis should succeed");
    assert!(
        stdout.contains("ISO")
            || stdout.contains("Dimension")
            || stdout.contains("Completeness")
            || stdout.contains("Quality"),
        "Should contain analysis output"
    );

    Ok(())
}

#[test]
fn test_analyze_json_output() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&["analyze", file_path, "--format", "json"])?;

    assert!(success, "JSON format should succeed");
    assert!(stdout.contains("{"), "Should contain JSON");

    Ok(())
}

#[test]
fn test_analyze_csv_output() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&["analyze", file_path, "--format", "csv"])?;

    assert!(success, "CSV format should succeed");
    assert!(
        stdout.contains(",") || stdout.contains("Dimension"),
        "Should contain CSV or analysis output"
    );

    Ok(())
}

#[test]
fn test_analyze_with_output_file() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let temp_dir = tempdir()?;
    let output_path = temp_dir.path().join("output.json");
    let output_path_str = output_path.to_str().unwrap();

    let (success, _stdout, _stderr) = run_cli_command(&[
        "analyze",
        file_path,
        "--format",
        "json",
        "--output",
        output_path_str,
    ])?;

    assert!(success, "Output to file should succeed");
    assert!(output_path.exists(), "Output file should be created");

    Ok(())
}

#[test]
fn test_analyze_streaming_mode() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, _stdout, _stderr) = run_cli_command(&["analyze", file_path, "--streaming"])?;

    assert!(success, "Streaming mode should succeed");

    Ok(())
}

#[test]
fn test_analyze_with_sample() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, _stdout, _stderr) = run_cli_command(&["analyze", file_path, "--sample", "100"])?;

    assert!(success, "Sample option should succeed");

    Ok(())
}

#[test]
fn test_analyze_detailed_metrics() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&["analyze", file_path, "--detailed"])?;

    assert!(success, "Detailed metrics should succeed");
    assert!(
        stdout.contains("ISO")
            || stdout.contains("Dimension")
            || stdout.contains("Completeness")
            || stdout.contains("Quality"),
        "Should show detailed analysis"
    );

    Ok(())
}

#[test]
fn test_analyze_threshold_profiles() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    // Test default profile
    let (success, _stdout, _stderr) = run_cli_command(&["analyze", file_path])?;
    assert!(success, "Default profile should succeed");

    // Test strict profile
    let (success, _stdout, _stderr) =
        run_cli_command(&["analyze", file_path, "--threshold-profile", "strict"])?;
    assert!(success, "Strict profile should succeed");

    // Test lenient profile
    let (success, _stdout, _stderr) =
        run_cli_command(&["analyze", file_path, "--threshold-profile", "lenient"])?;
    assert!(success, "Lenient profile should succeed");

    Ok(())
}

#[test]
fn test_analyze_file_not_found() -> Result<()> {
    let (success, _stdout, stderr) = run_cli_command(&["analyze", "nonexistent.csv"])?;

    assert!(!success, "Should fail for non-existent file");
    assert!(
        stderr.contains("File not found")
            || stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Impossibile trovare"),
        "Should show file not found error"
    );

    Ok(())
}

// ============================================================================
// Report Subcommand Tests
// ============================================================================

#[test]
fn test_report_html_generation() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let temp_dir = tempdir()?;
    let output_path = temp_dir.path().join("report.html");
    let output_path_str = output_path.to_str().unwrap();

    let (success, _stdout, _stderr) =
        run_cli_command(&["report", file_path, "--output", output_path_str])?;

    assert!(success, "HTML report generation should succeed");
    assert!(output_path.exists(), "HTML file should be created");

    let html_content = fs::read_to_string(&output_path)?;
    assert!(
        html_content.contains("<!DOCTYPE html>") || html_content.contains("<html"),
        "Should be valid HTML"
    );

    Ok(())
}

#[test]
fn test_report_pdf_generation() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let temp_dir = tempdir()?;
    let output_path = temp_dir.path().join("report.pdf");
    let output_path_str = output_path.to_str().unwrap();

    let (success, _stdout, _stderr) = run_cli_command(&[
        "report",
        file_path,
        "--format",
        "pdf",
        "--output",
        output_path_str,
    ])?;

    // PDF generation may fail without headless chrome, so we just check it doesn't crash
    if success {
        assert!(
            output_path.exists(),
            "PDF file should be created if successful"
        );
    }

    Ok(())
}

#[test]
fn test_report_with_template() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let temp_dir = tempdir()?;
    let output_path = temp_dir.path().join("report.html");
    let output_path_str = output_path.to_str().unwrap();

    let (success, _stdout, _stderr) = run_cli_command(&[
        "report",
        file_path,
        "--template",
        "executive",
        "--output",
        output_path_str,
    ])?;

    assert!(success, "Report with template should succeed");

    Ok(())
}

// ============================================================================
// Batch Subcommand Tests
// ============================================================================

#[test]
fn test_batch_directory() -> Result<()> {
    let temp_dir = tempdir()?;

    // Create test files in directory
    let mut file1 = fs::File::create(temp_dir.path().join("test1.csv"))?;
    writeln!(file1, "a,b\n1,2")?;
    let mut file2 = fs::File::create(temp_dir.path().join("test2.csv"))?;
    writeln!(file2, "c,d\n3,4")?;

    let dir_path = temp_dir.path().to_str().unwrap();
    let output_dir = temp_dir.path().join("output");
    fs::create_dir(&output_dir)?;
    let output_dir_str = output_dir.to_str().unwrap();

    let (success, _stdout, _stderr) =
        run_cli_command(&["batch", dir_path, "--output", output_dir_str])?;

    assert!(success, "Batch processing should succeed");

    Ok(())
}

#[test]
fn test_batch_with_glob_pattern() -> Result<()> {
    let temp_dir = tempdir()?;

    // Create multiple test files
    let mut file1 = fs::File::create(temp_dir.path().join("test1.csv"))?;
    writeln!(file1, "a,b\n1,2")?;
    let mut file2 = fs::File::create(temp_dir.path().join("test2.csv"))?;
    writeln!(file2, "c,d\n3,4")?;

    let pattern = format!("{}/*.csv", temp_dir.path().display());
    let output_dir = temp_dir.path().join("output");
    fs::create_dir(&output_dir)?;
    let output_dir_str = output_dir.to_str().unwrap();

    let (success, _stdout, _stderr) =
        run_cli_command(&["batch", &pattern, "--output", output_dir_str])?;

    // May fail on Windows with glob patterns, that's ok
    if success {
        assert!(output_dir.exists(), "Output directory should exist");
    }

    Ok(())
}

// ============================================================================
// Benchmark Subcommand Tests
// ============================================================================

#[test]
fn test_benchmark_basic() -> Result<()> {
    let test_file = create_test_csv()?;
    let file_path = test_file.path().to_str().unwrap();

    let (success, stdout, _stderr) = run_cli_command(&["benchmark", file_path])?;

    assert!(success, "Benchmark should succeed");
    assert!(
        stdout.contains("Benchmark") || stdout.contains("Performance") || stdout.contains("ms"),
        "Should show benchmark results"
    );

    Ok(())
}

#[test]
fn test_benchmark_info() -> Result<()> {
    let (success, stdout, _stderr) = run_cli_command(&["benchmark", "--info"])?;

    assert!(success, "Benchmark info should succeed");
    assert!(
        stdout.contains("Engine") || stdout.contains("engine"),
        "Should show engine information"
    );

    Ok(())
}
