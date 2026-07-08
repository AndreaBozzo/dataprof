//! Prove that a cleaning step actually improved the data.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example before_after_cleaning
//! ```
//!
//! The scenario: you wrote a cleaning script. Did it help? Profile the raw file,
//! persist that report, run the cleaning, profile the result, and diff the two.
//! Persisting the "before" report matters: weeks later you can still answer
//! "what did this data look like when we onboarded it?" without keeping the raw
//! file around.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use dataprof::{ColumnProfile, ProfileReport, Profiler};

const RAW: &str = "\
signup_id,email,plan,monthly_eur
S-1,ada@example.com,pro,29.00
S-2,,free,0.00
S-3,grace@example.com,PRO,29.00
S-4,,,
S-5,alan@example.com,free,0.00
S-5,alan@example.com,free,0.00
S-7,,enterprise,-99.00
S-8,edsger@example.com,pro,29.00
";

/// What our (imaginary) cleaning script does: drop rows with no email, drop the
/// duplicated signup, normalise `plan` casing, and reject the negative price.
const CLEAN: &str = "\
signup_id,email,plan,monthly_eur
S-1,ada@example.com,pro,29.00
S-3,grace@example.com,pro,29.00
S-5,alan@example.com,free,0.00
S-8,edsger@example.com,pro,29.00
";

fn profile(path: &Path) -> Result<ProfileReport> {
    Ok(Profiler::new()
        .positive_columns(vec!["monthly_eur".to_string()])
        .identifier_columns(vec!["signup_id".to_string()])
        .analyze_file(path)?)
}

fn null_percentage(col: &ColumnProfile) -> f64 {
    100.0 * col.null_count as f64 / col.total_count.max(1) as f64
}

fn by_name(report: &ProfileReport) -> BTreeMap<&str, &ColumnProfile> {
    report
        .column_profiles
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect()
}

fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;

    let raw_path = dir.path().join("signups_raw.csv");
    let clean_path = dir.path().join("signups_clean.csv");
    write!(std::fs::File::create(&raw_path)?, "{RAW}")?;
    write!(std::fs::File::create(&clean_path)?, "{CLEAN}")?;

    let before = profile(&raw_path)?;

    // Persist the baseline. `ProfileReport` is serde-serialisable, so a report is
    // just JSON: commit it, ship it to object storage, or attach it to a PR.
    let baseline_path = dir.path().join("baseline.json");
    serde_json::to_writer_pretty(std::fs::File::create(&baseline_path)?, &before)?;

    let before: ProfileReport = serde_json::from_reader(std::fs::File::open(&baseline_path)?)?;
    let after = profile(&clean_path)?;

    println!("## Cleaning summary\n");
    println!(
        "| | rows | quality |\n|---|---:|---:|\n| before | {} | {:.1} |\n| after | {} | {:.1} |",
        before.execution.rows_processed,
        before.quality_score().unwrap_or(0.0),
        after.execution.rows_processed,
        after.quality_score().unwrap_or(0.0),
    );

    let dropped = before.execution.rows_processed - after.execution.rows_processed;
    let gained = after.quality_score().unwrap_or(0.0) - before.quality_score().unwrap_or(0.0);
    println!("\n{dropped} row(s) dropped, {gained:+.1} quality points\n");

    println!("## Per-column null rate\n");
    println!("| column | before | after | delta |");
    println!("|---|---:|---:|---:|");

    let (before_cols, after_cols) = (by_name(&before), by_name(&after));
    for (name, before_col) in &before_cols {
        let Some(after_col) = after_cols.get(name) else {
            println!(
                "| {name} | {:.0}% | *dropped* | |",
                null_percentage(before_col)
            );
            continue;
        };
        let (b, a) = (null_percentage(before_col), null_percentage(after_col));
        println!("| {name} | {b:.0}% | {a:.0}% | {:+.0}pp |", a - b);
    }

    // The headline numbers can improve while a specific defect survives. Check the
    // defects you set out to fix, not just the score.
    println!("\n## Did the defects go away?\n");
    let checks: [(&str, bool); 3] = [
        (
            "duplicate signup_id",
            after_cols
                .get("signup_id")
                .and_then(|c| c.unique_count.map(|d| d < c.total_count - c.null_count))
                .unwrap_or(false),
        ),
        (
            "negative monthly_eur",
            after
                .quality
                .as_ref()
                .and_then(|q| q.metrics.accuracy.as_ref())
                .is_some_and(|a| a.negative_values_in_positive > 0),
        ),
        (
            "missing email",
            after_cols.get("email").is_some_and(|c| c.null_count > 0),
        ),
    ];

    for (defect, still_present) in checks {
        println!(
            "  {} {defect}",
            if still_present {
                "STILL THERE"
            } else {
                "fixed      "
            }
        );
    }

    Ok(())
}
