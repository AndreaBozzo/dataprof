//! Accept or reject an incoming dataset before it reaches your warehouse.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example etl_quality_gate
//! ```
//!
//! The scenario: a daily drop lands in a staging bucket. You want the pipeline to
//! stop on the bad file rather than propagate it downstream, and you want the
//! rejection reason in the logs. The gate below is a plain function over a
//! `ProfileReport`, so it composes into any orchestrator.
//!
//! This example profiles a good file and a bad one and prints both verdicts, so
//! it always exits 0. A real gate would `std::process::exit(1)` on rejection.

use std::io::Write;
use std::path::Path;

use anyhow::Result;
use dataprof::{ProfileReport, Profiler};

const GOOD_DROP: &str = "\
transaction_id,account,amount_eur,booked_at
T-1001,ACC-1,120.00,2026-01-04
T-1002,ACC-2,88.50,2026-01-04
T-1003,ACC-1,45.25,2026-01-05
T-1004,ACC-3,310.00,2026-01-05
T-1005,ACC-2,17.99,2026-01-06
";

const BAD_DROP: &str = "\
transaction_id,account,amount_eur,booked_at
T-2001,ACC-1,120.00,2026-01-04
T-2002,,88.50,2026-01-04
T-2002,ACC-1,-45.25,2026-01-05
T-2004,,,2026-01-05
T-2005,,17.99,2026-01-06
";

/// What the warehouse is willing to accept.
struct Gate {
    min_quality_score: f64,
    required_columns: &'static [&'static str],
    key_column: &'static str,
    positive_columns: &'static [&'static str],
    max_missing_percentage: f64,
}

const GATE: Gate = Gate {
    min_quality_score: 90.0,
    required_columns: &["transaction_id", "account", "amount_eur", "booked_at"],
    key_column: "transaction_id",
    positive_columns: &["amount_eur"],
    max_missing_percentage: 5.0,
};

/// Return every reason this dataset must not be loaded. Empty means "accept".
///
/// Each check answers a question an on-call engineer would actually ask, and the
/// message names the column, so the pipeline log is enough to triage without
/// re-running the profiler.
fn violations(report: &ProfileReport, gate: &Gate) -> Vec<String> {
    let mut reasons = Vec::new();

    for required in gate.required_columns {
        if !report.column_profiles.iter().any(|c| &c.name == required) {
            reasons.push(format!("missing required column `{required}`"));
        }
    }

    let Some(quality) = &report.quality else {
        reasons.push("quality assessment was skipped".to_string());
        return reasons;
    };

    let score = quality.score();
    if score < gate.min_quality_score {
        reasons.push(format!(
            "quality score {score:.1} is below the {:.1} threshold",
            gate.min_quality_score
        ));
    }

    if let Some(completeness) = &quality.metrics.completeness {
        // `missing_values_ratio` is reported as a percentage, not a 0..1 fraction.
        if completeness.missing_values_ratio > gate.max_missing_percentage {
            reasons.push(format!(
                "{:.1}% of cells are missing, above the {:.1}% allowance",
                completeness.missing_values_ratio, gate.max_missing_percentage
            ));
        }
        // `null_columns` lists columns past the configured null threshold (50% by
        // default), not only columns that are entirely null.
        for null_column in &completeness.null_columns {
            reasons.push(format!("column `{null_column}` is mostly null"));
        }
    }

    // A key that repeats means the upstream job double-wrote, or we are about to
    // create duplicates on load. Either way, do not proceed.
    if let Some(key) = report
        .column_profiles
        .iter()
        .find(|c| c.name == gate.key_column)
    {
        let present = key.total_count - key.null_count;
        if let Some(distinct) = key.unique_count
            && distinct < present
        {
            reasons.push(format!(
                "key `{}` has {present} values but only {distinct} distinct",
                gate.key_column
            ));
        }
        if key.null_count > 0 {
            reasons.push(format!(
                "key `{}` has {} null value(s)",
                gate.key_column, key.null_count
            ));
        }
    }

    if let Some(accuracy) = &quality.metrics.accuracy
        && accuracy.negative_values_in_positive > 0
    {
        reasons.push(format!(
            "{} negative value(s) in {:?}",
            accuracy.negative_values_in_positive, gate.positive_columns
        ));
    }

    reasons
}

fn profile(path: &Path) -> Result<ProfileReport> {
    Ok(Profiler::new()
        .positive_columns(
            GATE.positive_columns
                .iter()
                .map(|s| s.to_string())
                .collect(),
        )
        .identifier_columns(vec![GATE.key_column.to_string()])
        .analyze_file(path)?)
}

fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;

    for (label, contents) in [("good_drop.csv", GOOD_DROP), ("bad_drop.csv", BAD_DROP)] {
        let path = dir.path().join(label);
        write!(std::fs::File::create(&path)?, "{contents}")?;

        let report = profile(&path)?;
        let reasons = violations(&report, &GATE);

        println!("{label}");
        match reasons.is_empty() {
            true => println!(
                "  ACCEPT -- quality {:.1}/100\n",
                report.quality_score().unwrap_or(0.0)
            ),
            false => {
                println!("  REJECT -- {} violation(s):", reasons.len());
                for reason in &reasons {
                    println!("    - {reason}");
                }
                println!("  a real pipeline would exit(1) here\n");
            }
        }
    }

    Ok(())
}
