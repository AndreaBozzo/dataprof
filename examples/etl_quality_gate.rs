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
//! rejection reason in the logs. [`QualityPolicy`] states the policy as data and
//! returns a structured result, so the gate composes into any orchestrator.
//!
//! This example profiles four drops and prints all three verdicts, so it always
//! exits 0. A real gate would `std::process::exit(1)` on anything but a pass.

use std::io::Write;
use std::path::Path;

use anyhow::Result;
use dataprof::{ProfileReport, Profiler, QualityDimension, QualityPolicy, StopCondition, Verdict};

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

const KEY_COLUMN: &str = "transaction_id";
const REQUIRED_COLUMNS: &[&str] = &["transaction_id", "account", "amount_eur", "booked_at"];

/// What the warehouse is willing to accept. Every threshold is a 0..=100
/// percentage, matching what the report reports.
fn policy() -> QualityPolicy {
    QualityPolicy::new()
        .min_quality_score(90.0)
        .max_null_percentage(KEY_COLUMN, 0.0)
        .max_null_percentage_any(5.0)
        .max_duplicate_rows(0)
        // Without these, a run that never computed quality would leave the
        // score check unevaluated rather than failing. A pipeline wants the
        // misconfiguration to be loud.
        .require_quality()
        .require_dimension(QualityDimension::Completeness)
}

/// Schema presence, which the numeric policy deliberately does not cover.
///
/// A column absent from a report was either projected away or not in the
/// source, and the report does not record which, so the gate leaves it
/// undecided. Here the run profiles everything, so absence means absence.
fn missing_columns(report: &ProfileReport) -> Vec<&'static str> {
    REQUIRED_COLUMNS
        .iter()
        .filter(|required| {
            !report
                .column_profiles
                .iter()
                .any(|column| &column.name == *required)
        })
        .copied()
        .collect()
}

fn profile(path: &Path, max_rows: Option<u64>) -> Result<ProfileReport> {
    let mut profiler = Profiler::new().identifier_columns(vec![KEY_COLUMN.to_string()]);
    if let Some(max_rows) = max_rows {
        profiler = profiler.stop_when(StopCondition::MaxRows(max_rows));
    }
    Ok(profiler.analyze_file(path)?)
}

fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    // Same rows as the good drop, but the last one repeats the first.
    let duplicated_drop = format!("{GOOD_DROP}T-1001,ACC-1,120.00,2026-01-04\n");
    // The fourth drop is the third one read under a row cap: the duplicate sits
    // past the cap, so the scan never witnesses it. The policy asks about the
    // whole source, and a clean prefix is not a clean source.
    let drops: [(&str, &str, Option<u64>); 4] = [
        ("good_drop.csv", GOOD_DROP, None),
        ("bad_drop.csv", BAD_DROP, None),
        ("duplicated_drop.csv", &duplicated_drop, None),
        ("capped_scan.csv", &duplicated_drop, Some(3)),
    ];

    for (label, contents, max_rows) in drops {
        let path = dir.path().join(label);
        write!(std::fs::File::create(&path)?, "{contents}")?;

        let report = profile(&path, max_rows)?;
        let result = policy().evaluate(&report)?;

        println!("{label}");
        let missing = missing_columns(&report);
        for column in &missing {
            println!("  REJECT -- missing required column `{column}`");
        }

        // Three verdicts, not two. `Inconclusive` means nothing was violated
        // and something could not be checked: an unanalyzed metric, or a scan
        // that did not reach as far as the policy asks. Treating it as a pass
        // is exactly the mistake to avoid.
        if result.verdict == Verdict::Pass && missing.is_empty() {
            println!(
                "  ACCEPT -- quality {:.1}/100\n",
                report.quality_score().unwrap_or_default()
            );
            continue;
        }

        let outcome = match result.verdict {
            Verdict::Fail => "REJECT",
            _ => "HOLD",
        };
        // Both, always. A drop can fail one requirement while another went
        // unchecked, and printing only the failure hides the evidence gap.
        let reported: Vec<_> = result.violations().chain(result.unevaluated()).collect();
        println!("  {outcome} -- {} finding(s):", reported.len());
        for check in reported {
            let where_ = check.column.clone().unwrap_or_else(|| {
                check
                    .dimension
                    .map_or_else(|| "report".to_string(), |d| d.to_string())
            });
            println!("    - [{}] {where_}: {}", check.code, check.message);
            if let Some(observed) = &check.observed {
                println!("        observed {observed}, expected {}", check.expected);
            }
        }
        println!("  a real pipeline would exit(1) here\n");
    }

    Ok(())
}
