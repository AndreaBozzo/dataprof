//! Inspect a messy CSV the way you would on the first day with an unfamiliar export.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example messy_csv_inspection
//! ```
//!
//! The scenario: a partner sends you `orders.csv`. Nobody can tell you whether
//! `order_id` is really unique, whether `email` is populated, or why the finance
//! team's totals look wrong. One pass answers all three.

use std::io::Write;

use anyhow::Result;
use dataprof::{ColumnStats, PatternCategory, Profiler};

/// A realistic export: a null-heavy column, a duplicated key, a negative amount,
/// a column that looks numeric but is not, and PII we should notice but not print.
const MESSY_ORDERS: &str = "\
order_id,customer_email,amount_eur,discount_code,shipped_at
1001,ada@example.com,49.90,,2026-01-04
1002,grace@example.com,120.00,,2026-01-04
1003,,15.50,SPRING,2026-01-05
1004,alan@example.com,-30.00,,2026-01-05
1005,,89.99,,2026-01-06
1006,edsger@example.com,not_available,SPRING,2026-01-06
1006,edsger@example.com,64.00,,2026-01-07
1008,,22.10,,2026-01-07
1009,barbara@example.com,310.00,SUMMER,2026-01-08
1010,,45.00,,2026-01-08
";

fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("orders.csv");
    write!(std::fs::File::create(&path)?, "{MESSY_ORDERS}")?;

    // `amount_eur` should never be negative; telling the profiler makes the
    // violation an accuracy signal instead of just an unusual number.
    let report = Profiler::new()
        .positive_columns(vec!["amount_eur".to_string()])
        .identifier_columns(vec!["order_id".to_string()])
        .analyze_file(&path)?;

    println!(
        "orders.csv: {} rows x {} columns\n",
        report.execution.rows_processed,
        report.column_profiles.len()
    );

    println!(
        "{:<16} {:<10} {:>7} {:>9}",
        "column", "type", "nulls", "distinct"
    );
    println!("{}", "-".repeat(46));
    for col in &report.column_profiles {
        let null_pct = 100.0 * col.null_count as f64 / col.total_count.max(1) as f64;
        let distinct = col
            .unique_count
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<16} {:<10} {:>6.0}% {:>9}",
            col.name,
            format!("{:?}", col.data_type).to_lowercase(),
            null_pct,
            distinct
        );
    }

    println!("\nwhat to worry about");
    println!("{}", "-".repeat(46));

    for col in &report.column_profiles {
        let null_pct = 100.0 * col.null_count as f64 / col.total_count.max(1) as f64;
        if null_pct >= 20.0 {
            println!(
                "  {}: {:.0}% missing -- is it optional, or is the export broken?",
                col.name, null_pct
            );
        }

        // `order_id` is declared an identifier, so anything short of fully
        // distinct is a duplicated key.
        if let (Some(distinct), true) = (col.unique_count, col.name == "order_id") {
            let present = col.total_count - col.null_count;
            if distinct < present {
                println!(
                    "  {}: {} rows but only {} distinct -- duplicate key",
                    col.name, present, distinct
                );
            }
        }

        // Report *that* we found PII, never the values themselves.
        if let Some(patterns) = &col.patterns {
            for p in patterns {
                if matches!(
                    p.category,
                    PatternCategory::Contact | PatternCategory::Financial
                ) {
                    println!(
                        "  {}: looks like {} ({:.0}% of rows) -- treat as sensitive",
                        col.name, p.name, p.match_percentage
                    );
                }
            }
        }

        if let ColumnStats::Numeric(stats) = &col.stats
            && stats.min < 0.0
            && col.name == "amount_eur"
        {
            println!(
                "  {}: minimum is {:.2}, but amounts should never be negative",
                col.name, stats.min
            );
        }
    }

    if let Some(quality) = &report.quality {
        println!("\noverall quality: {:.1}/100", quality.score());
        if let Some(accuracy) = &quality.metrics.accuracy
            && accuracy.negative_values_in_positive > 0
        {
            println!(
                "  {} negative value(s) in a positive-only column",
                accuracy.negative_values_in_positive
            );
        }
        if let Some(uniqueness) = &quality.metrics.uniqueness
            && uniqueness.duplicate_rows > 0
        {
            println!("  {} fully duplicate row(s)", uniqueness.duplicate_rows);
        }
        // `amount_eur` holds the literal `not_available`. Type inference tolerates
        // it, so the tell is the consistency score, not the reported data type.
        if let Some(consistency) = &quality.metrics.consistency
            && consistency.data_type_consistency < 100.0
        {
            println!(
                "  type consistency is {:.1}% -- some cells do not match their column's type",
                consistency.data_type_consistency
            );
        }
    }

    Ok(())
}
