use crate::types::*;
use colored::*;

pub struct TerminalReporter;

impl TerminalReporter {
    pub fn report(report: &QualityReport) {
        println!();
        Self::print_header(&report.file_info);
        Self::print_scan_info(&report.scan_info);
        Self::print_quality_issues(&report.issues);
        Self::print_column_summary(&report.column_profiles);
        Self::print_recommendations(&report.issues);
    }
    
    fn print_header(file_info: &FileInfo) {
        let title = format!("📊 {} ", file_info.path).bold().blue();
        println!("{}", title);
        println!("{}", "═".repeat(50));
        
        if let Some(rows) = file_info.total_rows {
            println!("📁 Rows: {} | Columns: {} | Size: {:.1} MB",
                format!("{}", rows).yellow(),
                file_info.total_columns.to_string().yellow(),
                file_info.file_size_mb
            );
        } else {
            println!("📁 Columns: {} | Size: {:.1} MB",
                file_info.total_columns.to_string().yellow(),
                file_info.file_size_mb
            );
        }
    }
    
    fn print_scan_info(scan_info: &ScanInfo) {
        println!("\n✅ {} | ⏱️  {:.1}s",
            format!("Scanned: {} rows", scan_info.rows_scanned).green(),
            scan_info.scan_time_ms as f64 / 1000.0
        );
        
        if scan_info.sampling_ratio < 1.0 {
            println!("📊 Sample rate: {:.1}%", scan_info.sampling_ratio * 100.0);
        }
    }
    
    fn print_quality_issues(issues: &[QualityIssue]) {
        if issues.is_empty() {
            println!("\n✨ {}", "No quality issues found!".green().bold());
            return;
        }
        
        println!("\n⚠️  {} {}", 
            "QUALITY ISSUES FOUND:".red().bold(),
            format!("({})", issues.len()).red()
        );
        
        // Ordina per severity
        let mut sorted_issues = issues.to_vec();
        sorted_issues.sort_by_key(|i| match i.severity() {
            Severity::High => 0,
            Severity::Medium => 1,
            Severity::Low => 2,
        });
        
        for (idx, issue) in sorted_issues.iter().enumerate() {
            let severity_icon = match issue.severity() {
                Severity::High => "🔴",
                Severity::Medium => "🟡",
                Severity::Low => "🔵",
            };
            
            print!("\n{}. {} ", idx + 1, severity_icon);
            
            match issue {
                QualityIssue::MixedDateFormats { column, formats } => {
                    println!("[{}]: Mixed date formats", column.yellow());
                    for (format, count) in formats {
                        println!("   - {}: {} rows", format, count);
                    }
                }
                QualityIssue::NullValues { column, count, percentage } => {
                    println!("[{}]: {} null values ({:.1}%)", 
                        column.yellow(), 
                        format!("{}", count).red(),
                        percentage
                    );
                }
                QualityIssue::Duplicates { column, count } => {
                    println!("[{}]: {} duplicate values", 
                        column.yellow(), 
                        format!("{}", count).red()
                    );
                }
                QualityIssue::Outliers { column, values, threshold } => {
                    println!("[{}]: Outliers detected (>{}σ)", 
                        column.yellow(), 
                        threshold
                    );
                    for (_i, val) in values.iter().take(3).enumerate() {
                        println!("   - {}", val);
                    }
                    if values.len() > 3 {
                        println!("   ... and {} more", values.len() - 3);
                    }
                }
                QualityIssue::MixedTypes { column, types } => {
                    println!("[{}]: Mixed data types", column.yellow());
                    for (dtype, count) in types {
                        println!("   - {}: {} rows", dtype, count);
                    }
                }
            }
        }
    }
    
    fn print_column_summary(profiles: &[ColumnProfile]) {
        println!("\n📋 {}", "COLUMN SUMMARY:".bold());
        
        for profile in profiles.iter().take(5) {  // Mostra prime 5 colonne
            print!("\n[{}] ", profile.name.cyan());
            print!("{} ", format!("{:?}", profile.data_type).dimmed());
            
            if let Some(unique) = profile.unique_count {
                print!("| {} unique ", unique.to_string().green());
            }
            
            if profile.null_count > 0 {
                print!("| {} nulls ", profile.null_count.to_string().red());
            }
            
            // Mostra patterns trovati
            for pattern in &profile.patterns {
                if pattern.match_percentage > 50.0 {
                    print!("| {} ", format!("{}✓", pattern.name).green());
                }
            }
            
            println!();
            
            // Mostra stats inline
            match &profile.stats {
                ColumnStats::Numeric { min, max, mean, .. } => {
                    println!("   └─ Range: [{:.2} - {:.2}], Mean: {:.2}", 
                        min, max, mean);
                }
                ColumnStats::Text { min_length, max_length, avg_length } => {
                    println!("   └─ Length: [{} - {}], Avg: {:.1}", 
                        min_length, max_length, avg_length);
                }
                _ => {}
            }
        }
        
        if profiles.len() > 5 {
            println!("\n... and {} more columns", profiles.len() - 5);
        }
    }
    
    fn print_recommendations(issues: &[QualityIssue]) {
        if issues.is_empty() {
            return;
        }
        
        println!("\n💡 {}", "QUICK FIX COMMANDS:".bold().green());
        
        let mut fixes = Vec::new();
        
        for issue in issues {
            match issue {
                QualityIssue::MixedDateFormats { .. } => {
                    if !fixes.contains(&"standardize-dates") {
                        fixes.push("standardize-dates");
                    }
                }
                QualityIssue::Duplicates { .. } => {
                    if !fixes.contains(&"remove-duplicates") {
                        fixes.push("remove-duplicates");
                    }
                }
                _ => {}
            }
        }
        
        if !fixes.is_empty() {
            println!("   dataprof fix <file> --{} --output cleaned.csv", 
                fixes.join(" --"));
        }
        
        println!("   dataprof export-duckdb <file> --clean");
    }
}

// Helper per progress bar
use indicatif::{ProgressBar, ProgressStyle};

pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {msg} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-")
    );
    pb.set_message(message.to_string());
    pb
}