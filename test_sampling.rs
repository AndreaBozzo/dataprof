use dataprof::analyze_csv_with_sampling;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = Path::new("examples/sales_data_problematic.csv");
    
    println!("🧪 Testing analyze_csv_with_sampling...\n");
    
    let report = analyze_csv_with_sampling(file_path)?;
    
    println!("📊 File: {}", report.file_info.path);
    println!("📁 Total Rows: {:?}", report.file_info.total_rows);
    println!("📁 Total Columns: {}", report.file_info.total_columns);
    println!("📁 File Size: {:.1} MB", report.file_info.file_size_mb);
    
    println!("\n⏱️  Scan Time: {} ms", report.scan_info.scan_time_ms);
    println!("📊 Rows Scanned: {}", report.scan_info.rows_scanned);
    println!("📊 Sampling Ratio: {:.1}%", report.scan_info.sampling_ratio * 100.0);
    
    println!("\n⚠️  Quality Issues Found: {}", report.issues.len());
    for issue in &report.issues {
        match issue {
            dataprof::QualityIssue::NullValues { column, count, percentage } => {
                println!("  - [{}]: {} null values ({:.1}%)", column, count, percentage);
            }
            dataprof::QualityIssue::Outliers { column, values, threshold } => {
                println!("  - [{}]: {} outliers (>{}σ)", column, values.len(), threshold);
                for (i, val) in values.iter().take(3).enumerate() {
                    println!("    {}", val);
                }
            }
            dataprof::QualityIssue::Duplicates { column, count } => {
                println!("  - [{}]: {} duplicates", column, count);
            }
            _ => {
                println!("  - {:?}", issue);
            }
        }
    }
    
    println!("\n📋 Column Summary:");
    for profile in &report.column_profiles {
        print!("[{}] {:?}", profile.name, profile.data_type);
        if profile.null_count > 0 {
            print!(" | {} nulls", profile.null_count);
        }
        if let Some(unique) = profile.unique_count {
            print!(" | {} unique", unique);
        }
        println!();
    }
    
    Ok(())
}