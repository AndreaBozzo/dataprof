use dataprof::*;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_basic_csv_analysis() {
    // Crea CSV di test
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    
    writeln!(file, "id,name,email,amount,date").unwrap();
    writeln!(file, "1,Alice,alice@example.com,100.50,2024-01-01").unwrap();
    writeln!(file, "2,Bob,bob@example.com,200.75,2024-01-02").unwrap();
    writeln!(file, "3,Charlie,charlie@example,150.00,01/02/2024").unwrap(); // Email invalida, formato data diverso
    writeln!(file, "4,David,,120.00,2024-01-04").unwrap(); // Email mancante
    
    // Analizza
    let sampler = Sampler::new(0.1);
    let (df, _) = sampler.sample_csv(&file_path).unwrap();
    let profiles = analyze_dataframe(&df);
    let issues = QualityChecker::check_dataframe(&df, &profiles);
    
    // Verifica risultati
    assert_eq!(df.width(), 5);
    assert_eq!(df.height(), 4);
    
    // Dovrebbe trovare issues
    assert!(!issues.is_empty());
    
    // Verifica pattern detection
    let email_profile = profiles.iter().find(|p| p.name == "email").unwrap();
    assert!(!email_profile.patterns.is_empty());
}

#[test]
fn test_large_file_sampling() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("large.csv");
    let mut file = File::create(&file_path).unwrap();
    
    // Header
    writeln!(file, "id,value").unwrap();
    
    // Genera 100k righe
    for i in 0..100_000 {
        writeln!(file, "{},{}", i, i * 2).unwrap();
    }
    
    // Test sampling
    let sampler = Sampler::new(10.0);
    let (df, sample_info) = sampler.sample_csv(&file_path).unwrap();
    
    // Verifica che abbia fatto sampling
    assert!(df.height() < 100_000);
    assert!(sample_info.sampling_ratio < 1.0);
}

#[test]
fn test_quality_issues_detection() {
    use polars::prelude::*;
    
    // Crea DataFrame con issues
    let df = df![
        "id" => [1, 2, 3, 4, 5],
        "value" => [10.0, 20.0, 30.0, 1000.0, 40.0], // 1000 è outlier
        "category" => ["A", "A", "B", "B", "A"],
        "date" => ["2024-01-01", "2024-01-02", "01/03/2024", "2024-01-04", "05/01/2024"], // Formati misti
    ].unwrap();
    
    let profiles = analyze_dataframe(&df);
    let issues = QualityChecker::check_dataframe(&df, &profiles);
    
    // Dovrebbe trovare outlier e date miste
    let outlier_issue = issues.iter().find(|i| matches!(i, QualityIssue::Outliers { .. }));
    assert!(outlier_issue.is_some());
    
    let date_issue = issues.iter().find(|i| matches!(i, QualityIssue::MixedDateFormats { .. }));
    assert!(date_issue.is_some());
}