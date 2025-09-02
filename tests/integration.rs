use dataprof::{analyze_csv, DataType};
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_basic_csv_analysis() {
    // Create test CSV
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();

    writeln!(file, "id,name,email,amount,signup_date").unwrap();
    writeln!(file, "1,Alice,alice@example.com,100.50,2024-01-01").unwrap();
    writeln!(file, "2,Bob,bob@example.com,200.75,2024-01-02").unwrap();
    writeln!(file, "3,Charlie,charlie@invalid,150.00,2024-01-03").unwrap(); // Invalid email
    writeln!(file, "4,David,,120.00,2024-01-04").unwrap(); // Missing email

    // Analyze
    let profiles = analyze_csv(&file_path).unwrap();

    // Verify results
    assert_eq!(profiles.len(), 5);

    // Check ID column (should be Integer)
    let id_profile = profiles.iter().find(|p| p.name == "id").unwrap();
    assert!(matches!(id_profile.data_type, DataType::Integer));
    assert_eq!(id_profile.null_count, 0);

    // Check email column (should have Email pattern)
    let email_profile = profiles.iter().find(|p| p.name == "email").unwrap();
    assert!(matches!(email_profile.data_type, DataType::String));
    assert_eq!(email_profile.null_count, 1); // David's missing email

    // Should detect email pattern (75% of non-null values are valid emails)
    let has_email_pattern = email_profile
        .patterns
        .iter()
        .any(|p| p.name == "Email" && p.match_percentage > 50.0);
    assert!(has_email_pattern);

    // Check date column (should be Date type)
    let date_profile = profiles.iter().find(|p| p.name == "signup_date").unwrap();
    assert!(matches!(date_profile.data_type, DataType::Date));
}

#[test]
fn test_pattern_detection() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("patterns.csv");
    let mut file = File::create(&file_path).unwrap();

    writeln!(file, "email,phone,url").unwrap();
    writeln!(file, "john@example.com,555-123-4567,https://example.com").unwrap();
    writeln!(file, "jane@test.org,(555) 234-5678,http://test.org").unwrap();
    writeln!(file, "invalid-email,555.345.6789,not-a-url").unwrap();

    let profiles = analyze_csv(&file_path).unwrap();

    // Email column should detect Email pattern
    let email_profile = profiles.iter().find(|p| p.name == "email").unwrap();
    let email_pattern = email_profile
        .patterns
        .iter()
        .find(|p| p.name == "Email")
        .unwrap();
    assert_eq!(email_pattern.match_count, 2); // 2 out of 3 are valid emails

    // Phone column should detect US phone pattern
    let phone_profile = profiles.iter().find(|p| p.name == "phone").unwrap();
    let phone_pattern = phone_profile
        .patterns
        .iter()
        .find(|p| p.name == "Phone (US)")
        .unwrap();
    assert_eq!(phone_pattern.match_count, 3); // All 3 match US phone pattern

    // URL column should detect URL pattern
    let url_profile = profiles.iter().find(|p| p.name == "url").unwrap();
    let url_pattern = url_profile
        .patterns
        .iter()
        .find(|p| p.name == "URL")
        .unwrap();
    assert_eq!(url_pattern.match_count, 2); // 2 out of 3 are valid URLs
}

#[test]
fn test_numeric_analysis() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("numbers.csv");
    let mut file = File::create(&file_path).unwrap();

    writeln!(file, "integers,floats,mixed").unwrap();
    writeln!(file, "1,1.5,text").unwrap();
    writeln!(file, "2,2.7,456").unwrap();
    writeln!(file, "3,3.9,more text").unwrap();

    let profiles = analyze_csv(&file_path).unwrap();

    // Integers column
    let int_profile = profiles.iter().find(|p| p.name == "integers").unwrap();
    assert!(matches!(int_profile.data_type, DataType::Integer));

    // Floats column
    let float_profile = profiles.iter().find(|p| p.name == "floats").unwrap();
    assert!(matches!(float_profile.data_type, DataType::Float));

    // Mixed column (should be String since not all numeric)
    let mixed_profile = profiles.iter().find(|p| p.name == "mixed").unwrap();
    assert!(matches!(mixed_profile.data_type, DataType::String));
}
