use regex::Regex;

use crate::types::DataType;

pub fn infer_type(data: &[String]) -> DataType {
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();
    if non_empty.is_empty() {
        return DataType::String;
    }

    // Check dates first (before numeric to catch date-like numbers)
    let date_formats = [
        r"^\d{4}-\d{2}-\d{2}$", // YYYY-MM-DD
        r"^\d{2}/\d{2}/\d{4}$", // DD/MM/YYYY or MM/DD/YYYY
        r"^\d{2}-\d{2}-\d{4}$", // DD-MM-YYYY
    ];

    for pattern in &date_formats {
        if let Ok(regex) = Regex::new(pattern) {
            let date_matches = non_empty.iter().filter(|s| regex.is_match(s)).count();
            if date_matches as f64 / non_empty.len() as f64 > 0.8 {
                return DataType::Date;
            }
        }
    }

    // Check if all are integers
    let integer_count = non_empty
        .iter()
        .filter(|s| s.parse::<i64>().is_ok())
        .count();

    if integer_count == non_empty.len() {
        return DataType::Integer;
    }

    // Check if all are floats
    let float_count = non_empty
        .iter()
        .filter(|s| s.parse::<f64>().is_ok())
        .count();

    if float_count == non_empty.len() {
        return DataType::Float;
    }

    DataType::String
}
