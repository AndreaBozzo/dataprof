//! Datetime statistics calculation
//!
//! # Date Format Handling
//!
//! ## Unambiguous Formats (Recommended)
//! - ISO 8601: `2023-01-15` (YYYY-MM-DD)
//! - ISO with slashes: `2023/01/15` (YYYY/MM/DD)
//! - ISO datetime: `2023-01-15T10:30:00`
//!
//! ## Ambiguous Formats
//! **WARNING**: Dates like `05/06/2023` are ambiguous and interpreted as European (DD/MM/YYYY):
//! - `15/01/2023` → January 15, 2023
//! - `15-01-2023` → January 15, 2023
//! - `15.01.2023` → January 15, 2023
//!
//! For unambiguous parsing, use ISO 8601 format (YYYY-MM-DD).

use crate::types::ColumnStats;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};
use std::collections::HashMap;

/// Parse result containing both date and optional time component
struct ParsedDateTime {
    date: NaiveDate,
    datetime: Option<NaiveDateTime>,
}

pub fn calculate_datetime_stats(data: &[String]) -> ColumnStats {
    // Parse once, store both date and optional datetime
    let parsed: Vec<ParsedDateTime> = data.iter().filter_map(|s| parse_flexible_full(s)).collect();

    if parsed.is_empty() {
        return empty_datetime_stats();
    }

    // Extract dates for date-based calculations
    let dates: Vec<NaiveDate> = parsed.iter().map(|p| p.date).collect();

    // Calculate min/max and duration
    let min_date = dates.iter().min().unwrap();
    let max_date = dates.iter().max().unwrap();
    let duration_days = (*max_date - *min_date).num_days() as f64;

    // Build distributions from dates
    let year_distribution = build_year_distribution(&dates);
    let month_distribution = build_month_distribution(&dates);
    let day_of_week_distribution = build_day_of_week_distribution(&dates);

    // Build hour distribution from datetimes (if any have time components)
    let datetimes: Vec<NaiveDateTime> = parsed.iter().filter_map(|p| p.datetime).collect();

    let hour_distribution = if datetimes.is_empty() {
        None
    } else {
        Some(build_hour_distribution(&datetimes))
    };

    ColumnStats::DateTime {
        min_datetime: min_date.format("%Y-%m-%d").to_string(),
        max_datetime: max_date.format("%Y-%m-%d").to_string(),
        duration_days,
        year_distribution,
        month_distribution,
        day_of_week_distribution,
        hour_distribution,
    }
}

fn empty_datetime_stats() -> ColumnStats {
    ColumnStats::DateTime {
        min_datetime: String::new(),
        max_datetime: String::new(),
        duration_days: 0.0,
        year_distribution: HashMap::new(),
        month_distribution: HashMap::new(),
        day_of_week_distribution: HashMap::new(),
        hour_distribution: None,
    }
}

fn parse_flexible_full(s: &str) -> Option<ParsedDateTime> {
    let trimmed = s.trim();

    // Try datetime formats first (these have time components)
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return Some(ParsedDateTime {
            date: dt.date(),
            datetime: Some(dt),
        });
    }

    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Some(ParsedDateTime {
            date: dt.date(),
            datetime: Some(dt),
        });
    }

    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%d/%m/%Y %H:%M:%S") {
        return Some(ParsedDateTime {
            date: dt.date(),
            datetime: Some(dt),
        });
    }

    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(ParsedDateTime {
            date: dt.date(),
            datetime: Some(dt),
        });
    }

    // Try date-only formats (no time component)
    // NOTE: Order matters for ambiguous formats (DD/MM/YYYY vs MM/DD/YYYY)
    // European formats are tried first. For unambiguous parsing, use ISO 8601.
    let date_formats = vec![
        "%Y-%m-%d", // ISO: 2023-01-15 (unambiguous)
        "%d/%m/%Y", // European: 15/01/2023 (DD/MM/YYYY)
        "%d-%m-%Y", // European: 15-01-2023 (DD-MM-YYYY)
        "%d.%m.%Y", // European: 15.01.2023 (DD.MM.YYYY)
        "%Y/%m/%d", // ISO slash: 2023/01/15 (unambiguous)
        "%m/%d/%Y", // US: 01/15/2023 (MM/DD/YYYY) - fallback if European fails
    ];

    for format in date_formats {
        if let Ok(date) = NaiveDate::parse_from_str(trimmed, format) {
            return Some(ParsedDateTime {
                date,
                datetime: None,
            });
        }
    }

    None
}

fn build_year_distribution(dates: &[NaiveDate]) -> HashMap<i32, usize> {
    let mut dist = HashMap::new();
    for date in dates {
        *dist.entry(date.year()).or_insert(0) += 1;
    }
    dist
}

fn build_month_distribution(dates: &[NaiveDate]) -> HashMap<u32, usize> {
    let mut dist = HashMap::new();
    for date in dates {
        *dist.entry(date.month()).or_insert(0) += 1;
    }
    dist
}

fn build_day_of_week_distribution(dates: &[NaiveDate]) -> HashMap<String, usize> {
    let mut dist = HashMap::new();
    for date in dates {
        let day_name = weekday_name(date.weekday());
        *dist.entry(day_name.to_string()).or_insert(0) += 1;
    }
    dist
}

fn weekday_name(weekday: Weekday) -> &'static str {
    match weekday {
        Weekday::Mon => "Monday",
        Weekday::Tue => "Tuesday",
        Weekday::Wed => "Wednesday",
        Weekday::Thu => "Thursday",
        Weekday::Fri => "Friday",
        Weekday::Sat => "Saturday",
        Weekday::Sun => "Sunday",
    }
}

fn build_hour_distribution(datetimes: &[NaiveDateTime]) -> HashMap<u32, usize> {
    let mut dist = HashMap::new();
    for dt in datetimes {
        *dist.entry(dt.hour()).or_insert(0) += 1;
    }
    dist
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iso_date() {
        let parsed = parse_flexible_full("2023-01-15").unwrap();
        assert_eq!(parsed.date.year(), 2023);
        assert_eq!(parsed.date.month(), 1);
        assert_eq!(parsed.date.day(), 15);
        assert!(parsed.datetime.is_none());
    }

    #[test]
    fn test_parse_european_format() {
        let parsed = parse_flexible_full("15/01/2023").unwrap();
        assert_eq!(parsed.date.day(), 15);
        assert_eq!(parsed.date.month(), 1);
        assert_eq!(parsed.date.year(), 2023);
        assert!(parsed.datetime.is_none());
    }

    #[test]
    fn test_parse_us_format() {
        let parsed = parse_flexible_full("01/15/2023").unwrap();
        assert_eq!(parsed.date.month(), 1);
        assert_eq!(parsed.date.day(), 15);
        assert!(parsed.datetime.is_none());
    }

    #[test]
    fn test_parse_datetime_iso() {
        let parsed = parse_flexible_full("2023-01-15T10:30:00").unwrap();
        assert_eq!(parsed.date.year(), 2023);
        assert!(parsed.datetime.is_some());
        let dt = parsed.datetime.unwrap();
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn test_year_distribution() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 6, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        ];
        let dist = build_year_distribution(&dates);
        assert_eq!(dist.get(&2023), Some(&2));
        assert_eq!(dist.get(&2024), Some(&1));
    }

    #[test]
    fn test_month_distribution() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
        ];
        let dist = build_month_distribution(&dates);
        assert_eq!(dist.get(&1), Some(&2));
        assert_eq!(dist.get(&2), Some(&1));
    }

    #[test]
    fn test_day_of_week_distribution() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(), // Monday
            NaiveDate::from_ymd_opt(2023, 1, 3).unwrap(), // Tuesday
            NaiveDate::from_ymd_opt(2023, 1, 9).unwrap(), // Monday again
        ];
        let dist = build_day_of_week_distribution(&dates);
        assert_eq!(dist.get("Monday"), Some(&2));
        assert_eq!(dist.get("Tuesday"), Some(&1));
    }

    #[test]
    fn test_duration_calculation() {
        let data = vec!["2023-01-01".to_string(), "2023-01-31".to_string()];
        let stats = calculate_datetime_stats(&data);

        match stats {
            ColumnStats::DateTime { duration_days, .. } => {
                assert_eq!(duration_days, 30.0);
            }
            _ => panic!("Expected DateTime stats"),
        }
    }

    #[test]
    fn test_hour_distribution() {
        let data = vec![
            "2023-01-01T10:00:00".to_string(),
            "2023-01-01T10:30:00".to_string(),
            "2023-01-01T14:00:00".to_string(),
        ];
        let stats = calculate_datetime_stats(&data);

        match stats {
            ColumnStats::DateTime {
                hour_distribution, ..
            } => {
                let dist = hour_distribution.unwrap();
                assert_eq!(dist.get(&10), Some(&2));
                assert_eq!(dist.get(&14), Some(&1));
            }
            _ => panic!("Expected DateTime stats"),
        }
    }

    #[test]
    fn test_empty_data() {
        let data: Vec<String> = vec![];
        let stats = calculate_datetime_stats(&data);

        match stats {
            ColumnStats::DateTime {
                min_datetime,
                max_datetime,
                duration_days,
                ..
            } => {
                assert!(min_datetime.is_empty());
                assert!(max_datetime.is_empty());
                assert_eq!(duration_days, 0.0);
            }
            _ => panic!("Expected DateTime stats"),
        }
    }
}
