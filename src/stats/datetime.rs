use crate::types::ColumnStats;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};
use std::collections::HashMap;

pub fn calculate_datetime_stats(data: &[String]) -> ColumnStats {
    let dates = parse_dates(data);

    if dates.is_empty() {
        return empty_datetime_stats();
    }

    // Calculate min/max and duration
    let min_date = dates.iter().min().unwrap();
    let max_date = dates.iter().max().unwrap();
    let duration_days = (*max_date - *min_date).num_days() as f64;

    // Build distributions
    let year_distribution = build_year_distribution(&dates);
    let month_distribution = build_month_distribution(&dates);
    let day_of_week_distribution = build_day_of_week_distribution(&dates);

    // Check if times present (only for datetime, not date)
    let hour_distribution = detect_and_build_hour_distribution(data);

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

fn parse_dates(data: &[String]) -> Vec<NaiveDate> {
    data.iter().filter_map(|s| parse_flexible(s)).collect()
}

fn parse_flexible(s: &str) -> Option<NaiveDate> {
    let trimmed = s.trim();

    // Try datetime formats first (extract date part)
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.date());
    }

    // Additional datetime format variations
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.date());
    }

    // Try date-only formats
    let formats = vec![
        "%Y-%m-%d", // ISO: 2023-01-15
        "%d/%m/%Y", // European: 15/01/2023
        "%d-%m-%Y", // European: 15-01-2023
        "%d.%m.%Y", // European: 15.01.2023
        "%Y/%m/%d", // ISO slash: 2023/01/15
        "%m/%d/%Y", // US: 01/15/2023
    ];

    for format in formats {
        if let Ok(date) = NaiveDate::parse_from_str(trimmed, format) {
            return Some(date);
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

fn detect_and_build_hour_distribution(data: &[String]) -> Option<HashMap<u32, usize>> {
    let datetimes: Vec<NaiveDateTime> = data
        .iter()
        .filter_map(|s| parse_datetime_with_time(s.trim()))
        .collect();

    if datetimes.is_empty() {
        return None;
    }

    let mut dist = HashMap::new();
    for dt in datetimes {
        *dist.entry(dt.hour()).or_insert(0) += 1;
    }
    Some(dist)
}

fn parse_datetime_with_time(s: &str) -> Option<NaiveDateTime> {
    let formats = vec![
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f", // With milliseconds
    ];

    for format in formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, format) {
            return Some(dt);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iso_date() {
        let date = parse_flexible("2023-01-15");
        assert!(date.is_some());
        assert_eq!(date.unwrap().year(), 2023);
        assert_eq!(date.unwrap().month(), 1);
        assert_eq!(date.unwrap().day(), 15);
    }

    #[test]
    fn test_parse_european_format() {
        let date = parse_flexible("15/01/2023");
        assert!(date.is_some());
        assert_eq!(date.unwrap().day(), 15);
        assert_eq!(date.unwrap().month(), 1);
        assert_eq!(date.unwrap().year(), 2023);
    }

    #[test]
    fn test_parse_us_format() {
        let date = parse_flexible("01/15/2023");
        assert!(date.is_some());
        assert_eq!(date.unwrap().month(), 1);
        assert_eq!(date.unwrap().day(), 15);
    }

    #[test]
    fn test_parse_datetime_iso() {
        let date = parse_flexible("2023-01-15T10:30:00");
        assert!(date.is_some());
        assert_eq!(date.unwrap().year(), 2023);
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
