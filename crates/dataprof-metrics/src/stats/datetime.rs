//! Datetime statistics calculation
//!
//! # Date Format Handling
//!
//! ## Unambiguous Formats (Recommended)
//! - ISO 8601: `2023-01-15` (YYYY-MM-DD)
//! - ISO with slashes: `2023/01/15` (YYYY/MM/DD)
//! - ISO datetime: `2023-01-15T10:30:00`
//!
//! ## Day/month order
//! Dashed and dotted dates are read day-first (`15-01-2023`, `15.01.2023`).
//! Slash dates exist in both orders, so the order is decided **once per
//! column** from the column's own values ([`resolve_slash_order`], #811): a
//! column holding `12/31/2024` is month-first, and its `01/02/2024` is then
//! 2 January, not 1 February. A column where nothing decides it is read
//! day-first, and one holding values that only work in opposite orders is
//! reported as mixed. [`SlashDateOrder`] records which happened.
//!
//! For unambiguous parsing, use ISO 8601 format (YYYY-MM-DD).
//!
//! ## UTC Offsets
//!
//! A value carrying an RFC 3339 offset (`Z`, `+HH:MM`, `-HH:MM`) designates an
//! *instant*, and **dataprof normalizes it to UTC** before it reaches any
//! statistic or quality predicate. `2024-01-15T23:00:00-05:00` therefore counts
//! as 2024-01-16 at hour 04, not as 2024-01-15 at hour 23.
//!
//! The alternative — keeping each value's own wall clock — makes two values that
//! name the same instant compare as different and two values that name different
//! instants compare as equal, and it compares an offset-bearing value against a
//! UTC "now" in the Timeliness dimension. Normalizing costs the local reading of
//! an hour distribution; it buys min/max, duration, and every comparison being
//! about one timeline.
//!
//! A value with no offset carries no timezone to normalize, so it is read as it
//! was written. Mixing the two in one column mixes local wall clocks with UTC
//! instants; that is a property of the data, not something this module can fix.

use crate::types::{ColumnStats, DateTimeStats, SlashDateOrder};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};
use std::collections::HashMap;

/// Parse result containing both date and optional time component
struct ParsedDateTime {
    date: NaiveDate,
    datetime: Option<NaiveDateTime>,
}

/// Slash date forms, day-first. Swapping `%d` and `%m` gives the month-first
/// form of each.
const SLASH_DATETIME_DAY_FIRST: &str = "%d/%m/%Y %H:%M:%S";
const SLASH_DATETIME_MONTH_FIRST: &str = "%m/%d/%Y %H:%M:%S";
const SLASH_DATE_DAY_FIRST: &str = "%d/%m/%Y";
const SLASH_DATE_MONTH_FIRST: &str = "%m/%d/%Y";

/// Whether `value` parses as a slash date or datetime in `format_date` /
/// `format_datetime`.
fn parses_slash(value: &str, format_date: &str, format_datetime: &str) -> bool {
    NaiveDate::parse_from_str(value, format_date).is_ok()
        || NaiveDateTime::parse_from_str(value, format_datetime).is_ok()
}

/// Decide how a column's slash dates are read, from the column's own values.
///
/// A value that parses only day-first (`31/12/2024`) is evidence for
/// day-first, one that parses only month-first (`12/31/2024`) for
/// month-first, and one that parses both ways (`01/02/2024`) is evidence for
/// neither. Returns `None` when no value is a slash date. Resolving per value
/// instead read every ambiguous value of a US column day-first, so half its
/// dates were wrong by months with nothing in the report to say so (#811).
pub fn resolve_slash_order<S: AsRef<str>>(values: &[S]) -> Option<SlashDateOrder> {
    let mut slash_dates = 0usize;
    let mut day_only = 0usize;
    let mut month_only = 0usize;
    for value in values {
        let trimmed = value.as_ref().trim();
        // Most values of most columns carry no slash; skip the four parses.
        if !trimmed.contains('/') {
            continue;
        }
        let day_first = parses_slash(trimmed, SLASH_DATE_DAY_FIRST, SLASH_DATETIME_DAY_FIRST);
        let month_first = parses_slash(trimmed, SLASH_DATE_MONTH_FIRST, SLASH_DATETIME_MONTH_FIRST);
        match (day_first, month_first) {
            (true, true) => slash_dates += 1,
            (true, false) => {
                slash_dates += 1;
                day_only += 1;
            }
            (false, true) => {
                slash_dates += 1;
                month_only += 1;
            }
            (false, false) => {}
        }
    }
    match (slash_dates, day_only, month_only) {
        (0, _, _) => None,
        (_, 0, 0) => Some(SlashDateOrder::AssumedDayFirst),
        (_, _, 0) => Some(SlashDateOrder::DayFirst),
        (_, 0, _) => Some(SlashDateOrder::MonthFirst),
        _ => Some(SlashDateOrder::Mixed),
    }
}

/// Whether ambiguous slash values are read month-first under `order`.
///
/// Every order but `MonthFirst` reads them day-first. In a `Mixed` column the
/// values that only parse month-first still do, through the fallback.
fn reads_month_first(order: Option<SlashDateOrder>) -> bool {
    order == Some(SlashDateOrder::MonthFirst)
}

pub fn calculate_datetime_stats(data: &[String]) -> ColumnStats {
    ColumnStats::DateTime(compute_datetime_stats(data))
}

/// Compute datetime stats and return the inner struct directly.
pub fn compute_datetime_stats(data: &[String]) -> DateTimeStats {
    let slash_date_order = resolve_slash_order(data);
    let month_first = reads_month_first(slash_date_order);
    let parsed: Vec<ParsedDateTime> = data
        .iter()
        .filter_map(|s| parse_flexible_full(s, month_first))
        .collect();

    if parsed.is_empty() {
        return DateTimeStats::empty();
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

    DateTimeStats {
        min_datetime: min_date.format("%Y-%m-%d").to_string(),
        max_datetime: max_date.format("%Y-%m-%d").to_string(),
        duration_days,
        year_distribution,
        month_distribution,
        day_of_week_distribution,
        hour_distribution,
        slash_date_order,
    }
}

/// Parse one value, reading an ambiguous slash date month-first when
/// `month_first` is set and day-first otherwise. A slash value that parses in
/// only one order is read in that order either way.
fn parse_flexible_full(s: &str, month_first: bool) -> Option<ParsedDateTime> {
    let trimmed = s.trim();

    // An offset-bearing value is an instant, normalized to UTC before anything
    // else sees it — see the "UTC Offsets" section in the module docs. `Z`
    // values are unaffected (their offset is already zero); the rule only moves
    // values written at a non-zero offset.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        let utc = dt.naive_utc();
        return Some(ParsedDateTime {
            date: utc.date(),
            datetime: Some(utc),
        });
    }

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

    // The column's order first, then the other one: a value that parses in
    // only one order is read in that order, which is what `resolve_slash_order`
    // counted it as.
    let (slash_datetimes, slash_dates) = if month_first {
        (
            [SLASH_DATETIME_MONTH_FIRST, SLASH_DATETIME_DAY_FIRST],
            [SLASH_DATE_MONTH_FIRST, SLASH_DATE_DAY_FIRST],
        )
    } else {
        (
            [SLASH_DATETIME_DAY_FIRST, SLASH_DATETIME_MONTH_FIRST],
            [SLASH_DATE_DAY_FIRST, SLASH_DATE_MONTH_FIRST],
        )
    };

    for format in slash_datetimes {
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, format) {
            return Some(ParsedDateTime {
                date: dt.date(),
                datetime: Some(dt),
            });
        }
    }

    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(ParsedDateTime {
            date: dt.date(),
            datetime: Some(dt),
        });
    }

    // Try date-only formats (no time component). Only the slash form exists in
    // both orders; which one is tried first is the column's decision.
    let date_formats = [
        "%Y-%m-%d",     // ISO: 2023-01-15 (unambiguous)
        slash_dates[0], // the column's slash order
        "%d-%m-%Y",     // European: 15-01-2023 (DD-MM-YYYY)
        "%d.%m.%Y",     // European: 15.01.2023 (DD.MM.YYYY)
        "%Y/%m/%d",     // ISO slash: 2023/01/15 (unambiguous)
        slash_dates[1], // the other slash order, for a value only it parses
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

/// Parse a raw quality-metric value and return its calendar date.
///
/// Unlike the descriptive datetime statistics path, quality predicates do not
/// normalize surrounding whitespace: a value must be directly parseable as it
/// appeared in the source. The calendar parser validates month/day ranges, so
/// shape-only strings such as `2024-13-45` are rejected.
///
/// Prefer this over [`parse_raw_datetime_year`] whenever the caller compares
/// values against each other or against a reference point. The supported
/// formats include `DD/MM/YYYY` and `MM/DD/YYYY`, which do not sort
/// lexicographically and cannot be ordered by year alone.
///
/// `order` is the column's [`resolve_slash_order`] result, so a value is read
/// the way the column's statistics read it.
pub(crate) fn parse_raw_datetime_date(s: &str, order: Option<SlashDateOrder>) -> Option<NaiveDate> {
    if !looks_like_raw_datetime_candidate(s) {
        return None;
    }
    parse_flexible_full(s, reads_month_first(order)).map(|parsed| parsed.date)
}

/// Parse a raw quality-metric value and return its calendar year.
///
/// A year is the right granularity only for thresholds expressed in whole
/// years. Anything finer must use [`parse_raw_datetime_date`]. The year of a
/// slash date is the same in either order, so no column order is needed.
pub(crate) fn parse_raw_datetime_year(s: &str) -> Option<i32> {
    parse_raw_datetime_date(s, None).map(|date| date.year())
}

/// Cheap shape check before attempting the multi-format chrono parser.
///
/// Every supported raw format starts with either `YYYY<sep>MM<sep>DD` or
/// `DD<sep>MM<sep>YYYY`. Malformed date-shaped values remain candidates so the
/// calendar parser can reject and count values such as `2024-13-45`.
fn looks_like_raw_datetime_candidate(s: &str) -> bool {
    if s != s.trim() || s.len() < 10 {
        return false;
    }

    let bytes = s.as_bytes();
    let ascii_digits = |range: std::ops::Range<usize>| {
        bytes
            .get(range)
            .is_some_and(|slice| slice.iter().all(u8::is_ascii_digit))
    };
    let supported_separator = |byte: u8| matches!(byte, b'-' | b'/' | b'.');

    let year_first = ascii_digits(0..4)
        && bytes.get(4).is_some_and(|byte| supported_separator(*byte))
        && bytes.get(7) == bytes.get(4);
    let year_last = ascii_digits(6..10)
        && bytes.get(2).is_some_and(|byte| supported_separator(*byte))
        && bytes.get(5) == bytes.get(2);

    year_first || year_last
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
        let parsed = parse_flexible_full("2023-01-15", false).unwrap();
        assert_eq!(parsed.date.year(), 2023);
        assert_eq!(parsed.date.month(), 1);
        assert_eq!(parsed.date.day(), 15);
        assert!(parsed.datetime.is_none());
    }

    #[test]
    fn test_parse_european_format() {
        let parsed = parse_flexible_full("15/01/2023", false).unwrap();
        assert_eq!(parsed.date.day(), 15);
        assert_eq!(parsed.date.month(), 1);
        assert_eq!(parsed.date.year(), 2023);
        assert!(parsed.datetime.is_none());
    }

    #[test]
    fn test_parse_us_format() {
        let parsed = parse_flexible_full("01/15/2023", false).unwrap();
        assert_eq!(parsed.date.month(), 1);
        assert_eq!(parsed.date.day(), 15);
        assert!(parsed.datetime.is_none());
    }

    #[test]
    fn test_parse_datetime_iso() {
        let parsed = parse_flexible_full("2023-01-15T10:30:00", false).unwrap();
        assert_eq!(parsed.date.year(), 2023);
        assert!(parsed.datetime.is_some());
        let dt = parsed.datetime.unwrap();
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn raw_datetime_year_validates_the_complete_calendar_value() {
        assert_eq!(parse_raw_datetime_year("2024-02-29"), Some(2024));
        assert_eq!(parse_raw_datetime_year("2024-02-29T10:30:00Z"), Some(2024));
        assert_eq!(parse_raw_datetime_year("1800-01-01"), Some(1800));
        assert_eq!(parse_raw_datetime_year("2200-01-01"), Some(2200));
        assert_eq!(parse_raw_datetime_year("2024-13-45"), None);
        assert_eq!(parse_raw_datetime_year("2023-02-29"), None);
        assert_eq!(parse_raw_datetime_year(" 2024-02-29"), None);
        assert_eq!(parse_raw_datetime_year("ordinary text value"), None);
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
            ColumnStats::DateTime(d) => {
                assert_eq!(d.duration_days, 30.0);
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
            ColumnStats::DateTime(d) => {
                let dist = d.hour_distribution.unwrap();
                assert_eq!(dist.get(&10), Some(&2));
                assert_eq!(dist.get(&14), Some(&1));
            }
            _ => panic!("Expected DateTime stats"),
        }
    }

    fn values(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    #[test]
    fn the_column_decides_the_slash_order() {
        for (column, expected) in [
            (
                &["12/31/2024", "01/02/2024"][..],
                Some(SlashDateOrder::MonthFirst),
            ),
            (
                &["31/12/2024", "01/02/2024"][..],
                Some(SlashDateOrder::DayFirst),
            ),
            (
                &["01/02/2024", "03/04/2024"][..],
                Some(SlashDateOrder::AssumedDayFirst),
            ),
            (
                &["12/31/2024", "31/12/2024"][..],
                Some(SlashDateOrder::Mixed),
            ),
            // Datetimes carry the same evidence as dates.
            (
                &["12/31/2024 10:00:00", "01/02/2024 09:00:00"][..],
                Some(SlashDateOrder::MonthFirst),
            ),
            // Other forms, nulls and junk are no evidence either way.
            (&["2024-01-15", "15.01.2024", "", "junk"][..], None),
            (
                &["2024-01-15", "12/31/2024", "junk"][..],
                Some(SlashDateOrder::MonthFirst),
            ),
        ] {
            assert_eq!(resolve_slash_order(column), expected, "{column:?}");
        }
    }

    /// A US export (#811): every value is month-first, and the three that
    /// also parse day-first used to be read day-first.
    #[test]
    fn a_month_first_column_reads_its_ambiguous_dates_month_first() {
        let data = values(&[
            "12/31/2024",
            "01/02/2024",
            "03/04/2024",
            "12/30/2024",
            "05/06/2024",
            "11/29/2024",
        ]);

        let stats = compute_datetime_stats(&data);

        assert_eq!(stats.slash_date_order, Some(SlashDateOrder::MonthFirst));
        assert_eq!(stats.min_datetime, "2024-01-02");
        assert_eq!(stats.max_datetime, "2024-12-31");
        assert_eq!(
            stats.month_distribution,
            HashMap::from([(12, 2), (1, 1), (3, 1), (5, 1), (11, 1)])
        );
    }

    #[test]
    fn a_day_first_column_is_read_as_before() {
        let data = values(&["31/12/2024", "01/02/2024", "03/04/2024"]);

        let stats = compute_datetime_stats(&data);

        assert_eq!(stats.slash_date_order, Some(SlashDateOrder::DayFirst));
        assert_eq!(stats.min_datetime, "2024-02-01");
        assert_eq!(
            stats.month_distribution,
            HashMap::from([(12, 1), (2, 1), (4, 1)])
        );
    }

    #[test]
    fn a_month_first_datetime_column_parses_every_value() {
        // No month-first datetime form was tried before, so `12/31/2024
        // 10:00:00` failed to parse at all.
        let data = values(&["12/31/2024 10:00:00", "01/02/2024 09:30:00"]);

        let stats = compute_datetime_stats(&data);

        assert_eq!(stats.slash_date_order, Some(SlashDateOrder::MonthFirst));
        assert_eq!(stats.min_datetime, "2024-01-02");
        assert_eq!(stats.max_datetime, "2024-12-31");
        assert_eq!(
            stats.hour_distribution,
            Some(HashMap::from([(10, 1), (9, 1)]))
        );
    }

    #[test]
    fn a_column_without_slash_dates_records_no_order() {
        let data = values(&["2024-01-15", "15.01.2024"]);
        assert_eq!(compute_datetime_stats(&data).slash_date_order, None);
    }

    #[test]
    fn quality_parsing_follows_the_column_order() {
        let month_first = Some(SlashDateOrder::MonthFirst);
        assert_eq!(
            parse_raw_datetime_date("01/02/2024", month_first),
            NaiveDate::from_ymd_opt(2024, 1, 2)
        );
        for order in [
            None,
            Some(SlashDateOrder::DayFirst),
            Some(SlashDateOrder::AssumedDayFirst),
            Some(SlashDateOrder::Mixed),
        ] {
            assert_eq!(
                parse_raw_datetime_date("01/02/2024", order),
                NaiveDate::from_ymd_opt(2024, 2, 1),
                "{order:?}"
            );
        }
        // A value that parses one way only is read that way under any order.
        assert_eq!(
            parse_raw_datetime_date("12/31/2024", None),
            NaiveDate::from_ymd_opt(2024, 12, 31)
        );
    }

    #[test]
    fn test_empty_data() {
        let data: Vec<String> = vec![];
        let stats = calculate_datetime_stats(&data);

        match stats {
            ColumnStats::DateTime(d) => {
                assert!(d.min_datetime.is_empty());
                assert!(d.max_datetime.is_empty());
                assert_eq!(d.duration_days, 0.0);
            }
            _ => panic!("Expected DateTime stats"),
        }
    }
}
