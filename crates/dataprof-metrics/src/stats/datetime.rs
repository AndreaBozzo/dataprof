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

use crate::types::{ColumnStats, DATE_GAP_YEAR_LIMIT, DateGap, DateTimeStats, SlashDateOrder};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};
use std::collections::{BTreeMap, HashMap};

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
    let mut tally = SlashTally::default();
    for value in values {
        tally.observe(value.as_ref().trim());
    }
    tally.order()
}

/// How a value reads as a slash date: in which orders it parses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlashReading {
    /// Not a slash date in either order.
    None,
    /// Only day-first (`31/12/2024`).
    DayOnly,
    /// Only month-first (`12/31/2024`).
    MonthOnly,
    /// Both ways (`01/02/2024`): the column's order decides it.
    Ambiguous,
}

fn slash_reading(trimmed: &str) -> SlashReading {
    // Most values of most columns carry no slash; skip the four parses.
    if !trimmed.contains('/') {
        return SlashReading::None;
    }
    let day_first = parses_slash(trimmed, SLASH_DATE_DAY_FIRST, SLASH_DATETIME_DAY_FIRST);
    let month_first = parses_slash(trimmed, SLASH_DATE_MONTH_FIRST, SLASH_DATETIME_MONTH_FIRST);
    match (day_first, month_first) {
        (true, true) => SlashReading::Ambiguous,
        (true, false) => SlashReading::DayOnly,
        (false, true) => SlashReading::MonthOnly,
        (false, false) => SlashReading::None,
    }
}

/// The evidence [`resolve_slash_order`] decides from, counted value by value
/// so a stream can decide it over every value it read.
#[derive(Debug, Clone, Copy, Default)]
struct SlashTally {
    slash_dates: usize,
    day_only: usize,
    month_only: usize,
}

impl SlashTally {
    fn observe(&mut self, trimmed: &str) -> SlashReading {
        let reading = slash_reading(trimmed);
        match reading {
            SlashReading::None => {}
            SlashReading::Ambiguous => self.slash_dates += 1,
            SlashReading::DayOnly => {
                self.slash_dates += 1;
                self.day_only += 1;
            }
            SlashReading::MonthOnly => {
                self.slash_dates += 1;
                self.month_only += 1;
            }
        }
        reading
    }

    fn merge(&mut self, other: &Self) {
        self.slash_dates += other.slash_dates;
        self.day_only += other.day_only;
        self.month_only += other.month_only;
    }

    fn order(&self) -> Option<SlashDateOrder> {
        match (self.slash_dates, self.day_only, self.month_only) {
            (0, _, _) => None,
            (_, 0, 0) => Some(SlashDateOrder::AssumedDayFirst),
            (_, _, 0) => Some(SlashDateOrder::DayFirst),
            (_, 0, _) => Some(SlashDateOrder::MonthFirst),
            _ => Some(SlashDateOrder::Mixed),
        }
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
    let mut accumulator = DateTimeAccumulator::new();
    for value in data {
        accumulator.update(value);
    }
    accumulator.finish()
}

/// Date statistics over every value a column streams, independent of the
/// values an engine retains (#1297).
///
/// The earliest and latest dates, the duration between them, the year, month,
/// weekday and hour distributions, the slash order and the largest gap
/// between consecutive distinct days (#1300) are all decided over every value
/// offered, so an engine that keeps only a bounded sample reports
/// what an in-memory source holding every value reports.
///
/// A slash date that parses both ways (`01/02/2024`) is read in an order the
/// whole column decides, which is known only once every value is in. Those
/// values are therefore tallied under both readings, and [`finish`] keeps the
/// one the column's order selects. Every other value reads the same in
/// either order.
///
/// [`finish`]: DateTimeAccumulator::finish
#[derive(Debug, Clone, Default)]
pub struct DateTimeAccumulator {
    slash: SlashTally,
    /// Values whose reading does not depend on the column's slash order.
    settled: DateTally,
    /// Ambiguous slash dates read day-first.
    ambiguous_day_first: DateTally,
    /// Ambiguous slash dates read month-first.
    ambiguous_month_first: DateTally,
}

impl DateTimeAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Offer one value. A value that does not parse as a date is ignored, as
    /// the statistics over a retained sample ignore it.
    pub fn update(&mut self, value: &str) {
        let trimmed = value.trim();
        if !may_parse_as_date(trimmed) {
            return;
        }
        match self.slash.observe(trimmed) {
            SlashReading::Ambiguous => {
                if let Some(parsed) = parse_flexible_full(trimmed, false) {
                    self.ambiguous_day_first.add(&parsed);
                }
                if let Some(parsed) = parse_flexible_full(trimmed, true) {
                    self.ambiguous_month_first.add(&parsed);
                }
            }
            _ => {
                if let Some(parsed) = parse_flexible_full(trimmed, false) {
                    self.settled.add(&parsed);
                }
            }
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.slash.merge(&other.slash);
        self.settled.merge(&other.settled);
        self.ambiguous_day_first.merge(&other.ambiguous_day_first);
        self.ambiguous_month_first
            .merge(&other.ambiguous_month_first);
    }

    /// Heap held beyond the struct itself: the year maps and the day
    /// bitmaps, the only parts that grow with the data (one entry each per
    /// distinct year). The month, weekday and hour counts are fixed arrays
    /// inside the struct.
    pub fn heap_bytes(&self) -> usize {
        [
            &self.settled,
            &self.ambiguous_day_first,
            &self.ambiguous_month_first,
        ]
        .iter()
        .map(|tally| tally.heap_bytes())
        .sum()
    }

    /// The statistics over every value offered.
    pub fn finish(&self) -> DateTimeStats {
        let slash_date_order = self.slash.order();
        let mut tally = self.settled.clone();
        tally.merge(if reads_month_first(slash_date_order) {
            &self.ambiguous_month_first
        } else {
            &self.ambiguous_day_first
        });
        let (Some(min_date), Some(max_date)) = (tally.min, tally.max) else {
            return DateTimeStats::empty();
        };
        let present = |counts: &[usize], first: u32| -> HashMap<u32, usize> {
            counts
                .iter()
                .zip(first..)
                .filter(|(count, _)| **count > 0)
                .map(|(count, key)| (key, *count))
                .collect()
        };
        let day_of_week_distribution = tally
            .weekdays
            .iter()
            .zip(WEEKDAYS_FROM_MONDAY)
            .filter(|(count, _)| **count > 0)
            .map(|(count, day)| (weekday_name(day).to_string(), *count))
            .collect();
        DateTimeStats {
            min_datetime: min_date.format("%Y-%m-%d").to_string(),
            max_datetime: max_date.format("%Y-%m-%d").to_string(),
            duration_days: (max_date - min_date).num_days() as f64,
            month_distribution: present(&tally.months, 1),
            hour_distribution: (tally.datetimes > 0).then(|| present(&tally.hours, 0)),
            year_distribution: tally.years,
            day_of_week_distribution,
            slash_date_order,
            largest_gap: tally.days.largest_gap(),
        }
    }
}

const WEEKDAYS_FROM_MONDAY: [Weekday; 7] = [
    Weekday::Mon,
    Weekday::Tue,
    Weekday::Wed,
    Weekday::Thu,
    Weekday::Fri,
    Weekday::Sat,
    Weekday::Sun,
];

/// The dates of a set of values: their extremes and how they distribute.
#[derive(Debug, Clone, Default)]
struct DateTally {
    min: Option<NaiveDate>,
    max: Option<NaiveDate>,
    years: HashMap<i32, usize>,
    /// Indexed by month, January first.
    months: [usize; 12],
    /// Indexed by weekday, Monday first.
    weekdays: [usize; 7],
    /// Indexed by hour, over the values that carry a time.
    hours: [usize; 24],
    /// Values that carry a time.
    datetimes: usize,
    /// The distinct days, for the largest gap between them.
    days: DaySet,
}

/// Words in a year's day bitmap: 366 days fit in six.
const DAY_WORDS: usize = 6;

/// The distinct days of a column, one bitmap per year indexed by the day of
/// the year, so the memory follows the distinct years as the year map does,
/// and the days come out in order for [`DaySet::largest_gap`].
#[derive(Debug, Clone)]
enum DaySet {
    Tracked(BTreeMap<i32, [u64; DAY_WORDS]>),
    /// The column spans more than [`DATE_GAP_YEAR_LIMIT`] distinct years, so
    /// its days are no longer kept and it reports no gap.
    OverLimit,
}

impl Default for DaySet {
    fn default() -> Self {
        Self::Tracked(BTreeMap::new())
    }
}

impl DaySet {
    fn add(&mut self, date: NaiveDate) {
        let Self::Tracked(years) = self else {
            return;
        };
        let day = date.ordinal0() as usize;
        years.entry(date.year()).or_insert([0; DAY_WORDS])[day / 64] |= 1 << (day % 64);
        self.check_limit();
    }

    fn merge(&mut self, other: &Self) {
        match (&mut *self, other) {
            (Self::Tracked(mine), Self::Tracked(theirs)) => {
                for (year, words) in theirs {
                    let entry = mine.entry(*year).or_insert([0; DAY_WORDS]);
                    for (mine, theirs) in entry.iter_mut().zip(words) {
                        *mine |= theirs;
                    }
                }
                self.check_limit();
            }
            _ => *self = Self::OverLimit,
        }
    }

    /// A set past the limit stops tracking. The decision reads the distinct
    /// years of every value offered, so a split stream merged decides as the
    /// whole stream does.
    fn check_limit(&mut self) {
        if matches!(self, Self::Tracked(years) if years.len() > DATE_GAP_YEAR_LIMIT) {
            *self = Self::OverLimit;
        }
    }

    /// Heap held by the bitmaps: each entry's key and words, and about a
    /// pointer of tree overhead.
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Tracked(years) => {
                years.len()
                    * (std::mem::size_of::<(i32, [u64; DAY_WORDS])>()
                        + std::mem::size_of::<usize>())
            }
            Self::OverLimit => 0,
        }
    }

    /// The longest stretch between consecutive distinct days, the earliest
    /// one when several are as long. A single day is a gap of zero from
    /// itself.
    fn largest_gap(&self) -> Option<DateGap> {
        let Self::Tracked(years) = self else {
            return None;
        };
        let days = years.iter().flat_map(|(year, words)| {
            words.iter().enumerate().flat_map(move |(index, word)| {
                let mut word = *word;
                std::iter::from_fn(move || {
                    (word != 0).then(|| {
                        let bit = word.trailing_zeros();
                        word &= word - 1;
                        let ordinal = (index * 64) as u32 + bit + 1;
                        NaiveDate::from_yo_opt(*year, ordinal)
                            .expect("only days of real dates are set")
                    })
                })
            })
        });
        let mut previous: Option<NaiveDate> = None;
        let mut largest: Option<(i64, NaiveDate, NaiveDate)> = None;
        for day in days {
            let from = previous.unwrap_or(day);
            let length = (day - from).num_days();
            if largest.is_none_or(|(longest, _, _)| length > longest) {
                largest = Some((length, from, day));
            }
            previous = Some(day);
        }
        largest.map(|(_, from, to)| DateGap::new(from, to))
    }
}

impl DateTally {
    /// Heap held by the year map (its slots, each a key, a count and a
    /// control byte) and the day bitmaps.
    fn heap_bytes(&self) -> usize {
        self.years.capacity() * (std::mem::size_of::<(i32, usize)>() + 1) + self.days.heap_bytes()
    }

    fn add(&mut self, parsed: &ParsedDateTime) {
        let date = parsed.date;
        self.min = Some(self.min.map_or(date, |min| min.min(date)));
        self.max = Some(self.max.map_or(date, |max| max.max(date)));
        *self.years.entry(date.year()).or_insert(0) += 1;
        self.days.add(date);
        self.months[date.month0() as usize] += 1;
        self.weekdays[date.weekday().num_days_from_monday() as usize] += 1;
        if let Some(datetime) = parsed.datetime {
            self.hours[datetime.hour() as usize] += 1;
            self.datetimes += 1;
        }
    }

    fn merge(&mut self, other: &Self) {
        self.min = match (self.min, other.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max = match (self.max, other.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        for (year, count) in &other.years {
            *self.years.entry(*year).or_insert(0) += count;
        }
        for (mine, theirs) in self.months.iter_mut().zip(other.months) {
            *mine += theirs;
        }
        for (mine, theirs) in self.weekdays.iter_mut().zip(other.weekdays) {
            *mine += theirs;
        }
        for (mine, theirs) in self.hours.iter_mut().zip(other.hours) {
            *mine += theirs;
        }
        self.datetimes += other.datetimes;
        self.days.merge(&other.days);
    }
}

/// Whether a trimmed value can parse as any form [`parse_flexible_full`]
/// reads, so the formats are tried only on values that might.
///
/// Every form starts with its date, and every date reads as a number, a
/// separator (`-`, `/` or `.`), a field of one or two digits (`%m` or `%d`),
/// the same separator again, and a number. A year may carry a sign, first or
/// last (`12/31/+2024`). Chrono skips
/// whitespace before a numeric field (`2025- 1-01` is a date), so this does
/// too. The accumulator is offered every value of every column, so this
/// shape check keeps the formats away from integers, decimals, free text and
/// codes such as UUIDs, which used to cost most of the time on a wide file.
fn may_parse_as_date(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    let count = |from: usize, accept: fn(&u8) -> bool| {
        bytes[from.min(bytes.len())..]
            .iter()
            .take_while(|byte| accept(byte))
            .count()
    };
    // The start of the next numeric field, past any whitespace before it.
    let field = |at: usize| at + count(at, u8::is_ascii_whitespace);

    let mut at = field(usize::from(matches!(bytes.first(), Some(b'+' | b'-'))));
    let leading = count(at, u8::is_ascii_digit);
    if leading == 0 {
        return false;
    }
    at += leading;
    let Some(separator) = bytes
        .get(at)
        .copied()
        .filter(|byte| matches!(byte, b'-' | b'/' | b'.'))
    else {
        return false;
    };
    at = field(at + 1);
    let middle = count(at, u8::is_ascii_digit);
    if !(1..=2).contains(&middle) {
        return false;
    }
    at += middle;
    if bytes.get(at) != Some(&separator) {
        return false;
    }
    // The last field is a year when the date ends with it, and may be signed.
    let last = field(at + 1);
    let digits_from = field(last + usize::from(matches!(bytes.get(last), Some(b'+' | b'-'))));
    bytes.get(digits_from).is_some_and(u8::is_ascii_digit)
}

/// Parse one value, reading an ambiguous slash date month-first when
/// `month_first` is set and day-first otherwise. A slash value that parses in
/// only one order is read in that order either way.
fn parse_flexible_full(s: &str, month_first: bool) -> Option<ParsedDateTime> {
    let trimmed = s.trim();

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

    // Every datetime format needs at least 11 bytes (`1-1-1T1:1:1`), so a
    // plain date such as `2024-01-15` goes straight to the date formats.
    if trimmed.len() > 10 {
        // An offset-bearing value is an instant, normalized to UTC before
        // anything else sees it — see the "UTC Offsets" section in the module
        // docs. `Z` values are unaffected (their offset is already zero); the
        // rule only moves values written at a non-zero offset.
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
            let utc = dt.naive_utc();
            return Some(ParsedDateTime {
                date: utc.date(),
                datetime: Some(utc),
            });
        }

        let datetime_formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            slash_datetimes[0],
            slash_datetimes[1],
            "%Y-%m-%dT%H:%M:%S%.f",
        ];
        for format in datetime_formats {
            if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, format) {
                return Some(ParsedDateTime {
                    date: dt.date(),
                    datetime: Some(dt),
                });
            }
        }
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
///
/// Runs on every value of every column, so the byte tests come before the
/// whitespace test, and that one compares lengths instead of contents.
fn looks_like_raw_datetime_candidate(s: &str) -> bool {
    if s.len() < 10 {
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

    (year_first || year_last) && s.trim().len() == s.len()
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

    fn stats_of(items: &[&str]) -> DateTimeStats {
        compute_datetime_stats(&values(items))
    }

    #[test]
    fn test_year_month_and_weekday_distributions() {
        // 2023-01-02 and 2023-01-09 are Mondays, 2024-01-02 a Tuesday.
        let stats = stats_of(&["2023-01-02", "2023-01-09", "2023-06-01", "2024-01-02"]);
        assert_eq!(stats.year_distribution.get(&2023), Some(&3));
        assert_eq!(stats.year_distribution.get(&2024), Some(&1));
        assert_eq!(stats.month_distribution.get(&1), Some(&3));
        assert_eq!(stats.month_distribution.get(&6), Some(&1));
        assert_eq!(stats.month_distribution.len(), 2);
        assert_eq!(stats.day_of_week_distribution.get("Monday"), Some(&2));
        assert_eq!(stats.day_of_week_distribution.get("Tuesday"), Some(&1));
        assert_eq!(stats.day_of_week_distribution.get("Thursday"), Some(&1));
        assert_eq!(stats.day_of_week_distribution.len(), 3);
        assert!(stats.hour_distribution.is_none());
    }

    #[test]
    fn values_that_cannot_be_dates_skip_the_formats() {
        let stats = stats_of(&["12", "-3.5", "1e5", "text", "2024-01-15"]);
        assert_eq!(stats.min_datetime, "2024-01-15");
        assert_eq!(stats.year_distribution.values().sum::<usize>(), 1);
        // The pre-filter never drops a value the formats would read.
        for value in [
            "2024-01-15",
            "15/01/2024",
            "1/2/2024",
            "15-01-2024",
            "15.01.2024",
            "2024/01/15",
            "2024-01-15T10:30:00",
            "2024-01-15 10:30:00",
            "2024-01-15T10:30:00.125",
            "2024-01-15T10:30:00+02:00",
            "01/15/2024 10:30:00",
            "+2024-01-15",
        ] {
            assert!(parse_flexible_full(value, false).is_some(), "{value}");
            assert!(may_parse_as_date(value), "{value}");
        }
        for value in [
            "20240115",
            "2024",
            "1.5e3",
            "-1.5e3",
            "12.5",
            "a1b2-c3d4-e5",
            "12345678-1a2b-3c4d-5e6f-7a8b9c0d1e2f",
            "00000012-0012-x",
            "2024-01/15",
        ] {
            assert!(!may_parse_as_date(value), "{value}");
        }
    }

    /// The parser is the reference: every padded variant it reads, the
    /// pre-filter must keep. Chrono skips whitespace before a numeric field,
    /// so `2025- 1-01` is a date (Codex review on #1301).
    #[test]
    fn the_pre_filter_keeps_every_padded_value_the_parser_reads() {
        let forms = [
            "2025-01-05",
            "2025/01/05",
            "05/01/2025",
            "05-01-2025",
            "05.01.2025",
            "2025-01-05T10:30:00",
            "2025-01-05 10:30:00",
            "05/01/2025 10:30:00",
            "+2025-01-05",
            // A signed year in the last field (Copilot review on #1301).
            "05/01/+2025",
            "05-01/-2025",
            "05.01.+2025",
            "05-01-+2025",
            "05/01/+2025 10:30:00",
        ];
        let mut parsed = 0;
        let mut dropped = Vec::new();
        for form in forms {
            // Every position right after a separator or a sign, where a
            // numeric field starts.
            let starts: Vec<usize> = form
                .char_indices()
                .filter(|(_, c)| matches!(c, '-' | '/' | '.' | '+' | ' ' | 'T' | ':'))
                .map(|(i, _)| i + 1)
                .collect();
            for start in starts {
                for (pad, unpadded) in [(" ", false), ("  ", false), ("\t", false), ("", true)] {
                    let field = &form[start..];
                    let single = field.strip_prefix('0').filter(|_| unpadded);
                    let value = match single {
                        Some(rest) => format!("{}{rest}", &form[..start]),
                        None => format!("{}{pad}{field}", &form[..start]),
                    };
                    for month_first in [false, true] {
                        if parse_flexible_full(&value, month_first).is_some() {
                            parsed += 1;
                            if !may_parse_as_date(value.trim()) {
                                dropped.push(value.clone());
                            }
                        }
                    }
                }
            }
        }
        assert!(parsed > 50, "only {parsed} variants parsed");
        let stats = stats_of(
            &["2024-01-01"; 4]
                .iter()
                .copied()
                .chain(["2025- 1-01"])
                .collect::<Vec<_>>(),
        );
        assert_eq!(stats.max_datetime, "2025-01-01");
        let stats = stats_of(&["01/02/2024", "12/31/+2024"]);
        assert_eq!(stats.slash_date_order, Some(SlashDateOrder::MonthFirst));
        assert_eq!(stats.min_datetime, "2024-01-02");
        assert_eq!(stats.max_datetime, "2024-12-31");
        dropped.dedup();
        assert!(dropped.is_empty(), "dropped: {dropped:?}");
    }

    #[test]
    fn the_year_maps_report_the_heap_they_hold() {
        let mut accumulator = DateTimeAccumulator::new();
        assert_eq!(accumulator.heap_bytes(), 0);
        for year in 1000..3000 {
            accumulator.update(&format!("{year}-01-05"));
            accumulator.update(&format!("01/02/{year}"));
        }
        // Three maps of 2,000 years each: at least the entries themselves.
        assert!(accumulator.heap_bytes() >= 3 * 2000 * std::mem::size_of::<(i32, usize)>());
    }

    #[test]
    fn split_and_merged_streams_give_the_one_stream_statistics() {
        // The month-first evidence arrives in the second half, after the
        // ambiguous value it decides has already been read.
        let first = ["01/02/2024", "2023-05-06T07:08:09", "05/06/2024 10:00:00"];
        let second = ["12/31/2024", "2022-01-01"];
        let whole = stats_of(&[&first[..], &second[..]].concat());

        let mut left = DateTimeAccumulator::new();
        first.iter().for_each(|value| left.update(value));
        let mut right = DateTimeAccumulator::new();
        second.iter().for_each(|value| right.update(value));
        left.merge(&right);
        let merged = left.finish();

        assert_eq!(whole.slash_date_order, Some(SlashDateOrder::MonthFirst));
        assert_eq!(merged.slash_date_order, whole.slash_date_order);
        assert_eq!(merged.min_datetime, "2022-01-01");
        assert_eq!(merged.max_datetime, "2024-12-31");
        assert_eq!(merged.min_datetime, whole.min_datetime);
        assert_eq!(merged.max_datetime, whole.max_datetime);
        assert_eq!(merged.duration_days, whole.duration_days);
        assert_eq!(merged.year_distribution, whole.year_distribution);
        // 01/02/2024 month-first is 2 January; day-first it would be February.
        assert_eq!(whole.month_distribution.get(&2), None);
        assert_eq!(merged.month_distribution, whole.month_distribution);
        assert_eq!(
            merged.day_of_week_distribution,
            whole.day_of_week_distribution
        );
        assert_eq!(merged.hour_distribution, whole.hour_distribution);
        assert_eq!(whole.hour_distribution.as_ref().unwrap().len(), 2);
    }

    fn gap(items: &[&str]) -> Option<(i64, String, String)> {
        stats_of(items)
            .largest_gap
            .map(|gap| (gap.days, gap.from, gap.to))
    }

    fn some_gap(days: i64, from: &str, to: &str) -> Option<(i64, String, String)> {
        Some((days, from.to_string(), to.to_string()))
    }

    #[test]
    fn the_largest_gap_is_between_consecutive_distinct_days() {
        let daily: Vec<String> = (0..400)
            .map(|day| {
                (NaiveDate::from_ymd_opt(2023, 12, 1).unwrap() + chrono::Days::new(day)).to_string()
            })
            .collect();
        let daily: Vec<&str> = daily.iter().map(String::as_str).collect();
        // Contiguous across a year end and a leap day, with duplicates.
        let mut repeated = daily.clone();
        repeated.extend(&daily[..50]);
        assert_eq!(gap(&repeated), some_gap(1, "2023-12-01", "2023-12-02"));
        // An interior month removed: 2024-02-01 to 2024-02-29 missing.
        let holed: Vec<&str> = daily
            .iter()
            .copied()
            .filter(|day| !day.starts_with("2024-02"))
            .collect();
        assert_eq!(gap(&holed), some_gap(30, "2024-01-31", "2024-03-01"));
        // The earliest of two equal gaps.
        assert_eq!(
            gap(&["2024-01-01", "2024-01-03", "2024-01-05"]),
            some_gap(2, "2024-01-01", "2024-01-03")
        );
        // One distinct day, read any number of times.
        assert_eq!(
            gap(&["2024-05-06", "2024-05-06T10:00:00"]),
            some_gap(0, "2024-05-06", "2024-05-06")
        );
        // No date at all.
        assert_eq!(gap(&["", "junk"]), None);
        assert_eq!(gap(&[]), None);
    }

    #[test]
    fn the_gap_reads_days_as_the_other_statistics_do() {
        // An offset moves the instant to the next UTC day (module docs).
        assert_eq!(
            gap(&["2024-01-15", "2024-01-15T23:00:00-05:00"]),
            some_gap(1, "2024-01-15", "2024-01-16")
        );
        // The month-first evidence comes last: 01/02/2024 is 2 January, so
        // the days are 1, 2 January and 31 December.
        assert_eq!(
            gap(&["01/01/2024", "01/02/2024", "12/31/2024"]),
            some_gap(364, "2024-01-02", "2024-12-31")
        );
        // Day-first, 01/02/2024 is 1 February.
        assert_eq!(
            gap(&["01/01/2024", "01/02/2024", "31/12/2024"]),
            some_gap(334, "2024-02-01", "2024-12-31")
        );
    }

    #[test]
    fn a_split_stream_gives_the_one_stream_gap() {
        let values: Vec<String> = (0..300)
            .filter(|day| !(100..140).contains(day))
            .map(|day| {
                (NaiveDate::from_ymd_opt(2022, 6, 1).unwrap() + chrono::Days::new(day)).to_string()
            })
            .collect();
        let whole = compute_datetime_stats(&values).largest_gap;
        assert_eq!(whole.as_ref().map(|gap| gap.days), Some(41));
        // Interleaved halves, so neither half sees the hole alone.
        let mut left = DateTimeAccumulator::new();
        let mut right = DateTimeAccumulator::new();
        for (index, value) in values.iter().enumerate() {
            if index % 2 == 0 {
                left.update(value);
            } else {
                right.update(value);
            }
        }
        left.merge(&right);
        assert_eq!(left.finish().largest_gap, whole);
    }

    #[test]
    fn past_the_year_limit_no_gap_is_reported() {
        // Years 0 to 9999, the limit exactly.
        let mut within = DateTimeAccumulator::new();
        for year in 0..DATE_GAP_YEAR_LIMIT {
            within.update(&format!("{year:04}-01-05"));
        }
        assert_eq!(within.finish().largest_gap.map(|gap| gap.days), Some(366));
        assert!(within.heap_bytes() >= DATE_GAP_YEAR_LIMIT * 52);
        // One more year, offered to a separate half and merged.
        let mut other = DateTimeAccumulator::new();
        other.update("+12000-01-05");
        assert_eq!(other.finish().max_datetime, "+12000-01-05");
        let mut over = within.clone();
        over.merge(&other);
        assert_eq!(over.finish().largest_gap, None);
        // Every other statistic stays.
        assert_eq!(over.finish().min_datetime, "0000-01-05");
        within.update("+12000-01-05");
        assert_eq!(within.finish().largest_gap, None);
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
