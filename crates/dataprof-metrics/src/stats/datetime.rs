//! Datetime statistics calculation
//!
//! # Date Format Handling
//!
//! ## Unambiguous Formats (Recommended)
//! - ISO 8601: `2023-01-15` (YYYY-MM-DD)
//! - ISO with slashes: `2023/01/15` (YYYY/MM/DD)
//! - ISO datetime: `2023-01-15T10:30:00`, or with a space for the `T`
//!   (`2023-01-15 10:30:00`), each with an optional fraction and offset
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
//! as 2024-01-16 at hour 04, not as 2024-01-15 at hour 23, and so does
//! `2024-01-15 23:00:00-05:00`, the form Python's `str()` gives an aware
//! datetime (#1335). An hour-only offset (`+00`) is not RFC 3339 and stays text.
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

use crate::analysis::inference::is_null_like_token;
use crate::core::config::IsoQualityConfig;
use crate::quality::TimelinessClockMode;
use crate::types::{ColumnStats, DATE_GAP_YEAR_LIMIT, DateGap, DateTimeStats, SlashDateOrder};
use chrono::{Datelike, Days, NaiveDate, NaiveDateTime, Timelike, Utc, Weekday};
use std::collections::{BTreeMap, HashMap};

/// The reference date and stale cutoff against which timeliness judges dates
/// during a single-pass scan (#788 step 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelinessWindow {
    pub reference_date: NaiveDate,
    pub stale_cutoff: NaiveDate,
}

impl TimelinessWindow {
    /// Build a window from `reference_date` and `max_data_age_years`, using the
    /// Gregorian mean year so fractional policies stay meaningful.
    pub fn new(reference_date: NaiveDate, max_data_age_years: f64) -> Self {
        let age_days = (max_data_age_years * 365.2425).floor() as u64;
        let stale_cutoff = reference_date
            .checked_sub_days(Days::new(age_days))
            .unwrap_or(NaiveDate::MIN);
        Self {
            reference_date,
            stale_cutoff,
        }
    }

    /// Build a window and clock mode from `thresholds` and an optional fixed
    /// `reference_date`. Captures UTC today once when `reference_date` is `None`.
    pub fn from_config(
        thresholds: &IsoQualityConfig,
        reference_date: Option<NaiveDate>,
    ) -> (Self, TimelinessClockMode) {
        match reference_date {
            Some(date) => (
                Self::new(date, thresholds.max_data_age_years),
                TimelinessClockMode::Fixed,
            ),
            None => (
                Self::new(Utc::now().date_naive(), thresholds.max_data_age_years),
                TimelinessClockMode::Rolling,
            ),
        }
    }
}

/// Both calendar readings of a single value while its column's slash order is
/// still open. For any value other than an ambiguous slash date (`01/02/2024`),
/// both fields hold the same `NaiveDate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DualDate {
    pub day_first: NaiveDate,
    pub month_first: NaiveDate,
}

/// Per-column timeliness counts derived from [`DateTimeAccumulator`] once the
/// column's slash order is settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColumnDateSummary {
    pub checked: usize,
    pub valid_dates: usize,
    pub invalid_dates: usize,
    pub future_dates: usize,
    pub stale_dates: usize,
    pub slash_order: Option<SlashDateOrder>,
}

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
pub(crate) fn reads_month_first(order: Option<SlashDateOrder>) -> bool {
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
    /// Non-null values offered to this accumulator.
    checked: usize,
    /// Freshness/future window used to tally timeliness counts in the same
    /// pass as date statistics (#788 step 4). `None` when constructed without
    /// a timeliness window.
    timeliness_window: Option<TimelinessWindow>,
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

    /// Construct an accumulator that also counts timeliness future/stale dates
    /// against `window` in the same pass (#788 step 4).
    pub fn with_timeliness_window(window: TimelinessWindow) -> Self {
        Self {
            timeliness_window: Some(window),
            ..Self::default()
        }
    }

    /// The timeliness window this accumulator counted against, if any.
    pub fn timeliness_window(&self) -> Option<TimelinessWindow> {
        self.timeliness_window
    }

    /// The column's slash date order decided over every value offered so far.
    pub fn slash_order(&self) -> Option<SlashDateOrder> {
        self.slash.order()
    }

    /// Total valid calendar dates parsed over every value offered.
    pub fn valid_dates(&self) -> usize {
        let ambiguous = if reads_month_first(self.slash.order()) {
            &self.ambiguous_month_first
        } else {
            &self.ambiguous_day_first
        };
        self.settled.valid_dates + ambiguous.valid_dates
    }

    /// Timeliness counts over every non-null value offered, resolved under the
    /// column's final slash order. Returns `None` if this accumulator was not
    /// configured for `window`.
    pub(crate) fn timeliness_summary(&self, window: TimelinessWindow) -> Option<ColumnDateSummary> {
        if self.timeliness_window != Some(window) {
            return None;
        }
        let slash_order = self.slash.order();
        let ambiguous = if reads_month_first(slash_order) {
            &self.ambiguous_month_first
        } else {
            &self.ambiguous_day_first
        };
        let valid_dates = self.settled.valid_dates + ambiguous.valid_dates;
        let future_dates = self.settled.future_dates + ambiguous.future_dates;
        let stale_dates = self.settled.stale_dates + ambiguous.stale_dates;
        Some(ColumnDateSummary {
            checked: self.checked,
            valid_dates,
            invalid_dates: self.checked.saturating_sub(valid_dates),
            future_dates,
            stale_dates,
            slash_order,
        })
    }

    /// Offer one value. Null-like tokens are ignored; non-null values increment
    /// the checked count, and values that parse as dates update the statistics
    /// and timeliness tallies.
    pub fn update(&mut self, value: &str) {
        self.update_returning_date(value);
    }

    /// Like [`Self::update`], returning the parsed `(day_first, month_first)`
    /// dates for row-level pair comparison without reparsing.
    pub fn update_returning_date(&mut self, value: &str) -> Option<DualDate> {
        let trimmed = value.trim();
        if is_null_like_token(trimmed) {
            return None;
        }
        self.checked += 1;
        if !may_parse_as_date(trimmed) {
            return None;
        }
        let window = self.timeliness_window;
        match self.slash.observe(trimmed) {
            SlashReading::Ambiguous => {
                let dmy = parse_flexible_full(trimmed, false);
                let mdy = parse_flexible_full(trimmed, true);
                if let Some(ref parsed) = dmy {
                    self.ambiguous_day_first.add(parsed, window);
                }
                if let Some(ref parsed) = mdy {
                    self.ambiguous_month_first.add(parsed, window);
                }
                match (dmy, mdy) {
                    (Some(d), Some(m)) => Some(DualDate {
                        day_first: d.date,
                        month_first: m.date,
                    }),
                    (Some(d), None) => Some(DualDate {
                        day_first: d.date,
                        month_first: d.date,
                    }),
                    (None, Some(m)) => Some(DualDate {
                        day_first: m.date,
                        month_first: m.date,
                    }),
                    (None, None) => None,
                }
            }
            _ => {
                if let Some(parsed) = parse_flexible_full(trimmed, false) {
                    let date = parsed.date;
                    self.settled.add(&parsed, window);
                    Some(DualDate {
                        day_first: date,
                        month_first: date,
                    })
                } else {
                    None
                }
            }
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.timeliness_window = match (self.timeliness_window, other.timeliness_window) {
            (Some(a), Some(b)) if a == b => Some(a),
            (None, Some(b)) if self.checked == 0 => Some(b),
            (Some(a), None) if other.checked == 0 => Some(a),
            _ => None,
        };
        self.checked += other.checked;
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
    /// Valid parsed calendar dates in this partition.
    valid_dates: usize,
    /// Valid dates strictly after the timeliness window's reference date.
    future_dates: usize,
    /// Valid dates strictly before the timeliness window's stale cutoff.
    stale_dates: usize,
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

    fn add(&mut self, parsed: &ParsedDateTime, window: Option<TimelinessWindow>) {
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
        self.valid_dates += 1;
        if let Some(window) = window {
            if date > window.reference_date {
                self.future_dates += 1;
            }
            if date < window.stale_cutoff {
                self.stale_dates += 1;
            }
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
        self.valid_dates += other.valid_dates;
        self.future_dates += other.future_dates;
        self.stale_dates += other.stale_dates;
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
            // A naive spaced value with a fraction, as Python writes a
            // datetime with microseconds; inference types it a date (#1335).
            "%Y-%m-%d %H:%M:%S%.f",
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

/// The date or wall-clock time of one text cell, with day first for an
/// ambiguous slash date. An offset-bearing value is handled by the caller.
pub(crate) fn parse_local_date_or_datetime(s: &str) -> Option<(NaiveDate, Option<NaiveDateTime>)> {
    parse_flexible_full(s, false).map(|parsed| (parsed.date, parsed.datetime))
}

/// Parse a date value using the same reading as [`DateTimeAccumulator`]
/// (`s.trim()`, `may_parse_as_date`, `parse_flexible_full`), honoring
/// `order` for ambiguous slash dates (#788 step 4).
pub fn parse_date_value(s: &str, order: Option<SlashDateOrder>) -> Option<NaiveDate> {
    let trimmed = s.trim();
    if !may_parse_as_date(trimmed) {
        return None;
    }
    parse_flexible_full(trimmed, reads_month_first(order)).map(|parsed| parsed.date)
}

/// Parse both `(day_first, month_first)` calendar readings of one cell using
/// the same reading as [`DateTimeAccumulator`] (#788 step 4).
pub fn parse_dual_date(s: &str) -> Option<DualDate> {
    let trimmed = s.trim();
    if is_null_like_token(trimmed) || !may_parse_as_date(trimmed) {
        return None;
    }
    match slash_reading(trimmed) {
        SlashReading::Ambiguous => {
            let dmy = parse_flexible_full(trimmed, false).map(|p| p.date);
            let mdy = parse_flexible_full(trimmed, true).map(|p| p.date);
            match (dmy, mdy) {
                (Some(day_first), Some(month_first)) => Some(DualDate {
                    day_first,
                    month_first,
                }),
                (Some(d), None) | (None, Some(d)) => Some(DualDate {
                    day_first: d,
                    month_first: d,
                }),
                (None, None) => None,
            }
        }
        _ => parse_flexible_full(trimmed, false).map(|parsed| DualDate {
            day_first: parsed.date,
            month_first: parsed.date,
        }),
    }
}

/// Parse a quality-metric date value and return its calendar date using the
/// same reading as [`DateTimeAccumulator`] (#788 step 4).
///
/// Prefer this over [`parse_raw_datetime_year`] whenever the caller compares
/// values against each other or against a reference point. The supported
/// formats include `DD/MM/YYYY` and `MM/DD/YYYY`, which do not sort
/// lexicographically and cannot be ordered by year alone.
///
/// `order` is the column's [`resolve_slash_order`] result, so a value is read
/// the way the column's statistics read it.
pub(crate) fn parse_raw_datetime_date(s: &str, order: Option<SlashDateOrder>) -> Option<NaiveDate> {
    parse_date_value(s, order)
}

/// Parse a quality-metric date value and return its calendar year using the
/// same reading as [`DateTimeAccumulator`] (#788 step 4).
///
/// A year is the right granularity only for thresholds expressed in whole
/// years. Anything finer must use [`parse_raw_datetime_date`]. The year of a
/// slash date is the same in either order, so no column order is needed.
pub(crate) fn parse_raw_datetime_year(s: &str) -> Option<i32> {
    parse_raw_datetime_date(s, None).map(|date| date.year())
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
        // Surrounding whitespace is trimmed, matching DateTimeAccumulator (#788 step 4).
        assert_eq!(parse_raw_datetime_year(" 2024-02-29"), Some(2024));
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
            "2024-01-15 10:30:00.125",
            "2024-01-15 10:30:00+02:00",
            "2024-01-15 10:30:00.125Z",
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

    /// Exact copy of the pre-#788-step-4 timeliness pre-filter
    /// (`looks_like_raw_datetime_candidate`) so the corpus test runs both
    /// readers side by side and enforces an explicit decision for every
    /// disagreement.
    fn legacy_looks_like_raw_datetime_candidate(s: &str) -> bool {
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

    /// Exact pre-#788-step-4 timeliness date reader (`parse_raw_datetime_date`).
    fn legacy_timeliness_reader(s: &str, order: Option<SlashDateOrder>) -> Option<NaiveDate> {
        if !legacy_looks_like_raw_datetime_candidate(s) {
            return None;
        }
        parse_flexible_full(s, reads_month_first(order)).map(|parsed| parsed.date)
    }

    /// Corpus test (#788 step 4): runs the old timeliness date reader and the
    /// unified date statistics reader (`parse_date_value` / `DateTimeAccumulator`)
    /// over a comprehensive corpus of date spellings drawn from the repo's
    /// fixtures (`raw_date_forms.json`, `edge_cases_nightmare.csv`,
    /// `spaced_datetimes.csv`, `decimals.csv`, `pattern_detection.json`), #1305,
    /// #1335, and #815.
    ///
    /// Every value where the two readers disagree MUST appear in
    /// `DECIDED_DIFFERENCES` with its expected old reading, new reading, and
    /// rationale. Any unlisted or resolved difference fails the test so no
    /// spelling changes silently.
    #[test]
    fn date_reader_corpus_lists_and_decides_every_difference() {
        let corpus: &[&str] = &[
            // --- Canonical forms from raw_date_forms.json ---
            "2024-03-14",
            "14/03/2024",
            "03/14/2024",
            "14-03-2024",
            "14.03.2024",
            "2024/03/14",
            "2024-03-14 10:30:00",
            "14/03/2024 10:30:00",
            "03/14/2024 10:30:00",
            "2024-03-14T10:30:00",
            "2024-03-14T10:30:00.123",
            "2024-03-14T10:30:00Z",
            "2024-03-14T10:30:00z",
            "2024-03-14T10:30:00+02:00",
            "2024-03-14T10:30:00-05:30",
            "2024-03-14T10:30:00.123+02:00",
            // --- #1335 spaced datetimes (spaced_datetimes.csv) ---
            "2026-06-01 12:00:00+00:00",
            "2026-06-01 12:05:00.250Z",
            "2026-06-02 08:30:00+02:00",
            "2026-06-02 23:59:59-05:00",
            "2026-06-02 09:00:00.5",
            "2026-06-01 12:00:00+24:00",
            "2026-06-01 12:00:00+00:60",
            "2026-06-01 12:00:00+00",
            "2024-03-14T10:30:00+0200",
            "2024-03-14T10:30:00 EST",
            // --- #1305 & decimals.csv slash dates ---
            "12/01/2024",
            "01/12/2024",
            "01/05/2024",
            "02/03/2024",
            "12/30/2023",
            "12/31/2023",
            "31/12/2023",
            "03/04/2024",
            "13/04/2024",
            "25/12/2023",
            "01/01/2024",
            "30/06/2024",
            "12/31/2024 10:00:00",
            "01/02/2024 09:30:00",
            // --- #815 compact 8-digit dates (must stay unparsed in both) ---
            "20240115",
            "20231231",
            "18000101",
            "21991231",
            // --- edge_cases_nightmare.csv & pattern_detection.json ---
            "2024-02-29",
            "2023-02-29",
            "1900-01-01",
            "9999-12-31",
            "0000-00-00",
            "2024-00-00",
            "2024-13-45",
            "2024-02-30",
            "2024-13-14",
            "2024-03-14T10:",
            "2024-01-15T10:30",
            "2024-03-14T25:30:00",
            "2024-03-14T10:30:00junk",
            "2024-W01-1",
            "2024-001",
            "2024-366",
            "2024.01.01",
            "Tue, 01 Jan 2024 00:00:00 GMT",
            "Jan 1, 2024",
            "Monday, January 1, 2024",
            "1st Jan 2024",
            "Jan 1st, 2024",
            "2024年1月1日",
            "1-Jan-2024",
            "Mon Jan 1 2024",
            "01 JAN 2024",
            "1 January 2024",
            "not-a-date",
            "東京",
            "1234567890",
            // --- Category A: surrounding whitespace (raw_date_forms.json, rfc3339_timestamps.rs) ---
            " 2024-03-14",
            "2024-03-14 ",
            " 2024-01-15T10:30:00Z ",
            "\t01/02/2024\n",
            // --- Category B: single-digit month or day (edge_cases_nightmare.csv, datetime.rs) ---
            "1/1/2024",
            "1/2/2024",
            "01/2/2024",
            "1/02/2024",
            "1/31/2024",
            "31/1/2024",
            "2024-1-5",
            "2024-01-5",
            "2024-1-05",
            "5-1-2024",
            "5.1.2024",
            "2024/1/5",
            "1/2/2024 10:30:00",
            "2024-1-5T10:30:00",
            "2024-1-5 10:30:00",
            // Note: old reader accidentally accepted `2024-01-5 10:30:00` because
            // len >= 10 and bytes[4] == bytes[7] == '-'; both accept it.
            "2024-01-5 10:30:00",
            // --- Category C: signed or non-4-digit years (datetime.rs, edge_cases_nightmare.csv) ---
            "+2024-01-15",
            "-2024-01-15",
            "05/01/+2025",
            "05-01--2025",
            "05-01/-2025", // mixed separators: rejected by both
            "+12000-01-05",
            "999-01-05",
            "01/05/999",
            "24-01-01",
            "01.01.24",
            // --- Category D: internal whitespace before numeric fields (datetime.rs) ---
            // Note: old reader accidentally accepted single-space `2025- 1-01`
            // and `05/ 1/2025` (len 10, seps at fixed offsets) while rejecting
            // double-space `2025-  1-01` and `05/  1/2025`.
            "2025- 1-01",
            "2025-  1-01",
            "05/ 1/2025",
            "05/  1/2025",
            "05/01/ 2025",
        ];

        // Every expected (value, month_first, new_date, reason) disagreement.
        // In every case the old reader returned `None` due to its fixed-offset
        // `looks_like_raw_datetime_candidate` pre-filter, whereas the unified
        // date statistics reader (`DateTimeAccumulator` / `parse_flexible_full`)
        // parses the date.
        let d = |y, m, day| NaiveDate::from_ymd_opt(y, m, day).unwrap();
        let decided: &[(&str, bool, NaiveDate, &str)] = &[
            // Category A: surrounding whitespace — new reader (date statistics) wins,
            // matching LexicalTally type inference, DateTimeAccumulator, and numeric rules.
            (" 2024-03-14", false, d(2024, 3, 14), "whitespace trimmed"),
            (" 2024-03-14", true, d(2024, 3, 14), "whitespace trimmed"),
            ("2024-03-14 ", false, d(2024, 3, 14), "whitespace trimmed"),
            ("2024-03-14 ", true, d(2024, 3, 14), "whitespace trimmed"),
            (
                " 2024-01-15T10:30:00Z ",
                false,
                d(2024, 1, 15),
                "whitespace trimmed",
            ),
            (
                " 2024-01-15T10:30:00Z ",
                true,
                d(2024, 1, 15),
                "whitespace trimmed",
            ),
            ("\t01/02/2024\n", false, d(2024, 2, 1), "whitespace trimmed"),
            ("\t01/02/2024\n", true, d(2024, 1, 2), "whitespace trimmed"),
            // Category B: single-digit month/day — new reader (date statistics) wins;
            // old fixed-offset pre-filter missed short fields (including `1/31/2024`
            // which already settled MonthFirst in DateTimeAccumulator).
            ("1/1/2024", false, d(2024, 1, 1), "single-digit fields"),
            ("1/1/2024", true, d(2024, 1, 1), "single-digit fields"),
            ("1/2/2024", false, d(2024, 2, 1), "single-digit fields"),
            ("1/2/2024", true, d(2024, 1, 2), "single-digit fields"),
            ("01/2/2024", false, d(2024, 2, 1), "single-digit fields"),
            ("01/2/2024", true, d(2024, 1, 2), "single-digit fields"),
            ("1/02/2024", false, d(2024, 2, 1), "single-digit fields"),
            ("1/02/2024", true, d(2024, 1, 2), "single-digit fields"),
            ("1/31/2024", false, d(2024, 1, 31), "single-digit fields"),
            ("1/31/2024", true, d(2024, 1, 31), "single-digit fields"),
            ("31/1/2024", false, d(2024, 1, 31), "single-digit fields"),
            ("31/1/2024", true, d(2024, 1, 31), "single-digit fields"),
            ("2024-1-5", false, d(2024, 1, 5), "single-digit fields"),
            ("2024-1-5", true, d(2024, 1, 5), "single-digit fields"),
            ("2024-01-5", false, d(2024, 1, 5), "single-digit fields"),
            ("2024-01-5", true, d(2024, 1, 5), "single-digit fields"),
            ("2024-1-05", false, d(2024, 1, 5), "single-digit fields"),
            ("2024-1-05", true, d(2024, 1, 5), "single-digit fields"),
            ("5-1-2024", false, d(2024, 1, 5), "single-digit fields"),
            ("5-1-2024", true, d(2024, 1, 5), "single-digit fields"),
            ("5.1.2024", false, d(2024, 1, 5), "single-digit fields"),
            ("5.1.2024", true, d(2024, 1, 5), "single-digit fields"),
            ("2024/1/5", false, d(2024, 1, 5), "single-digit fields"),
            ("2024/1/5", true, d(2024, 1, 5), "single-digit fields"),
            (
                "1/2/2024 10:30:00",
                false,
                d(2024, 2, 1),
                "single-digit fields",
            ),
            (
                "1/2/2024 10:30:00",
                true,
                d(2024, 1, 2),
                "single-digit fields",
            ),
            (
                "2024-1-5T10:30:00",
                false,
                d(2024, 1, 5),
                "single-digit fields",
            ),
            (
                "2024-1-5T10:30:00",
                true,
                d(2024, 1, 5),
                "single-digit fields",
            ),
            (
                "2024-1-5 10:30:00",
                false,
                d(2024, 1, 5),
                "single-digit fields",
            ),
            (
                "2024-1-5 10:30:00",
                true,
                d(2024, 1, 5),
                "single-digit fields",
            ),
            // Category C: signed or non-4-digit years — new reader (date statistics) wins
            // because DateTimeAccumulator already parses chrono %Y directly (note 2-digit
            // years like `24-01-01` and `01.01.24` parse as year 0024 AD in chrono %Y,
            // matching DateTimeStats; flagged as a separate gap rather than changed silently).
            ("+2024-01-15", false, d(2024, 1, 15), "signed year"),
            ("+2024-01-15", true, d(2024, 1, 15), "signed year"),
            ("-2024-01-15", false, d(-2024, 1, 15), "signed year"),
            ("-2024-01-15", true, d(-2024, 1, 15), "signed year"),
            ("05/01/+2025", false, d(2025, 1, 5), "signed year"),
            ("05/01/+2025", true, d(2025, 5, 1), "signed year"),
            ("05-01--2025", false, d(-2025, 1, 5), "signed year"),
            ("05-01--2025", true, d(-2025, 1, 5), "signed year"),
            ("+12000-01-05", false, d(12000, 1, 5), "extended year"),
            ("+12000-01-05", true, d(12000, 1, 5), "extended year"),
            ("999-01-05", false, d(999, 1, 5), "short year"),
            ("999-01-05", true, d(999, 1, 5), "short year"),
            ("01/05/999", false, d(999, 5, 1), "short year"),
            ("01/05/999", true, d(999, 1, 5), "short year"),
            (
                "24-01-01",
                false,
                d(24, 1, 1),
                "two-digit year in chrono %Y",
            ),
            ("24-01-01", true, d(24, 1, 1), "two-digit year in chrono %Y"),
            (
                "01.01.24",
                false,
                d(24, 1, 1),
                "two-digit year in chrono %Y",
            ),
            ("01.01.24", true, d(24, 1, 1), "two-digit year in chrono %Y"),
            // Category D: internal whitespace before numeric fields — new reader wins,
            // removing the old fixed-offset split between 1 space and 2 spaces.
            ("2025-  1-01", false, d(2025, 1, 1), "internal whitespace"),
            ("2025-  1-01", true, d(2025, 1, 1), "internal whitespace"),
            ("05/  1/2025", false, d(2025, 1, 5), "internal whitespace"),
            ("05/  1/2025", true, d(2025, 5, 1), "internal whitespace"),
            ("05/01/ 2025", false, d(2025, 1, 5), "internal whitespace"),
            ("05/01/ 2025", true, d(2025, 5, 1), "internal whitespace"),
        ];

        type DiffMap<'a> = HashMap<(&'a str, bool), (Option<NaiveDate>, Option<NaiveDate>)>;

        let mut expected_diffs: DiffMap<'_> = HashMap::new();
        for &(value, mf, new_date, _reason) in decided {
            assert!(
                corpus.contains(&value),
                "decided entry {value:?} missing from corpus"
            );
            let prev = expected_diffs.insert((value, mf), (None, Some(new_date)));
            assert!(prev.is_none(), "duplicate decided entry ({value:?}, {mf})");
        }

        let mut actual_diffs: DiffMap<'_> = HashMap::new();
        for &value in corpus {
            for (mf, order) in [(false, None), (true, Some(SlashDateOrder::MonthFirst))] {
                let old = legacy_timeliness_reader(value, order);
                let new = parse_date_value(value, order);
                // Verify DateTimeAccumulator itself produces the exact same date.
                let mut acc = DateTimeAccumulator::new();
                let dual = acc.update_returning_date(value);
                let acc_date = dual.map(|dd| if mf { dd.month_first } else { dd.day_first });
                assert_eq!(
                    new, acc_date,
                    "parse_date_value disagreed with DateTimeAccumulator on {value:?} (mf={mf})"
                );
                if old != new {
                    actual_diffs.insert((value, mf), (old, new));
                }
            }
        }

        let mut unexpected = Vec::new();
        for (key, got) in &actual_diffs {
            if expected_diffs.get(key) != Some(got) {
                unexpected.push(format!(
                    "{key:?}: actual={got:?}, expected={:?}",
                    expected_diffs.get(key)
                ));
            }
        }
        let mut stale = Vec::new();
        for (key, exp) in &expected_diffs {
            if !actual_diffs.contains_key(key) {
                stale.push(format!("{key:?}: expected={exp:?} no longer disagrees"));
            }
        }
        assert!(
            unexpected.is_empty() && stale.is_empty(),
            "corpus disagreements out of sync:\nunexpected: {unexpected:#?}\nstale: {stale:#?}"
        );

        // Compact #815 forms must stay unparsed by both readers until #815.
        for compact in ["20240115", "20231231", "18000101", "21991231"] {
            assert_eq!(legacy_timeliness_reader(compact, None), None, "{compact}");
            assert_eq!(parse_date_value(compact, None), None, "{compact}");
        }
    }
}
