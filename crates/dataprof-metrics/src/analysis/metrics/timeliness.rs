//! Timeliness Dimension (ISO 8000-8)
//!
//! Measures the currentness and temporal validity of data.
//! Key metrics: future dates, stale data ratio, temporal violations.

use super::utils::{extract_date, identifier_words};
use crate::analysis::inference::is_null_like_token;
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::quality::{TimelinessClock, TimelinessClockMode};
use crate::stats::datetime::{
    DateTimeAccumulator, DualDate, TimelinessWindow, parse_dual_date, reads_month_first,
    resolve_slash_order,
};
use crate::types::{ColumnProfile, ColumnStats, SlashDateOrder};
use chrono::{NaiveDate, Utc};
use std::collections::{HashMap, HashSet};

/// Start/end role-word pairs checked automatically when not overridden by
/// `temporal_pairs`.
const TEMPORAL_ROLE_PAIRS: [(&str, &str); 6] = [
    ("start_date", "end_date"),
    ("start", "end"),
    ("created_at", "updated_at"),
    ("created", "updated"),
    ("begin_date", "end_date"),
    ("from_date", "to_date"),
];

/// Full-stream start/end ordering counts for one candidate column pair,
/// covering all four `(start_month_first, end_month_first)` combinations so
/// slash-date order can be decided after every value has been read (#788 step 4).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PairOrderCounts {
    /// Rows where both columns parsed as calendar dates (independent of slash
    /// order).
    pub pairs_checked: usize,
    /// Rows with `start > end`, indexed by `[usize::from(start_mf)][usize::from(end_mf)]`.
    pub violations: [[usize; 2]; 2],
}

impl PairOrderCounts {
    /// Observe one row where both columns parsed as dates.
    pub fn observe(&mut self, start: DualDate, end: DualDate) {
        self.pairs_checked += 1;
        if start.day_first == start.month_first && end.day_first == end.month_first {
            if start.day_first > end.day_first {
                self.violations[0][0] += 1;
                self.violations[0][1] += 1;
                self.violations[1][0] += 1;
                self.violations[1][1] += 1;
            }
        } else {
            if start.day_first > end.day_first {
                self.violations[0][0] += 1;
            }
            if start.day_first > end.month_first {
                self.violations[0][1] += 1;
            }
            if start.month_first > end.day_first {
                self.violations[1][0] += 1;
            }
            if start.month_first > end.month_first {
                self.violations[1][1] += 1;
            }
        }
    }

    /// Merge counts from another partition.
    pub fn merge(&mut self, other: &Self) {
        self.pairs_checked += other.pairs_checked;
        for i in 0..2 {
            for j in 0..2 {
                self.violations[i][j] += other.violations[i][j];
            }
        }
    }

    /// Violations under the resolved `(start_month_first, end_month_first)`
    /// orders.
    pub fn violations_for(&self, start_month_first: bool, end_month_first: bool) -> usize {
        self.violations[usize::from(start_month_first)][usize::from(end_month_first)]
    }
}

/// Cap on unrelated `(start, end)` cross-products tracked for the `lone`
/// role-pair fallback. Matching-rest pairs and declared pairs are always
/// tracked regardless of table width.
const LONE_PAIR_CARTESIAN_CAP: usize = 256;

/// Row-level tracker for start/end date ordering over every scanned row
/// (#788 step 4, #787).
///
/// Tracks every declared pair plus every candidate role-word pair that
/// `TimelinessCalculator::column_pairs` could select once column types are
/// finalized. Null cells on one row skip that row alone without shifting
/// partner rows, and all four slash-order combinations are accumulated in
/// O(1) memory per pair.
#[derive(Debug, Clone, Default)]
pub struct TemporalPairTracker {
    declared_pairs: Vec<(String, String)>,
    declared_columns: HashSet<String>,
    known_columns: Vec<String>,
    known_set: HashSet<String>,
    /// Tracked column name -> slot index in `current_row`.
    tracked_slots: HashMap<String, usize>,
    /// Candidate pairs `(start_name, end_name, start_slot, end_slot, counts)`.
    pairs: Vec<TrackedPair>,
    pair_index: HashMap<(String, String), usize>,
    /// Scratch buffer holding the current row's parsed dates by slot.
    current_row: Vec<Option<DualDate>>,
    /// Pairs a merged partition read rows for without counting them: their
    /// counts would cover only part of the data, so they are never tracked
    /// again and the calculator falls back to the sample for them.
    unavailable: HashSet<(String, String)>,
}

#[derive(Debug, Clone)]
struct TrackedPair {
    start_name: String,
    end_name: String,
    start_slot: usize,
    end_slot: usize,
    counts: PairOrderCounts,
}

impl TemporalPairTracker {
    /// Create a tracker configured with any explicitly declared `temporal_pairs`.
    pub fn new(declared_pairs: &[(String, String)]) -> Self {
        let declared_columns: HashSet<String> = declared_pairs
            .iter()
            .flat_map(|(s, e)| [s.clone(), e.clone()])
            .collect();
        Self {
            declared_pairs: declared_pairs.to_vec(),
            declared_columns,
            known_columns: Vec::new(),
            known_set: HashSet::new(),
            tracked_slots: HashMap::new(),
            pairs: Vec::new(),
            pair_index: HashMap::new(),
            current_row: Vec::new(),
            unavailable: HashSet::new(),
        }
    }

    /// Whether at least one candidate pair is being tracked. When `false`,
    /// per-row tracking does no work.
    pub fn is_enabled(&self) -> bool {
        !self.pairs.is_empty()
    }

    /// Whether `name` participates in any tracked candidate pair.
    pub fn tracks_column(&self, name: &str) -> bool {
        self.tracked_slots.contains_key(name)
    }

    /// Register a slice of column names (e.g. a table or CSV header).
    pub fn register_columns(&mut self, names: &[String]) {
        let mut added = false;
        for name in names {
            if self.known_set.insert(name.clone()) {
                self.known_columns.push(name.clone());
                added = true;
            }
        }
        if added {
            self.refresh_candidates();
        }
    }

    /// Register one newly discovered column (e.g. a mid-stream JSON key).
    /// Any candidate pair involving this column had nulls in all prior rows,
    /// so starting its counts at zero is exact.
    pub fn register_column(&mut self, name: &str) {
        if self.known_set.insert(name.to_string()) {
            self.known_columns.push(name.to_string());
            self.refresh_candidates();
        }
    }

    fn slot_for(&mut self, name: &str) -> usize {
        if let Some(&slot) = self.tracked_slots.get(name) {
            return slot;
        }
        let slot = self.current_row.len();
        self.tracked_slots.insert(name.to_string(), slot);
        self.current_row.push(None);
        slot
    }

    fn add_pair(&mut self, start: &str, end: &str) {
        if start == end {
            return;
        }
        let key = (start.to_string(), end.to_string());
        if self.pair_index.contains_key(&key) || self.unavailable.contains(&key) {
            return;
        }
        let start_slot = self.slot_for(start);
        let end_slot = self.slot_for(end);
        let idx = self.pairs.len();
        self.pairs.push(TrackedPair {
            start_name: start.to_string(),
            end_name: end.to_string(),
            start_slot,
            end_slot,
            counts: PairOrderCounts::default(),
        });
        self.pair_index.insert(key, idx);
    }

    fn refresh_candidates(&mut self) {
        let declared_pairs = self.declared_pairs.clone();
        for (start, end) in &declared_pairs {
            if start != end && self.known_set.contains(start) && self.known_set.contains(end) {
                self.add_pair(start, end);
            }
        }

        let candidates: Vec<(String, String)> = {
            let mut found = Vec::new();
            for (start_role, end_role) in &TEMPORAL_ROLE_PAIRS {
                let starts: Vec<(&str, Vec<String>)> = self
                    .known_columns
                    .iter()
                    .filter(|name| !self.declared_columns.contains(name.as_str()))
                    .filter_map(|name| Some((name.as_str(), words_beside_role(name, start_role)?)))
                    .collect();
                let ends: Vec<(&str, Vec<String>)> = self
                    .known_columns
                    .iter()
                    .filter(|name| !self.declared_columns.contains(name.as_str()))
                    .filter_map(|name| Some((name.as_str(), words_beside_role(name, end_role)?)))
                    .collect();
                if starts.is_empty() || ends.is_empty() {
                    continue;
                }
                let allow_all = starts.len().saturating_mul(ends.len()) <= LONE_PAIR_CARTESIAN_CAP;
                for (start_name, start_rest) in &starts {
                    let start_trimmed = without_date_words(start_rest);
                    for (end_name, end_rest) in &ends {
                        if start_name == end_name {
                            continue;
                        }
                        if allow_all
                            || start_rest == end_rest
                            || start_trimmed == without_date_words(end_rest)
                        {
                            found.push(((*start_name).to_string(), (*end_name).to_string()));
                        }
                    }
                }
            }
            found
        };

        // The tracked set must depend on the columns known, not on the order
        // they were registered in: a reader that registers keys one by one
        // (JSON) can add an unrelated pair while the cross-product is still
        // under the cap. Once the cap is crossed, drop every pair the whole
        // schema would not select, so it falls back to the sample as it does
        // on a reader that registers the full header at once (CSV).
        let keep: HashSet<(&str, &str)> = declared_pairs
            .iter()
            .map(|(start, end)| (start.as_str(), end.as_str()))
            .chain(
                candidates
                    .iter()
                    .map(|(start, end)| (start.as_str(), end.as_str())),
            )
            .collect();
        self.retain_pairs(|start, end| keep.contains(&(start, end)));

        for (start, end) in candidates {
            self.add_pair(&start, &end);
        }
    }

    /// Keep only the tracked pairs `keep` accepts, and reindex them.
    fn retain_pairs(&mut self, keep: impl Fn(&str, &str) -> bool) {
        if self
            .pairs
            .iter()
            .all(|pair| keep(&pair.start_name, &pair.end_name))
        {
            return;
        }
        self.pairs
            .retain(|pair| keep(&pair.start_name, &pair.end_name));
        self.pair_index = self
            .pairs
            .iter()
            .enumerate()
            .map(|(index, pair)| ((pair.start_name.clone(), pair.end_name.clone()), index))
            .collect();
    }

    /// Record an already-parsed date for `column` in the current row.
    pub fn record_date(&mut self, column: &str, date: Option<DualDate>) {
        if let Some(&slot) = self.tracked_slots.get(column) {
            self.current_row[slot] = date;
        }
    }

    /// Parse and record a raw cell for `column` in the current row if `column`
    /// is part of any tracked candidate pair.
    pub fn record_text(&mut self, column: &str, value: Option<&str>) {
        if let Some(&slot) = self.tracked_slots.get(column) {
            self.current_row[slot] = value.and_then(parse_dual_date);
        }
    }

    /// Commit the current row to every candidate pair whose endpoints both
    /// parsed as calendar dates, then reset the row buffer.
    ///
    /// The buffer is cleared even when no pair is tracked: a pair can be
    /// dropped (cap crossed) or added (new column) between rows, and a date
    /// left in a slot would otherwise pair with a later row's partner.
    pub fn finish_row(&mut self) {
        for pair in &mut self.pairs {
            if let (Some(start), Some(end)) = (
                self.current_row[pair.start_slot],
                self.current_row[pair.end_slot],
            ) {
                pair.counts.observe(start, end);
            }
        }
        self.current_row.fill(None);
    }

    /// Drop candidate pairs referencing columns outside `names`.
    pub fn retain_columns(&mut self, names: &[String]) {
        let selected: HashSet<&str> = names.iter().map(String::as_str).collect();
        self.known_columns
            .retain(|name| selected.contains(name.as_str()));
        self.known_set
            .retain(|name| selected.contains(name.as_str()));
        let kept: Vec<(String, String, PairOrderCounts)> = self
            .pairs
            .iter()
            .filter(|pair| {
                selected.contains(pair.start_name.as_str())
                    && selected.contains(pair.end_name.as_str())
            })
            .map(|pair| (pair.start_name.clone(), pair.end_name.clone(), pair.counts))
            .collect();
        self.tracked_slots.clear();
        self.pairs.clear();
        self.pair_index.clear();
        self.current_row.clear();
        for (start, end, counts) in kept {
            self.add_pair(&start, &end);
            let idx = self.pair_index[&(start, end)];
            self.pairs[idx].counts = counts;
        }
    }

    /// Merge another partition's pair counts into this tracker.
    pub fn merge(&mut self, other: &Self) {
        for pair in &other.declared_pairs {
            if !self.declared_pairs.contains(pair) {
                self.declared_pairs.push(pair.clone());
                self.declared_columns.insert(pair.0.clone());
                self.declared_columns.insert(pair.1.clone());
            }
        }
        // Registering the other side's columns applies the combined schema's
        // admission rule, the cap included, to this side's pairs. Adding
        // columns only ever removes pairs, so a pair still admitted was
        // tracked by every side that saw both of its columns, and a side that
        // missed a column read only nulls for it: its counts add exactly. A
        // pair the combined schema does not admit is not taken from the other
        // side and falls back to the sample.
        self.register_columns(&other.known_columns);

        // A pair the other side read both columns of without counting it
        // (it was unavailable there) covers part of the rows only: make it
        // unavailable here too rather than report a partial count as exact.
        self.unavailable.extend(other.unavailable.iter().cloned());
        let partial: HashSet<(String, String)> = self
            .pairs
            .iter()
            .map(|pair| (pair.start_name.clone(), pair.end_name.clone()))
            .filter(|key| {
                self.unavailable.contains(key)
                    || (other.known_set.contains(&key.0)
                        && other.known_set.contains(&key.1)
                        && !other.pair_index.contains_key(key))
            })
            .collect();
        self.unavailable.extend(partial.iter().cloned());
        self.retain_pairs(|start, end| !partial.contains(&(start.to_string(), end.to_string())));

        for other_pair in &other.pairs {
            let key = (other_pair.start_name.clone(), other_pair.end_name.clone());
            if let Some(&index) = self.pair_index.get(&key) {
                self.pairs[index].counts.merge(&other_pair.counts);
            }
        }
    }

    /// Extract the accumulated per-pair counts.
    pub fn finish(&self) -> HashMap<(String, String), PairOrderCounts> {
        self.pairs
            .iter()
            .map(|pair| {
                (
                    (pair.start_name.clone(), pair.end_name.clone()),
                    pair.counts,
                )
            })
            .collect()
    }
}

/// Full-stream timeliness counters accumulated over every scanned value and
/// row (#788 step 4).
#[derive(Debug, Clone)]
pub struct TimelinessTallies {
    /// Per-column date statistics & timeliness accumulators.
    pub columns: HashMap<String, DateTimeAccumulator>,
    /// Candidate start/end pair ordering counts over every scanned row.
    pub pairs: HashMap<(String, String), PairOrderCounts>,
    /// Clock mode captured at scan start (`Fixed` or `Rolling`).
    pub clock_mode: TimelinessClockMode,
    /// Freshness/future window that `columns` were counted against.
    pub window: TimelinessWindow,
}

/// What may follow a one-word role inside a single word of a name, besides
/// one of [`DATE_WORDS`]: an inflection (`starts_at`, `ended_at`). The date
/// word is there because an all-lowercase name runs it into the role, as
/// PostgreSQL folds `StartDate` to `startdate`.
const ROLE_INFLECTIONS: [&str; 3] = ["s", "ed", "ing"];

/// Words that say a column holds a date rather than which date it holds.
const DATE_WORDS: [&str; 6] = ["date", "time", "timestamp", "at", "on", "ts"];

/// Whether `name` holds `role`, by the rules of [`words_beside_role`].
#[cfg(test)]
fn holds_role(name: &str, role: &str) -> bool {
    words_beside_role(name, role).is_some()
}

/// The words of `name` left once the words holding `role` are taken out, in
/// lower case, or `None` when `name` does not hold `role`: `trip` for
/// `trip_start` and for `tripEndDate` under `end_date`. They are what ties a
/// start column to its end column (#913).
///
/// A name holds `role` as words of its own, never inside a longer word
/// (#882): `end` is in `end_date` and `endDate`, not in `send_date` or
/// `weekend_date`; `to_date` is in `valid_to_date`, not in `photo_date`.
///
/// A one-word role also matches a word that starts with it and continues with
/// one of [`ROLE_INFLECTIONS`] or [`DATE_WORDS`]. Each such word contains the
/// role, so the endings keep pairs the substring match it replaces found, and
/// add none. A role of several words is matched across a camelCase name too,
/// which the substring match could not do: `fromDate` holds `from_date`.
fn words_beside_role(name: &str, role: &str) -> Option<Vec<String>> {
    let words = identifier_words(name);
    let role_words = identifier_words(role);
    let first = if let [role] = role_words.as_slice() {
        words.iter().position(|word| {
            word.get(..role.len())
                .is_some_and(|head| head.eq_ignore_ascii_case(role))
                && word.get(role.len()..).is_some_and(|rest| {
                    rest.is_empty()
                        || ROLE_INFLECTIONS
                            .iter()
                            .chain(&DATE_WORDS)
                            .any(|ending| rest.eq_ignore_ascii_case(ending))
                })
        })?
    } else if role_words.is_empty() {
        return None;
    } else {
        words.windows(role_words.len()).position(|window| {
            window
                .iter()
                .zip(&role_words)
                .all(|(word, role_word)| word.eq_ignore_ascii_case(role_word))
        })?
    };
    let role_span = first..first + role_words.len();
    Some(
        words
            .iter()
            .enumerate()
            .filter(|(index, _)| !role_span.contains(index))
            .map(|(_, word)| word.to_ascii_lowercase())
            .collect(),
    )
}

/// `rest` without its trailing [`DATE_WORDS`]: `trip` for what
/// `trip_start_date` leaves under `start`, the same as `trip_end` leaves
/// under `end` (#941).
fn without_date_words(rest: &[String]) -> &[String] {
    let kept = rest
        .iter()
        .rposition(|word| !DATE_WORDS.contains(&word.as_str()))
        .map_or(0, |last| last + 1);
    &rest[..kept]
}

/// Timeliness metrics container
#[derive(Debug)]
pub(crate) struct TimelinessMetrics {
    pub future_dates_count: usize,
    pub stale_data_ratio: f64,
    pub temporal_violations: usize,
    pub invalid_date_values: usize,
    pub date_values_checked: usize,
    pub temporal_pairs_checked: usize,
    /// Checked values that parsed as dates: the denominator of
    /// `stale_data_ratio`.
    pub valid_dates: usize,
    /// Valid dates older than the freshness policy: the count behind
    /// `stale_data_ratio`.
    pub stale_dates: usize,
    /// Distinct start/end column pairs compared, so the most pairs one row
    /// can add to `temporal_pairs_checked`.
    pub temporal_column_pairs: usize,
    /// The start/end columns of each pair compared. A pair whose values do
    /// not come from the same rows is not compared, so it is not here.
    pub compared_pairs: Vec<(String, String)>,
}

/// Whether the values held for two columns line up row by row, so that value
/// `k` of each came from the same source row.
///
/// Values that keep a column's every row, nulls in place, line up by
/// construction. Engines hand the calculators each column's non-null values
/// instead, sampled from one seed: two columns without a null are offered one
/// value per row, make the same choices, and keep the same rows slot by slot.
/// A column with nulls skips rows, and from its first null on its slots hold
/// other rows than its partner's (#787).
pub(crate) fn rows_line_up(
    first: &ColumnProfile,
    first_held: usize,
    second: &ColumnProfile,
    second_held: usize,
) -> bool {
    let every_row = first_held == first.total_count && second_held == second.total_count;
    let null_free =
        first.null_count == 0 && second.null_count == 0 && first.total_count == second.total_count;
    first_held == second_held && (every_row || null_free)
}

/// The order `name`'s slash dates are read in: the order its date statistics
/// record, decided over every value the column streamed (#1297), so the score
/// reads the dates the report describes (#1305). Resolving it from `values`
/// instead read a retained sample, which on a large source can miss the one
/// value that settles the order.
///
/// A column without date statistics (one typed other than date, named as
/// temporal by a hint) has no recorded order, and falls back to `values`.
fn slash_order(
    name: &str,
    values: &[String],
    column_profiles: &[ColumnProfile],
) -> Option<SlashDateOrder> {
    match column_profiles
        .iter()
        .find(|profile| profile.name == name)
        .map(|profile| &profile.stats)
    {
        Some(ColumnStats::DateTime(stats)) => stats.slash_date_order,
        _ => resolve_slash_order(values),
    }
}

/// Start/end ordering counts over the pairs that could be compared.
struct TemporalOrdering {
    violations: usize,
    pairs_checked: usize,
    compared_pairs: Vec<(String, String)>,
}

/// Per-value date counts over the temporal columns.
struct DateSummary {
    future_dates: usize,
    stale_dates: usize,
    valid_dates: usize,
    checked: usize,
    invalid_dates: usize,
}

/// Calculator for timeliness dimension metrics
pub(crate) struct TimelinessCalculator<'a> {
    thresholds: &'a IsoQualityConfig,
    /// "Now" for every comparison this calculator makes.
    ///
    /// Captured once at construction so a single report cannot straddle a
    /// midnight boundary and classify two equal values differently.
    reference_date: NaiveDate,
    /// Whether the caller fixed `reference_date` or it is today's.
    mode: TimelinessClockMode,
}

impl<'a> TimelinessCalculator<'a> {
    /// Measure against `reference_date`, or against today's UTC date when it
    /// is `None` (#903).
    pub fn new(thresholds: &'a IsoQualityConfig, reference_date: Option<NaiveDate>) -> Self {
        match reference_date {
            Some(date) => Self::with_reference_date(thresholds, date),
            None => Self {
                thresholds,
                reference_date: Utc::now().date_naive(),
                mode: TimelinessClockMode::Rolling,
            },
        }
    }

    /// Construct with a fixed "now", so assertions do not rot as the
    /// calendar advances.
    pub(crate) fn with_reference_date(
        thresholds: &'a IsoQualityConfig,
        reference_date: NaiveDate,
    ) -> Self {
        Self::with_clock(thresholds, reference_date, TimelinessClockMode::Fixed)
    }

    /// Construct with an explicit reference date and clock mode, so a rolling
    /// clock captured at scan start stays locked to one day while still
    /// recording `TimelinessClockMode::Rolling` (#788 step 4).
    pub(crate) fn with_clock(
        thresholds: &'a IsoQualityConfig,
        reference_date: NaiveDate,
        mode: TimelinessClockMode,
    ) -> Self {
        Self {
            thresholds,
            reference_date,
            mode,
        }
    }

    /// The `(reference_date, stale_cutoff)` window this calculator judges dates
    /// against.
    pub(crate) fn window(&self) -> TimelinessWindow {
        TimelinessWindow::new(self.reference_date, self.thresholds.max_data_age_years)
    }

    /// What this calculator measures against, as the report records it.
    pub fn clock(&self) -> TimelinessClock {
        TimelinessClock::new(
            self.mode,
            self.reference_date,
            self.thresholds.max_data_age_years,
        )
    }

    /// Calculate timeliness metrics directly from full-stream
    /// [`TimelinessTallies`] (#788 step 4).
    ///
    /// Returns `Ok(Some(...))` when every temporal column's accumulator was
    /// counted against `self.window()`, or `Ok(None)` when the caller selected
    /// custom thresholds that do not match the tallied window (so the caller
    /// can fall back to `calculate`).
    pub fn calculate_from_tallies(
        &self,
        tallies: &TimelinessTallies,
        temporal_columns: &[String],
        declared_pairs: &[(String, String)],
    ) -> Result<Option<TimelinessMetrics>, DataProfilerError> {
        let window = self.window();
        if tallies.window != window {
            return Ok(None);
        }

        let mut future_count = 0usize;
        let mut stale_dates = 0usize;
        let mut valid_dates = 0usize;
        let mut checked = 0usize;
        let mut invalid_dates = 0usize;

        for column_name in temporal_columns {
            // A temporal column the engine did not tally would drop its dates
            // from every count: fall back to the sample instead.
            let Some(acc) = tallies.columns.get(column_name) else {
                return Ok(None);
            };
            let Some(summary) = acc.timeliness_summary(window) else {
                return Ok(None);
            };
            future_count += summary.future_dates;
            stale_dates += summary.stale_dates;
            valid_dates += summary.valid_dates;
            checked += summary.checked;
            invalid_dates += summary.invalid_dates;
        }

        let stale_data_ratio = if valid_dates == 0 {
            0.0
        } else {
            (stale_dates as f64 / valid_dates as f64) * 100.0
        };

        let mut violations = 0usize;
        let mut pairs_checked = 0usize;
        let mut compared_pairs = Vec::new();
        for (start_name, end_name) in Self::column_pairs_with(
            |name| tallies.columns.contains_key(name),
            temporal_columns,
            declared_pairs,
        ) {
            let start_mf = reads_month_first(
                tallies
                    .columns
                    .get(start_name)
                    .and_then(DateTimeAccumulator::slash_order),
            );
            let end_mf = reads_month_first(
                tallies
                    .columns
                    .get(end_name)
                    .and_then(DateTimeAccumulator::slash_order),
            );
            // A pair the tracker did not follow (past `LONE_PAIR_CARTESIAN_CAP`)
            // has no row counts: fall back to the sample rather than report
            // it compared over zero rows.
            let Some(counts) = tallies
                .pairs
                .get(&(start_name.to_string(), end_name.to_string()))
                .copied()
            else {
                return Ok(None);
            };
            pairs_checked += counts.pairs_checked;
            violations += counts.violations_for(start_mf, end_mf);
            compared_pairs.push((start_name.to_string(), end_name.to_string()));
        }

        Ok(Some(TimelinessMetrics {
            future_dates_count: future_count,
            stale_data_ratio,
            temporal_violations: violations,
            invalid_date_values: invalid_dates,
            date_values_checked: checked,
            temporal_pairs_checked: pairs_checked,
            valid_dates,
            stale_dates,
            temporal_column_pairs: compared_pairs.len(),
            compared_pairs,
        }))
    }

    /// Calculate timeliness dimension metrics
    pub fn calculate(
        &self,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
        declared_pairs: &[(String, String)],
        column_profiles: &[ColumnProfile],
    ) -> Result<TimelinessMetrics, DataProfilerError> {
        let summary = self.calculate_date_summary(data, temporal_columns, column_profiles);
        let stale_data_ratio = if summary.valid_dates == 0 {
            0.0
        } else {
            (summary.stale_dates as f64 / summary.valid_dates as f64) * 100.0
        };
        let ordering = Self::count_temporal_violations(
            data,
            temporal_columns,
            declared_pairs,
            column_profiles,
        )?;

        Ok(TimelinessMetrics {
            future_dates_count: summary.future_dates,
            stale_data_ratio,
            temporal_violations: ordering.violations,
            invalid_date_values: summary.invalid_dates,
            date_values_checked: summary.checked,
            temporal_pairs_checked: ordering.pairs_checked,
            valid_dates: summary.valid_dates,
            stale_dates: summary.stale_dates,
            temporal_column_pairs: ordering.compared_pairs.len(),
            compared_pairs: ordering.compared_pairs,
        })
    }

    /// Assess every non-null value in the selected temporal columns.
    ///
    /// `date_values_checked` is the complete denominator. Values that do not
    /// parse as real calendar dates remain visible as `invalid_date_values`
    /// instead of disappearing from both the metric and its score.
    fn calculate_date_summary(
        &self,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
        column_profiles: &[ColumnProfile],
    ) -> DateSummary {
        let mut future_count = 0;
        let mut stale_dates = 0;
        let mut valid_dates = 0;
        let mut checked = 0;
        let mut invalid_dates = 0;

        let window = self.window();
        let threshold_date = window.stale_cutoff;

        for column_name in temporal_columns {
            let Some(column_data) = data.get(column_name) else {
                continue;
            };
            // Read the column's slash dates in the order its statistics record.
            let order = slash_order(column_name, column_data, column_profiles);
            for value in column_data {
                if is_null_like_token(value.trim()) {
                    continue;
                }
                checked += 1;

                if let Some(date) = extract_date(value, order) {
                    valid_dates += 1;
                    // Compare the whole date, not its year. A year-only test
                    // cannot see any future date inside the current calendar
                    // year, which on 1 January hides an entire year of them.
                    if date > self.reference_date {
                        future_count += 1;
                    }
                    if date < threshold_date {
                        stale_dates += 1;
                    }
                } else {
                    invalid_dates += 1;
                }
            }
        }

        DateSummary {
            future_dates: future_count,
            stale_dates,
            valid_dates,
            checked,
            invalid_dates,
        }
    }

    /// The start/end column pairs to compare, like start_date/end_date or
    /// created_at/updated_at, in the order `temporal_columns` lists the starts.
    ///
    /// A start column pairs with the end column that shares the rest of its
    /// name: `trip_start` with `trip_end`, `billing_start` with `billing_end`
    /// (#913). A start that fits no end that way falls back to the rest without
    /// a trailing date word, so `trip_start_date` pairs with `trip_end` (#941),
    /// but only with an end no start fits by its whole rest: `start_time`
    /// keeps `end_time` beside `start_date`/`end_date`, and `start` keeps `end`
    /// beside `start_date`.
    ///
    /// A start that fits several ends, or an end that fits several starts, is
    /// left unpaired rather than guessed. Names that share nothing still pair
    /// when they are the only start and the only end, as
    /// `created_at`/`last_updated_at` are.
    ///
    /// Declared pairs come first and are compared as given (#942). A column
    /// named in one takes no part in the pairing by name, which pairs the
    /// rest: declaring `trip_start`/`trip_end` leaves `trip_start_date` free
    /// to pair by its own name.
    fn column_pairs<'d>(
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &'d [String],
        declared_pairs: &'d [(String, String)],
    ) -> Vec<(&'d str, &'d str)> {
        Self::column_pairs_with(
            |name| data.contains_key(name),
            temporal_columns,
            declared_pairs,
        )
    }

    fn column_pairs_with<'d>(
        has_column: impl Fn(&str) -> bool,
        temporal_columns: &'d [String],
        declared_pairs: &'d [(String, String)],
    ) -> Vec<(&'d str, &'d str)> {
        // The role patterns overlap: `start_date`/`end_date` is matched by both
        // ("start_date", "end_date") and ("start", "end"). Without this the same
        // two columns are compared once per matching pattern, and every pair and
        // every violation is counted as many times as patterns happened to hit.
        let mut evaluated: HashSet<(&str, &str)> = HashSet::new();
        let mut pairs = Vec::new();

        let declared: HashSet<&str> = declared_pairs
            .iter()
            .flat_map(|(start, end)| [start.as_str(), end.as_str()])
            .collect();
        for (start_name, end_name) in declared_pairs {
            if start_name != end_name
                && has_column(start_name)
                && has_column(end_name)
                && evaluated.insert((start_name, end_name))
            {
                pairs.push((start_name.as_str(), end_name.as_str()));
            }
        }

        for (start_col, end_col) in &TEMPORAL_ROLE_PAIRS {
            // Walk `temporal_columns`, not a HashMap: HashMap iteration order
            // is randomized. A name the caller listed but the data does not
            // carry fills no role, so it cannot hide one that has values.
            let holders = |role: &str| -> Vec<(&'d str, Vec<String>)> {
                temporal_columns
                    .iter()
                    .filter(|name| has_column(name) && !declared.contains(name.as_str()))
                    .filter_map(|name| Some((name.as_str(), words_beside_role(name, role)?)))
                    .collect()
            };
            let starts = holders(start_col);
            let ends = holders(end_col);
            let lone = starts.len() == 1 && ends.len() == 1;

            // A column cannot be both ends of the same comparison: "start" and
            // "end" both match a lone `start_end_date`, which would compare it
            // with itself and report a clean pair that was never checked.
            let ends_fitting = |start_name: &str, fits: &dyn Fn(&[String]) -> bool| {
                ends.iter()
                    .filter(|(end_name, end_rest)| *end_name != start_name && fits(end_rest))
                    .map(|(end_name, _)| *end_name)
                    .collect::<Vec<_>>()
            };
            let whole: Vec<Vec<&str>> = starts
                .iter()
                .map(|(start_name, start_rest)| {
                    ends_fitting(start_name, &|end_rest| lone || end_rest == start_rest)
                })
                .collect();
            let claimed: HashSet<&str> = whole.iter().flatten().copied().collect();
            let chosen: Vec<Option<&str>> = starts
                .iter()
                .zip(&whole)
                .map(|((start_name, start_rest), whole)| {
                    let fitting = if whole.is_empty() {
                        let start_rest = without_date_words(start_rest);
                        ends_fitting(start_name, &|end_rest| {
                            without_date_words(end_rest) == start_rest
                        })
                        .into_iter()
                        .filter(|end_name| !claimed.contains(end_name))
                        .collect()
                    } else {
                        whole.clone()
                    };
                    match fitting.as_slice() {
                        [end_name] => Some(*end_name),
                        _ => None,
                    }
                })
                .collect();

            for ((start_name, _), end_name) in starts.iter().zip(&chosen) {
                let Some(end_name) = *end_name else {
                    continue;
                };
                let rivals = chosen
                    .iter()
                    .filter(|other| **other == Some(end_name))
                    .count();
                if rivals == 1 && evaluated.insert((*start_name, end_name)) {
                    pairs.push((*start_name, end_name));
                }
            }
        }
        pairs
    }

    /// Count temporal ordering violations (e.g., end_date < start_date). The
    /// pair count is the denominator that makes the violation count
    /// interpretable; it is not bounded by the number of date-typed values.
    ///
    /// Only column pairs whose values [line up by row](rows_line_up) are
    /// compared. Comparing the others would pair a start date from one row
    /// with an end date from another and count violations nobody made.
    fn count_temporal_violations(
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
        declared_pairs: &[(String, String)],
        column_profiles: &[ColumnProfile],
    ) -> Result<TemporalOrdering, DataProfilerError> {
        let mut violations = 0;
        let mut pairs_checked = 0;
        let mut compared = Vec::new();
        let profile = |name: &str| column_profiles.iter().find(|profile| profile.name == name);

        for (start_name, end_name) in Self::column_pairs(data, temporal_columns, declared_pairs) {
            let (Some(start_values), Some(end_values)) = (data.get(start_name), data.get(end_name))
            else {
                continue;
            };
            let (Some(start_profile), Some(end_profile)) = (profile(start_name), profile(end_name))
            else {
                continue;
            };
            if !rows_line_up(
                start_profile,
                start_values.len(),
                end_profile,
                end_values.len(),
            ) {
                continue;
            }
            compared.push((start_name.to_string(), end_name.to_string()));
            // Each column in its own order: a US start column paired with a
            // European end column is read correctly on both sides.
            let start_order = slash_order(start_name, start_values, column_profiles);
            let end_order = slash_order(end_name, end_values, column_profiles);

            for (start_val, end_val) in start_values.iter().zip(end_values.iter()) {
                if is_null_like_token(start_val.trim()) || is_null_like_token(end_val.trim()) {
                    continue;
                }

                // Invalid calendar values are already reported by
                // `invalid_date_values` and cannot form a meaningful pair.
                let (Some(start_date), Some(end_date)) = (
                    extract_date(start_val, start_order),
                    extract_date(end_val, end_order),
                ) else {
                    continue;
                };
                pairs_checked += 1;
                // Compare parsed dates. The previous string comparison only held
                // for ISO values: `DD/MM/YYYY` and `MM/DD/YYYY` sort by day and
                // month first, which both hid real inversions and invented ones.
                if start_date > end_date {
                    violations += 1;
                }
            }
        }

        Ok(TemporalOrdering {
            violations,
            pairs_checked,
            compared_pairs: compared,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Profiles for columns held whole, nulls in place, as a caller of
    /// `QualityMetrics::calculate_from_data` passes them: every pair lines up.
    fn whole(data: &HashMap<String, Vec<String>>) -> Vec<ColumnProfile> {
        data.iter()
            .map(|(name, values)| {
                let nulls = values
                    .iter()
                    .filter(|value| is_null_like_token(value.trim()))
                    .count();
                super::super::testing::string_profile(name, values.len(), nulls)
            })
            .collect()
    }

    trait Whole {
        fn calculate_whole(
            &self,
            data: &HashMap<String, Vec<String>>,
            temporal_columns: &[String],
        ) -> Result<TimelinessMetrics, DataProfilerError>;
    }

    impl Whole for TimelinessCalculator<'_> {
        fn calculate_whole(
            &self,
            data: &HashMap<String, Vec<String>>,
            temporal_columns: &[String],
        ) -> Result<TimelinessMetrics, DataProfilerError> {
            self.calculate(data, temporal_columns, &[], &whole(data))
        }
    }

    #[test]
    fn inferred_dates_are_not_assessed_without_explicit_temporal_columns() {
        let data = HashMap::from([(
            "observed_on".to_string(),
            vec!["2020-01-01".to_string(), "2021-01-01".to_string()],
        )]);
        let config = IsoQualityConfig::default();

        let metrics = TimelinessCalculator::new(&config, None)
            .calculate_whole(&data, &[])
            .expect("timeliness metrics");

        assert_eq!(metrics.date_values_checked, 0);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.temporal_pairs_checked, 0);
    }

    #[test]
    fn explicit_temporal_columns_assess_parseable_values_even_in_mixed_columns() {
        let data = HashMap::from([(
            "event_value".to_string(),
            vec!["2020-01-01".to_string(), "not-a-date".to_string()],
        )]);
        let config = IsoQualityConfig::default();

        let metrics = TimelinessCalculator::new(&config, None)
            .calculate_whole(&data, &["event_value".to_string()])
            .expect("timeliness metrics");

        assert_eq!(metrics.date_values_checked, 2);
        assert_eq!(metrics.invalid_date_values, 1);
    }

    #[test]
    fn invalid_calendar_values_are_not_compared_as_temporal_pairs() {
        let data = HashMap::from([
            ("start".to_string(), vec!["2024-13-45".to_string()]),
            ("end".to_string(), vec!["2024-01-01".to_string()]),
        ]);
        let config = IsoQualityConfig::default();

        let metrics = TimelinessCalculator::new(&config, None)
            .calculate_whole(&data, &["start".to_string(), "end".to_string()])
            .expect("timeliness metrics");

        assert_eq!(metrics.invalid_date_values, 1);
        assert_eq!(metrics.temporal_pairs_checked, 0);
        assert_eq!(metrics.temporal_violations, 0);
    }

    #[test]
    fn temporal_ordering_requires_both_columns_to_be_explicit() {
        let data = HashMap::from([
            (
                "start".to_string(),
                vec!["2024-01-02".to_string(), "2024-01-01".to_string()],
            ),
            (
                "end".to_string(),
                vec!["2024-01-01".to_string(), "2024-01-02".to_string()],
            ),
        ]);
        let config = IsoQualityConfig::default();
        let calculator = TimelinessCalculator::new(&config, None);

        let partial = calculator
            .calculate_whole(&data, &["start".to_string()])
            .expect("partial timeliness metrics");
        assert_eq!(partial.temporal_pairs_checked, 0);

        let complete = calculator
            .calculate_whole(&data, &["start".to_string(), "end".to_string()])
            .expect("complete timeliness metrics");
        assert_eq!(complete.temporal_pairs_checked, 2);
        assert_eq!(complete.temporal_violations, 1);
    }

    #[test]
    fn ambiguous_roles_form_no_pair_in_either_column_order() {
        // #913: two starts and one end that names neither. The first start
        // listed used to win, so the result followed the column order.
        let data = HashMap::from([
            ("primary_start".to_string(), vec!["2024-01-01".to_string()]),
            (
                "secondary_start".to_string(),
                vec!["2024-01-03".to_string()],
            ),
            ("end".to_string(), vec!["2024-01-02".to_string()]),
        ]);

        for order in [
            ["primary_start", "secondary_start", "end"],
            ["secondary_start", "primary_start", "end"],
        ] {
            let metrics = metrics_for(&config(), &data, &order);
            assert_eq!(metrics.temporal_pairs_checked, 0, "{order:?}");
            assert_eq!(metrics.temporal_violations, 0, "{order:?}");
            assert!(metrics.compared_pairs.is_empty(), "{order:?}");
        }
    }

    #[test]
    fn each_start_pairs_with_the_end_that_shares_its_prefix() {
        // #913: `billing_start` used to be compared with `trip_end`, the first
        // end listed, and the billing pair was never checked.
        let dates = |values: [&str; 3]| values.map(str::to_string).to_vec();
        let data = HashMap::from([
            (
                "billing_start".to_string(),
                dates(["2024-06-01", "2024-07-01", "2024-08-01"]),
            ),
            (
                "trip_start".to_string(),
                dates(["2024-01-01", "2024-02-01", "2024-03-01"]),
            ),
            (
                "trip_end".to_string(),
                dates(["2024-01-05", "2024-02-05", "2024-03-05"]),
            ),
            (
                "billing_end".to_string(),
                dates(["2024-06-30", "2024-07-31", "2024-08-31"]),
            ),
        ]);

        for order in [
            ["billing_start", "trip_start", "trip_end", "billing_end"],
            ["trip_start", "trip_end", "billing_start", "billing_end"],
        ] {
            let metrics = metrics_for(&config(), &data, &order);
            assert_eq!(metrics.temporal_pairs_checked, 6, "{order:?}");
            assert_eq!(metrics.temporal_violations, 0, "{order:?}");
            let mut pairs = metrics.compared_pairs.clone();
            pairs.sort();
            assert_eq!(
                pairs,
                [
                    ("billing_start".to_string(), "billing_end".to_string()),
                    ("trip_start".to_string(), "trip_end".to_string()),
                ],
                "{order:?}"
            );
        }
    }

    #[test]
    fn a_start_with_two_fitting_ends_is_left_unpaired() {
        let data = HashMap::from([
            ("trip_start".to_string(), vec!["2024-01-05".to_string()]),
            ("trip_end".to_string(), vec!["2024-01-01".to_string()]),
            ("tripEnd".to_string(), vec!["2024-01-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["trip_start", "trip_end", "tripEnd"]);

        assert_eq!(metrics.temporal_pairs_checked, 0);
        assert_eq!(metrics.temporal_violations, 0);
    }

    #[test]
    fn a_trailing_date_word_does_not_keep_a_pair_apart() {
        // #941: the date word was one of the words compared, so with a second
        // pair present `trip_start_date` fit no end and was never checked.
        for [start, end] in [
            ["trip_start_date", "trip_end"],
            ["order_start_time", "order_end_date"],
            ["tripStartDate", "trip_enddate"],
        ] {
            let data = HashMap::from([
                (start.to_string(), vec!["2024-01-05".to_string()]),
                (end.to_string(), vec!["2024-01-01".to_string()]),
                ("billing_start".to_string(), vec!["2024-06-01".to_string()]),
                ("billing_end".to_string(), vec!["2024-06-30".to_string()]),
            ]);
            for order in [
                [start, end, "billing_start", "billing_end"],
                ["billing_end", "billing_start", end, start],
            ] {
                let metrics = metrics_for(&config(), &data, &order);
                assert_eq!(metrics.temporal_pairs_checked, 2, "{order:?}");
                assert_eq!(metrics.temporal_violations, 1, "{order:?}");
                let mut pairs = metrics.compared_pairs.clone();
                pairs.sort();
                let mut expected = vec![
                    (start.to_string(), end.to_string()),
                    ("billing_start".to_string(), "billing_end".to_string()),
                ];
                expected.sort();
                assert_eq!(pairs, expected, "{order:?}");
            }
        }
    }

    #[test]
    fn a_whole_name_match_wins_over_one_without_the_date_word() {
        // `trip_start` fits `trip_end` by its whole rest, so `trip_start_date`
        // cannot fall back to it.
        let data = HashMap::from([
            ("trip_start".to_string(), vec!["2024-01-05".to_string()]),
            (
                "trip_start_date".to_string(),
                vec!["2024-01-05".to_string()],
            ),
            ("trip_end".to_string(), vec!["2024-01-01".to_string()]),
            ("billing_start".to_string(), vec!["2024-06-01".to_string()]),
            ("billing_end".to_string(), vec!["2024-06-30".to_string()]),
        ]);
        let metrics = metrics_for(
            &config(),
            &data,
            &[
                "trip_start",
                "trip_start_date",
                "trip_end",
                "billing_start",
                "billing_end",
            ],
        );

        assert_eq!(
            metrics.compared_pairs,
            [
                ("trip_start".to_string(), "trip_end".to_string()),
                ("billing_start".to_string(), "billing_end".to_string()),
            ]
        );
        assert_eq!(metrics.temporal_violations, 1);
    }

    #[test]
    fn a_lone_start_keeps_its_end_beside_a_start_date() {
        let data = HashMap::from([
            ("start".to_string(), vec!["2024-01-05".to_string()]),
            ("start_date".to_string(), vec!["2024-01-01".to_string()]),
            ("end".to_string(), vec!["2024-01-01".to_string()]),
        ]);
        for order in [
            ["start", "start_date", "end"],
            ["end", "start_date", "start"],
        ] {
            let metrics = metrics_for(&config(), &data, &order);
            assert_eq!(
                metrics.compared_pairs,
                [("start".to_string(), "end".to_string())],
                "{order:?}"
            );
            assert_eq!(metrics.temporal_violations, 1, "{order:?}");
        }
    }

    #[test]
    fn date_and_time_pairs_are_both_checked() {
        // Without the date word all four leave nothing under `start`/`end`;
        // the whole rest still tells the date pair from the time pair.
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2024-01-01".to_string()]),
            ("start_time".to_string(), vec!["2024-01-05".to_string()]),
            ("end_date".to_string(), vec!["2024-01-05".to_string()]),
            ("end_time".to_string(), vec!["2024-01-01".to_string()]),
        ]);
        for order in [
            ["start_date", "start_time", "end_date", "end_time"],
            ["end_time", "end_date", "start_time", "start_date"],
        ] {
            let metrics = metrics_for(&config(), &data, &order);
            assert_eq!(metrics.temporal_pairs_checked, 2, "{order:?}");
            assert_eq!(metrics.temporal_violations, 1, "{order:?}");
            let mut pairs = metrics.compared_pairs.clone();
            pairs.sort();
            assert_eq!(
                pairs,
                [
                    ("start_date".to_string(), "end_date".to_string()),
                    ("start_time".to_string(), "end_time".to_string()),
                ],
                "{order:?}"
            );
        }
    }

    /// Metrics with `declared` pairs, over `temporal_columns` as the metrics
    /// pass them: hinted columns, then the columns of the declared pairs.
    fn metrics_with_pairs(
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[&str],
        declared: &[(&str, &str)],
    ) -> TimelinessMetrics {
        let named: Vec<String> = temporal_columns
            .iter()
            .map(|name| name.to_string())
            .collect();
        let declared: Vec<(String, String)> = declared
            .iter()
            .map(|(start, end)| (start.to_string(), end.to_string()))
            .collect();
        TimelinessCalculator::with_reference_date(&config(), reference())
            .calculate(data, &named, &declared, &whole(data))
            .expect("timeliness metrics")
    }

    fn sorted(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
        let mut pairs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(start, end)| (start.as_str(), end.as_str()))
            .collect();
        pairs.sort();
        pairs
    }

    #[test]
    fn a_declared_pair_is_compared_whatever_its_names() {
        // #942: no role word fits these names, so they were never compared.
        let data = HashMap::from([
            ("valid_from".to_string(), vec!["2024-01-05".to_string()]),
            ("valid_until".to_string(), vec!["2024-01-01".to_string()]),
            ("checkin".to_string(), vec!["2024-03-01".to_string()]),
            ("checkout".to_string(), vec!["2024-03-04".to_string()]),
        ]);
        let columns = ["valid_from", "valid_until", "checkin", "checkout"];

        let metrics = metrics_with_pairs(&data, &columns, &[]);
        assert!(metrics.compared_pairs.is_empty());

        let declared = [("valid_from", "valid_until"), ("checkin", "checkout")];
        let metrics = metrics_with_pairs(&data, &columns, &declared);
        assert_eq!(metrics.temporal_pairs_checked, 2);
        assert_eq!(metrics.temporal_violations, 1);
        assert_eq!(
            sorted(&metrics.compared_pairs),
            [("checkin", "checkout"), ("valid_from", "valid_until")]
        );
    }

    #[test]
    fn a_declared_pair_settles_names_that_are_ambiguous() {
        // The #913 case, which pairs nothing by name.
        let data = HashMap::from([
            ("primary_start".to_string(), vec!["2024-01-01".to_string()]),
            (
                "secondary_start".to_string(),
                vec!["2024-01-03".to_string()],
            ),
            ("end".to_string(), vec!["2024-01-02".to_string()]),
        ]);
        let metrics = metrics_with_pairs(
            &data,
            &["primary_start", "secondary_start", "end"],
            &[("secondary_start", "end")],
        );

        assert_eq!(metrics.temporal_pairs_checked, 1);
        assert_eq!(metrics.temporal_violations, 1);
        assert_eq!(
            sorted(&metrics.compared_pairs),
            [("secondary_start", "end")]
        );
    }

    #[test]
    fn columns_outside_the_declared_pairs_still_pair_by_name() {
        let data = HashMap::from([
            ("valid_from".to_string(), vec!["2024-01-01".to_string()]),
            ("valid_until".to_string(), vec!["2024-01-05".to_string()]),
            ("billing_start".to_string(), vec!["2024-06-30".to_string()]),
            ("billing_end".to_string(), vec!["2024-06-01".to_string()]),
        ]);
        let metrics = metrics_with_pairs(
            &data,
            &["billing_start", "billing_end", "valid_from", "valid_until"],
            &[("valid_from", "valid_until")],
        );

        assert_eq!(metrics.temporal_pairs_checked, 2);
        assert_eq!(metrics.temporal_violations, 1);
        assert_eq!(
            sorted(&metrics.compared_pairs),
            [
                ("billing_start", "billing_end"),
                ("valid_from", "valid_until")
            ]
        );
    }

    #[test]
    fn a_column_in_a_declared_pair_is_not_paired_again_by_name() {
        // `start` and `end` would pair by name; declaring `start` with
        // `finish` says which end it has.
        let data = HashMap::from([
            ("start".to_string(), vec!["2024-01-03".to_string()]),
            ("end".to_string(), vec!["2024-01-01".to_string()]),
            ("finish".to_string(), vec!["2024-01-05".to_string()]),
        ]);
        let metrics =
            metrics_with_pairs(&data, &["start", "end", "finish"], &[("start", "finish")]);

        assert_eq!(metrics.temporal_pairs_checked, 1);
        assert_eq!(metrics.temporal_violations, 0);
        assert_eq!(sorted(&metrics.compared_pairs), [("start", "finish")]);
    }

    // ---------------------------------------------------------------- #378
    // Deterministic coverage for the timeliness calculator. Every case pins
    // "now" through `with_reference_date`, so no assertion here changes result
    // as the calendar advances.

    /// A fixed "today" for every case below.
    fn reference() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 15).expect("valid reference date")
    }

    /// Default freshness policy is 5 years, so the stale cutoff is 2021.
    fn config() -> IsoQualityConfig {
        IsoQualityConfig::default()
    }

    fn column(name: &str, values: &[&str]) -> HashMap<String, Vec<String>> {
        HashMap::from([(
            name.to_string(),
            values.iter().map(|value| value.to_string()).collect(),
        )])
    }

    fn metrics_for(
        thresholds: &IsoQualityConfig,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[&str],
    ) -> TimelinessMetrics {
        let named: Vec<String> = temporal_columns
            .iter()
            .map(|name| name.to_string())
            .collect();
        TimelinessCalculator::with_reference_date(thresholds, reference())
            .calculate_whole(data, &named)
            .expect("timeliness metrics")
    }

    #[test]
    fn no_date_columns_assesses_nothing() {
        let data = column("label", &["alpha", "beta"]);
        let metrics = metrics_for(&config(), &data, &[]);

        assert_eq!(metrics.date_values_checked, 0);
        assert_eq!(metrics.invalid_date_values, 0);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
        assert_eq!(metrics.temporal_pairs_checked, 0);
        assert_eq!(metrics.temporal_violations, 0);
    }

    #[test]
    fn empty_column_assesses_nothing() {
        let data = column("event_date", &[]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.date_values_checked, 0);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn single_recent_value_is_neither_future_nor_stale() {
        let data = column("event_date", &["2026-06-14"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.date_values_checked, 1);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn past_dates_within_the_freshness_window_are_clean() {
        let data = column("event_date", &["2022-01-01", "2024-06-01", "2026-06-14"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.date_values_checked, 3);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn future_dates_inside_the_current_year_are_counted() {
        // Regression: comparing years alone reported 0 future dates for every
        // value between today and 31 December.
        let data = column(
            "event_date",
            &[
                "2026-06-16", // tomorrow
                "2026-07-01", // later the same year
                "2026-12-31", // last day of the same year
                "2027-01-01", // next year, caught even by the year-only check
            ],
        );
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(
            metrics.future_dates_count, 4,
            "every value after the reference date is in the future"
        );
        assert_eq!(metrics.date_values_checked, 4);
    }

    #[test]
    fn one_future_date_among_past_ones_is_counted_once() {
        let data = column("event_date", &["2026-06-14", "2026-06-16", "2025-01-01"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.future_dates_count, 1);
    }

    #[test]
    fn the_reference_date_itself_is_not_future() {
        let data = column("event_date", &["2026-06-15"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.future_dates_count, 0, "today is not in the future");
    }

    #[test]
    fn values_beyond_the_stale_threshold_are_reported_as_a_percentage() {
        // Reference year 2026 and a 5-year policy put the cutoff at 2021.
        let data = column(
            "event_date",
            &["2019-01-01", "2020-12-31", "2021-01-01", "2026-01-01"],
        );
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(
            metrics.stale_data_ratio, 75.0,
            "three of four values fall before the whole-date cutoff"
        );
    }

    #[test]
    fn a_shorter_freshness_policy_makes_more_values_stale() {
        let mut thresholds = config();
        thresholds.max_data_age_years = 2.0; // cutoff 2024
        let data = column("event_date", &["2022-01-01", "2023-01-01", "2025-01-01"]);
        let metrics = metrics_for(&thresholds, &data, &["event_date"]);

        assert!(
            (metrics.stale_data_ratio - (2.0 / 3.0 * 100.0)).abs() < 1e-9,
            "expected two of three stale, got {}",
            metrics.stale_data_ratio
        );
    }

    #[test]
    fn fractional_years_change_the_whole_date_cutoff() {
        let data = column("event_date", &["2024-01-01", "2024-06-15", "2026-06-15"]);
        let mut thresholds = config();
        thresholds.max_data_age_years = 2.0;
        let whole = metrics_for(&thresholds, &data, &["event_date"]);
        thresholds.max_data_age_years = 2.5;
        let fractional = metrics_for(&thresholds, &data, &["event_date"]);
        assert!((whole.stale_data_ratio - 100.0 / 3.0).abs() < 1e-12);
        assert_eq!(fractional.stale_data_ratio, 0.0);
    }

    #[test]
    fn ordered_temporal_values_report_no_violation() {
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2023-01-01".to_string()]),
            ("end_date".to_string(), vec!["2023-06-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_violations, 0);
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn out_of_order_temporal_values_report_a_violation() {
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2023-06-01".to_string()]),
            ("end_date".to_string(), vec!["2023-01-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_violations, 1);
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn day_first_dates_are_ordered_by_calendar_not_by_text() {
        // Regression: `start_val > end_val` compared raw strings. `DD/MM/YYYY`
        // sorts by day first, so a real inversion read as clean.
        let data = HashMap::from([
            ("start_date".to_string(), vec!["15/01/2023".to_string()]),
            ("end_date".to_string(), vec!["20/03/2022".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(
            metrics.temporal_violations, 1,
            "January 2023 starts after March 2022 ends"
        );
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn day_first_dates_in_valid_order_report_no_violation() {
        // The same bug in the other direction: text ordering invented a
        // violation for a correctly ordered pair.
        let data = HashMap::from([
            ("start_date".to_string(), vec!["20/03/2022".to_string()]),
            ("end_date".to_string(), vec!["15/01/2023".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(
            metrics.temporal_violations, 0,
            "March 2022 to January 2023 runs forwards"
        );
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn a_month_first_column_dates_its_ambiguous_values_month_first() {
        // #811: `12/31/2025` makes the column month-first, so `07/01/2026` is
        // 1 July, after the 15 June reference. Read day-first, as each value
        // used to be, it was 7 January and not in the future.
        let data = column("event_date", &["12/31/2025", "07/01/2026"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.future_dates_count, 1);
        assert_eq!(metrics.date_values_checked, 2);
        assert_eq!(metrics.invalid_date_values, 0);
    }

    #[test]
    fn month_first_pairs_are_compared_in_their_own_order() {
        // #811: both columns are month-first and every pair runs forwards.
        // Value by value, `01/10/2026` became 1 October and `02/01/2026`
        // became 2 January, inventing a violation.
        let data = HashMap::from([
            (
                "start_date".to_string(),
                vec!["12/31/2025".to_string(), "01/10/2026".to_string()],
            ),
            (
                "end_date".to_string(),
                vec!["01/13/2026".to_string(), "02/01/2026".to_string()],
            ),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_violations, 0);
        assert_eq!(metrics.temporal_pairs_checked, 2);
    }

    #[test]
    fn overlapping_role_patterns_evaluate_a_column_pair_once() {
        // Regression: `start_date`/`end_date` matched both the
        // ("start_date", "end_date") and ("start", "end") patterns, so every
        // row of the pair was compared and counted twice.
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2023-05-01".to_string()]),
            ("end_date".to_string(), vec!["2023-04-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(
            metrics.temporal_pairs_checked, 1,
            "one row of one column pair is one comparison"
        );
        assert_eq!(metrics.temporal_violations, 1);
    }

    #[test]
    fn null_tokens_in_a_pair_are_skipped_rather_than_compared() {
        let data = HashMap::from([
            (
                "start_date".to_string(),
                vec!["2023-06-01".to_string(), "".to_string()],
            ),
            (
                "end_date".to_string(),
                vec!["2023-01-01".to_string(), "2023-01-01".to_string()],
            ),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_pairs_checked, 1, "only one complete pair");
        assert_eq!(metrics.temporal_violations, 1);
    }

    #[test]
    fn unparseable_values_are_invalid_rather_than_future_or_stale() {
        // Absence rule: a value that is not a date must not be silently read as
        // a valid timestamp in either direction.
        let data = column("event_date", &["2024-13-45", "not a date", "2026-06-14"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(
            metrics.date_values_checked, 3,
            "the denominator is complete"
        );
        assert_eq!(metrics.invalid_date_values, 2);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn invalid_values_do_not_form_temporal_pairs() {
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2024-13-45".to_string()]),
            ("end_date".to_string(), vec!["2023-01-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_pairs_checked, 0);
        assert_eq!(metrics.temporal_violations, 0);
    }

    #[test]
    fn a_named_column_absent_from_the_data_does_not_hide_a_later_pair() {
        // A temporal column the caller named but the data does not carry must
        // not stop the search: the next name matching the same role, and
        // present, is still a valid pair.
        let data = HashMap::from([
            ("primary_start".to_string(), vec!["2023-06-01".to_string()]),
            ("end".to_string(), vec!["2023-01-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["missing_start", "primary_start", "end"]);

        assert_eq!(metrics.temporal_pairs_checked, 1);
        assert_eq!(metrics.temporal_violations, 1);
    }

    #[test]
    fn a_role_matches_words_of_the_name_not_parts_of_a_word() {
        let holds = [
            ("start_date", "start"),
            ("startDate", "start"),
            ("StartDate", "start"),
            ("startdate", "start"),
            ("starts_at", "start"),
            ("started_at", "start"),
            ("ends_at", "end"),
            ("ended_at", "end"),
            ("ENDDATE", "end"),
            ("createdAt", "created"),
            ("record_created_at", "created_at"),
            ("valid_to_date", "to_date"),
            ("orderStartDate", "start_date"),
        ];
        let misses = [
            ("send_date", "end"),
            ("weekend_date", "end"),
            ("vendor_date", "end"),
            ("endorsed_date", "end"),
            ("restart_date", "start"),
            ("photo_date", "to_date"),
            ("startup_date", "start"),
            ("start_time", "start_date"),
        ];
        let wrong: Vec<_> = holds
            .iter()
            .filter(|(name, role)| !holds_role(name, role))
            .map(|(name, role)| format!("{name} should hold {role}"))
            .chain(
                misses
                    .iter()
                    .filter(|(name, role)| holds_role(name, role))
                    .map(|(name, role)| format!("{name} should not hold {role}")),
            )
            .collect();
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// 17 `start_*` and 16 `end_*x` names: 272 unrelated combinations, past
    /// `LONE_PAIR_CARTESIAN_CAP`, none sharing a stem.
    fn wide_schema() -> Vec<String> {
        ('a'..='q')
            .map(|c| format!("start_{c}"))
            .chain(('b'..='q').map(|c| format!("end_{c}x")))
            .collect()
    }

    fn date(text: &str) -> Option<DualDate> {
        crate::stats::datetime::parse_dual_date(text)
    }

    #[test]
    fn a_merge_never_takes_a_pair_the_combined_schema_does_not_admit() {
        // The narrow partition tracks the lone pair and counts one row; the
        // wide one is past the cap and never tracks it. In either merge
        // order the pair must be absent, so timeliness falls back to the
        // sample instead of reporting the narrow side's rows as exact.
        let pair = ("start_a".to_string(), "end_bx".to_string());
        let mut narrow = TemporalPairTracker::new(&[]);
        narrow.register_columns(&[pair.0.clone(), pair.1.clone()]);
        narrow.record_date(&pair.0, date("2024-03-10"));
        narrow.record_date(&pair.1, date("2024-03-01"));
        narrow.finish_row();
        assert_eq!(narrow.finish()[&pair].pairs_checked, 1);
        let mut wide = TemporalPairTracker::new(&[]);
        wide.register_columns(&wide_schema());
        assert!(!wide.finish().contains_key(&pair));

        let mut wide_first = wide.clone();
        wide_first.merge(&narrow);
        assert!(!wide_first.finish().contains_key(&pair));
        let mut narrow_first = narrow.clone();
        narrow_first.merge(&wide);
        assert!(!narrow_first.finish().contains_key(&pair));
    }

    #[test]
    fn a_merge_adds_the_counts_of_a_pair_both_sides_admit() {
        let pair = ("start_date".to_string(), "end_date".to_string());
        let names = [pair.0.clone(), pair.1.clone()];
        let mut left = TemporalPairTracker::new(&[]);
        left.register_columns(&names);
        left.record_date(&pair.0, date("2024-03-10"));
        left.record_date(&pair.1, date("2024-03-01"));
        left.finish_row();
        // A partition that never saw `end_date` read only nulls for it.
        let mut right = TemporalPairTracker::new(&[]);
        right.register_columns(&names[..1]);
        right.finish_row();
        let mut other = TemporalPairTracker::new(&[]);
        other.register_columns(&names);
        other.record_date(&pair.0, date("2024-03-01"));
        other.record_date(&pair.1, date("2024-03-10"));
        other.finish_row();

        left.merge(&right);
        left.merge(&other);
        let counts = left.finish()[&pair];
        assert_eq!(counts.pairs_checked, 2);
        assert_eq!(counts.violations_for(false, false), 1);
    }

    #[test]
    fn a_row_buffer_is_cleared_after_its_last_pair_is_dropped() {
        // `start_a` holds a date when the cap drops every pair. A later
        // same-stem `end_a` pairs with `start_a` again: it must not see the
        // earlier row's date.
        let mut tracker = TemporalPairTracker::new(&[]);
        tracker.register_columns(&["start_a".to_string(), "end_bx".to_string()]);
        tracker.record_date("start_a", date("2024-03-10"));
        tracker.register_columns(&wide_schema());
        assert!(!tracker.is_enabled());
        tracker.finish_row();
        tracker.register_column("end_a");
        assert!(tracker.is_enabled());
        tracker.record_date("end_a", date("2024-03-01"));
        tracker.finish_row();
        let counts = tracker.finish()[&("start_a".to_string(), "end_a".to_string())];
        assert_eq!(counts.pairs_checked, 0);
    }
}
