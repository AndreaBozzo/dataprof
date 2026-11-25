//! Bounded repeat counts for explicitly requested groups of columns.

use std::collections::{HashMap, hash_map::Entry};

#[cfg(feature = "arrow")]
use arrow_array::Array;
use dataprof_core::{AnalysisOptions, DataProfilerError};
use twox_hash::XxHash3_128;

#[cfg(feature = "arrow")]
use super::canonical_value::encode_arrow;
use super::canonical_value::{NULL_TAG, encode_text, sample_value};

const SAMPLE_LIMIT: usize = 5;
const LENGTH_BYTES: usize = size_of::<u64>();

/// Duplicate evidence for one ordered group of key columns.
#[derive(
    Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub struct CompositeKeySummary {
    /// Column names in tuple order.
    pub columns: Vec<String>,
    /// Rows with at least one non-null key cell.
    pub rows_counted: usize,
    /// Rows skipped because every key cell is null.
    pub rows_skipped_all_null: usize,
    /// Distinct keys observed, or a lower bound once the limit is exceeded.
    pub distinct_keys: usize,
    /// Extra occurrences after the first occurrence of a key.
    /// A lower bound once the limit is exceeded.
    pub repeated_rows: usize,
    /// Keys seen at least twice. A lower bound once the limit is exceeded.
    pub distinct_repeated_keys: usize,
    /// Whether all the counted rows fit the distinct limit.
    /// Counts depend on 128-bit hash equality.
    pub is_exact: bool,
    /// Up to five repeated keys in canonical form, with null cells preserved.
    pub sample_repeated_keys: Vec<Vec<Option<String>>>,
}

/// Reuses one buffer per requested column and one for the framed key.
/// Only the requested columns accumulate evidence.
#[derive(Default)]
pub struct CompositeKeyTracker {
    fields: HashMap<String, usize>,
    values: Vec<Vec<u8>>,
    keys: Vec<Key>,
    frame: Vec<u8>,
    limit: usize,
    unsupported_field: Option<String>,
}

struct Key {
    columns: Vec<String>,
    fields: Vec<usize>,
    counted: usize,
    skipped: usize,
    occurrences: HashMap<u128, usize>,
    overflowed: bool,
    samples: Vec<Vec<u8>>,
}

impl CompositeKeyTracker {
    /// Prepare the groups before the first row. Column names may be discovered
    /// later, as in JSON streams.
    pub fn new(options: &AnalysisOptions) -> Self {
        let mut tracker = Self {
            limit: options.unique_keys_exact_limit(),
            ..Self::default()
        };
        for columns in options.unique_keys() {
            let fields = columns
                .iter()
                .map(|name| {
                    let next = tracker.fields.len();
                    *tracker.fields.entry(name.clone()).or_insert(next)
                })
                .collect();
            tracker.keys.push(Key {
                columns: columns.clone(),
                fields,
                counted: 0,
                skipped: 0,
                occurrences: HashMap::new(),
                overflowed: false,
                samples: Vec::new(),
            });
        }
        tracker.values.resize_with(tracker.fields.len(), Vec::new);
        tracker
    }

    /// True when the profile has requested this column in any key.
    pub fn contains(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// True when at least one group was requested.
    pub fn is_enabled(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Record a text cell. A missing field in the row is implicitly null.
    pub fn text_field(&mut self, name: &str, value: Option<&str>) {
        if let Some(&index) = self.fields.get(name) {
            self.values[index].clear();
            encode_text(value, &mut self.values[index]);
        }
    }

    #[cfg(feature = "arrow")]
    /// Record an Arrow cell whose type has already been validated.
    pub fn arrow_field(
        &mut self,
        name: &str,
        array: &dyn Array,
        row: usize,
    ) -> Result<(), DataProfilerError> {
        if let Some(&index) = self.fields.get(name) {
            self.values[index].clear();
            encode_arrow(array, row, &mut self.values[index]).map_err(|error| {
                DataProfilerError::invalid_config(
                    &format!("unique_keys cannot read column {name:?}: {error}"),
                    "Choose scalar key columns.",
                )
            })?;
        }
        Ok(())
    }

    /// Record that the source supplied a value without a scalar encoding.
    pub fn unsupported_field(&mut self, name: &str) {
        if self.unsupported_field.is_none() && self.contains(name) {
            self.unsupported_field = Some(name.to_owned());
        }
    }

    /// Count the row and clear its cells. The tag and byte length of each
    /// payload delimit values even when text and binary contain separators.
    pub fn finish_row(&mut self) {
        for key in &mut self.keys {
            let mut all_null = true;
            self.frame.clear();
            for &index in &key.fields {
                let cell = self.values[index].as_slice();
                let (tag, payload) = cell.split_first().unwrap_or((&NULL_TAG, &[]));
                all_null &= *tag == NULL_TAG;
                self.frame.push(*tag);
                self.frame
                    .extend_from_slice(&(payload.len() as u64).to_be_bytes());
                self.frame.extend_from_slice(payload);
            }
            if all_null {
                key.skipped += 1;
            } else {
                key.counted += 1;
                key.observe(&self.frame, self.limit);
            }
        }
        for value in &mut self.values {
            value.clear();
        }
    }

    /// Add evidence from another partition with the same configured groups.
    /// Samples are kept from repeats witnessed within each partition.
    pub fn merge(&mut self, other: Self) {
        if self.unsupported_field.is_none() {
            self.unsupported_field = other.unsupported_field;
        }
        for (own, incoming) in self.keys.iter_mut().zip(other.keys) {
            debug_assert_eq!(own.columns, incoming.columns);
            own.counted += incoming.counted;
            own.skipped += incoming.skipped;
            own.overflowed |= incoming.overflowed;
            for (digest, count) in incoming.occurrences {
                let room = own.occurrences.len() < self.limit;
                match own.occurrences.entry(digest) {
                    Entry::Occupied(mut entry) => *entry.get_mut() += count,
                    Entry::Vacant(entry) if room => {
                        entry.insert(count);
                    }
                    Entry::Vacant(_) => own.overflowed = true,
                }
            }
            for sample in incoming.samples {
                own.sample(&sample);
            }
        }
    }

    /// Summarize each requested group, including groups on empty input.
    pub fn summaries(&self) -> Result<Vec<CompositeKeySummary>, DataProfilerError> {
        if let Some(name) = &self.unsupported_field {
            return Err(DataProfilerError::invalid_config(
                &format!("unique_keys names column {name:?} with an unsupported value type"),
                "Choose scalar key columns.",
            ));
        }
        Ok(self.keys.iter().map(Key::summary).collect())
    }
}

impl Key {
    fn observe(&mut self, frame: &[u8], limit: usize) {
        let digest = XxHash3_128::oneshot(frame);
        let room = self.occurrences.len() < limit;
        match self.occurrences.entry(digest) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
                if *entry.get() == 2 {
                    self.sample(frame);
                }
            }
            Entry::Vacant(entry) if room => {
                entry.insert(1);
            }
            Entry::Vacant(_) => self.overflowed = true,
        }
    }

    fn sample(&mut self, frame: &[u8]) {
        if let Err(index) = self
            .samples
            .binary_search_by(|sample| sample.as_slice().cmp(frame))
            && index < SAMPLE_LIMIT
        {
            if self.samples.len() == SAMPLE_LIMIT {
                self.samples.pop();
            }
            self.samples.insert(index, frame.to_vec());
        }
    }

    fn summary(&self) -> CompositeKeySummary {
        let (repeated_rows, distinct_repeated_keys) = self
            .occurrences
            .values()
            .fold((0, 0), |(rows, distinct), &count| {
                (rows + count - 1, distinct + usize::from(count > 1))
            });
        CompositeKeySummary {
            columns: self.columns.clone(),
            rows_counted: self.counted,
            rows_skipped_all_null: self.skipped,
            distinct_keys: self.occurrences.len(),
            repeated_rows,
            distinct_repeated_keys,
            is_exact: !self.overflowed,
            sample_repeated_keys: self
                .samples
                .iter()
                .map(|frame| read_sample(frame))
                .collect(),
        }
    }
}

fn read_sample(mut frame: &[u8]) -> Vec<Option<String>> {
    let mut sample = Vec::new();
    let mut cell = Vec::new();
    while let Some((tag, tail)) = frame.split_first() {
        let (length, tail) = tail.split_at(LENGTH_BYTES);
        let length = u64::from_be_bytes(length.try_into().expect("key length width")) as usize;
        let (payload, rest) = tail.split_at(length);
        cell.clear();
        cell.push(*tag);
        cell.extend_from_slice(payload);
        sample.push(sample_value(&cell));
        frame = rest;
    }
    sample
}

/// Settings recorded for key comparisons. Empty requests omit this setting.
#[derive(
    Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub struct UniqueKeySettings {
    /// Requested ordered groups.
    pub groups: Vec<Vec<String>>,
    /// Retained distinct keys per group.
    pub exact_limit: usize,
}

impl Default for UniqueKeySettings {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            exact_limit: AnalysisOptions::default().unique_keys_exact_limit(),
        }
    }
}

impl UniqueKeySettings {
    /// Record the effective settings, including the default limit.
    pub fn from_options(options: &AnalysisOptions) -> Option<Self> {
        (!options.unique_keys().is_empty()).then(|| Self {
            groups: options.unique_keys().to_vec(),
            exact_limit: options.unique_keys_exact_limit(),
        })
    }
}
