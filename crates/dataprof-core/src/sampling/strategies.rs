use std::collections::HashMap;
use std::fmt::Write;

/// How rows are selected for profiling.
///
/// Strategies divide into two families, which differ in cost. *Streaming*
/// strategies decide each row as it arrives and add no memory. *Fixed-size*
/// strategies — [`Random`](Self::Random) and [`Reservoir`](Self::Reservoir) —
/// cannot know the final sample until the source ends, so they hold `size` rows
/// in memory and the profile is computed from that buffer.
///
/// Every strategy is applied by `RowSampler`, which carries the state they need
/// across rows.
#[derive(Debug, Clone)]
pub enum SamplingStrategy {
    /// No sampling - analyze all data
    None,

    /// A uniform random sample of exactly `size` rows (or every row, when the
    /// source is shorter).
    ///
    /// Over a source of unknown length this is reservoir sampling, so it holds
    /// `size` rows in memory and is equivalent to
    /// [`Reservoir`](Self::Reservoir); both names are kept because both are
    /// familiar.
    Random { size: usize },

    /// A uniform random sample of exactly `size` rows, selected with Algorithm
    /// R over a single pass.
    ///
    /// Holds `size` rows in memory: membership is not final until the source
    /// ends, and statistics folded in earlier could not be retracted.
    Reservoir { size: usize },

    /// Up to `samples_per_stratum` rows for each distinct combination of
    /// `key_columns` observed in the data.
    ///
    /// Strata are discovered as rows arrive, so the sample size grows with the
    /// number of distinct keys rather than being fixed up front.
    Stratified {
        key_columns: Vec<String>,
        samples_per_stratum: usize,
    },

    /// Grow the sample until the mean of every numeric column is precise
    /// enough, bounded by `initial_size` and `max_size`.
    ///
    /// `confidence_level` sets a target *relative standard error* of
    /// `1.0 - confidence_level`: at `0.95`, sampling stops once the standard
    /// error of each numeric column's mean is within 5% of that mean. Precision
    /// is measured from the sampled data, so a low-variance column stops early
    /// and a volatile one runs to `max_size`.
    ///
    /// A source with no numeric columns has no measurable precision, so it
    /// always samples `max_size` rows.
    Progressive {
        initial_size: usize,
        confidence_level: f64,
        max_size: usize,
    },

    /// Every `interval`-th row, starting at the first.
    Systematic { interval: usize },

    /// Rows whose `weight_column` holds a number at or above
    /// `weight_threshold`.
    ///
    /// The caller states what matters: there is no built-in notion of an
    /// important row. Rows where the column is absent or unparseable are
    /// excluded. Note that this is a filter, not a probability-weighted sample —
    /// the resulting profile describes the rows that met the threshold, not the
    /// source as a whole.
    Importance {
        weight_column: String,
        weight_threshold: f64,
    },

    /// Several strategies applied in order.
    ///
    /// Streaming stages act as filters that a row must pass in sequence. At
    /// most one fixed-size stage may appear, and it must be last, since it
    /// draws its sample from whatever the filters let through.
    MultiStage { stages: Vec<SamplingStrategy> },
}

/// The cross-row state that stateful strategies need.
///
/// One instance lives for a whole scan. Recreating it per row — which is what
/// a stateless helper amounts to — makes every stateful strategy behave as if
/// each row were the first, which is how stratified sampling came to return
/// nothing and reservoir sampling came to return everything.
#[derive(Debug, Default)]
pub struct SamplingState {
    /// Rows already taken by a `Progressive` stage.
    progressive_samples: usize,

    /// Rows already taken from each observed stratum.
    stratum_samples: HashMap<String, usize>,
}

impl SamplingState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rows a `Progressive` stage has taken so far.
    pub fn progressive_taken(&self) -> usize {
        self.progressive_samples
    }

    /// Record that a `Progressive` stage took a row.
    pub fn record_progressive(&mut self) {
        self.progressive_samples += 1;
    }

    /// Take `row` for its stratum if that stratum still has room.
    ///
    /// The stratum key is the joined values of `key_columns`. A row missing any
    /// key column belongs to no stratum and is not sampled — silently folding
    /// it into a partial key would merge unrelated rows into one stratum.
    pub fn take_from_stratum(
        &mut self,
        row: super::sampler::RowView<'_>,
        key_columns: &[String],
        samples_per_stratum: usize,
    ) -> bool {
        if key_columns.is_empty() {
            return false;
        }

        // Length-prefixed so field boundaries are unambiguous: joining on a
        // separator would merge ("a|b", "c") and ("a", "b|c") into one stratum
        // and apply the cap to both together.
        let mut stratum = String::new();
        for column in key_columns {
            let Some(value) = row.get(column) else {
                return false;
            };
            let _ = write!(stratum, "{}:{}", value.len(), value);
        }

        let taken = self.stratum_samples.entry(stratum).or_insert(0);
        if *taken < samples_per_stratum {
            *taken += 1;
            true
        } else {
            false
        }
    }

    /// Number of distinct strata observed so far.
    pub fn strata_seen(&self) -> usize {
        self.stratum_samples.len()
    }
}

impl SamplingStrategy {
    /// Create adaptive strategy based on data characteristics
    pub fn adaptive(total_rows: Option<usize>, file_size_mb: f64) -> Self {
        match (total_rows, file_size_mb) {
            (Some(rows), size_mb) if rows <= 10_000 && size_mb < 10.0 => SamplingStrategy::None,
            (Some(rows), _) if rows <= 100_000 => SamplingStrategy::Random { size: 10_000 },
            (Some(rows), _) if rows <= 1_000_000 => SamplingStrategy::Progressive {
                initial_size: 10_000,
                confidence_level: 0.95,
                max_size: 50_000,
            },
            (_, size_mb) if size_mb > 1000.0 => SamplingStrategy::MultiStage {
                stages: vec![
                    SamplingStrategy::Systematic { interval: 100 },
                    SamplingStrategy::Progressive {
                        initial_size: 5_000,
                        confidence_level: 0.99,
                        max_size: 25_000,
                    },
                ],
            },
            _ => SamplingStrategy::Reservoir { size: 100_000 },
        }
    }

    /// Create stratified sampling strategy
    pub fn stratified(key_columns: Vec<String>, samples_per_stratum: usize) -> Self {
        Self::Stratified {
            key_columns,
            samples_per_stratum,
        }
    }

    /// Create an importance filter over a caller-nominated weight column.
    pub fn importance(weight_column: impl Into<String>, weight_threshold: f64) -> Self {
        Self::Importance {
            weight_column: weight_column.into(),
            weight_threshold,
        }
    }

    pub fn target_sample_size(&self) -> Option<usize> {
        match self {
            SamplingStrategy::None => None,
            SamplingStrategy::Random { size } => Some(*size),
            SamplingStrategy::Reservoir { size } => Some(*size),
            SamplingStrategy::Stratified {
                samples_per_stratum,
                ..
            } => Some(*samples_per_stratum),
            SamplingStrategy::Progressive { max_size, .. } => Some(*max_size),
            SamplingStrategy::Systematic { .. } => None,
            SamplingStrategy::Importance { .. } => None,
            SamplingStrategy::MultiStage { stages } => {
                // Return the minimum target size across all stages
                stages.iter().filter_map(|s| s.target_sample_size()).min()
            }
        }
    }

    /// Get description of the sampling strategy
    pub fn description(&self) -> String {
        match self {
            SamplingStrategy::None => "Full dataset analysis".to_string(),
            SamplingStrategy::Random { size } => format!("Random sampling ({} records)", size),
            SamplingStrategy::Reservoir { size } => {
                format!("Reservoir sampling ({} records)", size)
            }
            SamplingStrategy::Stratified {
                key_columns,
                samples_per_stratum,
            } => {
                format!(
                    "Stratified by {} ({} per stratum)",
                    key_columns.join(", "),
                    samples_per_stratum
                )
            }
            SamplingStrategy::Progressive {
                initial_size,
                confidence_level,
                max_size,
            } => {
                format!(
                    "Progressive sampling ({}-{} records, {}% confidence)",
                    initial_size,
                    max_size,
                    (confidence_level * 100.0) as u8
                )
            }
            SamplingStrategy::Systematic { interval } => {
                format!("Systematic (every {}th record)", interval)
            }
            SamplingStrategy::Importance {
                weight_column,
                weight_threshold,
            } => {
                format!("Importance filter ({weight_column} >= {weight_threshold:.2})")
            }
            SamplingStrategy::MultiStage { stages } => {
                format!("Multi-stage ({} stages)", stages.len())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sampling::sampler::RowView;

    // Behavioural coverage of each strategy lives with the runtime that
    // applies them, in `sampler.rs`; a strategy on its own is only data.

    #[test]
    fn test_stratum_state_persists_across_rows() {
        let headers = vec!["region".to_string()];
        let mut state = SamplingState::new();
        let keys = vec!["region".to_string()];

        let north = vec!["north".to_string()];
        let south = vec!["south".to_string()];

        assert!(state.take_from_stratum(RowView::new(&headers, &north), &keys, 2));
        assert!(state.take_from_stratum(RowView::new(&headers, &north), &keys, 2));
        assert!(
            !state.take_from_stratum(RowView::new(&headers, &north), &keys, 2),
            "the third northern row exceeds the per-stratum cap"
        );
        assert!(
            state.take_from_stratum(RowView::new(&headers, &south), &keys, 2),
            "a different stratum has its own budget"
        );
        assert_eq!(state.strata_seen(), 2);
    }

    #[test]
    fn test_stratum_requires_every_key_column() {
        let headers = vec!["region".to_string()];
        let values = vec!["north".to_string()];
        let mut state = SamplingState::new();
        let keys = vec!["region".to_string(), "segment".to_string()];

        assert!(
            !state.take_from_stratum(RowView::new(&headers, &values), &keys, 5),
            "a row missing a key column belongs to no stratum"
        );
        assert_eq!(state.strata_seen(), 0);
    }

    #[test]
    fn test_importance_constructor_names_its_column() {
        let strategy = SamplingStrategy::importance("risk", 0.8);
        match strategy {
            SamplingStrategy::Importance {
                ref weight_column,
                weight_threshold,
            } => {
                assert_eq!(weight_column, "risk");
                assert_eq!(weight_threshold, 0.8);
            }
            other => panic!("expected an importance filter, got {other:?}"),
        }
        assert!(strategy.description().contains("risk"));
    }

    #[test]
    fn test_adaptive_strategy() {
        // Small dataset - should use no sampling
        let small = SamplingStrategy::adaptive(Some(5_000), 1.0);
        matches!(small, SamplingStrategy::None);

        // Medium dataset - should use random sampling
        let medium = SamplingStrategy::adaptive(Some(50_000), 10.0);
        matches!(medium, SamplingStrategy::Random { .. });

        // Large file - should use multi-stage
        let large = SamplingStrategy::adaptive(Some(10_000_000), 2000.0);
        matches!(large, SamplingStrategy::MultiStage { .. });
    }
}
