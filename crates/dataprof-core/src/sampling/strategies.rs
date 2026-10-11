/// How rows are selected for profiling.
///
/// Strategies divide into two families, which differ in cost.
/// [`Systematic`](Self::Systematic) decides each row as it arrives and adds no
/// memory. [`Random`](Self::Random) and [`Reservoir`](Self::Reservoir) cannot
/// know the final sample until the source ends, so they hold `size` rows in
/// memory and the profile is computed from that buffer.
///
/// Every strategy is applied by `RowSampler`.
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

    /// Every `interval`-th row, starting at the first.
    Systematic { interval: usize },
}

impl SamplingStrategy {
    /// Get description of the sampling strategy
    pub fn description(&self) -> String {
        match self {
            SamplingStrategy::None => "Full dataset analysis".to_string(),
            SamplingStrategy::Random { size } => format!("Random sampling ({} records)", size),
            SamplingStrategy::Reservoir { size } => {
                format!("Reservoir sampling ({} records)", size)
            }
            SamplingStrategy::Systematic { interval } => {
                format!("Systematic (every {}th record)", interval)
            }
        }
    }
}
