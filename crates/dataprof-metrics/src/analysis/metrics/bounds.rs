//! Confidence bounds on quality scores computed over a retained sample.
//!
//! A streaming run keeps a uniform reservoir sample of each column's
//! non-null values, and the sampled dimensions are computed from it. The
//! score a sample gives is not the score of the whole source, so a decision
//! about the whole source needs to know how far apart the two can be.
//!
//! Every sampled dimension score is `100 - 100 * sum(share)` over one or more
//! shares, each a count of failing values over a count of checked values,
//! summed over columns. The checks themselves are fixed by the sample: a
//! column's type, its dominant lexical form and decimal scale, its detected
//! pattern and its outlier fences all come from the values retained. The
//! bounds cover the share of *all* the source's values that fail those same
//! checks.
//!
//! For each column, the failing and checked values per sampled value are
//! means of bounded variables over a simple random sample drawn without
//! replacement from the column's non-null values. The Chernoff bound in its
//! Kullback-Leibler form (Hoeffding 1963, Theorem 1) holds for such means,
//! and it holds for sampling without replacement too (ibid., Theorem 4).
//! Every mean gets a two-sided interval at a share of the error budget, so
//! every interval in one report holds at once with probability at least
//! [`SCORE_BOUNDS_CONFIDENCE`] (a union bound). A column whose sample holds
//! every one of its values contributes its counts exactly.

use super::accuracy::AccuracyCalculator;
use super::consistency::ConsistencyCalculator;
use super::precision::PrecisionCalculator;
use super::timeliness::TimelinessCalculator;
use super::utils::is_likely_date_column;
use super::validity::ValidityCalculator;
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::quality::{ScoreBounds, ScoreInterval};
use crate::types::{ColumnProfile, QualityDimension, QualityMetrics};
use dataprof_core::SemanticHints;
use std::collections::HashMap;

/// Probability that every interval in one [`ScoreBounds`] holds at once.
pub const SCORE_BOUNDS_CONFIDENCE: f64 = 0.999;

/// What one column contributes to one share of a dimension score.
#[derive(Debug, Clone, Copy)]
struct ColumnCounts {
    /// Failures counted in the column's sample.
    failures: usize,
    /// Values the share's denominator counted in the sample.
    checked: usize,
    /// The most failures a single value can add.
    max_failures_per_value: usize,
    /// The most values a single sampled unit adds to `checked`: one for a
    /// column's value, the number of column pairs for a row of pairs.
    max_checked_per_value: usize,
    /// Values in the column's sample.
    sampled: usize,
    /// Non-null values of the column in the whole source, which is what the
    /// sample was drawn from.
    population: usize,
}

impl ColumnCounts {
    /// True when the sample holds every value the column has.
    fn is_exact(&self) -> bool {
        self.sampled >= self.population
    }

    /// How many means this column adds to the union bound.
    fn bounded_means(&self) -> usize {
        match (self.is_exact(), self.max_failures_per_value) {
            (true, _) => 0,
            (false, 0) => 1,
            (false, _) => 2,
        }
    }
}

/// One share of a dimension score: failures over checked values, summed over
/// the columns that took part.
type Share = Vec<ColumnCounts>;

/// Bound the full-source scores of an assessment whose `sampled` components
/// were computed over the retained sample in `data`.
///
/// `None` when nothing was sampled: every score is then exact already.
///
/// `timeliness` must be the calculator the run itself used, so the bounds
/// judge dates against the same reference day as the score.
#[allow(clippy::too_many_arguments)]
pub(crate) fn score_bounds(
    thresholds: &IsoQualityConfig,
    timeliness: &TimelinessCalculator<'_>,
    data: &HashMap<String, Vec<String>>,
    column_profiles: &[ColumnProfile],
    semantic_hints: &SemanticHints,
    temporal_columns: &[String],
    metrics: &QualityMetrics,
    sampled: &[String],
) -> Result<Option<ScoreBounds>, DataProfilerError> {
    if sampled.is_empty() {
        return Ok(None);
    }
    let is_sampled = |component: &str| sampled.iter().any(|label| label == component);

    // Shares for every sampled dimension first: the error budget is split
    // over all of them, so no interval can be computed before the count of
    // bounded means is known. `None` in the map is a sampled dimension this
    // module has no bound for.
    let wanted: Vec<QualityDimension> = QualityDimension::all()
        .into_iter()
        .filter(|dimension| metrics.dimension_score(*dimension).is_some())
        .filter(|dimension| match dimension {
            QualityDimension::Uniqueness => {
                is_sampled("key_uniqueness") || is_sampled("duplicate_rows")
            }
            other => is_sampled(&other.to_string()),
        })
        .collect();
    let shares = collect_shares(
        &wanted,
        thresholds,
        timeliness,
        data,
        column_profiles,
        &semantic_hints.positive_columns,
        temporal_columns,
    )?;

    let bounded_means: usize = shares
        .values()
        .flatten()
        .flatten()
        .flatten()
        .map(ColumnCounts::bounded_means)
        .sum();
    let error_budget = 1.0 - SCORE_BOUNDS_CONFIDENCE;
    // Two tails per mean.
    let log_term = (2.0 * bounded_means.max(1) as f64 / error_budget).ln();

    let mut raw: HashMap<QualityDimension, Option<(f64, f64)>> = HashMap::new();
    for dimension in QualityDimension::all() {
        let Some(score) = metrics.dimension_score(dimension) else {
            continue;
        };
        let interval = match shares.get(&dimension) {
            None => Some((score, score)),
            Some(None) => None,
            Some(Some(dimension_shares)) => Some(dimension_interval(dimension_shares, log_term)),
        };
        raw.insert(dimension, interval);
    }

    let mut overall: Option<(f64, f64)> = Some((0.0, 0.0));
    let mut total_weight = 0.0;
    for dimension in metrics.assessed_dimensions() {
        let weight = dimension_weight(metrics, dimension);
        match (overall, raw.get(&dimension).copied().flatten()) {
            (Some((lower, upper)), Some((dimension_lower, dimension_upper))) => {
                overall = Some((
                    lower + weight * dimension_lower,
                    upper + weight * dimension_upper,
                ));
                total_weight += weight;
            }
            _ => overall = None,
        }
    }
    let overall_score = overall
        .filter(|_| total_weight > 0.0)
        .map(|(lower, upper)| outward(lower / total_weight, (upper / total_weight).min(100.0)));

    Ok(Some(ScoreBounds {
        confidence_level: SCORE_BOUNDS_CONFIDENCE,
        overall_score,
        dimension_scores: QualityDimension::all()
            .into_iter()
            .map(|dimension| {
                let interval = raw
                    .get(&dimension)
                    .copied()
                    .flatten()
                    .map(|(lower, upper)| outward(lower, upper));
                (dimension.to_string(), interval)
            })
            .collect(),
    }))
}

fn dimension_weight(metrics: &QualityMetrics, dimension: QualityDimension) -> f64 {
    let weights = metrics.score_weights;
    match dimension {
        QualityDimension::Completeness => weights.completeness,
        QualityDimension::Consistency => weights.consistency,
        QualityDimension::Uniqueness => weights.uniqueness,
        QualityDimension::Accuracy => weights.accuracy,
        QualityDimension::Timeliness => weights.timeliness,
        QualityDimension::Validity => weights.validity,
        QualityDimension::Precision => weights.precision,
    }
}

/// Round an interval outward to the report's two decimals, so the saved
/// interval still contains what the unrounded one did. The small slack keeps
/// a value that is already on a two-decimal boundary, such as an exact
/// score, from being pushed one step out by its own representation error.
fn outward(lower: f64, upper: f64) -> ScoreInterval {
    const SLACK: f64 = 1e-9;
    ScoreInterval {
        lower: (((lower + SLACK) * 100.0).floor() / 100.0).clamp(0.0, 100.0),
        upper: (((upper - SLACK) * 100.0).ceil() / 100.0).clamp(0.0, 100.0),
    }
}

/// The score interval `100 - 100 * sum(share)` spans when each share lies
/// in its own interval.
fn dimension_interval(shares: &[Share], log_term: f64) -> (f64, f64) {
    let (lowest, highest) = shares
        .iter()
        .map(|share| share_interval(share, log_term))
        .fold((0.0, 0.0), |(low, high), (share_low, share_high)| {
            (low + share_low, high + share_high)
        });
    (
        (100.0 - 100.0 * highest).clamp(0.0, 100.0),
        (100.0 - 100.0 * lowest).clamp(0.0, 100.0),
    )
}

/// Interval for the whole-source share `sum(failures) / sum(checked)`.
///
/// Each column's totals are bounded twice: statistically, from its sample
/// means, and deterministically, since the sample is part of the source (the
/// source holds at least what the sample showed, and at most that plus the
/// unseen values each failing or checked). The tighter of the two is kept.
fn share_interval(share: &[ColumnCounts], log_term: f64) -> (f64, f64) {
    let (mut failures_low, mut failures_high) = (0.0, 0.0);
    let (mut checked_low, mut checked_high) = (0.0, 0.0);
    let mut most_per_value = 0usize;
    for column in share {
        most_per_value = most_per_value.max(column.max_failures_per_value);
        let failures = column.failures as f64;
        let checked = column.checked as f64;
        if column.is_exact() {
            failures_low += failures;
            failures_high += failures;
            checked_low += checked;
            checked_high += checked;
            continue;
        }
        let sampled = column.sampled as f64;
        let population = column.population as f64;
        let per_value = column.max_failures_per_value as f64;
        let checked_per_value = column.max_checked_per_value as f64;
        let unseen = population - sampled;

        let (failure_mean_low, failure_mean_high) = if column.max_failures_per_value == 0 {
            (0.0, 0.0)
        } else {
            mean_interval(failures / (sampled * per_value), column.sampled, log_term)
        };
        let (checked_mean_low, checked_mean_high) = mean_interval(
            checked / (sampled * checked_per_value),
            column.sampled,
            log_term,
        );

        failures_low += (population * per_value * failure_mean_low).max(failures);
        failures_high +=
            (population * per_value * failure_mean_high).min(failures + unseen * per_value);
        checked_low += (population * checked_per_value * checked_mean_low).max(checked);
        checked_high += (population * checked_per_value * checked_mean_high)
            .min(checked + unseen * checked_per_value);
    }

    let low = if checked_high > 0.0 {
        failures_low / checked_high
    } else {
        0.0
    };
    let high = if checked_low > 0.0 {
        failures_high / checked_low
    } else if failures_high > 0.0 {
        // No value the share checks may exist outside the sample, yet some
        // might: nothing bounds the share below its largest possible value.
        most_per_value as f64
    } else {
        0.0
    };
    (low, high)
}

/// Two-sided interval for the population mean of a variable in `[0, 1]`
/// whose sample mean over `n` draws is `mean`: every `q` with
/// `n * KL(mean || q) <= log_term`.
fn mean_interval(mean: f64, n: usize, log_term: f64) -> (f64, f64) {
    let mean = mean.clamp(0.0, 1.0);
    let n = n as f64;
    let inside = |q: f64| n * bernoulli_kl(mean, q) <= log_term;

    // KL(mean || q) grows as q moves away from mean in either direction, so
    // each bound is a bisection on one side.
    let (mut outside, mut within) = (1.0, mean);
    for _ in 0..64 {
        let mid = (outside + within) / 2.0;
        if inside(mid) {
            within = mid;
        } else {
            outside = mid;
        }
    }
    let high = if inside(1.0) { 1.0 } else { within };

    let (mut outside, mut within) = (0.0, mean);
    for _ in 0..64 {
        let mid = (outside + within) / 2.0;
        if inside(mid) {
            within = mid;
        } else {
            outside = mid;
        }
    }
    let low = if inside(0.0) { 0.0 } else { within };
    (low, high)
}

/// Kullback-Leibler divergence between Bernoulli(`p`) and Bernoulli(`q`).
fn bernoulli_kl(p: f64, q: f64) -> f64 {
    let term = |a: f64, b: f64| {
        if a == 0.0 {
            0.0
        } else if b == 0.0 {
            f64::INFINITY
        } else {
            a * (a / b).ln()
        }
    };
    term(p, q) + term(1.0 - p, 1.0 - q)
}

/// Shares for each dimension in `wanted`, keyed by dimension. `None` is a
/// sampled dimension this module has no bound for.
///
/// Every calculator reads only the columns in the map it is given, so a map
/// holding one column yields that column's counts, with the checks the full
/// run fixed from the sample. Columns are copied one at a time and visited in
/// profile order: memory stays flat on wide tables, and the sums below are
/// formed in the same order on every engine.
fn collect_shares(
    wanted: &[QualityDimension],
    thresholds: &IsoQualityConfig,
    timeliness: &TimelinessCalculator<'_>,
    data: &HashMap<String, Vec<String>>,
    column_profiles: &[ColumnProfile],
    positive_columns: &[String],
    temporal_columns: &[String],
) -> Result<HashMap<QualityDimension, Option<Vec<Share>>>, DataProfilerError> {
    let wants = |dimension: QualityDimension| wanted.contains(&dimension);
    let accuracy = AccuracyCalculator::new(thresholds);
    let (mut types, mut formats, mut encodings) = (Vec::new(), Vec::new(), Vec::new());
    let (mut outliers, mut violations) = (Vec::new(), Vec::new());
    let (mut stale, mut date_failures) = (Vec::new(), Vec::new());
    let (mut invalid, mut imprecise) = (Vec::new(), Vec::new());

    for profile in column_profiles {
        let Some(values) = data.get(&profile.name) else {
            continue;
        };
        let sampled = values.len();
        // Reservoirs hold non-null values only. A sample holding more than
        // that was drawn from every value, nulls included.
        let non_null = profile.total_count.saturating_sub(profile.null_count);
        let population = if sampled <= non_null {
            non_null
        } else {
            profile.total_count.max(sampled)
        };
        let counts = |failures, checked, max_failures_per_value| ColumnCounts {
            failures,
            checked,
            max_failures_per_value,
            max_checked_per_value: 1,
            sampled,
            population,
        };
        let single = HashMap::from([(profile.name.clone(), values.clone())]);

        if wants(QualityDimension::Consistency) {
            let metrics = ConsistencyCalculator::calculate(&single, column_profiles)?;
            types.push(counts(
                metrics.inconsistent_values,
                metrics.values_checked,
                1,
            ));
            // A value can be both a minority date format and a minority
            // decimal separator, but only in a column whose name announces
            // dates.
            let format_rules = 1 + usize::from(is_likely_date_column(&profile.name));
            formats.push(counts(
                metrics.format_violations,
                metrics.values_checked,
                format_rules,
            ));
            encodings.push(counts(metrics.encoding_issues, metrics.values_checked, 1));
        }
        if wants(QualityDimension::Accuracy) {
            let metrics = accuracy.calculate_with_positive_columns(
                &single,
                column_profiles,
                positive_columns,
            )?;
            // A column with too few numbers for an outlier test in the sample
            // was not tested, so it adds nothing to the outlier share.
            if metrics.outlier_values_checked > 0 {
                outliers.push(counts(metrics.outliers, metrics.outlier_values_checked, 1));
            }
            violations.push(counts(
                metrics.range_violations + metrics.negative_values_in_positive,
                metrics.numeric_values_checked,
                AccuracyCalculator::max_violations_per_value(&profile.name, positive_columns),
            ));
        }
        if wants(QualityDimension::Timeliness) && temporal_columns.contains(&profile.name) {
            let metrics = timeliness.calculate(&single, temporal_columns, column_profiles)?;
            stale.push(counts(metrics.stale_dates, metrics.valid_dates, 1));
            // A value is either a date, which may be in the future, or invalid.
            date_failures.push(counts(
                metrics.future_dates_count + metrics.invalid_date_values,
                metrics.date_values_checked,
                1,
            ));
        }
        if wants(QualityDimension::Validity) {
            let metrics = ValidityCalculator::calculate(&single, column_profiles);
            invalid.push(counts(metrics.invalid_values, metrics.values_checked, 1));
        }
        if wants(QualityDimension::Precision) {
            let metrics = PrecisionCalculator::calculate(&single, column_profiles);
            imprecise.push(counts(
                metrics.inconsistent_precision_values,
                metrics.numeric_values_checked,
                1,
            ));
        }
    }

    let mut timeliness_shares = Some(vec![stale, date_failures]);
    if wants(QualityDimension::Timeliness) {
        match temporal_pairs(timeliness, data, column_profiles, temporal_columns)? {
            Pairs::None => {}
            Pairs::Aligned(pairs) => {
                if let Some(shares) = timeliness_shares.as_mut() {
                    shares.push(vec![pairs]);
                }
            }
            Pairs::Unaligned => timeliness_shares = None,
        }
    }
    let mut dimension_shares = HashMap::from([
        (
            QualityDimension::Consistency,
            Some(vec![types, formats, encodings]),
        ),
        (QualityDimension::Accuracy, Some(vec![outliers, violations])),
        (QualityDimension::Timeliness, timeliness_shares),
        (QualityDimension::Validity, Some(vec![invalid])),
        (QualityDimension::Precision, Some(vec![imprecise])),
    ]);
    Ok(wanted
        .iter()
        .map(|dimension| {
            // A sampled uniqueness component is an estimated key count or a
            // duplicate scan over an aligned sample: neither is a share of
            // failing values, so neither is bounded here.
            let shares = dimension_shares.remove(dimension).flatten();
            (*dimension, shares)
        })
        .collect())
}

enum Pairs {
    /// No start/end ordering was compared.
    None,
    /// Ordering was compared on reservoirs holding the same rows slot by
    /// slot, so the compared rows are a uniform sample of the source's rows.
    Aligned(ColumnCounts),
    /// Ordering was compared on reservoirs that do not hold the same rows.
    Unaligned,
}

/// Counts for the start/end ordering share, treating each sampled row as one
/// draw from the source's rows.
///
/// The calculator compares only pairs whose values line up by row (see
/// [`rows_line_up`](super::timeliness::rows_line_up)), so every compared pair
/// is a uniform sample of the source's rows. Their counts are pooled, which
/// needs every compared column to hold the same number of values drawn from
/// the same number of rows.
fn temporal_pairs(
    calculator: &TimelinessCalculator<'_>,
    data: &HashMap<String, Vec<String>>,
    column_profiles: &[ColumnProfile],
    temporal_columns: &[String],
) -> Result<Pairs, DataProfilerError> {
    let metrics = calculator.calculate(data, temporal_columns, column_profiles)?;
    if metrics.temporal_pairs_checked == 0 {
        return Ok(Pairs::None);
    }
    let sizes: Vec<(usize, usize)> = metrics
        .compared_pairs
        .iter()
        .flat_map(|(start, end)| [start, end])
        .filter_map(|name| {
            let profile = column_profiles
                .iter()
                .find(|profile| &profile.name == name)?;
            Some((data.get(name)?.len(), profile.total_count))
        })
        .collect();
    let Some(&(sampled, population)) = sizes.first() else {
        return Ok(Pairs::Unaligned);
    };
    if sizes.iter().any(|&size| size != (sampled, population)) {
        return Ok(Pairs::Unaligned);
    }
    Ok(Pairs::Aligned(ColumnCounts {
        failures: metrics.temporal_violations,
        checked: metrics.temporal_pairs_checked,
        max_failures_per_value: metrics.temporal_column_pairs,
        max_checked_per_value: metrics.temporal_column_pairs,
        sampled,
        population: population.max(sampled),
    }))
}

#[cfg(test)]
mod tests {
    use super::super::MetricsCalculator;
    use super::super::testing::string_profile;
    use super::*;
    use crate::types::{DataType, Pattern, PatternCategory};
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use rand::seq::SliceRandom;

    fn profile(name: &str, data_type: DataType, total: usize, nulls: usize) -> ColumnProfile {
        let mut profile = string_profile(name, total, nulls);
        profile.data_type = data_type;
        profile
    }

    fn bifurcated(
        data: &HashMap<String, Vec<String>>,
        profiles: &[ColumnProfile],
    ) -> super::super::BifurcatedResult {
        MetricsCalculator::new()
            .calculate_bifurcated_metrics_with_all_semantic_hints(
                data,
                profiles,
                None,
                &SemanticHints::default(),
                None,
            )
            .unwrap()
    }

    /// Bounds for `data`, whose columns were drawn from sources as large as
    /// the `profiles` say.
    fn bounds_for(data: &HashMap<String, Vec<String>>, profiles: &[ColumnProfile]) -> ScoreBounds {
        bifurcated(data, profiles)
            .score_bounds
            .expect("sampled dimensions carry bounds")
    }

    fn score(
        data: &HashMap<String, Vec<String>>,
        profiles: &[ColumnProfile],
        dimension: QualityDimension,
    ) -> f64 {
        bifurcated(data, profiles)
            .metrics
            .dimension_score(dimension)
            .unwrap()
    }

    fn interval(bounds: &ScoreBounds, dimension: QualityDimension) -> ScoreInterval {
        bounds.dimension_scores[&dimension.to_string()]
            .unwrap_or_else(|| panic!("{dimension} has no interval"))
    }

    fn contains(bound: ScoreInterval, value: f64) -> bool {
        bound.lower <= value && value <= bound.upper
    }

    /// Outward rounding to two decimals is the only width left.
    fn is_point(bound: ScoreInterval) -> bool {
        bound.upper - bound.lower <= 0.01 + 1e-9
    }

    fn strings<T: ToString>(values: impl IntoIterator<Item = T>) -> Vec<String> {
        values.into_iter().map(|value| value.to_string()).collect()
    }

    /// A column of whole numbers whose first `junk` values are not numbers.
    fn integers_with_junk(len: usize, junk: usize) -> Vec<String> {
        (0..len)
            .map(|index| {
                if index < junk {
                    format!("x{index}")
                } else {
                    (index % 97).to_string()
                }
            })
            .collect()
    }

    fn sample(values: &[String], size: usize, seed: u64) -> Vec<String> {
        let mut shuffled = values.to_vec();
        shuffled.shuffle(&mut SmallRng::seed_from_u64(seed));
        shuffled.truncate(size);
        shuffled
    }

    /// Every dimension the bounds cover, each with failures of its own kind.
    fn every_dimension() -> (HashMap<String, Vec<String>>, Vec<ColumnProfile>) {
        let mut amount = strings((0..300).map(|index| format!("{}.{:02}", index % 50, index % 7)));
        amount[0] = "3.5".into();
        amount[1] = "1,5".into();
        let mut age = strings((0..300).map(|index| 20 + index % 40));
        age[0] = "400".into();
        age[1] = "9000".into();
        let mut created = strings((0..300).map(|index| format!("2021-03-{:02}", 1 + index % 28)));
        created[0] = "2999-01-01".into();
        created[1] = "1901-01-01".into();
        created[2] = "2021-02-30".into();
        let mut email = strings((0..300).map(|index| format!("user{index}@example.com")));
        email[0] = "nobody".into();
        email[1] = "\u{c3}\u{a9}t@example.com".into();
        let mut code = integers_with_junk(300, 9);
        code[10] = "1.5".into();

        let data = HashMap::from([
            ("amount".to_string(), amount),
            ("age".to_string(), age),
            ("created".to_string(), created),
            ("email".to_string(), email),
            ("code".to_string(), code),
        ]);
        let mut email_profile = profile("email", DataType::String, 300, 0);
        email_profile.patterns = Some(vec![Pattern {
            name: "Email".to_string(),
            regex: String::new(),
            match_count: 298,
            match_percentage: 99.33,
            category: PatternCategory::Contact,
            confidence: 0.9,
        }]);
        let profiles = vec![
            profile("amount", DataType::Float, 300, 0),
            profile("age", DataType::Integer, 300, 0),
            profile("created", DataType::Date, 300, 0),
            email_profile,
            profile("code", DataType::Integer, 300, 0),
        ];
        (data, profiles)
    }

    #[test]
    fn a_sample_holding_every_value_bounds_each_score_to_itself() {
        // With nothing unseen, an interval can only be its score rounded
        // outward. This pins the shares to the score formulas: a share left
        // out, or one counted differently, moves the interval off the score.
        let (data, profiles) = every_dimension();
        let result = bifurcated(&data, &profiles);
        let bounds = result.score_bounds.unwrap();

        for dimension in [
            QualityDimension::Consistency,
            QualityDimension::Accuracy,
            QualityDimension::Timeliness,
            QualityDimension::Validity,
            QualityDimension::Precision,
        ] {
            let score = result
                .metrics
                .dimension_score(dimension)
                .unwrap_or_else(|| panic!("{dimension} was not assessed"));
            assert!(score < 100.0, "{dimension} sees no failure in the fixture");
            let bound = interval(&bounds, dimension);
            assert!(
                contains(bound, score) && is_point(bound),
                "{dimension}: score {score} against {bound:?}"
            );
        }
        // The duplicate scan over a sample is not bounded, so neither is an
        // overall score that weighs it.
        assert!(bounds.dimension_scores["uniqueness"].is_none());
        assert!(bounds.overall_score.is_none());

        let mut thresholds = IsoQualityConfig::default();
        thresholds.score_weights.uniqueness = 0.0;
        let result = MetricsCalculator::with_thresholds(thresholds)
            .calculate_bifurcated_metrics_with_all_semantic_hints(
                &data,
                &profiles,
                None,
                &SemanticHints::default(),
                None,
            )
            .unwrap();
        let overall = result.metrics.overall_score().unwrap();
        let bound = result.score_bounds.unwrap().overall_score.unwrap();
        assert!(
            contains(bound, overall) && is_point(bound),
            "{overall} against {bound:?}"
        );
    }

    #[test]
    fn the_full_source_score_lies_within_the_bounds_of_its_samples() {
        let source = integers_with_junk(20_000, 600);
        let profiles = vec![profile("code", DataType::Integer, 20_000, 0)];
        let full = HashMap::from([("code".to_string(), source.clone())]);
        let exact = score(&full, &profiles, QualityDimension::Consistency);

        for seed in 0..25 {
            let retained = HashMap::from([("code".to_string(), sample(&source, 2_000, seed))]);
            let bound = interval(
                &bounds_for(&retained, &profiles),
                QualityDimension::Consistency,
            );
            assert!(
                contains(bound, exact),
                "seed {seed}: {exact} outside {bound:?}"
            );
            assert!(bound.upper - bound.lower < 5.0, "seed {seed}: {bound:?}");
        }
    }

    #[test]
    fn columns_count_by_their_size_in_the_source_not_in_the_sample() {
        // `dense` fills its reservoir from 20,000 values; `sparse` has 1,000
        // non-null values, all retained. The sample holds as many of each, so
        // its pooled score gives the sparse column's failures twenty times
        // their weight in the source.
        let dense = integers_with_junk(20_000, 0);
        let sparse = integers_with_junk(1_000, 500);
        let profiles = vec![
            profile("dense", DataType::Integer, 20_000, 0),
            profile("sparse", DataType::Integer, 20_000, 19_000),
        ];
        let full = HashMap::from([
            ("dense".to_string(), dense.clone()),
            ("sparse".to_string(), sparse.clone()),
        ]);
        let exact = score(&full, &profiles, QualityDimension::Consistency);
        let retained = HashMap::from([
            ("dense".to_string(), sample(&dense, 1_000, 7)),
            ("sparse".to_string(), sparse),
        ]);
        let sampled = score(&retained, &profiles, QualityDimension::Consistency);
        let bound = interval(
            &bounds_for(&retained, &profiles),
            QualityDimension::Consistency,
        );

        assert!((sampled - 75.0).abs() < 1e-9, "{sampled}");
        assert!(contains(bound, exact), "{exact} outside {bound:?}");
        assert!(bound.lower > sampled, "{bound:?} should exclude {sampled}");
    }

    #[test]
    fn an_exact_dimension_keeps_its_score_beside_a_sampled_one() {
        let source = integers_with_junk(20_000, 600);
        let retained = HashMap::from([("code".to_string(), sample(&source, 2_000, 3))]);
        let profiles = vec![profile("code", DataType::Integer, 20_100, 100)];
        let result = bifurcated(&retained, &profiles);
        let bounds = result.score_bounds.unwrap();

        let completeness = result
            .metrics
            .dimension_score(QualityDimension::Completeness)
            .unwrap();
        let bound = interval(&bounds, QualityDimension::Completeness);
        assert!(
            contains(bound, completeness) && is_point(bound),
            "{bound:?}"
        );
        let consistency = interval(&bounds, QualityDimension::Consistency);
        assert!(
            consistency.upper - consistency.lower > 0.5,
            "{consistency:?}"
        );
    }

    /// `start_date`/`end_date` columns of `rows` rows, where every
    /// `inverted_every`-th row ends before it starts.
    fn date_pairs(rows: usize, inverted_every: usize) -> (Vec<String>, Vec<String>) {
        (0..rows)
            .map(|row| {
                let start = format!("2021-03-{:02}", 2 + row % 27);
                let end = if row % inverted_every == 0 {
                    "2021-03-01".to_string()
                } else {
                    format!("2021-04-{:02}", 1 + row % 28)
                };
                (start, end)
            })
            .unzip()
    }

    #[test]
    fn row_aligned_temporal_ordering_is_bounded_like_any_share() {
        let (starts, ends) = date_pairs(20_000, 40);
        let full_profiles = vec![
            profile("start_date", DataType::Date, 20_000, 0),
            profile("end_date", DataType::Date, 20_000, 0),
        ];
        let full = HashMap::from([
            ("start_date".to_string(), starts.clone()),
            ("end_date".to_string(), ends.clone()),
        ]);
        let exact = score(&full, &full_profiles, QualityDimension::Timeliness);
        assert!(exact < 100.0, "no ordering violation in the fixture");

        // Null-free columns sample the same rows, slot by slot.
        let mut rows: Vec<usize> = (0..20_000).collect();
        rows.shuffle(&mut SmallRng::seed_from_u64(11));
        rows.truncate(2_000);
        let retained = HashMap::from([
            (
                "start_date".to_string(),
                rows.iter().map(|&row| starts[row].clone()).collect(),
            ),
            (
                "end_date".to_string(),
                rows.iter().map(|&row| ends[row].clone()).collect(),
            ),
        ]);
        let bounds = bounds_for(&retained, &full_profiles);
        let bound = interval(&bounds, QualityDimension::Timeliness);
        assert!(contains(bound, exact), "{exact} outside {bound:?}");
        assert!(bound.upper - bound.lower > 0.5, "{bound:?}");

        // With every row retained the ordering share is exact too.
        let bound = interval(
            &bounds_for(&full, &full_profiles),
            QualityDimension::Timeliness,
        );
        assert!(
            contains(bound, exact) && is_point(bound),
            "{exact} against {bound:?}"
        );
    }

    #[test]
    fn a_date_column_outside_the_pair_does_not_unbound_its_ordering() {
        // Only the compared pair has to line up. A third date column with
        // nulls is in no pair, and used to leave timeliness unbounded.
        let (starts, ends) = date_pairs(2_000, 40);
        let observed: Vec<String> = starts.iter().take(1_900).cloned().collect();
        let retained = HashMap::from([
            ("start_date".to_string(), starts),
            ("end_date".to_string(), ends),
            ("observed_on".to_string(), observed),
        ]);
        let profiles = vec![
            profile("start_date", DataType::Date, 20_000, 0),
            profile("end_date", DataType::Date, 20_000, 0),
            profile("observed_on", DataType::Date, 20_000, 1_000),
        ];
        let timeliness = bifurcated(&retained, &profiles).metrics.timeliness.unwrap();
        assert_eq!(timeliness.temporal_pairs_checked, 2_000);
        assert!(timeliness.temporal_violations > 0);
        let bounds = bounds_for(&retained, &profiles);
        assert!(bounds.dimension_scores["timeliness"].is_some());
    }

    #[test]
    fn temporal_ordering_over_unaligned_reservoirs_is_not_compared() {
        // A column with nulls skips rows in its reservoir, so its slots stop
        // holding the same rows as the other column's. The pair is not
        // compared (#787), so neither the score nor its bounds read it, and
        // timeliness is bounded by its per-value shares alone.
        let (starts, ends) = date_pairs(2_000, 40);
        let retained = HashMap::from([
            ("start_date".to_string(), starts),
            ("end_date".to_string(), ends),
        ]);
        let profiles = vec![
            profile("start_date", DataType::Date, 20_000, 500),
            profile("end_date", DataType::Date, 20_000, 0),
        ];
        let timeliness = bifurcated(&retained, &profiles).metrics.timeliness.unwrap();
        assert_eq!(timeliness.temporal_pairs_checked, 0);
        assert_eq!(timeliness.temporal_violations, 0);

        let bounds = bounds_for(&retained, &profiles);
        assert!(bounds.dimension_scores["timeliness"].is_some());
        assert!(bounds.overall_score.is_some());
    }

    #[test]
    fn no_sampled_dimension_means_no_bounds() {
        let (data, profiles) = every_dimension();
        let result = bifurcated(&data, &profiles);
        let thresholds = IsoQualityConfig::default();
        let bounds = score_bounds(
            &thresholds,
            &TimelinessCalculator::new(&thresholds),
            &data,
            &profiles,
            &SemanticHints::default(),
            &[],
            &result.metrics,
            &[],
        )
        .unwrap();
        assert!(bounds.is_none());
    }

    #[test]
    fn saved_intervals_are_rounded_outward() {
        let interval = outward(99.061, 99.069);
        assert_eq!((interval.lower, interval.upper), (99.06, 99.07));
        // A value already on a two-decimal boundary, as an exact score is,
        // stays put rather than being pushed out by representation error.
        let score = (0.1 + 0.2) * 100.0;
        assert_ne!(score, 30.0);
        let interval = outward(score, score);
        assert_eq!((interval.lower, interval.upper), (30.0, 30.0));
        let interval = outward(-0.5, 100.5);
        assert_eq!((interval.lower, interval.upper), (0.0, 100.0));
    }

    #[test]
    fn a_clean_sample_bounds_its_share_near_the_edge() {
        // For a sample of n showing no failure the bound is about
        // log_term / n, where a normal approximation would give nothing.
        let (low, high) = mean_interval(0.0, 10_000, 10.0);
        assert_eq!(low, 0.0);
        assert!((high - 0.001).abs() < 0.000_01, "{high}");
        let (low, high) = mean_interval(1.0, 10_000, 10.0);
        assert_eq!(high, 1.0);
        assert!((low - 0.999).abs() < 0.000_01, "{low}");
    }
}
