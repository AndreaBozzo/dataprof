//! What changed between two reports.
//!
//! A pipeline that keeps a baseline report asks the same questions of every
//! new one: did quality move, did a column appear or vanish, did a column's
//! null rate change. [`ReportDelta`] answers them in one typed value, so the
//! answer does not depend on which language computed it, and so a policy over
//! drift can be written against a contract rather than a dictionary.
//!
//! # The contract
//!
//! The two reports are `a` and `b`, compared in that order. The serialized
//! shape is the one `ProfileReport.compare()` returns in Python, and it is
//! stable: later releases may add keys, never remove or rename one.
//!
//! - **Signs.** Every change is `b - a`. A positive `abs` on a score is an
//!   improvement, because a score is higher when quality is better. A positive
//!   `null_pct_delta` is a degradation, because it is more nulls.
//! - **Precision.** Each side's number is taken at its serialized precision
//!   (2 decimals on a 0-100 scale), and the change is rounded to it too, so a
//!   report and its saved document compare alike.
//! - **No invented numbers.** A number one side does not carry, an
//!   unassessed dimension, an empty column or an empty report, leaves the
//!   change `None`. `rel_pct` is `None` as well when `a` is zero, where a
//!   relative change is undefined.
//! - **Provenance.** Each number carries the [`Evidence`] behind it on both
//!   sides, so an exact baseline compared with a sampled or truncated run says
//!   so instead of reading a sampling difference as a change in the data.
//! - **Definitions.** `metric_semantics.comparable` is `Some(true)` only when
//!   both reports record every definition this build knows, with the same
//!   values. A difference in a measurement whose definition changed between
//!   releases is not a difference in the data.
//!
//! Nothing here decides whether a change is acceptable; that is a policy's
//! job. A window-over-window comparison reuses the same vocabulary: `a` and
//! `b` name the two sides without saying which came first.
//!
//! # Example
//!
//! ```no_run
//! use dataprof_runtime::{ProfileReport, ReportDelta};
//!
//! fn drift(baseline: &ProfileReport, current: &ProfileReport) {
//!     let delta = ReportDelta::between(baseline, current);
//!     if let Some(change) = delta.quality_score.abs {
//!         println!("quality moved {change:+.2} points");
//!     }
//!     for name in &delta.schema.removed {
//!         println!("column {name} is gone");
//!     }
//! }
//! ```

use std::collections::{BTreeMap, HashSet};

use dataprof_core::QualityDimension;
use dataprof_core::serde_helpers::rounded_2;
use serde::ser::{SerializeMap, Serializer};

use crate::profile_report::{MetricSemantics, ProfileReport};
use crate::quality_gate::{
    Evidence, EvidenceGap, Provenance, sampled_component_evidence, scan_coverage, scan_evidence,
};

/// The same fact about each side of a comparison.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct Sides<T> {
    /// The first report.
    pub a: T,
    /// The second report.
    pub b: T,
}

/// Which report a side is, and how much of its source it read.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct ReportReference {
    /// The report's identifier. `None` for a flat summary, which does not
    /// record one.
    pub id: Option<String>,
    /// When the report was generated. `None` for a flat summary.
    pub timestamp: Option<String>,
    /// The source identifier: a path, a query, or an in-memory label.
    pub source: String,
    /// Rows the scan read. `None` for a flat summary that did not record it.
    pub rows_processed: Option<usize>,
    /// Whether the scan covered the whole source.
    pub evidence: Evidence,
}

/// One score on both sides and how it moved.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct ScoreChange {
    /// The score in `a`, or `None` when `a` did not assess it.
    pub a: Option<f64>,
    /// The score in `b`, or `None` when `b` did not assess it.
    pub b: Option<f64>,
    /// `b - a` in score points, when both sides have a score.
    pub abs: Option<f64>,
    /// `b - a` as a percentage of `a`, when both have a score and `a` is not
    /// zero.
    pub rel_pct: Option<f64>,
    /// The evidence behind each side's score: the scan's coverage, and
    /// whether the score was computed from a retained sample of the rows.
    pub evidence: Sides<Evidence>,
}

impl ScoreChange {
    fn new(a: Option<f64>, b: Option<f64>, evidence: Sides<Evidence>) -> Self {
        let (abs, rel_pct) = match (a, b) {
            (Some(a), Some(b)) => (
                Some(rounded_2(b - a)),
                (a != 0.0).then(|| rounded_2((b - a) / a.abs() * 100.0)),
            ),
            _ => (None, None),
        };
        Self {
            a,
            b,
            abs,
            rel_pct,
            evidence,
        }
    }
}

/// One quality dimension's change. Serialized as an entry of the
/// `dimensions` object, keyed by the dimension's name.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DimensionChange {
    /// The dimension.
    pub dimension: QualityDimension,
    /// Its score on both sides.
    pub score: ScoreChange,
}

/// One column's change. Serialized as an entry of the `columns` object, keyed
/// by the column's name. Each side's number covers what that side's scan
/// read; see [`ReportReference::evidence`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct ColumnChange {
    /// The column's name.
    #[serde(skip)]
    pub name: String,
    /// Null percentage in `a`, or `None` when `a` has no such column or the
    /// column holds no values.
    pub null_pct_a: Option<f64>,
    /// Null percentage in `b`, under the same rules.
    pub null_pct_b: Option<f64>,
    /// `null_pct_b - null_pct_a`, when both are present. Positive means more
    /// nulls.
    pub null_pct_delta: Option<f64>,
}

/// Each side's recorded measurement definitions, and whether they agree.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct SemanticsComparison {
    /// What `a` recorded, or `None` when it predates the record.
    pub a: Option<MetricSemantics>,
    /// What `b` recorded.
    pub b: Option<MetricSemantics>,
    /// `Some(true)` when both record every definition this build knows and
    /// they match; `Some(false)` when they record different ones; `None`
    /// when either side leaves a definition unrecorded, which includes every
    /// report written before 0.12.
    pub comparable: Option<bool>,
}

/// Which columns each side has.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct SchemaChange {
    /// In `b` only, in `b`'s order.
    pub added: Vec<String>,
    /// In `a` only, in `a`'s order.
    pub removed: Vec<String>,
    /// In both, in `a`'s order.
    pub common: Vec<String>,
}

/// What changed between two reports. See the [module docs](self) for the
/// contract.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct ReportDelta {
    /// The overall quality score.
    pub quality_score: ScoreChange,
    /// Every quality dimension this build knows, in [`QualityDimension::all`]
    /// order, assessed or not.
    #[serde(serialize_with = "serialize_dimensions")]
    pub dimensions: Vec<DimensionChange>,
    /// The union of both sides' columns: `a`'s in its order, then the ones
    /// only `b` has, in `b`'s order.
    #[serde(serialize_with = "serialize_columns")]
    pub columns: Vec<ColumnChange>,
    /// Whether the two reports measured the same way.
    pub metric_semantics: SemanticsComparison,
    /// Columns added, removed and shared.
    pub schema: SchemaChange,
    /// Which two reports were compared.
    pub reports: Sides<ReportReference>,
}

impl ReportDelta {
    /// Compare two reports, `a` first.
    pub fn between(a: &ProfileReport, b: &ProfileReport) -> Self {
        Self::between_sides(&ReportSide::from_report(a), &ReportSide::from_report(b))
    }

    /// Compare two sides, either of which may come from a flat summary.
    pub fn between_sides(a: &ReportSide, b: &ReportSide) -> Self {
        let pair = |a: Evidence, b: Evidence| Sides { a, b };
        let quality_score = ScoreChange::new(
            a.quality_score,
            b.quality_score,
            pair(a.overall_evidence, b.overall_evidence),
        );
        let dimensions = a
            .dimensions
            .iter()
            .zip(&b.dimensions)
            .map(|(left, right)| {
                debug_assert_eq!(left.dimension, right.dimension);
                DimensionChange {
                    dimension: left.dimension,
                    score: ScoreChange::new(
                        left.score,
                        right.score,
                        pair(left.evidence, right.evidence),
                    ),
                }
            })
            .collect();

        let a_names: HashSet<&str> = a.columns.iter().map(|(name, _)| name.as_str()).collect();
        let b_names: HashSet<&str> = b.columns.iter().map(|(name, _)| name.as_str()).collect();
        let a_nulls: BTreeMap<&str, Option<f64>> = a
            .columns
            .iter()
            .map(|(name, pct)| (name.as_str(), *pct))
            .collect();
        let b_nulls: BTreeMap<&str, Option<f64>> = b
            .columns
            .iter()
            .map(|(name, pct)| (name.as_str(), *pct))
            .collect();
        let added: Vec<String> = b
            .columns
            .iter()
            .filter(|(name, _)| !a_names.contains(name.as_str()))
            .map(|(name, _)| name.clone())
            .collect();
        let columns = a
            .columns
            .iter()
            .map(|(name, _)| name)
            .chain(&added)
            .map(|name| {
                let null_pct_a = a_nulls.get(name.as_str()).copied().flatten();
                let null_pct_b = b_nulls.get(name.as_str()).copied().flatten();
                ColumnChange {
                    name: name.clone(),
                    null_pct_a,
                    null_pct_b,
                    null_pct_delta: null_pct_a.zip(null_pct_b).map(|(a, b)| rounded_2(b - a)),
                }
            })
            .collect();
        let (removed, common) = a
            .columns
            .iter()
            .map(|(name, _)| name.clone())
            .partition(|name| !b_names.contains(name.as_str()));

        let comparable = match (&a.metric_semantics, &b.metric_semantics) {
            (Some(left), Some(right)) if left.is_fully_recorded() && right.is_fully_recorded() => {
                Some(left == right)
            }
            _ => None,
        };

        Self {
            quality_score,
            dimensions,
            columns,
            metric_semantics: SemanticsComparison {
                a: a.metric_semantics.clone(),
                b: b.metric_semantics.clone(),
                comparable,
            },
            schema: SchemaChange {
                added,
                removed,
                common,
            },
            reports: Sides {
                a: a.reference.clone(),
                b: b.reference.clone(),
            },
        }
    }
}

fn serialize_dimensions<S: Serializer>(
    dimensions: &[DimensionChange],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(dimensions.len()))?;
    for change in dimensions {
        map.serialize_entry(&change.dimension.to_string(), &change.score)?;
    }
    map.end()
}

fn serialize_columns<S: Serializer>(
    columns: &[ColumnChange],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(columns.len()))?;
    for change in columns {
        map.serialize_entry(&change.name, change)?;
    }
    map.end()
}

/// What a comparison reads from one report.
///
/// Built from a [`ProfileReport`], or from a flat summary (the document
/// [`ProfileReport::summary_json`] writes, and the one Python's `to_dict()`
/// and pre-0.12 `save()` produced). A flat summary never becomes a
/// `ProfileReport`, because it cannot acquire provenance it never recorded,
/// but it carries everything a comparison reads, and what it did not record
/// is read as unrecorded.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ReportSide {
    reference: ReportReference,
    quality_score: Option<f64>,
    overall_evidence: Evidence,
    dimensions: Vec<DimensionSide>,
    columns: Vec<(String, Option<f64>)>,
    metric_semantics: Option<MetricSemantics>,
}

#[derive(Debug, Clone, PartialEq)]
struct DimensionSide {
    dimension: QualityDimension,
    score: Option<f64>,
    evidence: Evidence,
}

/// One entry per column name, in order, holding the first profile of a
/// repeated name. Most input paths refuse duplicate names, but a report
/// rebuilt from a document can carry them, and the quality gate reads the
/// first profile too, so the two never disagree about which one a name means.
fn first_of_each_name(
    columns: impl Iterator<Item = (String, Option<f64>)>,
) -> Vec<(String, Option<f64>)> {
    let mut seen = HashSet::new();
    columns
        .filter(|(name, _)| seen.insert(name.clone()))
        .collect()
}

/// A number at its serialized precision; a non-finite one serializes as
/// null, so it is no number at all.
fn at_precision(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite()).map(rounded_2)
}

impl ReportSide {
    /// Read a profiled or loaded report.
    pub fn from_report(report: &ProfileReport) -> Self {
        let scan = scan_evidence(&report.execution);
        let quality = report.quality.as_ref();
        let scores = quality.map(|assessment| assessment.scores());
        let sampled = quality.map(|assessment| assessment.sampled_dimensions());
        let assessed = quality
            .map(|assessment| assessment.metrics.assessed_dimensions())
            .unwrap_or_default();
        let unlisted = quality
            .map(|assessment| assessment.unlisted_components())
            .unwrap_or_default();
        // No assessment means no score for evidence to describe; the scan's
        // coverage is all there is to say.
        let evidence = |provenance| match &sampled {
            Some(sampled) => scan.and(sampled_component_evidence(
                sampled.as_deref(),
                &unlisted,
                &assessed,
                provenance,
            )),
            None => scan,
        };
        let dimensions = QualityDimension::all()
            .into_iter()
            .map(|dimension| DimensionSide {
                dimension,
                score: at_precision(
                    scores
                        .as_ref()
                        .and_then(|scores| scores.dimension_scores.get(&dimension.to_string()))
                        .copied()
                        .flatten(),
                ),
                evidence: evidence(Provenance::Dimension(dimension)),
            })
            .collect();
        Self {
            reference: ReportReference {
                id: Some(report.id.clone()),
                timestamp: Some(report.timestamp.clone()),
                source: report.source_identifier(),
                rows_processed: Some(report.execution.rows_processed),
                evidence: scan,
            },
            quality_score: at_precision(scores.as_ref().and_then(|scores| scores.overall_score)),
            overall_evidence: evidence(Provenance::Overall),
            dimensions,
            columns: first_of_each_name(report.column_profiles.iter().map(|column| {
                let pct = (column.total_count > 0)
                    .then(|| column.null_count as f64 / column.total_count as f64 * 100.0);
                (column.name.clone(), at_precision(pct))
            })),
            metric_semantics: report.metric_semantics.clone(),
        }
    }

    /// Read a flat summary document.
    ///
    /// A field an older release did not write reads as unrecorded: scan
    /// coverage it does not state is `coverage_unrecorded`, never complete.
    /// Fails when the document lacks `source` or `columns`, or holds a
    /// field of the wrong type.
    pub fn from_summary_json(text: &str) -> serde_json::Result<Self> {
        let summary: Summary = serde_json::from_str(text)?;
        let execution = &summary.execution;
        let known_gap = scan_coverage(
            execution.source_exhausted == Some(false) || execution.truncation_reason.is_some(),
            execution.sampling_applied == Some(true),
            execution.error_count.is_some_and(|count| count > 0),
        );
        let recorded = execution.source_exhausted.is_some()
            && execution.sampling_applied.is_some()
            && execution.error_count.is_some();
        // A recorded gap is named; otherwise an unrecorded fact is a gap of
        // its own, because unknown coverage is not full coverage.
        let scan = match known_gap {
            Evidence::Complete if !recorded => Evidence::Incomplete {
                reason: EvidenceGap::CoverageUnrecorded,
            },
            known => known,
        };
        let quality = summary.quality.as_ref();
        // The dimensions the overall score weighs. An overall score always
        // weighs at least one, so an empty list beside a score was not
        // recorded: a missing list reads as empty here, as it does in the
        // Python loader.
        let assessed: Option<Vec<QualityDimension>> = quality.and_then(|quality| {
            let parsed: Vec<QualityDimension> = quality
                .assessed_dimensions
                .iter()
                // A dimension a later release added is dropped here: when it
                // was sampled, its component label is unknown and
                // `sampled_component_evidence` leaves the overall score
                // unrecorded; when it was exact it changes no coverage.
                .filter_map(|name| name.parse().ok())
                .collect();
            (!(parsed.is_empty() && quality.overall_score.is_some())).then_some(parsed)
        });
        let evidence = |provenance| {
            let Some(quality) = quality else {
                return scan;
            };
            let sampled = quality.sampled_dimensions.as_deref();
            let coverage = match (provenance, &assessed) {
                // Without the weights, a sampled component may or may not
                // reach the overall score; only "nothing was sampled" settles it.
                (Provenance::Overall, None) if sampled.is_some_and(|s| s.is_empty()) => {
                    Evidence::Complete
                }
                (Provenance::Overall, None) => Evidence::Incomplete {
                    reason: EvidenceGap::CoverageUnrecorded,
                },
                // A flat summary records no exact list, so no component can
                // be found missing from both: "not sampled" reads as exact
                // until the summary records one.
                (_, assessed) => sampled_component_evidence(
                    sampled,
                    &[],
                    assessed.as_deref().unwrap_or_default(),
                    provenance,
                ),
            };
            scan.and(coverage)
        };
        let dimensions = QualityDimension::all()
            .into_iter()
            .map(|dimension| DimensionSide {
                dimension,
                score: at_precision(
                    quality
                        .and_then(|quality| quality.dimension_scores.get(&dimension.to_string()))
                        .copied()
                        .flatten(),
                ),
                evidence: evidence(Provenance::Dimension(dimension)),
            })
            .collect();
        Ok(Self {
            reference: ReportReference {
                id: None,
                timestamp: None,
                source: summary.source,
                rows_processed: execution.rows_processed,
                evidence: scan,
            },
            quality_score: at_precision(quality.and_then(|quality| quality.overall_score)),
            overall_evidence: evidence(Provenance::Overall),
            dimensions,
            columns: first_of_each_name(
                summary
                    .columns
                    .into_iter()
                    .map(|column| (column.name, at_precision(column.null_percentage))),
            ),
            metric_semantics: summary.metric_semantics,
        })
    }
}

/// The fields of a flat summary a comparison reads. Everything else in the
/// document is ignored.
#[derive(serde::Deserialize)]
struct Summary {
    source: String,
    execution: SummaryExecution,
    columns: Vec<SummaryColumn>,
    quality: Option<SummaryQuality>,
    #[serde(default)]
    metric_semantics: Option<MetricSemantics>,
}

#[derive(serde::Deserialize)]
struct SummaryExecution {
    rows_processed: Option<usize>,
    source_exhausted: Option<bool>,
    truncation_reason: Option<String>,
    sampling_applied: Option<bool>,
    error_count: Option<usize>,
}

#[derive(serde::Deserialize)]
struct SummaryColumn {
    name: String,
    null_percentage: Option<f64>,
}

#[derive(serde::Deserialize)]
struct SummaryQuality {
    overall_score: Option<f64>,
    #[serde(default)]
    dimension_scores: BTreeMap<String, Option<f64>>,
    #[serde(default)]
    assessed_dimensions: Vec<String>,
    /// Absent in a summary written before dataprof recorded it, which reads
    /// as unrecorded coverage, never as "nothing was sampled".
    sampled_dimensions: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete() -> Sides<Evidence> {
        Sides {
            a: Evidence::Complete,
            b: Evidence::Complete,
        }
    }

    #[test]
    fn a_zero_baseline_has_an_absolute_change_and_no_relative_one() {
        let change = ScoreChange::new(Some(0.0), Some(50.0), complete());
        assert_eq!(change.abs, Some(50.0));
        assert_eq!(change.rel_pct, None);
    }

    #[test]
    fn a_change_is_rounded_to_the_serialized_precision() {
        // 80.1 - 80.0 is 0.0999... in binary; the contract states 0.1. The
        // relative change divides that same unrounded difference, as the
        // Python implementation that shipped did: 0.1249..., so 0.12.
        let change = ScoreChange::new(Some(80.0), Some(80.1), complete());
        assert_eq!(change.abs, Some(0.1));
        assert_eq!(change.rel_pct, Some(0.12));
    }

    #[test]
    fn a_missing_side_leaves_both_changes_absent() {
        for (a, b) in [(None, Some(1.0)), (Some(1.0), None), (None, None)] {
            let change = ScoreChange::new(a, b, complete());
            assert_eq!((change.abs, change.rel_pct), (None, None));
        }
    }

    #[test]
    fn a_non_finite_input_is_no_number() {
        assert_eq!(at_precision(Some(f64::NAN)), None);
        assert_eq!(at_precision(Some(f64::INFINITY)), None);
        assert_eq!(at_precision(Some(12.345)), Some(12.35));
    }
}
