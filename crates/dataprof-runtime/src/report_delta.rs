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
//! - **Configuration.** `configuration` lists each recorded setting that moves
//!   a score on unchanged data (the score weights, the semantic hints, the
//!   pattern locale and column selection) on both
//!   sides, with `same`: `Some(true)` when both recorded it and agree,
//!   `Some(false)` when they differ, `None` when either did not record it.
//!   Configuration is not comparability: a score that moved under a different
//!   setting may not reflect a change in the data. Each policy reads only the
//!   settings it depends on.
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

use dataprof_core::serde_helpers::rounded_2;
use dataprof_core::{
    DataSource, DataType, FileFormat, QualityDimension, QualityScoreWeights, SemanticHintBinding,
    SemanticHintKind,
};
use serde::ser::{SerializeMap, Serializer};

use crate::profile_report::{
    AnalysisSettings, ColumnSelection, MetricSemantics, PatternLocale, ProfileReport,
};
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
    /// The inferred type in `a`, serialized as its lowercase name. `None`
    /// when the column is absent, empty or all-null, or when its type or
    /// counts were not recorded.
    #[serde(serialize_with = "serialize_data_type")]
    pub data_type_a: Option<DataType>,
    /// The inferred type in `b`, under the same rules.
    #[serde(serialize_with = "serialize_data_type")]
    pub data_type_b: Option<DataType>,
}

fn serialize_data_type<S: Serializer>(
    data_type: &Option<DataType>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serde::Serialize::serialize(&data_type.as_ref().map(DataType::name), serializer)
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

/// One recorded setting on each side, and whether the two agree.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct SettingComparison<T> {
    /// What `a` recorded, or `None` when it did not record the setting.
    pub a: Option<T>,
    /// What `b` recorded.
    pub b: Option<T>,
    /// `Some(true)` when both recorded the setting and it is the same,
    /// `Some(false)` when it differs, `None` when either side did not
    /// record it.
    pub same: Option<bool>,
}

impl<T: PartialEq> SettingComparison<T> {
    fn new(a: Option<T>, b: Option<T>) -> Self {
        let same = match (&a, &b) {
            (Some(left), Some(right)) => Some(left == right),
            _ => None,
        };
        Self { a, b, same }
    }
}

/// The semantic hints a report recorded, by the configuration field that
/// carries each, read from its hint bindings.
///
/// Each list is sorted and keeps the repetitions a report recorded. A report
/// written since #1004 records a repeated column once, but one written before
/// it records the column twice and scanned it twice, which moved the
/// timeliness score, so such a report and one listing the column once are not
/// the same configuration.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct RecordedHints {
    /// Columns named by `positive_columns`, sorted.
    pub positive_columns: Vec<String>,
    /// Columns named by `identifier_columns`, sorted.
    pub identifier_columns: Vec<String>,
    /// Columns named by `temporal_columns`, sorted.
    pub temporal_columns: Vec<String>,
    /// Columns named by `temporal_pairs`, sorted. A report records which
    /// columns the pairs named, not which column pairs with which.
    pub temporal_pair_columns: Vec<String>,
}

impl RecordedHints {
    fn from_bindings(bindings: &[SemanticHintBinding]) -> Self {
        let columns = |kind: SemanticHintKind| {
            let mut names: Vec<String> = bindings
                .iter()
                .filter(|binding| binding.kind == kind)
                .map(|binding| binding.column.clone())
                .collect();
            names.sort();
            names
        };
        Self {
            positive_columns: columns(SemanticHintKind::Positive),
            identifier_columns: columns(SemanticHintKind::Identifier),
            temporal_columns: columns(SemanticHintKind::Temporal),
            temporal_pair_columns: columns(SemanticHintKind::TemporalPair),
        }
    }
}

/// The recorded settings that move a score on unchanged data, on each side.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct ConfigurationComparison {
    /// The weights behind the overall score. `None` on a side with no quality
    /// assessment, or on a flat summary written before 0.12, which could drop
    /// custom weights.
    pub score_weights: SettingComparison<QualityScoreWeights>,
    /// The semantic hints. `None` on a side written before 0.10, which did
    /// not record them.
    pub semantic_hints: SettingComparison<RecordedHints>,
    /// The locale detected patterns were ranked with, which the validity
    /// score reads. `None` on a side written before dataprof recorded it.
    pub pattern_locale: SettingComparison<PatternLocale>,
    /// The column selection, preserving the order of listed names. `None`
    /// on a side that did not record its analysis settings.
    pub columns: SettingComparison<ColumnSelection>,
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
    /// The settings each report was profiled with that move its scores.
    pub configuration: ConfigurationComparison,
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

        let a_names: HashSet<&str> = a
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        let b_names: HashSet<&str> = b
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        let a_columns: BTreeMap<&str, &ColumnSide> = a
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column))
            .collect();
        let b_columns: BTreeMap<&str, &ColumnSide> = b
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column))
            .collect();
        let added: Vec<String> = b
            .columns
            .iter()
            .filter(|column| !a_names.contains(column.name.as_str()))
            .map(|column| column.name.clone())
            .collect();
        let columns = a
            .columns
            .iter()
            .map(|column| &column.name)
            .chain(&added)
            .map(|name| {
                let a = a_columns.get(name.as_str());
                let b = b_columns.get(name.as_str());
                let null_pct_a = a.and_then(|column| column.null_pct);
                let null_pct_b = b.and_then(|column| column.null_pct);
                ColumnChange {
                    name: name.clone(),
                    null_pct_a,
                    null_pct_b,
                    null_pct_delta: null_pct_a.zip(null_pct_b).map(|(a, b)| rounded_2(b - a)),
                    data_type_a: a
                        .filter(|column| column.populated() == Some(true))
                        .and_then(|column| column.data_type.clone()),
                    data_type_b: b
                        .filter(|column| column.populated() == Some(true))
                        .and_then(|column| column.data_type.clone()),
                }
            })
            .collect();
        let (removed, common) = a
            .columns
            .iter()
            .map(|column| column.name.clone())
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
            configuration: ConfigurationComparison {
                score_weights: SettingComparison::new(a.score_weights, b.score_weights),
                semantic_hints: SettingComparison::new(
                    a.semantic_hints.clone(),
                    b.semantic_hints.clone(),
                ),
                pattern_locale: SettingComparison::new(a.pattern_locale, b.pattern_locale),
                columns: SettingComparison::new(
                    a.column_selection.clone(),
                    b.column_selection.clone(),
                ),
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
    columns: Vec<ColumnSide>,
    metric_semantics: Option<MetricSemantics>,
    score_weights: Option<QualityScoreWeights>,
    semantic_hints: Option<RecordedHints>,
    /// The `identifier_columns` hint in the order it was given, which
    /// [`RecordedHints`] sorts away. The first one present is the key the
    /// uniqueness score reads, so the quality gate compares this order.
    identifier_order: Option<Vec<String>>,
    pattern_locale: Option<PatternLocale>,
    /// Which columns the side profiled, `None` when it did not record it.
    column_selection: Option<ColumnSelection>,
    /// Whether the source declares its columns up front, so the column set
    /// does not depend on how many rows were read.
    schema_declared: bool,
}

/// The identifier hint's columns in binding order, the order they were given.
fn identifier_order(bindings: &[SemanticHintBinding]) -> Vec<String> {
    bindings
        .iter()
        .filter(|binding| binding.kind == SemanticHintKind::Identifier)
        .map(|binding| binding.column.clone())
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
struct DimensionSide {
    dimension: QualityDimension,
    score: Option<f64>,
    evidence: Evidence,
}

/// What a comparison reads about one column.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnSide {
    pub(crate) name: String,
    /// The null percentage at its serialized precision, `None` when the
    /// column holds no values.
    pub(crate) null_pct: Option<f64>,
    /// The inferred type, `None` on a flat summary that did not record one
    /// this build knows.
    pub(crate) data_type: Option<DataType>,
    /// `(total_count, null_count)`, `None` on a flat summary that did not
    /// record both. Exact where the rounded percentage is not: 99.999% null
    /// rounds to 100.00 and still holds a value.
    pub(crate) counts: Option<(usize, usize)>,
}

impl ColumnSide {
    /// At least one value read for the column is not null. `None` when the
    /// counts were not recorded.
    pub(crate) fn populated(&self) -> Option<bool> {
        self.counts.map(|(total, nulls)| nulls < total)
    }

    /// Values were read for the column and every one is null. `None` when
    /// the counts were not recorded.
    pub(crate) fn all_null(&self) -> Option<bool> {
        self.counts
            .map(|(total, nulls)| total > 0 && nulls == total)
    }
}

/// One entry per column name, in order, holding the first profile of a
/// repeated name. Most input paths refuse duplicate names, but a report
/// rebuilt from a document can carry them, and the quality gate reads the
/// first profile too, so the two never disagree about which one a name means.
fn first_of_each_name(columns: impl Iterator<Item = ColumnSide>) -> Vec<ColumnSide> {
    let mut seen = HashSet::new();
    columns
        .filter(|column| seen.insert(column.name.clone()))
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
                ColumnSide {
                    name: column.name.clone(),
                    null_pct: at_precision(pct),
                    data_type: Some(column.data_type.clone()),
                    counts: Some((column.total_count, column.null_count)),
                }
            })),
            metric_semantics: report.metric_semantics.clone(),
            // The weights behind the score this build reports: recorded, or
            // the defaults, which did not change from 0.10 (when weights
            // became configurable) to 0.12, and which a document written
            // before 0.10 has its scores recomputed with.
            score_weights: quality.map(|assessment| assessment.metrics.score_weights),
            // Bindings are recorded from 0.10 (schema version 1) on, and an
            // empty list there means no hints were given.
            semantic_hints: (report.schema_version >= 1)
                .then(|| RecordedHints::from_bindings(&report.semantic_hint_bindings)),
            identifier_order: (report.schema_version >= 1)
                .then(|| identifier_order(&report.semantic_hint_bindings)),
            pattern_locale: report
                .analysis_settings
                .as_ref()
                .map(|settings| settings.pattern_locale),
            column_selection: report
                .analysis_settings
                .as_ref()
                .map(|settings| settings.columns.clone()),
            schema_declared: declares_schema(&report.data_source),
        }
    }

    /// The identifier hint in the order it was given, or `None` when the side
    /// did not record its hints.
    pub(crate) fn identifier_order(&self) -> Option<&[String]> {
        self.identifier_order.as_deref()
    }

    /// Which columns the side profiled, or `None` when it did not record it.
    pub(crate) fn column_selection(&self) -> Option<&ColumnSelection> {
        self.column_selection.as_ref()
    }

    /// Whether the side's column set covers its whole source. A source that
    /// declares its columns up front (a CSV header, a Parquet schema, a
    /// DataFrame, a query result) lists them however few rows were read;
    /// otherwise the columns are the keys of the records read, and only a scan
    /// of every record covers the source.
    pub(crate) fn schema_evidence(&self) -> Evidence {
        if self.schema_declared {
            Evidence::Complete
        } else {
            self.reference.evidence
        }
    }

    /// The side's columns, one per name, in its order.
    pub(crate) fn columns(&self) -> &[ColumnSide] {
        &self.columns
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
            columns: first_of_each_name(summary.columns.into_iter().map(|column| ColumnSide {
                name: column.name,
                null_pct: at_precision(column.null_percentage),
                // A name this build does not know comes from a later release and
                // reads as unrecorded, since this build has no type to name it with.
                data_type: column.data_type.as_deref().and_then(DataType::from_name),
                counts: column.total_count.zip(column.null_count),
            })),
            // A 0.12 summary, the first to record `metric_semantics`, omitted
            // only default weights; earlier ones could drop custom weights too.
            score_weights: quality.and_then(|quality| {
                quality.score_weights.or_else(|| {
                    summary
                        .metric_semantics
                        .is_some()
                        .then(QualityScoreWeights::default)
                })
            }),
            semantic_hints: summary
                .schema_version
                .is_some_and(|version| version >= 1)
                .then(|| RecordedHints::from_bindings(&summary.semantic_hint_bindings)),
            identifier_order: summary
                .schema_version
                .is_some_and(|version| version >= 1)
                .then(|| identifier_order(&summary.semantic_hint_bindings)),
            metric_semantics: summary.metric_semantics,
            pattern_locale: summary
                .analysis_settings
                .as_ref()
                .map(|settings| settings.pattern_locale),
            column_selection: summary.analysis_settings.map(|settings| settings.columns),
            // A summary records the kind of source but not a file's format,
            // so only the kinds that always declare their columns count.
            schema_declared: matches!(
                summary.source_type,
                Some(SummarySourceType::Dataframe | SummarySourceType::Query)
            ),
        })
    }
}

/// The field's absence represents a legacy summary; JSON null is malformed.
/// Include its name because serde_json's default type error omits the key.
fn summary_metric_semantics<'de, D>(deserializer: D) -> Result<Option<MetricSemantics>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::profile_report::present_value(deserializer).map_err(|error| {
        <D::Error as serde::de::Error>::custom(format!("metric_semantics: {error}"))
    })
}

fn summary_schema_version<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::profile_report::present_value(deserializer)
        .map_err(|error| <D::Error as serde::de::Error>::custom(format!("schema_version: {error}")))
}

/// An absent value belongs to a legacy summary; an explicit null does not.
fn summary_analysis_settings<'de, D>(deserializer: D) -> Result<Option<AnalysisSettings>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::profile_report::present_value(deserializer).map_err(|error| {
        <D::Error as serde::de::Error>::custom(format!("analysis_settings: {error}"))
    })
}

/// Only an absent key may use the summary version's legacy weight semantics.
fn summary_score_weights<'de, D>(deserializer: D) -> Result<Option<QualityScoreWeights>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::profile_report::present_value(deserializer).map_err(|error| {
        <D::Error as serde::de::Error>::custom(format!("quality.score_weights: {error}"))
    })
}

/// The fields of a flat summary a comparison reads. Everything else in the
/// document is ignored.
#[derive(serde::Deserialize)]
struct Summary {
    source: String,
    /// Absent from a summary that did not record it, which declares nothing.
    #[serde(default)]
    source_type: Option<SummarySourceType>,
    execution: SummaryExecution,
    columns: Vec<SummaryColumn>,
    quality: Option<SummaryQuality>,
    #[serde(default, deserialize_with = "summary_metric_semantics")]
    metric_semantics: Option<MetricSemantics>,
    /// Absent before 0.10, whose summaries did not record hints either.
    #[serde(default, deserialize_with = "summary_schema_version")]
    schema_version: Option<u32>,
    #[serde(default)]
    semantic_hint_bindings: Vec<SemanticHintBinding>,
    /// Absent before 0.13. An explicit null is malformed, as in the
    /// canonical reader and Python's `from_dict()`.
    #[serde(default, deserialize_with = "summary_analysis_settings")]
    analysis_settings: Option<AnalysisSettings>,
}

/// A flat summary's `source_type`. A kind a later release adds declares
/// nothing this build can rely on.
#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum SummarySourceType {
    Dataframe,
    Query,
    #[serde(other)]
    Other,
}

/// Whether a source declares its columns up front. A JSON, JSONL or stream
/// source has the keys of the records read, and a format this build does not
/// know declares nothing it can rely on.
fn declares_schema(source: &DataSource) -> bool {
    match source {
        DataSource::File { format, .. } | DataSource::Bytes { format, .. } => {
            matches!(format, FileFormat::Csv | FileFormat::Parquet)
        }
        DataSource::DataFrame { .. } | DataSource::Query { .. } => true,
        DataSource::Stream { .. } => false,
    }
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
    #[serde(default)]
    data_type: Option<String>,
    #[serde(default)]
    total_count: Option<usize>,
    #[serde(default)]
    null_count: Option<usize>,
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
    /// Written whatever the weights since 0.13; a 0.12 summary omitted only
    /// the defaults, and earlier ones did not record them. Explicit null is
    /// malformed, as in Python's `from_dict()`.
    #[serde(default, deserialize_with = "summary_score_weights")]
    score_weights: Option<QualityScoreWeights>,
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
