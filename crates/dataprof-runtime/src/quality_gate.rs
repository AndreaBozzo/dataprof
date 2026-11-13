//! Declarative quality gates over a [`ProfileReport`].
//!
//! A pipeline that wants to stop on a bad extract has to turn a report into a
//! decision. Doing that by hand means re-deciding, at every call site, what an
//! absent metric means, whether a sampled scan can answer a question about the
//! whole source, and which number belongs in the log line. This module makes
//! that decision data: a [`QualityPolicy`] states the requirements and
//! [`QualityPolicy::evaluate`] returns a [`GateResult`] describing what was
//! checked and what happened.
//!
//! Evaluation never exits the process, prints, or mutates the report.
//!
//! # Verdicts and evidence
//!
//! The verdict is three-valued, because a decision and the evidence behind it
//! are separate facts. A requirement about the full source cannot be *passed*
//! by a truncated or sampled scan: nothing was observed that rules the failure
//! out. It can still be *failed* by one, when the scan witnessed a violation
//! that more scanning cannot retract. So:
//!
//! - [`Verdict::Fail`] — at least one requirement was conclusively violated.
//! - [`Verdict::Inconclusive`] — nothing was violated, and at least one
//!   requirement could not be evaluated (a metric was not analyzed, a column
//!   was not profiled, the evidence does not reach as far as the requirement).
//! - [`Verdict::Pass`] — every requirement was evaluated and met.
//!
//! `Fail` wins over `Inconclusive`: a witnessed violation is a decision.
//!
//! # Baseline-relative rules
//!
//! [`QualityPolicy::evaluate_against`] also reads a saved baseline, through the
//! [`ReportDelta`] between it and the report, and evaluates rules about change:
//! how far the overall score or a dimension score may drop, and how far a
//! column's null percentage may rise, in points, how far a column's
//! distinct count may fall, in percent of the baseline's (#1026), and how far
//! the row count may fall or grow, in percent of the baseline's (#1291). A
//! change is decided only when both sides measured the same thing the same
//! way. Otherwise the rule is left unevaluated, naming what differs:
//!
//! - for the validity score, and for the overall score when validity is
//!   assessed on one side only, pattern detection run on one side only
//!   ([`NotEvaluated::SettingChanged`] on `metric_packs`): validity scores
//!   the detected patterns. This names the cause ahead of the reasons below
//!   that name its effect, and needs both sides to record their packs;
//! - a value the baseline does not carry ([`NotEvaluated::NotInBaseline`]);
//! - for a distinct count, an estimate on either side, or a count either
//!   side did not record as exact ([`NotEvaluated::ApproximateCount`]);
//! - for a score or the row count, a different column set
//!   ([`NotEvaluated::SchemaChanged`]). A dataset-level rule has the columns
//!   as its subject, so a column that appeared or vanished changes what is
//!   measured, not how good it is. A
//!   column's own rules stay decisive, and a new column has no baseline rule
//!   until a policy names it;
//! - for the overall score, a different set of assessed dimensions
//!   ([`NotEvaluated::DimensionsChanged`]), since it averages those;
//! - a measurement definition the rule depends on that the two sides do not
//!   both record identically ([`NotEvaluated::DefinitionNotComparable`]);
//! - a setting that moves the score on unchanged data, such as the score
//!   weights or a semantic hint, that differs or that either side did not
//!   record ([`NotEvaluated::SettingChanged`],
//!   [`NotEvaluated::SettingUnrecorded`]). The declared temporal pairs are
//!   compared as pairs when both sides record them (0.14+), and otherwise
//!   agree only when neither side declared any. The timeliness clock is such
//!   a setting too: two
//!   reports measured against different reference dates are comparable only
//!   when the policy declares a rolling clock
//!   ([`QualityPolicy::timeliness_clock`]);
//! - under [`PolicyScope::FullSource`], evidence on either side that does not
//!   cover the whole source. The row count rules are the exception: a scan
//!   that stopped early read a lower bound of its source's rows, so it still
//!   settles the outcome that more rows could not reverse, as
//!   [`QualityPolicy::min_rows`] does (see
//!   [`QualityPolicy::max_row_count_drop`]). A score drop rule reads a score
//!   computed over a retained sample of a fully read source as its recorded
//!   whole-source interval, as the minimum score rules do, and decides when
//!   the change those intervals allow lies wholly on one side of the
//!   allowance (see [`QualityPolicy::max_quality_score_drop`]).
//!
//! A schema change from the baseline is reported as a [`Diagnostic`] in the
//! result's `baseline.diagnostics`, whatever the policy states: a column added,
//! removed, retyped while holding values on both sides, or holding only nulls
//! where the baseline had values. A diagnostic is a warning, never a fourth
//! verdict, and does not move the verdict.
//!
//! Rules about the column set decide on whether a column is there.
//! [`QualityPolicy::require_columns`] names columns the report must have;
//! [`QualityPolicy::forbid_removed_columns`] requires every baseline column,
//! and [`QualityPolicy::forbid_added_columns`] allows no column the baseline
//! lacked. A missing column fails such a rule only when the side without it
//! shows its source lacks it: that side recorded its column selection, the
//! selection included the column, and its column set covers the whole source.
//! A source that declares its columns up front (a CSV header, a Parquet
//! schema, a DataFrame, a query result) covers it however few rows were read;
//! for JSON, JSONL and streams the columns are the keys of the records read,
//! so only a scan of every record does. Otherwise the rule is unevaluated:
//! [`NotEvaluated::SettingChanged`] or [`NotEvaluated::SettingUnrecorded`] on
//! the setting `columns`, [`NotEvaluated::ColumnNotProfiled`] for a required
//! column the run did not select, or [`NotEvaluated::EvidenceIncomplete`].
//!
//! # Freshness
//!
//! [`QualityPolicy::max_date_age`] fails a column whose newest date lies too
//! many days before the timeliness reference date the report recorded
//! (#1290): a feed that stopped moving keeps passing every rule above, since
//! the same rows keep their counts, nulls and scores. A newest date read on
//! part of a source can only be overtaken by a newer one, so a fresh date
//! passes on any scan and a stale one fails only when the values behind it
//! cover the column. Slash dates are the exception: their day and month
//! order is decided from the values read, so a fresh one read on part of a
//! column passes only once that order cannot change.
//!
//! # Forbidden findings
//!
//! [`QualityPolicy::forbid_findings`] turns a findings code into a rule: the
//! report must hold no finding with it (#1066). The findings are derived as
//! [`ProfileReport::findings`] derives them, and a failed check carries them,
//! column and evidence included. How far a partial scan answers the rule
//! under [`PolicyScope::FullSource`] depends on what the finding is about:
//!
//! - how the source was read (`partial_scan`, `delimiter_ambiguous`,
//!   `header_not_split`): decided whatever the scan covered;
//! - something rows witness (`duplicate_rows`, `key_duplicates`,
//!   `future_dates`, `temporal_order_violations`, `ragged_rows`,
//!   `records_skipped`, `unterminated_quote`): a finding fails, and no finding
//!   passes only a scan that covered the source, as with `max_duplicate_rows`;
//! - a share of a column's values (`null_heavy`, `all_null`,
//!   `constant_column`, `mixed_types`, `locale_numbers`, `sensitive_pattern`):
//!   decided only on a scan that covered the source, as with the score rules.
//!   `mixed_types`, `locale_numbers` and `sensitive_pattern` read each
//!   column's retained value sample, so on a source larger than the sample
//!   they stay `quality_sampled` even when every row was read.
//!
//! A findings rule that could not look at all of the report and found nothing
//! leaves the check [`NotEvaluated::FindingNotEvaluated`], carrying the
//! rule's own reasons.
//!
//! # Example
//!
//! ```no_run
//! use dataprof_runtime::{ProfileReport, QualityPolicy, Verdict};
//!
//! fn gate(report: &ProfileReport) -> Result<bool, Box<dyn std::error::Error>> {
//!     let result = QualityPolicy::new()
//!         .min_quality_score(90.0)
//!         .max_null_count("customer_id", 0)
//!         .max_null_percentage_any(20.0)
//!         .require_quality()
//!         .evaluate(report)?;
//!
//!     if result.verdict != Verdict::Pass {
//!         for violation in result.violations() {
//!             eprintln!("{}: {}", violation.code, violation.message);
//!         }
//!     }
//!     Ok(result.passed())
//! }
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use dataprof_core::serde_helpers::rounded_2;
use dataprof_core::{
    ColumnProfile, ColumnStats, DataType, ExecutionMetadata, MetricPack, QualityDimension,
    SlashDateOrder,
};
use dataprof_metrics::{
    QualityAssessment, ScoreBounds, ScoreInterval, TimelinessClock, TimelinessClockMode,
};
use serde::Serialize;

use crate::findings::{
    Finding, FindingCode, FindingPolicy, FindingsInput, FindingsResult, UnevaluatedRule,
};
use crate::profile_report::{
    ColumnSelection, MetricSemantics, ProfileReport, QualityAnalysisStatus, ReportGenerator,
    SourceIdentity,
};
use crate::report_delta::{
    ColumnChange, ColumnSide, ConfigurationComparison, RecordedHints, ReportDelta, ReportPair,
    ReportSide, SchemaChange, ScoreChange, Sides,
};

/// Scores and percentages in a report live on a 0..=100 scale, and so do the
/// thresholds compared against them.
const PERCENTAGE_MAX: f64 = 100.0;

/// A policy that cannot be evaluated as written.
///
/// Raised before any requirement is checked: an unsatisfiable threshold is a
/// configuration mistake, and reporting it as a failed gate would blame the
/// data for it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PolicyError {
    /// A threshold on the 0..=100 percentage scale fell outside it, or was not
    /// a finite number. Quality scores and null percentages are percentages,
    /// not 0..1 ratios. A row count growth threshold has no upper end, so for
    /// [`CheckCode::MaxRowCountGrowth`] only a negative or non-finite value
    /// is out of range.
    #[non_exhaustive]
    ThresholdOutOfRange {
        /// The requirement that carried it.
        code: CheckCode,
        /// The column or dimension the requirement names, when it names one.
        subject: Option<String>,
        /// The value as supplied.
        value: f64,
    },
    /// The policy states no requirement at all. An empty policy passes every
    /// report, which is never what a gate is for.
    NoRequirements,
    /// The policy states a rule about change, and was evaluated without a
    /// baseline to measure the change from. Use
    /// [`QualityPolicy::evaluate_against`].
    BaselineRequired,
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ThresholdOutOfRange {
                code,
                subject,
                value,
            } => {
                write!(f, "{code} threshold ")?;
                if let Some(subject) = subject {
                    write!(f, "for `{subject}` ")?;
                }
                if *code == CheckCode::MaxRowCountGrowth {
                    write!(f, "must be a percentage of at least 0, got {value}")
                } else {
                    write!(
                        f,
                        "must be a percentage between 0 and {PERCENTAGE_MAX}, got {value}"
                    )
                }
            }
            Self::NoRequirements => write!(
                f,
                "policy states no requirement; a gate must check something"
            ),
            Self::BaselineRequired => write!(
                f,
                "policy states a rule about change, which needs a baseline report to compare \
                 against"
            ),
        }
    }
}

impl std::error::Error for PolicyError {}

/// How a rule about change reads the timeliness clocks of the two reports
/// (#1024).
///
/// The timeliness score counts dates later than a reference date and dates
/// older than a maximum age before it, so the same data scores differently
/// against another date. Every report records its clock, and the policy
/// states which difference between two clocks still measures the same thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TimelinessClockRule {
    /// Both reports must record the same reference date, time zone and
    /// maximum age. The default, and what a pipeline that passes a fixed
    /// `reference_date` to every run wants.
    #[default]
    Fixed,
    /// Each report measures against its own run date: when both read a
    /// rolling clock, in the same time zone and with the same maximum age,
    /// their reference dates may differ and the scores are compared as
    /// measured. A fixed clock on either side still needs the same date on
    /// both.
    Rolling,
}

impl fmt::Display for TimelinessClockRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fixed => write!(f, "fixed"),
            Self::Rolling => write!(f, "rolling"),
        }
    }
}

impl std::str::FromStr for TimelinessClockRule {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fixed" => Ok(Self::Fixed),
            "rolling" => Ok(Self::Rolling),
            _ => Err(format!(
                "Unknown timeliness clock: {s}. Valid clocks: fixed, rolling"
            )),
        }
    }
}

/// What the requirements in a policy are statements about.
///
/// A profiler reads what it is given. Whether that supports a claim about the
/// whole source depends on what is being claimed, so the policy says which
/// question it asks rather than letting the evaluator guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PolicyScope {
    /// The entire source. A requirement is evaluated only when the evidence
    /// covers it, or when the observed data already witnesses a violation that
    /// further scanning cannot retract.
    #[default]
    FullSource,
    /// Whatever the metric actually measured — the scanned rows, the retained
    /// quality sample, the profiled columns. Always evaluable, and says
    /// nothing about the rows that were not read.
    Observed,
}

impl fmt::Display for PolicyScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FullSource => write!(f, "full_source"),
            Self::Observed => write!(f, "observed"),
        }
    }
}

impl std::str::FromStr for PolicyScope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "full_source" => Ok(Self::FullSource),
            "observed" => Ok(Self::Observed),
            _ => Err(format!(
                "Unknown policy scope: {s}. Valid scopes: full_source, observed"
            )),
        }
    }
}

/// Why the data behind a number falls short of the whole source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EvidenceGap {
    /// Profiling stopped before the source was exhausted.
    Truncated,
    /// Rows were sampled rather than all read.
    Sampled,
    /// Records were skipped because they could not be turned into rows.
    RecordsSkipped,
    /// Every row was scanned, but this metric was computed from a retained
    /// sample of them.
    QualitySampled,
    /// The report does not record how its numbers were obtained: which quality
    /// components were sampled, or, in a flat summary from an older release,
    /// whether the scan read the whole source. Only reachable for a report
    /// read back from a document written before dataprof recorded it; a
    /// profiling run always records it. Unknown coverage is not full
    /// coverage, so a full-source requirement is left unevaluated.
    CoverageUnrecorded,
}

impl fmt::Display for EvidenceGap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated"),
            Self::Sampled => write!(f, "sampled"),
            Self::RecordsSkipped => write!(f, "records_skipped"),
            Self::QualitySampled => write!(f, "quality_sampled"),
            Self::CoverageUnrecorded => write!(f, "coverage_unrecorded"),
        }
    }
}

/// Whether the data behind a number covers the whole source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "coverage", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Evidence {
    /// Every row of the source is behind the number.
    Complete,
    /// Some of the source is not, for this reason.
    #[non_exhaustive]
    Incomplete {
        /// The gap between what was read and the whole source.
        reason: EvidenceGap,
    },
}

impl Evidence {
    /// True when the whole source is behind the number.
    pub fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }

    /// The gap, when there is one.
    pub fn gap(self) -> Option<EvidenceGap> {
        match self {
            Self::Complete => None,
            Self::Incomplete { reason } => Some(reason),
        }
    }

    /// The weaker of two evidence statements: a number is only as complete as
    /// the least complete input behind it. The left-hand gap wins a tie, so
    /// the scan's gap is named ahead of the quality sample's.
    pub(crate) fn and(self, other: Self) -> Self {
        match self {
            Self::Complete => other,
            incomplete => incomplete,
        }
    }
}

/// Stable identifier for the requirement a check came from.
///
/// Part of the serialized contract: a CI system matches on these, so they do
/// not change when the prose does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckCode {
    /// The scan must have read at least the threshold's number of rows.
    MinRows,
    /// The overall quality score must be at least the threshold.
    MinQualityScore,
    /// One ISO 25012 dimension score must be at least the threshold.
    MinDimensionScore,
    /// A column's null percentage must be at most the threshold.
    MaxNullPercentage,
    /// A column's null count must be at most the threshold.
    MaxNullCount,
    /// The duplicate-row count must be at most the threshold.
    MaxDuplicateRows,
    /// A column's newest date may lie at most the threshold's number of days
    /// before the timeliness reference date.
    MaxDateAge,
    /// A metric must have been analyzed at all.
    RequireMetric,
    /// The overall quality score may drop at most the threshold's number of
    /// points below the baseline's.
    MaxQualityScoreDrop,
    /// One dimension score may drop at most the threshold's number of points
    /// below the baseline's.
    MaxDimensionScoreDrop,
    /// The row count may fall at most the threshold's percentage of the
    /// baseline's.
    MaxRowCountDrop,
    /// The row count may grow at most the threshold's percentage of the
    /// baseline's.
    MaxRowCountGrowth,
    /// A column's null percentage may rise at most the threshold's number of
    /// points above the baseline's.
    MaxNullPercentageIncrease,
    /// A column's distinct count may fall at most the threshold's percentage
    /// of the baseline's, unless its distinct share of non-null values held.
    MaxDistinctDrop,
    /// A named column must be profiled.
    RequireColumn,
    /// A column the baseline profiled must still be profiled.
    ForbidRemovedColumn,
    /// A column the report profiled must be one the baseline profiled.
    ForbidAddedColumn,
    /// The report must hold no finding with the named code.
    ForbidFinding,
}

impl fmt::Display for CheckCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MinRows => write!(f, "min_rows"),
            Self::MinQualityScore => write!(f, "min_quality_score"),
            Self::MinDimensionScore => write!(f, "min_dimension_score"),
            Self::MaxNullPercentage => write!(f, "max_null_percentage"),
            Self::MaxNullCount => write!(f, "max_null_count"),
            Self::MaxDuplicateRows => write!(f, "max_duplicate_rows"),
            Self::MaxDateAge => write!(f, "max_date_age"),
            Self::RequireMetric => write!(f, "require_metric"),
            Self::MaxQualityScoreDrop => write!(f, "max_quality_score_drop"),
            Self::MaxDimensionScoreDrop => write!(f, "max_dimension_score_drop"),
            Self::MaxRowCountDrop => write!(f, "max_row_count_drop"),
            Self::MaxRowCountGrowth => write!(f, "max_row_count_growth"),
            Self::MaxNullPercentageIncrease => write!(f, "max_null_percentage_increase"),
            Self::MaxDistinctDrop => write!(f, "max_distinct_drop"),
            Self::RequireColumn => write!(f, "require_column"),
            Self::ForbidRemovedColumn => write!(f, "forbid_removed_column"),
            Self::ForbidAddedColumn => write!(f, "forbid_added_column"),
            Self::ForbidFinding => write!(f, "forbid_finding"),
        }
    }
}

/// A number a check read, or a threshold it compared against.
///
/// Percentages follow the report's 2dp serialization convention; counts are
/// whole numbers and are not widened into rounded floats.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum MetricValue {
    /// A count of rows or values.
    Count(usize),
    /// A 0..=100 percentage or score, or a change in one, in points.
    Percentage(#[serde(serialize_with = "dataprof_core::serde_helpers::round_2")] f64),
    /// A number of whole days. An age is negative when the date lies after
    /// the reference date.
    Days(i64),
}

impl fmt::Display for MetricValue {
    /// For logs and messages, not for the serialized contract: percentages
    /// print at the report's 2dp convention, counts as whole numbers.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(count) => write!(f, "{count}"),
            Self::Percentage(value) => write!(f, "{value:.2}"),
            Self::Days(days) => write!(f, "{days}"),
        }
    }
}

/// The constraint a check applied.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(tag = "comparison", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Expectation {
    /// The observed value must be at least `value`.
    #[non_exhaustive]
    AtLeast {
        /// The threshold.
        value: MetricValue,
    },
    /// The observed value must be at most `value`.
    #[non_exhaustive]
    AtMost {
        /// The threshold.
        value: MetricValue,
    },
    /// The metric must have been analyzed. No number is compared.
    Analyzed,
    /// The change from the baseline, `observed` minus the baseline's value,
    /// must be at least `value` points. A permitted drop is a negative value.
    #[non_exhaustive]
    ChangeAtLeast {
        /// The threshold, in points.
        value: MetricValue,
    },
    /// The change from the baseline must be at most `value` points.
    #[non_exhaustive]
    ChangeAtMost {
        /// The threshold, in points.
        value: MetricValue,
    },
    /// The change from the baseline as a percentage of the baseline's value,
    /// `(observed - baseline) / baseline * 100`, must be at least `value`. A
    /// permitted drop is a negative value.
    #[non_exhaustive]
    RelativeChangeAtLeast {
        /// The threshold, in percent of the baseline's value.
        value: MetricValue,
    },
    /// The change from the baseline as a percentage of the baseline's value
    /// must be at most `value`.
    #[non_exhaustive]
    RelativeChangeAtMost {
        /// The threshold, in percent of the baseline's value.
        value: MetricValue,
    },
    /// The column must be profiled. No number is compared.
    Present,
    /// The column must be one the baseline profiled. No number is compared.
    InBaseline,
}

impl fmt::Display for Expectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtLeast { value } => write!(f, "at least {value}"),
            Self::AtMost { value } => write!(f, "at most {value}"),
            Self::Analyzed => write!(f, "analyzed"),
            Self::ChangeAtLeast { value } => write!(f, "a change of at least {value}"),
            Self::ChangeAtMost { value } => write!(f, "a change of at most {value}"),
            Self::RelativeChangeAtLeast { value } => {
                write!(f, "a relative change of at least {value}%")
            }
            Self::RelativeChangeAtMost { value } => {
                write!(f, "a relative change of at most {value}%")
            }
            Self::Present => write!(f, "present"),
            Self::InBaseline => write!(f, "in the baseline"),
        }
    }
}

/// Why a requirement was not evaluated.
///
/// Absence is never read as zero and never as a pass. Each variant names a
/// different thing a caller may want to fix: a run that did not ask for the
/// metric, a dimension with nothing to measure, a column that was not
/// profiled, or a scan that does not reach as far as the requirement does.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[non_exhaustive]
pub enum NotEvaluated {
    /// The report carries no quality assessment, for this recorded reason.
    #[non_exhaustive]
    QualityUnavailable {
        /// The report's `quality_status` state, verbatim.
        quality_status: String,
    },
    /// Quality was computed, but this dimension had nothing to assess — no
    /// numeric values, no dates, no confidently detected pattern — or it was
    /// withheld because the run profiled a subset of columns.
    NotAssessed,
    /// The report holds no profile for the named column, and its column
    /// selection does not show the source lacks it: the run's `columns=`
    /// selection left it out, or the report predates the record of its
    /// selection. On a check with no column, the run profiled a `columns=`
    /// selection, which may have left source columns out.
    ColumnNotProfiled,
    /// The requirement asks about the full source, the evidence does not reach
    /// that far, and nothing observed settles it regardless.
    #[non_exhaustive]
    EvidenceIncomplete {
        /// The gap between the evidence and the requirement's scope.
        gap: EvidenceGap,
    },
    /// The baseline carries no value to measure a change from: it did not
    /// assess the metric, did not profile the column, or read no values for
    /// it.
    NotInBaseline,
    /// A distinct count the requirement reads is an estimate on at least one
    /// side, because the exact set of values spilled into its sketch, or the
    /// side does not record whether it is exact. An estimate falling is not a
    /// column collapsing, so the requirement decides on exact counts only.
    ApproximateCount,
    /// The two reports profiled different column sets, and the requirement
    /// is about a dataset-level score, whose subject is the column set. The
    /// result's `baseline.schema` names the columns that differ.
    SchemaChanged,
    /// The overall score averages a different set of assessed dimensions on
    /// each side, because a dimension was requested, or had something to
    /// assess, on one side only.
    DimensionsChanged,
    /// A measurement definition the requirement depends on is not recorded
    /// identically on both sides: one side predates the record, or the two
    /// differ. Either way the same data may measure differently.
    #[non_exhaustive]
    DefinitionNotComparable {
        /// The `metric_semantics` key.
        definition: String,
    },
    /// A setting that moves the score on unchanged data differs between the
    /// two reports.
    #[non_exhaustive]
    SettingChanged {
        /// The configuration field that differs.
        setting: String,
    },
    /// A setting that moves the score on unchanged data is not recorded on
    /// at least one side, so it cannot be shown to be the same.
    #[non_exhaustive]
    SettingUnrecorded {
        /// The configuration field that is unrecorded.
        setting: String,
    },
    /// The findings rule a `forbid_finding` check reads could not look at
    /// all of the report, and found nothing where it did look.
    #[non_exhaustive]
    FindingNotEvaluated {
        /// The rule's entries in [`FindingsResult::not_evaluated`], each with
        /// its own reason and the columns it covers.
        rules: Vec<UnevaluatedRule>,
    },
}

/// What happened to one requirement.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckStatus {
    /// Evaluated and met.
    Passed,
    /// Evaluated and violated.
    Failed,
    /// Not evaluated, for this reason.
    NotEvaluated(NotEvaluated),
}

/// One requirement and what the report said about it.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct Check {
    /// Which requirement this is.
    pub code: CheckCode,
    /// The column it concerns, when it concerns one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// The quality dimension it concerns, when it concerns one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<QualityDimension>,
    /// The findings code a `forbid_finding` check concerns. Absent on every
    /// other rule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finding: Option<FindingCode>,
    /// The constraint applied.
    pub expected: Expectation,
    /// The value read from the report. Absent when nothing was read: every
    /// unevaluated check, and every `require_metric` check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed: Option<MetricValue>,
    /// What the requirement is a statement about.
    pub scope: PolicyScope,
    /// Whether the data behind `observed` covers the whole source.
    pub evidence: Evidence,
    /// Where the whole-source value lies, when `observed` came from a retained
    /// sample and the report bounds it. A full-source requirement is decided
    /// on this interval rather than on `observed`: it passes when the whole
    /// interval meets the requirement, fails when none of it does, and is
    /// otherwise not evaluated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounds: Option<CheckBounds>,
    /// The baseline's side of a rule about change. Absent on every other
    /// rule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BaselineReading>,
    /// The findings a `forbid_finding` check found, each with its column and
    /// evidence, in the order [`FindingsResult::findings`] lists them. Empty,
    /// and not serialized, when there are none and on every other rule.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<Finding>,
    /// Passed, failed, or not evaluated.
    #[serde(flatten)]
    pub status: CheckStatus,
    /// A fixed sentence naming the outcome.
    ///
    /// Deliberately free of numbers: `observed` and `expected` carry those, so
    /// the prose is identical wherever a value would have been formatted and
    /// the Rust and Python implementations cannot drift on rounding inside a
    /// string.
    pub message: String,
}

impl Check {
    /// True when the check was evaluated and violated.
    pub fn is_violation(&self) -> bool {
        matches!(self.status, CheckStatus::Failed)
    }

    /// Why the check was not evaluated, if it was not.
    pub fn not_evaluated(&self) -> Option<&NotEvaluated> {
        match &self.status {
            CheckStatus::NotEvaluated(reason) => Some(reason),
            _ => None,
        }
    }
}

/// What the baseline said about the metric a rule about change reads.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct BaselineReading {
    /// The baseline's value, or `None` when it carries none.
    pub observed: Option<MetricValue>,
    /// Whether the data behind the baseline's value covers its whole source.
    pub evidence: Evidence,
    /// The check's `observed` minus the baseline's, at the report's
    /// serialized precision: in points, or, for a rule on a relative change
    /// such as `max_distinct_drop` and the row count rules, in percent of the
    /// baseline's value.
    /// `None` when either side has no value.
    pub change: Option<MetricValue>,
    /// Where the baseline's whole-source score lies, when its score came
    /// from a retained sample and the baseline bounds it, and a full-source
    /// score drop rule was decided on it. Absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounds: Option<CheckBounds>,
}

/// The two reports a baseline-relative evaluation compared, and how their
/// column sets differ.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct BaselineComparison {
    /// `a` is the baseline and `b` the report evaluated, as in a
    /// [`ReportDelta`] between them, with whether one build wrote both.
    pub reports: ReportPair,
    /// Columns added, removed and shared.
    pub schema: SchemaChange,
    /// The schema changes from the baseline, whatever the policy states. A
    /// diagnostic never moves the verdict. Empty when nothing changed.
    pub diagnostics: Vec<Diagnostic>,
    /// The clock rule the timeliness and overall score rules read.
    pub timeliness_clock: TimelinessClockRule,
}

/// Stable identifier for a kind of schema change.
///
/// Part of the serialized contract, like [`CheckCode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DiagnosticCode {
    /// The report profiled a column the baseline did not. Without a reason,
    /// the baseline profiled every column it selected that its source had,
    /// and this was not among them.
    ColumnAdded,
    /// The baseline profiled a column the report did not. Without a reason,
    /// the report's selection included it, so it was not in what the report
    /// read. With one, the report's column selection left it out or was not
    /// recorded.
    ColumnRemoved,
    /// The column holds values on both sides and its inferred type differs.
    /// A column whose values are all null on a side establishes no type there.
    ColumnTypeChanged,
    /// The column held values in the baseline, and every value read for it
    /// in the report is null.
    ColumnAllNull,
}

impl fmt::Display for DiagnosticCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnAdded => write!(f, "column_added"),
            Self::ColumnRemoved => write!(f, "column_removed"),
            Self::ColumnTypeChanged => write!(f, "column_type_changed"),
            Self::ColumnAllNull => write!(f, "column_all_null"),
        }
    }
}

/// A schema change between the baseline and the report.
///
/// A warning, not a fourth verdict: it is reported whatever the policy
/// states, and no rule decides on it. A requirement that would fail on it is
/// a policy entry of its own.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct Diagnostic {
    /// What changed.
    pub code: DiagnosticCode,
    /// The column it changed in.
    pub column: String,
    /// The inferred type on each side, `a` being the baseline. Present on
    /// [`DiagnosticCode::ColumnTypeChanged`] only.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_data_types"
    )]
    pub data_type: Option<Sides<DataType>>,
    /// A recorded setting that can produce the change on unchanged data,
    /// when one can. Serialized flat, as a check's reason is.
    #[serde(flatten)]
    pub setting: Option<SettingDifference>,
    /// A fixed sentence naming the change, free of numbers and names, like a
    /// check's message.
    pub message: String,
}

/// How a setting that can produce a diagnostic's change compares across the
/// two reports. A diagnostic carrying one may not reflect a change in the
/// data.
///
/// Two settings are read: `columns`, the column selection, on an added or
/// removed column, and `identifier_columns` on a type change to or from
/// `identifier`, the one type a setting assigns. An input path that types the
/// same data differently is not recorded, so it cannot be named here (#906).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SettingDifference {
    /// The setting differs between the two reports for this column.
    #[non_exhaustive]
    SettingChanged {
        /// The configuration field that differs.
        setting: String,
    },
    /// At least one side did not record the setting, so it cannot be shown
    /// to be the same.
    #[non_exhaustive]
    SettingUnrecorded {
        /// The configuration field that is unrecorded.
        setting: String,
    },
}

fn serialize_data_types<S: serde::Serializer>(
    types: &Option<Sides<DataType>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    types
        .as_ref()
        .map(|types| Sides {
            a: types.a.name(),
            b: types.b.name(),
        })
        .serialize(serializer)
}

/// The interval a sampled score was decided on.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct CheckBounds {
    /// Lowest value the whole-source score can take.
    #[serde(serialize_with = "dataprof_core::serde_helpers::round_2")]
    pub lower: f64,
    /// Highest value the whole-source score can take.
    #[serde(serialize_with = "dataprof_core::serde_helpers::round_2")]
    pub upper: f64,
    /// Probability that every interval in the report holds at once.
    pub confidence_level: f64,
}

/// The overall outcome of a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Verdict {
    /// Every requirement was evaluated and met.
    Pass,
    /// At least one requirement was conclusively violated.
    Fail,
    /// Nothing was violated, and at least one requirement could not be
    /// evaluated. Not a pass: the gate has no answer, not a positive one.
    Inconclusive,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => write!(f, "pass"),
            Self::Fail => write!(f, "fail"),
            Self::Inconclusive => write!(f, "inconclusive"),
        }
    }
}

/// The structured result of evaluating a [`QualityPolicy`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct GateResult {
    /// The overall outcome.
    pub verdict: Verdict,
    /// The scope every requirement was evaluated under.
    pub scope: PolicyScope,
    /// Whether the scan itself covered the whole source. A single check's
    /// evidence can be weaker still, when its metric came from a retained
    /// sample of the scanned rows.
    pub evidence: Evidence,
    /// Every requirement, in evaluation order.
    pub checks: Vec<Check>,
    /// The reports compared, when the policy was evaluated against a
    /// baseline. Absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BaselineComparison>,
    /// The build that wrote the report evaluated, which decided the metrics
    /// the verdict reads. `None` (serialized as `null`) when the report did
    /// not record one. The build evaluating the policy can be another.
    pub generator: Option<ReportGenerator>,
    /// The caller's version of the source the report describes, `None`
    /// (serialized as `null`) when none was recorded.
    pub source_identity: Option<SourceIdentity>,
}

impl GateResult {
    /// True only for [`Verdict::Pass`].
    ///
    /// An inconclusive result is not a pass: reading it as one is the mistake
    /// this API exists to prevent. Callers that need to tell "violated" from
    /// "could not tell" should match on [`GateResult::verdict`].
    pub fn passed(&self) -> bool {
        self.verdict == Verdict::Pass
    }

    /// The checks that were evaluated and violated.
    pub fn violations(&self) -> impl Iterator<Item = &Check> {
        self.checks.iter().filter(|check| check.is_violation())
    }

    /// The checks that could not be evaluated.
    pub fn unevaluated(&self) -> impl Iterator<Item = &Check> {
        self.checks
            .iter()
            .filter(|check| check.not_evaluated().is_some())
    }
}

/// A metric a policy requires to have been analyzed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequiredMetric {
    /// The quality assessment as a whole.
    Quality,
    /// One ISO 25012 dimension.
    Dimension(QualityDimension),
}

/// What a report must satisfy.
///
/// Build one with the chainable setters, then [`evaluate`](Self::evaluate) it
/// against a report. Requirements are evaluated in a fixed order that does not
/// depend on the order they were added, so two callers stating the same policy
/// get the same document out.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QualityPolicy {
    min_rows: Option<usize>,
    min_quality_score: Option<f64>,
    min_dimension_scores: BTreeMap<String, (QualityDimension, f64)>,
    max_null_percentage: BTreeMap<String, f64>,
    max_null_percentage_any: Option<f64>,
    max_null_count: BTreeMap<String, usize>,
    max_null_count_any: Option<usize>,
    max_duplicate_rows: Option<usize>,
    max_date_age: BTreeMap<String, u32>,
    require_metrics: Vec<RequiredMetric>,
    max_quality_score_drop: Option<f64>,
    max_dimension_score_drops: BTreeMap<String, (QualityDimension, f64)>,
    max_null_percentage_increase: BTreeMap<String, f64>,
    max_null_percentage_increase_any: Option<f64>,
    max_distinct_drop: BTreeMap<String, f64>,
    max_distinct_drop_any: Option<f64>,
    max_row_count_drop: Option<f64>,
    max_row_count_growth: Option<f64>,
    require_columns: BTreeSet<String>,
    forbid_removed_columns: bool,
    forbid_added_columns: bool,
    forbid_findings: BTreeSet<FindingCode>,
    timeliness_clock: TimelinessClockRule,
    scope: PolicyScope,
}

impl QualityPolicy {
    /// An empty policy. It states no requirement, so evaluating it as-is is a
    /// [`PolicyError::NoRequirements`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Require the scan to have read at least `min` rows, so an empty or
    /// truncated extract fails instead of passing every other check.
    pub fn min_rows(mut self, min: usize) -> Self {
        self.min_rows = Some(min);
        self
    }

    /// Require the overall quality score to be at least `min` (0..=100).
    pub fn min_quality_score(mut self, min: f64) -> Self {
        self.min_quality_score = Some(min);
        self
    }

    /// Require one dimension's score to be at least `min` (0..=100).
    pub fn min_dimension_score(mut self, dimension: QualityDimension, min: f64) -> Self {
        self.min_dimension_scores
            .insert(dimension.to_string(), (dimension, min));
        self
    }

    /// Require the named column's null percentage to be at most `max`
    /// (0..=100). Takes precedence over
    /// [`max_null_percentage_any`](Self::max_null_percentage_any) for that
    /// column.
    pub fn max_null_percentage(mut self, column: impl Into<String>, max: f64) -> Self {
        self.max_null_percentage.insert(column.into(), max);
        self
    }

    /// Require every other profiled column's null percentage to be at most
    /// `max` (0..=100).
    pub fn max_null_percentage_any(mut self, max: f64) -> Self {
        self.max_null_percentage_any = Some(max);
        self
    }

    /// Require the named column to hold at most `max` nulls, decided on the
    /// exact count. `max_null_count(column, 0)` states "no nulls" at any row
    /// count, where a zero null percentage is decided at the two decimals it
    /// is shown at. Takes precedence over
    /// [`max_null_count_any`](Self::max_null_count_any) for that column.
    pub fn max_null_count(mut self, column: impl Into<String>, max: usize) -> Self {
        self.max_null_count.insert(column.into(), max);
        self
    }

    /// Require every other profiled column to hold at most `max` nulls.
    pub fn max_null_count_any(mut self, max: usize) -> Self {
        self.max_null_count_any = Some(max);
        self
    }

    /// Require the report to count at most `max` duplicate rows.
    pub fn max_duplicate_rows(mut self, max: usize) -> Self {
        self.max_duplicate_rows = Some(max);
        self
    }

    /// Require the named column's newest date to lie at most `days` days
    /// before the timeliness reference date (#1290), so a feed that stopped
    /// moving fails while every other rule still passes it.
    ///
    /// The newest date is the column's `max_datetime`, a calendar date, and
    /// the reference date is the one the report's timeliness clock recorded
    /// (`reference_date`, or the run's date on a rolling clock), so the age is
    /// counted in calendar days and a saved report evaluated later decides
    /// the same. A date after the reference date is fresh. A newest date only
    /// gets newer as more values are read, so under [`PolicyScope::FullSource`]
    /// a fresh date passes whatever the scan missed, and a stale one fails
    /// only when the values behind it cover the column: every row read, and
    /// the retained sample the date statistics are computed from holding
    /// every non-null value. The exception is a column of slash dates whose
    /// day and month order the unread values could still change
    /// (`assumed_day_first` or `month_first`): there a fresh date read on
    /// part of the column is [`NotEvaluated::EvidenceIncomplete`] too. A column with no newest date (not typed as a
    /// date, all null, or statistics not computed) is
    /// [`NotEvaluated::NotAssessed`], and a report without a recorded clock is
    /// [`NotEvaluated::SettingUnrecorded`] on `timeliness_clock`.
    pub fn max_date_age(mut self, column: impl Into<String>, days: u32) -> Self {
        self.max_date_age.insert(column.into(), days);
        self
    }

    /// Require the report to carry a quality assessment at all.
    ///
    /// Without this, a report with no assessment leaves quality requirements
    /// unevaluated and the verdict inconclusive. With it, the absence is
    /// itself a violation — which is what a pipeline wants when quality is
    /// supposed to be configured on.
    pub fn require_quality(mut self) -> Self {
        self.require_metrics.push(RequiredMetric::Quality);
        self
    }

    /// Require a specific dimension to have been assessed.
    pub fn require_dimension(mut self, dimension: QualityDimension) -> Self {
        self.require_metrics
            .push(RequiredMetric::Dimension(dimension));
        self
    }

    /// Allow the overall quality score to drop at most `points` (0..=100)
    /// below the baseline's. Needs [`evaluate_against`](Self::evaluate_against).
    ///
    /// Under [`PolicyScope::FullSource`], a score computed over a retained
    /// sample on either side is read as its recorded whole-source interval,
    /// and the drop is decided when every change those intervals allow lies
    /// on one side of `points` (#1035). Each side's intervals hold together
    /// with that side's `confidence_level`; when both sides are sampled, the
    /// two hold together with probability at least the sum of the two levels
    /// minus one.
    pub fn max_quality_score_drop(mut self, points: f64) -> Self {
        self.max_quality_score_drop = Some(points);
        self
    }

    /// Allow one dimension's score to drop at most `points` (0..=100) below
    /// the baseline's. Needs [`evaluate_against`](Self::evaluate_against).
    /// A sampled score is decided on its interval, as in
    /// [`max_quality_score_drop`](Self::max_quality_score_drop).
    pub fn max_dimension_score_drop(mut self, dimension: QualityDimension, points: f64) -> Self {
        self.max_dimension_score_drops
            .insert(dimension.to_string(), (dimension, points));
        self
    }

    /// Allow the named column's null percentage to rise at most `points`
    /// (0..=100) above the baseline's. Takes precedence over
    /// [`max_null_percentage_increase_any`](Self::max_null_percentage_increase_any)
    /// for that column. Needs [`evaluate_against`](Self::evaluate_against).
    pub fn max_null_percentage_increase(mut self, column: impl Into<String>, points: f64) -> Self {
        self.max_null_percentage_increase
            .insert(column.into(), points);
        self
    }

    /// Allow every other column both reports profiled to have its null
    /// percentage rise at most `points` (0..=100) above the baseline's. A
    /// column only the evaluated report has is not covered: it has no
    /// baseline, and gets a rule only when the policy names it. Needs
    /// [`evaluate_against`](Self::evaluate_against).
    pub fn max_null_percentage_increase_any(mut self, points: f64) -> Self {
        self.max_null_percentage_increase_any = Some(points);
        self
    }

    /// Allow the named column's distinct count to fall at most `percent`
    /// (0..=100) of the baseline's (#1026). Fewer rows alone do not make a
    /// collapse, so the check fails only when the column's distinct share of
    /// non-null values fell by more than `percent` of the baseline's share as
    /// well. Decided on exact counts only: an estimate on either side leaves
    /// it [`NotEvaluated::ApproximateCount`]. Takes precedence over
    /// [`max_distinct_drop_any`](Self::max_distinct_drop_any) for that
    /// column. Needs [`evaluate_against`](Self::evaluate_against).
    pub fn max_distinct_drop(mut self, column: impl Into<String>, percent: f64) -> Self {
        self.max_distinct_drop.insert(column.into(), percent);
        self
    }

    /// [`max_distinct_drop`](Self::max_distinct_drop) for every other column
    /// both reports profiled. A column only the evaluated report has is not
    /// covered. Needs [`evaluate_against`](Self::evaluate_against).
    pub fn max_distinct_drop_any(mut self, percent: f64) -> Self {
        self.max_distinct_drop_any = Some(percent);
        self
    }

    /// Allow the row count to fall at most `percent` (0..=100) of the
    /// baseline's `rows_processed` (#1291), so a truncated extract that keeps
    /// its header and its shares fails. Needs
    /// [`evaluate_against`](Self::evaluate_against).
    ///
    /// A scan that stopped early read a lower bound of its source's rows. So
    /// under [`PolicyScope::FullSource`], an incomplete scan of this report
    /// still decides a pass and an incomplete baseline still decides a
    /// failure, and the other outcome is unevaluated: the rows a scan did not
    /// read could make up the difference. With both scans incomplete the rule
    /// is unevaluated. An allowance of 100% passes whatever the scans missed,
    /// since a count cannot fall by more. A different column set leaves it
    /// [`NotEvaluated::SchemaChanged`], as for a dataset-level score.
    pub fn max_row_count_drop(mut self, percent: f64) -> Self {
        self.max_row_count_drop = Some(percent);
        self
    }

    /// Allow the row count to grow at most `percent` (0 or more) of the
    /// baseline's `rows_processed` (#1291), so a join that fans out or a file
    /// published twice fails. The mirror of
    /// [`max_row_count_drop`](Self::max_row_count_drop): an incomplete scan
    /// of this report still decides a failure, and an incomplete baseline a
    /// pass. Needs [`evaluate_against`](Self::evaluate_against).
    pub fn max_row_count_growth(mut self, percent: f64) -> Self {
        self.max_row_count_growth = Some(percent);
        self
    }

    /// Require each named column to be profiled. A column missing from the
    /// report fails only when the report shows its source lacks it: the run's
    /// column selection included the column, and the column set covers the
    /// whole source (see the [module docs](self)).
    pub fn require_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.require_columns
            .extend(columns.into_iter().map(Into::into));
        self
    }

    /// Require every column the baseline profiled to be profiled still. The
    /// rule is about the columns the baseline recorded: a column its source
    /// held but it did not profile (a projection, or a key past a partial
    /// read) is not one it can require. Needs
    /// [`evaluate_against`](Self::evaluate_against).
    pub fn forbid_removed_columns(mut self) -> Self {
        self.forbid_removed_columns = true;
        self
    }

    /// Require every column the report profiled to be one the baseline
    /// profiled: a closed schema. Under [`PolicyScope::FullSource`], a report
    /// whose profiled columns may not be all of its source's also gets one
    /// check with no column, left unevaluated: a column selection that left
    /// columns out, one that was not recorded, or a column set covering part
    /// of the source. Needs [`evaluate_against`](Self::evaluate_against).
    pub fn forbid_added_columns(mut self) -> Self {
        self.forbid_added_columns = true;
        self
    }

    /// Fail when the report holds a finding with one of these codes (#1066),
    /// derived with the default [`FindingPolicy`](crate::FindingPolicy) thresholds, as
    /// [`ProfileReport::findings`] derives them. One check per code, in code
    /// order. See the [module docs](self) for when a partial scan decides
    /// one.
    pub fn forbid_findings<I>(mut self, codes: I) -> Self
    where
        I: IntoIterator<Item = FindingCode>,
    {
        self.forbid_findings.extend(codes);
        self
    }

    /// Declare how the timeliness clocks of the baseline and the report may
    /// differ, for the rules about change that read the timeliness score: the
    /// overall score drop, when both sides assessed timeliness, and the
    /// timeliness score drop. Defaults to [`TimelinessClockRule::Fixed`]. A
    /// difference the rule does not allow, or a clock either side did not
    /// record, leaves those rules unevaluated on the setting
    /// `timeliness_clock`.
    pub fn timeliness_clock(mut self, rule: TimelinessClockRule) -> Self {
        self.timeliness_clock = rule;
        self
    }

    /// Set what the requirements are statements about. Defaults to
    /// [`PolicyScope::FullSource`].
    pub fn scope(mut self, scope: PolicyScope) -> Self {
        self.scope = scope;
        self
    }

    /// Check that the policy is evaluable, without needing a report.
    ///
    /// Callers that read a policy out of configuration should do this at
    /// startup rather than discovering a bad threshold on the first dataset.
    pub fn validate(&self) -> Result<(), PolicyError> {
        if let Some(min) = self.min_quality_score {
            check_percentage(CheckCode::MinQualityScore, None, min)?;
        }
        for (dimension, min) in self.min_dimension_scores.values() {
            check_percentage(
                CheckCode::MinDimensionScore,
                Some(dimension.to_string()),
                *min,
            )?;
        }
        for (column, max) in &self.max_null_percentage {
            check_percentage(CheckCode::MaxNullPercentage, Some(column.clone()), *max)?;
        }
        if let Some(max) = self.max_null_percentage_any {
            check_percentage(CheckCode::MaxNullPercentage, None, max)?;
        }
        if let Some(points) = self.max_quality_score_drop {
            check_percentage(CheckCode::MaxQualityScoreDrop, None, points)?;
        }
        for (dimension, points) in self.max_dimension_score_drops.values() {
            check_percentage(
                CheckCode::MaxDimensionScoreDrop,
                Some(dimension.to_string()),
                *points,
            )?;
        }
        for (column, points) in &self.max_null_percentage_increase {
            check_percentage(
                CheckCode::MaxNullPercentageIncrease,
                Some(column.clone()),
                *points,
            )?;
        }
        if let Some(points) = self.max_null_percentage_increase_any {
            check_percentage(CheckCode::MaxNullPercentageIncrease, None, points)?;
        }
        for (column, percent) in &self.max_distinct_drop {
            check_percentage(CheckCode::MaxDistinctDrop, Some(column.clone()), *percent)?;
        }
        if let Some(percent) = self.max_distinct_drop_any {
            check_percentage(CheckCode::MaxDistinctDrop, None, percent)?;
        }
        if let Some(percent) = self.max_row_count_drop {
            check_percentage(CheckCode::MaxRowCountDrop, None, percent)?;
        }
        if let Some(percent) = self.max_row_count_growth
            && !(percent.is_finite() && percent >= 0.0)
        {
            return Err(PolicyError::ThresholdOutOfRange {
                code: CheckCode::MaxRowCountGrowth,
                subject: None,
                value: percent,
            });
        }
        if self.is_empty() {
            return Err(PolicyError::NoRequirements);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.min_rows.is_none()
            && self.min_quality_score.is_none()
            && self.min_dimension_scores.is_empty()
            && self.max_null_percentage.is_empty()
            && self.max_null_percentage_any.is_none()
            && self.max_null_count.is_empty()
            && self.max_null_count_any.is_none()
            && self.max_duplicate_rows.is_none()
            && self.max_date_age.is_empty()
            && self.require_metrics.is_empty()
            && self.require_columns.is_empty()
            && self.forbid_findings.is_empty()
            && !self.has_change_rules()
    }

    /// Whether the policy states a rule about change from a baseline.
    fn has_change_rules(&self) -> bool {
        self.max_quality_score_drop.is_some()
            || !self.max_dimension_score_drops.is_empty()
            || !self.max_null_percentage_increase.is_empty()
            || self.max_null_percentage_increase_any.is_some()
            || !self.max_distinct_drop.is_empty()
            || self.max_distinct_drop_any.is_some()
            || self.max_row_count_drop.is_some()
            || self.max_row_count_growth.is_some()
            || self.forbid_removed_columns
            || self.forbid_added_columns
    }

    /// Evaluate the policy against a report.
    ///
    /// Returns the structured outcome. The report is not modified, nothing is
    /// printed, and the process is not exited: what to do about a failing gate
    /// is the caller's decision. A policy with a rule about change is a
    /// [`PolicyError::BaselineRequired`] here; use
    /// [`evaluate_against`](Self::evaluate_against).
    pub fn evaluate(&self, report: &ProfileReport) -> Result<GateResult, PolicyError> {
        self.validate()?;
        if self.has_change_rules() {
            return Err(PolicyError::BaselineRequired);
        }
        let checks = self.absolute_checks(report);
        Ok(GateResult {
            verdict: verdict_of(&checks),
            scope: self.scope,
            evidence: scan_evidence(&report.execution),
            checks,
            baseline: None,
            generator: report.generator.clone(),
            source_identity: report.source_identity.clone(),
        })
    }

    /// Evaluate the policy against a report and a baseline it is compared
    /// with.
    ///
    /// Every requirement is evaluated as [`evaluate`](Self::evaluate) does,
    /// then each rule about change, in a fixed order: the overall score, the
    /// dimensions in [`QualityDimension::all`] order, the row count drop and
    /// then growth, then for the null increase and then the distinct drop
    /// rules the named columns in name order and every other shared column in
    /// the baseline's order.
    /// The baseline may come from a flat summary, which leaves what it did
    /// not record unrecorded. See the [module docs](self) for when a change
    /// is decided.
    pub fn evaluate_against(
        &self,
        report: &ProfileReport,
        baseline: &ReportSide,
    ) -> Result<GateResult, PolicyError> {
        self.validate()?;
        let current = ReportSide::from_report(report);
        let delta = ReportDelta::between_sides(baseline, &current);
        let identifiers = Sides {
            a: baseline.identifier_order(),
            b: current.identifier_order(),
        };
        let sides = Sides {
            a: baseline,
            b: &current,
        };
        let mut checks = self.absolute_checks(report);
        if let Some(points) = self.max_quality_score_drop {
            checks.push(self.score_drop_check(report, &delta, &sides, &identifiers, None, points));
        }
        for dimension in QualityDimension::all() {
            if let Some((_, points)) = self.max_dimension_score_drops.get(&dimension.to_string()) {
                checks.push(self.score_drop_check(
                    report,
                    &delta,
                    &sides,
                    &identifiers,
                    Some(dimension),
                    *points,
                ));
            }
        }
        if let Some(percent) = self.max_row_count_drop {
            checks.push(self.row_count_change_check(&delta, RowCountRule::Drop, percent));
        }
        if let Some(percent) = self.max_row_count_growth {
            checks.push(self.row_count_change_check(&delta, RowCountRule::Growth, percent));
        }
        checks.extend(self.null_increase_checks(&delta));
        checks.extend(self.distinct_drop_checks(&delta, baseline, &current));
        // Collected once, so a rule over every column does not rescan the
        // other side's columns for each one.
        let (baseline_names, current_names) = (column_names(baseline), column_names(&current));
        if self.forbid_removed_columns {
            for column in baseline.columns() {
                checks.push(self.forbid_removed_check(
                    baseline,
                    &current,
                    &current_names,
                    &column.name,
                ));
            }
        }
        if self.forbid_added_columns {
            for column in current.columns() {
                checks.push(self.forbid_added_check(
                    baseline,
                    &current,
                    &baseline_names,
                    &column.name,
                ));
            }
            // The per-column checks cover the columns the report found. When
            // those do not enumerate the source, a column the baseline lacked
            // may be among the ones it did not profile, which no per-column
            // check can name.
            if self.scope == PolicyScope::FullSource
                && let Some(reason) = unenumerated_columns(&current)
            {
                checks.push(self.unprofiled_columns_check(baseline, &current, reason));
            }
        }
        let diagnostics = schema_diagnostics(baseline, &current, &delta);
        Ok(GateResult {
            verdict: verdict_of(&checks),
            scope: self.scope,
            evidence: scan_evidence(&report.execution),
            checks,
            baseline: Some(BaselineComparison {
                reports: delta.reports,
                schema: delta.schema,
                diagnostics,
                timeliness_clock: self.timeliness_clock,
            }),
            generator: report.generator.clone(),
            source_identity: report.source_identity.clone(),
        })
    }

    /// The requirements about the report alone, in their fixed order.
    fn absolute_checks(&self, report: &ProfileReport) -> Vec<Check> {
        let scan = scan_evidence(&report.execution);
        let mut checks = Vec::new();

        for metric in required_metrics_in_order(&self.require_metrics) {
            checks.push(self.require_metric_check(report, metric));
        }
        if !self.require_columns.is_empty() {
            let side = ReportSide::from_report(report);
            let names = column_names(&side);
            for column in &self.require_columns {
                checks.push(self.require_column_check(&side, &names, column));
            }
        }
        if let Some(min) = self.min_rows {
            checks.push(self.row_count_check(report, scan, min));
        }
        if let Some(min) = self.min_quality_score {
            checks.push(self.quality_score_check(report, scan, min));
        }
        for dimension in QualityDimension::all() {
            if let Some((_, min)) = self.min_dimension_scores.get(&dimension.to_string()) {
                checks.push(self.dimension_score_check(report, scan, dimension, *min));
            }
        }
        checks.extend(self.null_percentage_checks(report, scan));
        checks.extend(self.null_count_checks(report, scan));
        if let Some(max) = self.max_duplicate_rows {
            checks.push(self.duplicate_rows_check(report, scan, max));
        }
        for (column, days) in &self.max_date_age {
            let profile = report
                .column_profiles
                .iter()
                .find(|candidate| &candidate.name == column);
            checks.push(self.date_age_check(report, column, profile, scan, *days));
        }
        if !self.forbid_findings.is_empty() {
            let input = FindingsInput::from(report);
            let findings = FindingPolicy::default().collect(&input);
            for &code in &self.forbid_findings {
                let sample = if input.reads_a_partial_sample(code) {
                    Evidence::Incomplete {
                        reason: EvidenceGap::QualitySampled,
                    }
                } else {
                    Evidence::Complete
                };
                checks.push(self.forbid_finding_check(&findings, scan.and(sample), code));
            }
        }
        checks
    }

    /// Decide one forbidden findings code.
    ///
    /// A finding present fails the check, except a finding about a share of
    /// the values ([`FindingReach::Share`]) under [`PolicyScope::FullSource`]
    /// on a scan that does not cover the source: a share over part of a
    /// source bounds nothing about the rest. No finding passes only when the
    /// rule looked at all of the report and, unless it is about how the
    /// source was read ([`FindingReach::Read`]), the scan covers the source
    /// or the scope is [`PolicyScope::Observed`].
    fn forbid_finding_check(
        &self,
        result: &FindingsResult,
        scan: Evidence,
        code: FindingCode,
    ) -> Check {
        let found: Vec<Finding> = result
            .findings
            .iter()
            .filter(|finding| finding.code == code)
            .cloned()
            .collect();
        let skipped: Vec<UnevaluatedRule> = result
            .not_evaluated
            .iter()
            .filter(|rule| rule.code == code)
            .cloned()
            .collect();
        let reach = FindingReach::of(code);
        let evidence = match reach {
            FindingReach::Read => Evidence::Complete,
            FindingReach::Witness | FindingReach::Share => scan,
        };
        let gap = match self.scope {
            PolicyScope::FullSource => evidence.gap(),
            PolicyScope::Observed => None,
        };
        let (status, message) = match (found.is_empty(), gap) {
            (false, Some(EvidenceGap::QualitySampled)) if reach == FindingReach::Share => (
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete {
                    gap: EvidenceGap::QualitySampled,
                }),
                "the finding holds for the retained sample of values, and the values outside \
                 it may change it",
            ),
            (false, Some(gap)) if reach == FindingReach::Share => (
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap }),
                "the finding holds for the rows read, and the rows that were not read may \
                 change it",
            ),
            (false, _) => (
                CheckStatus::Failed,
                "the report holds a finding the policy forbids",
            ),
            (true, _) if !skipped.is_empty() => (
                CheckStatus::NotEvaluated(NotEvaluated::FindingNotEvaluated { rules: skipped }),
                FINDING_NOT_EVALUATED,
            ),
            (true, Some(EvidenceGap::QualitySampled)) => (
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete {
                    gap: EvidenceGap::QualitySampled,
                }),
                "the finding's rule found nothing in the retained sample of values, and the \
                 values outside it may hold it",
            ),
            (true, Some(gap)) => (
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap }),
                "the finding's rule found nothing in the rows read, and the rows that were not \
                 read may hold it",
            ),
            (true, None) => (
                CheckStatus::Passed,
                "the finding's rule looked and found nothing",
            ),
        };
        // Where the rule could not look, the zero it found elsewhere is not a
        // count of the report's findings.
        let observed = match status {
            CheckStatus::NotEvaluated(NotEvaluated::FindingNotEvaluated { .. }) => None,
            _ => Some(MetricValue::Count(found.len())),
        };
        Check {
            code: CheckCode::ForbidFinding,
            column: None,
            dimension: None,
            finding: Some(code),
            expected: Expectation::AtMost {
                value: MetricValue::Count(0),
            },
            observed,
            scope: self.scope,
            evidence,
            bounds: None,
            baseline: None,
            findings: found,
            status,
            message: message.to_string(),
        }
    }

    /// Decide a comparison whose observed value is an average or a ratio.
    ///
    /// Such a value, computed over part of a source, bounds nothing about the
    /// rest of it, so under [`PolicyScope::FullSource`] incomplete evidence
    /// leaves the requirement unevaluated in both directions. Under
    /// [`PolicyScope::Observed`] the question is about the data that was
    /// measured, so the comparison always stands.
    fn decide_aggregate(&self, evidence: Evidence, satisfied: bool) -> CheckStatus {
        match (self.scope, evidence.gap()) {
            (PolicyScope::FullSource, Some(gap)) => {
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap })
            }
            _ if satisfied => CheckStatus::Passed,
            _ => CheckStatus::Failed,
        }
    }

    fn require_metric_check(&self, report: &ProfileReport, metric: RequiredMetric) -> Check {
        let dimension = match metric {
            RequiredMetric::Quality => None,
            RequiredMetric::Dimension(dimension) => Some(dimension),
        };
        // Availability is a property of the report, not of how much of the
        // source it covers: a sampled run still either analyzed the metric or
        // did not. So this check never consults evidence.
        let analyzed = match (report.quality.as_ref(), dimension) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(quality), Some(dimension)) => {
                quality.metrics.dimension_score(dimension).is_some()
            }
        };
        let message = match (analyzed, dimension) {
            (true, None) => "quality was analyzed",
            (false, None) => "the report carries no quality assessment",
            (true, Some(_)) => "the required dimension was assessed",
            (false, Some(_)) => "the required dimension was not assessed",
        };
        Check {
            code: CheckCode::RequireMetric,
            column: None,
            dimension,
            finding: None,
            findings: Vec::new(),
            expected: Expectation::Analyzed,
            observed: None,
            scope: self.scope,
            evidence: Evidence::Complete,
            bounds: None,
            baseline: None,
            status: if analyzed {
                CheckStatus::Passed
            } else {
                CheckStatus::Failed
            },
            message: message.to_string(),
        }
    }

    /// The mirror of the duplicate-row rule: rows already read do not stop
    /// existing when more are read, so a scan that read at least `min` rows
    /// settles the requirement whatever it missed. Fewer is a failure only
    /// when the scan covered the whole source, or under
    /// [`PolicyScope::Observed`]; under [`PolicyScope::FullSource`] the rows
    /// that were not read may make up the difference.
    fn row_count_check(&self, report: &ProfileReport, scan: Evidence, min: usize) -> Check {
        let observed = report.execution.rows_processed;
        let status = match (self.scope, scan.gap()) {
            _ if observed >= min => CheckStatus::Passed,
            (PolicyScope::FullSource, Some(gap)) => {
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap })
            }
            _ => CheckStatus::Failed,
        };
        let message = match status {
            CheckStatus::Passed => "the scan read at least the required number of rows",
            CheckStatus::Failed => "fewer rows were read than the required minimum",
            CheckStatus::NotEvaluated(_) => {
                "fewer rows were read than the required minimum, and the rows that were not \
                 read may make up the difference"
            }
        };
        Check {
            code: CheckCode::MinRows,
            column: None,
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected: Expectation::AtLeast {
                value: MetricValue::Count(min),
            },
            observed: Some(MetricValue::Count(observed)),
            scope: self.scope,
            evidence: scan,
            bounds: None,
            baseline: None,
            status,
            message: message.to_string(),
        }
    }

    fn quality_score_check(&self, report: &ProfileReport, scan: Evidence, min: f64) -> Check {
        let mut check = Check {
            code: CheckCode::MinQualityScore,
            column: None,
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected: Expectation::AtLeast {
                value: MetricValue::Percentage(min),
            },
            observed: None,
            scope: self.scope,
            evidence: scan.and(quality_evidence(
                report.quality.as_ref(),
                Provenance::Overall,
            )),
            bounds: None,
            baseline: None,
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let Some(quality) = report.quality.as_ref() else {
            return unavailable_quality(check, report);
        };
        let Some(score) = quality.score() else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::NotAssessed);
            check.message =
                "no quality dimension had anything to assess, so there is no overall score"
                    .to_string();
            return check;
        };
        check.observed = Some(MetricValue::Percentage(score));
        let bounds = quality
            .score_bounds()
            .and_then(|bounds| check_bounds(bounds, bounds.overall_score));
        self.decide_score(&mut check, score, min, bounds, "the overall quality score");
        check
    }

    fn dimension_score_check(
        &self,
        report: &ProfileReport,
        scan: Evidence,
        dimension: QualityDimension,
        min: f64,
    ) -> Check {
        let mut check = Check {
            code: CheckCode::MinDimensionScore,
            column: None,
            dimension: Some(dimension),
            finding: None,
            findings: Vec::new(),
            expected: Expectation::AtLeast {
                value: MetricValue::Percentage(min),
            },
            observed: None,
            scope: self.scope,
            evidence: scan.and(quality_evidence(
                report.quality.as_ref(),
                Provenance::Dimension(dimension),
            )),
            bounds: None,
            baseline: None,
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let Some(quality) = report.quality.as_ref() else {
            return unavailable_quality(check, report);
        };
        let Some(score) = quality
            .scores()
            .dimension_scores
            .get(&dimension.to_string())
            .copied()
            .flatten()
        else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::NotAssessed);
            check.message = "this dimension had nothing to assess in this run".to_string();
            return check;
        };
        check.observed = Some(MetricValue::Percentage(score));
        let bounds = quality.score_bounds().and_then(|bounds| {
            check_bounds(
                bounds,
                bounds
                    .dimension_scores
                    .get(&dimension.to_string())
                    .copied()
                    .flatten(),
            )
        });
        self.decide_score(&mut check, score, min, bounds, "this dimension's score");
        check
    }

    /// Decide a minimum on a score, on its whole-source interval when the
    /// only gap in the evidence is the quality sample and the report bounds
    /// the score. The interval settles the requirement when it lies wholly on
    /// one side of the minimum; a minimum inside it is left unevaluated.
    ///
    /// The score, its bounds and the minimum are compared at the serialized
    /// precision, as the rules about change are: the result and the saved
    /// report show them at two decimals, so a raw comparison failed a score
    /// shown as meeting its minimum, and a reloaded report then passed (#1085).
    fn decide_score(
        &self,
        check: &mut Check,
        score: f64,
        min: f64,
        bounds: Option<CheckBounds>,
        subject: &str,
    ) {
        let sampled = check.evidence.gap() == Some(EvidenceGap::QualitySampled);
        let min = rounded_2(min);
        match bounds {
            Some(bounds) if self.scope == PolicyScope::FullSource && sampled => {
                check.bounds = Some(bounds);
                check.status = if rounded_2(bounds.lower) >= min {
                    CheckStatus::Passed
                } else if rounded_2(bounds.upper) < min {
                    CheckStatus::Failed
                } else {
                    CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete {
                        gap: EvidenceGap::QualitySampled,
                    })
                };
                check.message = bounded_message(&check.status, subject);
            }
            _ => {
                check.status = self.decide_aggregate(check.evidence, rounded_2(score) >= min);
                check.message = aggregate_message(&check.status, subject);
            }
        }
    }

    /// One check per named column, in column-name order, then one per
    /// remaining profiled column when a wildcard limit is set, in report
    /// column order.
    ///
    /// The order is fixed by the data rather than by how the policy was
    /// written, so a policy read out of a JSON object — where key order is not
    /// preserved uniformly across languages — still evaluates identically.
    fn null_percentage_checks(&self, report: &ProfileReport, scan: Evidence) -> Vec<Check> {
        let mut checks = Vec::new();
        for (column, max) in &self.max_null_percentage {
            let profile = report
                .column_profiles
                .iter()
                .find(|candidate| &candidate.name == column);
            checks.push(self.null_percentage_check(column, profile, scan, *max));
        }
        if let Some(max) = self.max_null_percentage_any {
            for profile in &report.column_profiles {
                if !self.max_null_percentage.contains_key(&profile.name) {
                    checks.push(self.null_percentage_check(
                        &profile.name,
                        Some(profile),
                        scan,
                        max,
                    ));
                }
            }
        }
        checks
    }

    fn null_percentage_check(
        &self,
        column: &str,
        profile: Option<&ColumnProfile>,
        scan: Evidence,
        max: f64,
    ) -> Check {
        // Null counts accumulate over every scanned row rather than over the
        // retained quality sample, so only the scan's own gap applies here.
        let mut check = Check {
            code: CheckCode::MaxNullPercentage,
            column: Some(column.to_string()),
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected: Expectation::AtMost {
                value: MetricValue::Percentage(max),
            },
            observed: None,
            scope: self.scope,
            evidence: scan,
            bounds: None,
            baseline: None,
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let Some(profile) = profile else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::ColumnNotProfiled);
            check.message = "this column has no profile in the report".to_string();
            return check;
        };
        let Some(percentage) = null_percentage(profile) else {
            // No value was read for the column, so "what share of its values
            // are null" has no answer. Zero rows is not zero percent.
            check.status = CheckStatus::NotEvaluated(NotEvaluated::NotAssessed);
            check.message = "no values were read for this column".to_string();
            return check;
        };
        check.observed = Some(MetricValue::Percentage(percentage));
        // At the precision `observed` is shown at, as for scores (#1085).
        check.status =
            self.decide_aggregate(check.evidence, rounded_2(percentage) <= rounded_2(max));
        check.message = match check.status {
            CheckStatus::Passed => "this column's null percentage is within the allowance",
            CheckStatus::Failed => "this column's null percentage is above the allowance",
            CheckStatus::NotEvaluated(_) => {
                "the scan does not cover the whole source, and a null percentage over \
                 part of it bounds nothing about the rest"
            }
        }
        .to_string();
        check
    }

    /// The null-count ceilings, in the same order as the null-percentage
    /// ones: named columns in name order, then every other profiled column in
    /// report order.
    fn null_count_checks(&self, report: &ProfileReport, scan: Evidence) -> Vec<Check> {
        let mut checks = Vec::new();
        for (column, max) in &self.max_null_count {
            let profile = report
                .column_profiles
                .iter()
                .find(|candidate| &candidate.name == column);
            checks.push(self.null_count_check(column, profile, scan, *max));
        }
        if let Some(max) = self.max_null_count_any {
            for profile in &report.column_profiles {
                if !self.max_null_count.contains_key(&profile.name) {
                    checks.push(self.null_count_check(&profile.name, Some(profile), scan, max));
                }
            }
        }
        checks
    }

    /// A null count, like a duplicate count, only grows as more rows are read:
    /// nulls the scan witnessed above the allowance fail a full-source rule
    /// whatever the scan missed, and a count within it passes only when the
    /// scan covered the source. The count is exact, so nothing is rounded.
    /// Unlike a percentage, it has an answer when no value was read: zero.
    fn null_count_check(
        &self,
        column: &str,
        profile: Option<&ColumnProfile>,
        scan: Evidence,
        max: usize,
    ) -> Check {
        let mut check = Check {
            code: CheckCode::MaxNullCount,
            column: Some(column.to_string()),
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected: Expectation::AtMost {
                value: MetricValue::Count(max),
            },
            observed: None,
            scope: self.scope,
            evidence: scan,
            bounds: None,
            baseline: None,
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let Some(profile) = profile else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::ColumnNotProfiled);
            check.message = "this column has no profile in the report".to_string();
            return check;
        };
        let exceeded = profile.null_count > max;
        check.observed = Some(MetricValue::Count(profile.null_count));
        check.status = match (self.scope, scan.gap()) {
            (PolicyScope::FullSource, Some(_)) if exceeded => CheckStatus::Failed,
            (PolicyScope::FullSource, Some(gap)) => {
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap })
            }
            _ if exceeded => CheckStatus::Failed,
            _ => CheckStatus::Passed,
        };
        check.message = match check.status {
            CheckStatus::Passed => "this column's null count is within the allowance",
            CheckStatus::Failed => "nulls were observed in this column above the allowance",
            CheckStatus::NotEvaluated(_) => {
                "no nulls above the allowance were observed in this column, and the rows \
                 that were not read may hold more"
            }
        }
        .to_string();
        check
    }

    /// A duplicate count, like a null count, is a requirement that an incomplete scan
    /// can still settle in one direction: rows already witnessed as duplicates
    /// do not stop being duplicates when more rows are read, so an exact count
    /// above the allowance is a conclusive failure. A count at or below it is
    /// not a pass — the rows that were not read may hold more. An estimated
    /// count witnesses nothing and settles neither direction.
    fn duplicate_rows_check(&self, report: &ProfileReport, scan: Evidence, max: usize) -> Check {
        let mut check = Check {
            code: CheckCode::MaxDuplicateRows,
            column: None,
            dimension: Some(QualityDimension::Uniqueness),
            finding: None,
            findings: Vec::new(),
            expected: Expectation::AtMost {
                value: MetricValue::Count(max),
            },
            observed: None,
            scope: self.scope,
            evidence: scan.and(quality_evidence(
                report.quality.as_ref(),
                Provenance::Component("duplicate_rows"),
            )),
            bounds: None,
            baseline: None,
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let Some(quality) = report.quality.as_ref() else {
            return unavailable_quality(check, report);
        };
        let Some(uniqueness) = quality
            .metrics
            .uniqueness
            .as_ref()
            .filter(|metrics| metrics.rows_checked > 0)
        else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::NotAssessed);
            check.message = "no rows were scanned for duplicates in this run".to_string();
            return check;
        };
        let observed = uniqueness.duplicate_rows;
        let exceeded = observed > max;
        let estimated = uniqueness.duplicate_rows_approximate;
        check.observed = Some(MetricValue::Count(observed));
        check.status = match (self.scope, check.evidence.gap()) {
            // A witnessed violation is a decision, whatever the scan missed.
            (PolicyScope::FullSource, Some(_)) if exceeded && !estimated => CheckStatus::Failed,
            (PolicyScope::FullSource, Some(gap)) => {
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap })
            }
            _ if exceeded => CheckStatus::Failed,
            _ => CheckStatus::Passed,
        };
        check.message = match check.status {
            CheckStatus::Passed => "the duplicate-row count is within the allowance",
            CheckStatus::Failed if estimated => {
                "the estimated duplicate-row count is above the allowance"
            }
            CheckStatus::Failed => "duplicate rows were observed above the allowance",
            // Every row may have been read: the gap is the estimate, which
            // can neither witness duplicates nor rule them out (#819).
            CheckStatus::NotEvaluated(_) if estimated => {
                "the duplicate-row count is an estimate, which can neither witness \
                 duplicates above the allowance nor rule them out"
            }
            CheckStatus::NotEvaluated(_) => {
                "no duplicate above the allowance was observed, and the rows that were not \
                 read may hold more"
            }
        }
        .to_string();
        check
    }

    /// How old a column's newest date may be, in days before the timeliness
    /// reference date. See [`max_date_age`](Self::max_date_age).
    fn date_age_check(
        &self,
        report: &ProfileReport,
        column: &str,
        profile: Option<&ColumnProfile>,
        scan: Evidence,
        max_days: u32,
    ) -> Check {
        let mut check = Check {
            code: CheckCode::MaxDateAge,
            column: Some(column.to_string()),
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected: Expectation::AtMost {
                value: MetricValue::Days(i64::from(max_days)),
            },
            observed: None,
            scope: self.scope,
            evidence: scan,
            bounds: None,
            baseline: None,
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let Some(profile) = profile else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::ColumnNotProfiled);
            check.message = "this column has no profile in the report".to_string();
            return check;
        };
        let (newest, slash_order) = match newest_date(profile) {
            NewestDate::Date { date, order } => (date, order),
            absent => {
                check.status = CheckStatus::NotEvaluated(NotEvaluated::NotAssessed);
                check.message = match absent {
                    NewestDate::Malformed => {
                        "the newest date recorded for this column is not a YYYY-MM-DD date"
                    }
                    _ if profile.data_type == DataType::Date => {
                        "no newest date was recorded for this column"
                    }
                    _ => "this column is not typed as a date, so it has no newest date",
                }
                .to_string();
                return check;
            }
        };
        check.evidence = scan.and(date_sample_evidence(profile));
        let Some(quality) = report.quality.as_ref() else {
            return unavailable_quality(check, report);
        };
        let Some(clock) = quality
            .metrics
            .timeliness
            .as_ref()
            .and_then(|timeliness| timeliness.clock)
        else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::SettingUnrecorded {
                setting: "timeliness_clock".to_string(),
            });
            check.message =
                "the report records no timeliness clock to measure the age against".to_string();
            return check;
        };
        let age = (clock.reference_date - newest).num_days();
        check.observed = Some(MetricValue::Days(age));
        let fresh = age <= i64::from(max_days);
        // The newest date only moves forward as more values are read, so a
        // fresh one is a decision whatever the evidence missed, as long as
        // the values read keep the reading they were given. Slash dates are
        // read in an order the column's values decide (#811), and an order
        // the unread values can still change may move the dates already read.
        let settled = slash_order_settled(slash_order);
        check.status = match (self.scope, check.evidence.gap()) {
            (PolicyScope::FullSource, Some(gap)) if !(fresh && settled) => {
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap })
            }
            _ if fresh => CheckStatus::Passed,
            _ => CheckStatus::Failed,
        };
        check.message = match check.status {
            CheckStatus::Passed => "this column's newest date is within the allowed age",
            CheckStatus::Failed => "this column's newest date is older than the allowed age",
            CheckStatus::NotEvaluated(_) if fresh => {
                "the newest date read in this column is within the allowed age, and the values \
                 that were not read may change the day and month order its slash dates are \
                 read in"
            }
            CheckStatus::NotEvaluated(_) => {
                "the newest date read in this column is older than the allowed age, and the \
                 values that were not read may hold a newer one"
            }
        }
        .to_string();
        check
    }

    /// How far a score may drop below the baseline's. Both sides are read
    /// from the delta, at their serialized precision, so the decision is the
    /// one a saved report and its baseline document reproduce.
    fn score_drop_check(
        &self,
        report: &ProfileReport,
        delta: &ReportDelta,
        sides: &Sides<&ReportSide>,
        identifiers: &Sides<Option<&[String]>>,
        dimension: Option<QualityDimension>,
        points: f64,
    ) -> Check {
        let (code, change, subject) = match dimension {
            None => (
                CheckCode::MaxQualityScoreDrop,
                &delta.quality_score,
                "the overall quality score",
            ),
            Some(dimension) => (
                CheckCode::MaxDimensionScoreDrop,
                &delta
                    .dimensions
                    .iter()
                    .find(|change| change.dimension == dimension)
                    .expect("a delta lists every dimension this build knows")
                    .score,
                "this dimension's score",
            ),
        };
        let mut check = Check {
            code,
            column: None,
            dimension,
            finding: None,
            findings: Vec::new(),
            // `0.0 - points`, not `-points`: a permitted drop of zero
            // serializes as 0.0 rather than -0.0.
            expected: Expectation::ChangeAtLeast {
                value: MetricValue::Percentage(0.0 - points),
            },
            observed: change.b.map(MetricValue::Percentage),
            scope: self.scope,
            evidence: change.evidence.b,
            bounds: None,
            baseline: Some(baseline_reading(change.a, change.evidence.a, change.abs)),
            status: CheckStatus::Passed,
            message: String::new(),
        };
        if report.quality.is_none() {
            return unavailable_quality(check, report);
        }
        let schema_changed = !delta.schema.added.is_empty() || !delta.schema.removed.is_empty();
        // The overall score averages the dimensions that had something to
        // assess, so a dimension scored on one side only changes the average
        // itself, whatever the data did.
        let dimensions_changed = dimension.is_none()
            && delta
                .dimensions
                .iter()
                .any(|change| change.score.a.is_some() != change.score.b.is_some());
        let reason = if let Some(gap) = pack_gap(delta, dimension) {
            Some(gap)
        } else if change.b.is_none() {
            Some(NotEvaluated::NotAssessed)
        } else if change.a.is_none() {
            Some(NotEvaluated::NotInBaseline)
        } else if schema_changed {
            Some(NotEvaluated::SchemaChanged)
        } else if dimensions_changed {
            Some(NotEvaluated::DimensionsChanged)
        } else {
            definition_gap(
                delta.metric_semantics.a.as_ref(),
                delta.metric_semantics.b.as_ref(),
                score_definitions(dimension),
            )
            .or_else(|| {
                setting_gap(
                    &delta.configuration,
                    identifiers,
                    dimension,
                    self.timeliness_clock,
                    score_reads_clock(delta, dimension),
                )
            })
        };
        if let Some(reason) = reason {
            return not_evaluated_change(check, reason, subject);
        }
        let change_points = change
            .abs
            .expect("a delta has a change whenever both sides have a score");
        // Both sides and their change are at the serialized precision, so the
        // allowance is too: the decision is the one the document shows.
        let allowance = 0.0 - rounded_2(points);
        let bounded = match self.bounded_change(change, sides, dimension) {
            ChangeReading::Points => None,
            ChangeReading::Bounded(bounded) => Some(bounded),
            ChangeReading::Blocked(gap) => {
                return not_evaluated_change(
                    check,
                    NotEvaluated::EvidenceIncomplete { gap },
                    subject,
                );
            }
        };
        if let Some(bounded) = bounded {
            check.bounds = bounded.b;
            if let Some(baseline) = check.baseline.as_mut() {
                baseline.bounds = bounded.a;
            }
            check.status = if bounded.lower >= allowance {
                CheckStatus::Passed
            } else if bounded.upper < allowance {
                CheckStatus::Failed
            } else {
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete {
                    gap: EvidenceGap::QualitySampled,
                })
            };
            check.message = match check.status {
                CheckStatus::Passed => format!(
                    "{subject} was computed over a sample on at least one side, and every \
                     change the whole-source intervals allow is within the permitted drop"
                ),
                CheckStatus::Failed => format!(
                    "{subject} was computed over a sample on at least one side, and every \
                     change the whole-source intervals allow is a larger drop than permitted"
                ),
                CheckStatus::NotEvaluated(_) => format!(
                    "{subject} was computed over a sample on at least one side, and the \
                     permitted drop lies within the change the whole-source intervals allow"
                ),
            };
            return check;
        }
        check.status = self.decide_change(&change.evidence, change_points >= allowance);
        check.message = match check.status {
            CheckStatus::Passed => {
                format!("{subject} dropped no more than allowed from the baseline")
            }
            CheckStatus::Failed => format!("{subject} dropped more than allowed from the baseline"),
            CheckStatus::NotEvaluated(_) => evidence_message(subject),
        };
        check
    }

    /// The interval a full-source score change lies in, when at least one
    /// side's score came from a retained sample and every such side bounds
    /// it (#1035). A side whose score covers its whole source is a point.
    /// The change runs from the current side's lower bound minus the
    /// baseline's upper bound to the current upper bound minus the baseline
    /// lower bound, at the serialized precision.
    ///
    /// [`ChangeReading::Points`] leaves the decision to
    /// [`Self::decide_change`]: under [`PolicyScope::Observed`], and when
    /// both sides cover their whole source. A side with another gap, or a
    /// sampled score without a recorded interval, blocks the decision with
    /// its gap, the evaluated report's named first. Stored bounds on a score
    /// whose coverage is unrecorded are not read, as the minimum score rules
    /// do not read them: this build cannot tell which components such a
    /// score weighs, so it cannot tell what the interval vouches for.
    fn bounded_change(
        &self,
        change: &ScoreChange,
        sides: &Sides<&ReportSide>,
        dimension: Option<QualityDimension>,
    ) -> ChangeReading {
        if self.scope != PolicyScope::FullSource {
            return ChangeReading::Points;
        }
        type SideReading = Result<((f64, f64), Option<CheckBounds>), EvidenceGap>;
        let interval = |score: f64, evidence: Evidence, side: &ReportSide| -> SideReading {
            match evidence.gap() {
                None => Ok(((score, score), None)),
                Some(EvidenceGap::QualitySampled) => {
                    let interval = side.score_bounds().and_then(|bounds| {
                        let interval = match dimension {
                            None => bounds.overall_score,
                            Some(dimension) => bounds
                                .dimension_scores
                                .get(&dimension.to_string())
                                .copied()
                                .flatten(),
                        };
                        check_bounds(bounds, interval)
                    });
                    match interval {
                        Some(bounds) => Ok((
                            (rounded_2(bounds.lower), rounded_2(bounds.upper)),
                            Some(bounds),
                        )),
                        None => Err(EvidenceGap::QualitySampled),
                    }
                }
                Some(gap) => Err(gap),
            }
        };
        let (Some(a_score), Some(b_score)) = (change.a, change.b) else {
            return ChangeReading::Points;
        };
        let b = interval(b_score, change.evidence.b, sides.b);
        let a = interval(a_score, change.evidence.a, sides.a);
        let (((b_lower, b_upper), b), ((a_lower, a_upper), a)) = match (b, a) {
            (Err(gap), _) | (_, Err(gap)) => return ChangeReading::Blocked(gap),
            (Ok(b), Ok(a)) => (b, a),
        };
        if a.is_none() && b.is_none() {
            return ChangeReading::Points;
        }
        ChangeReading::Bounded(BoundedChange {
            lower: rounded_2(b_lower - a_upper),
            upper: rounded_2(b_upper - a_lower),
            a,
            b,
        })
    }

    /// One check per named column, in name order, then one per other column
    /// both reports profiled, in the baseline's order. A column only the
    /// evaluated report has gets no rule unless the policy names it.
    fn null_increase_checks(&self, delta: &ReportDelta) -> Vec<Check> {
        let index = ColumnIndex::new(delta);
        let mut checks = Vec::new();
        for (column, points) in &self.max_null_percentage_increase {
            checks.push(self.null_increase_check(delta, &index, column, *points));
        }
        if let Some(points) = self.max_null_percentage_increase_any {
            for column in &delta.schema.common {
                if !self.max_null_percentage_increase.contains_key(column) {
                    checks.push(self.null_increase_check(delta, &index, column, points));
                }
            }
        }
        checks
    }

    /// How far a column's null percentage may rise above the baseline's.
    /// Null counts accumulate over every scanned row, so each side's scan
    /// coverage is its evidence, and only the null-token definition can make
    /// the same data count differently. A column's own rule does not depend on
    /// the rest of the column set.
    fn null_increase_check(
        &self,
        delta: &ReportDelta,
        index: &ColumnIndex<'_>,
        column: &str,
        points: f64,
    ) -> Check {
        let change = index.changes.get(column).copied();
        let (null_pct_a, null_pct_b, null_pct_delta) = change.map_or(
            (None, None, None),
            |ColumnChange {
                 null_pct_a,
                 null_pct_b,
                 null_pct_delta,
                 ..
             }| (*null_pct_a, *null_pct_b, *null_pct_delta),
        );
        let in_report = index.in_report.contains(column);
        let in_baseline = index.in_baseline.contains(column);
        let evidence = Sides {
            a: delta.reports.a.evidence,
            b: delta.reports.b.evidence,
        };
        let mut check = Check {
            code: CheckCode::MaxNullPercentageIncrease,
            column: Some(column.to_string()),
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected: Expectation::ChangeAtMost {
                value: MetricValue::Percentage(points),
            },
            observed: null_pct_b.map(MetricValue::Percentage),
            scope: self.scope,
            evidence: evidence.b,
            bounds: None,
            baseline: Some(baseline_reading(null_pct_a, evidence.a, null_pct_delta)),
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let reason = if !in_report {
            Some(NotEvaluated::ColumnNotProfiled)
        } else if !in_baseline {
            Some(NotEvaluated::NotInBaseline)
        } else if null_pct_b.is_none() {
            Some(NotEvaluated::NotAssessed)
        } else if null_pct_a.is_none() {
            Some(NotEvaluated::NotInBaseline)
        } else {
            definition_gap(
                delta.metric_semantics.a.as_ref(),
                delta.metric_semantics.b.as_ref(),
                &[Definition::NullTokens],
            )
        };
        let subject = "this column's null percentage";
        if let Some(reason) = reason {
            return not_evaluated_change(check, reason, subject);
        }
        let change_points =
            null_pct_delta.expect("a delta has a change whenever both sides have a value");
        check.status = self.decide_change(&evidence, change_points <= rounded_2(points));
        check.message = match check.status {
            CheckStatus::Passed => {
                format!("{subject} rose no more than allowed above the baseline")
            }
            CheckStatus::Failed => format!("{subject} rose more than allowed above the baseline"),
            CheckStatus::NotEvaluated(_) => evidence_message(subject),
        };
        check
    }

    /// How far the row count may fall or grow from the baseline's, in percent
    /// of the baseline's `rows_processed` (#1291).
    fn row_count_change_check(
        &self,
        delta: &ReportDelta,
        rule: RowCountRule,
        percent: f64,
    ) -> Check {
        let (a, b) = (&delta.reports.a, &delta.reports.b);
        // A baseline that read no rows has no count to take a share of.
        let baseline_rows = a.rows_processed.filter(|&rows| rows > 0);
        let change = baseline_rows
            .zip(b.rows_processed)
            .map(|(a, b)| relative_change(a as f64, b as f64));
        let allowed = rounded_2(percent);
        let (code, expected) = match rule {
            // `0.0 - percent`, not `-percent`: a permitted drop of zero
            // serializes as 0.0 rather than -0.0.
            RowCountRule::Drop => (
                CheckCode::MaxRowCountDrop,
                Expectation::RelativeChangeAtLeast {
                    value: MetricValue::Percentage(0.0 - percent),
                },
            ),
            RowCountRule::Growth => (
                CheckCode::MaxRowCountGrowth,
                Expectation::RelativeChangeAtMost {
                    value: MetricValue::Percentage(percent),
                },
            ),
        };
        let check = Check {
            code,
            column: None,
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected,
            observed: b.rows_processed.map(MetricValue::Count),
            scope: self.scope,
            evidence: b.evidence,
            bounds: None,
            baseline: Some(BaselineReading {
                observed: a.rows_processed.map(MetricValue::Count),
                evidence: a.evidence,
                change: change.map(MetricValue::Percentage),
                bounds: None,
            }),
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let subject = "the row count";
        let schema_changed = !delta.schema.added.is_empty() || !delta.schema.removed.is_empty();
        let reason = if change.is_none() {
            Some(NotEvaluated::NotInBaseline)
        } else if schema_changed {
            Some(NotEvaluated::SchemaChanged)
        } else {
            None
        };
        if let Some(reason) = reason {
            return not_evaluated_change(check, reason, subject);
        }
        let change = change.expect("a decided rule has a change");
        let (satisfied, verb) = match rule {
            RowCountRule::Drop => (change >= 0.0 - allowed, "fell"),
            RowCountRule::Growth => (change <= allowed, "grew"),
        };
        let mut check = check;
        // A count cannot fall by more than all of it, so a drop allowance of
        // 100% (at the serialized precision) holds whatever a scan did not
        // read.
        check.status = if rule == RowCountRule::Drop && allowed >= PERCENTAGE_MAX {
            CheckStatus::Passed
        } else {
            self.decide_row_count_change(
                rule,
                &Sides {
                    a: a.evidence,
                    b: b.evidence,
                },
                satisfied,
            )
        };
        check.message = match check.status {
            CheckStatus::Passed => {
                format!("{subject} {verb} no more than allowed from the baseline")
            }
            CheckStatus::Failed => format!("{subject} {verb} more than allowed from the baseline"),
            CheckStatus::NotEvaluated(_) => format!(
                "{subject} {verb} {} than allowed from the baseline, and the rows a scan did not \
                 read may reverse that",
                if satisfied { "no more" } else { "more" }
            ),
        };
        check
    }

    /// Decide a row count change. Each scan read a lower bound of its
    /// source's rows, so a gap on one side can move the true change one way
    /// only: more unread rows in this report make the change larger, more in
    /// the baseline make it smaller. An outcome the unread rows could not
    /// reverse stands; the other is unevaluated, naming the gap of the side
    /// whose unread rows could reverse it. With both sides incomplete, either
    /// outcome can be reversed.
    fn decide_row_count_change(
        &self,
        rule: RowCountRule,
        evidence: &Sides<Evidence>,
        satisfied: bool,
    ) -> CheckStatus {
        let outcome = if satisfied {
            CheckStatus::Passed
        } else {
            CheckStatus::Failed
        };
        if self.scope == PolicyScope::Observed {
            return outcome;
        }
        // A larger true change reverses a drop rule's failure and a growth
        // rule's pass; a smaller one the opposite.
        let larger_reverses = match rule {
            RowCountRule::Drop => !satisfied,
            RowCountRule::Growth => satisfied,
        };
        let gap = if larger_reverses {
            evidence.b.gap()
        } else {
            evidence.a.gap()
        };
        match gap {
            Some(gap) => CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap }),
            None => outcome,
        }
    }

    /// One check per named column, in name order, then one per other column
    /// both reports profiled, in the baseline's order, as for
    /// [`null_increase_checks`](Self::null_increase_checks).
    fn distinct_drop_checks(
        &self,
        delta: &ReportDelta,
        baseline: &ReportSide,
        current: &ReportSide,
    ) -> Vec<Check> {
        if self.max_distinct_drop.is_empty() && self.max_distinct_drop_any.is_none() {
            return Vec::new();
        }
        fn by_name(side: &ReportSide) -> HashMap<&str, &ColumnSide> {
            side.columns()
                .iter()
                .map(|column| (column.name.as_str(), column))
                .collect()
        }
        let sides = Sides {
            a: by_name(baseline),
            b: by_name(current),
        };
        let mut checks = Vec::new();
        for (column, percent) in &self.max_distinct_drop {
            checks.push(self.distinct_drop_check(delta, &sides, column, *percent));
        }
        if let Some(percent) = self.max_distinct_drop_any {
            for column in &delta.schema.common {
                if !self.max_distinct_drop.contains_key(column) {
                    checks.push(self.distinct_drop_check(delta, &sides, column, percent));
                }
            }
        }
        checks
    }

    /// How far a column's distinct count may fall below the baseline's, in
    /// percent of the baseline's count (#1026). Distinct counts accumulate
    /// over every scanned row, so each side's scan coverage is its evidence.
    /// A count that fell with the rows is not a collapse: the check fails
    /// only when the distinct share of non-null values fell by more than the
    /// allowance too. Both changes are compared at the serialized precision.
    fn distinct_drop_check(
        &self,
        delta: &ReportDelta,
        sides: &Sides<HashMap<&str, &ColumnSide>>,
        column: &str,
        percent: f64,
    ) -> Check {
        let read = |side: &HashMap<&str, &ColumnSide>| {
            side.get(column).map(|column| DistinctReading::of(column))
        };
        let (a, b) = (read(&sides.a), read(&sides.b));
        let counted = |reading: Option<DistinctReading>| {
            reading.and_then(|reading| reading.non_null.filter(|&n| n > 0).and(reading.count))
        };
        let (count_a, count_b) = (counted(a), counted(b));
        let count_change = count_a
            .zip(count_b)
            .map(|(a, b)| relative_change(a as f64, b as f64));
        let evidence = Sides {
            a: delta.reports.a.evidence,
            b: delta.reports.b.evidence,
        };
        let mut check = Check {
            code: CheckCode::MaxDistinctDrop,
            column: Some(column.to_string()),
            dimension: None,
            finding: None,
            findings: Vec::new(),
            // `0.0 - percent`, not `-percent`: a permitted drop of zero
            // serializes as 0.0 rather than -0.0.
            expected: Expectation::RelativeChangeAtLeast {
                value: MetricValue::Percentage(0.0 - percent),
            },
            observed: count_b.map(MetricValue::Count),
            scope: self.scope,
            evidence: evidence.b,
            bounds: None,
            baseline: Some(BaselineReading {
                observed: count_a.map(MetricValue::Count),
                evidence: evidence.a,
                change: count_change.map(MetricValue::Percentage),
                bounds: None,
            }),
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let exact = |reading: Option<DistinctReading>| {
            reading.is_some_and(|reading| reading.approximate == Some(false))
        };
        let reason = if b.is_none() {
            Some(NotEvaluated::ColumnNotProfiled)
        } else if a.is_none() {
            Some(NotEvaluated::NotInBaseline)
        } else if count_b.is_none() {
            Some(NotEvaluated::NotAssessed)
        } else if count_a.is_none() {
            Some(NotEvaluated::NotInBaseline)
        } else if !exact(a) || !exact(b) {
            Some(NotEvaluated::ApproximateCount)
        } else {
            definition_gap(
                delta.metric_semantics.a.as_ref(),
                delta.metric_semantics.b.as_ref(),
                &[Definition::NullTokens, Definition::JsonNumberReading],
            )
        };
        let subject = "this column's distinct count";
        if let Some(reason) = reason {
            let mut check = not_evaluated_change(check, reason, subject);
            // A nested column holds values but has no distinct count.
            if count_b.is_none()
                && b.is_some_and(|b| b.count.is_none() && b.non_null.is_some_and(|n| n > 0))
            {
                check.message = "the report counted no distinct values for this column".into();
            }
            return check;
        }
        let (Some(a), Some(b), Some(count_change)) = (a, b, count_change) else {
            unreachable!("a counted column has a reading and a change on both sides")
        };
        let allowed = 0.0 - rounded_2(percent);
        let count_held = count_change >= allowed;
        let share_held = || {
            let share = |reading: DistinctReading| {
                reading.count.expect("counted") as f64 / reading.non_null.expect("counted") as f64
            };
            relative_change(share(a), share(b)) >= allowed
        };
        check.status = self.decide_change(&evidence, count_held || share_held());
        check.message = match check.status {
            CheckStatus::Passed if count_held => {
                format!("{subject} fell no more than allowed from the baseline")
            }
            CheckStatus::Passed => format!(
                "{subject} fell more than allowed from the baseline, and its distinct share of \
                 non-null values fell no more than allowed, so the drop follows fewer values"
            ),
            CheckStatus::Failed => format!(
                "{subject} and its distinct share of non-null values both fell more than allowed \
                 from the baseline"
            ),
            CheckStatus::NotEvaluated(_) => evidence_message(subject),
        };
        check
    }

    /// Whether the side without `column` shows its source lacks it: the
    /// side's selection included the column, so it was not in what the side
    /// read, and the side's column set covers the whole source. Under
    /// [`PolicyScope::Observed`] the question is about what was read, so a
    /// column set covering part of the source still decides. `unselected` is
    /// the reason when the selection left the column out.
    fn absence_status(
        &self,
        lacking: &ReportSide,
        column: &str,
        unselected: NotEvaluated,
    ) -> CheckStatus {
        match lacking.column_selection() {
            None => CheckStatus::NotEvaluated(NotEvaluated::SettingUnrecorded {
                setting: "columns".to_string(),
            }),
            Some(selection) if !selection.includes(column) => CheckStatus::NotEvaluated(unselected),
            Some(_) => match (self.scope, lacking.schema_evidence().gap()) {
                (PolicyScope::FullSource, Some(gap)) => {
                    CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap })
                }
                _ => CheckStatus::Failed,
            },
        }
    }

    fn require_column_check(
        &self,
        current: &ReportSide,
        current_names: &HashSet<&str>,
        column: &str,
    ) -> Check {
        let status = if current_names.contains(column) {
            CheckStatus::Passed
        } else {
            self.absence_status(current, column, NotEvaluated::ColumnNotProfiled)
        };
        let message = match &status {
            CheckStatus::Passed => "the required column was profiled",
            CheckStatus::Failed => "the required column is not in the source the report read",
            CheckStatus::NotEvaluated(NotEvaluated::ColumnNotProfiled) => {
                "the run's column selection left the required column out"
            }
            CheckStatus::NotEvaluated(NotEvaluated::SettingUnrecorded { .. }) => {
                "the report does not record which columns it selected, so a missing column \
                 cannot be shown to be absent from the source"
            }
            CheckStatus::NotEvaluated(_) => {
                "the required column is not in the records read, and the records that were not \
                 read may hold it"
            }
        };
        self.column_check(
            CheckCode::RequireColumn,
            column,
            Expectation::Present,
            current,
            None,
            status,
            message,
        )
    }

    fn forbid_removed_check(
        &self,
        baseline: &ReportSide,
        current: &ReportSide,
        current_names: &HashSet<&str>,
        column: &str,
    ) -> Check {
        let status = if current_names.contains(column) {
            CheckStatus::Passed
        } else {
            self.absence_status(
                current,
                column,
                NotEvaluated::SettingChanged {
                    setting: "columns".to_string(),
                },
            )
        };
        let message = match &status {
            CheckStatus::Passed => "the column the baseline profiled is still profiled",
            CheckStatus::Failed => {
                "a column the baseline profiled is not in the source the report read"
            }
            CheckStatus::NotEvaluated(NotEvaluated::SettingChanged { .. }) => {
                "the report's column selection left out a column the baseline profiled"
            }
            CheckStatus::NotEvaluated(NotEvaluated::SettingUnrecorded { .. }) => {
                "the report does not record which columns it selected, so a column the baseline \
                 profiled cannot be shown to be absent from its source"
            }
            CheckStatus::NotEvaluated(_) => {
                "a column the baseline profiled is not in the records the report read, and the \
                 records that were not read may hold it"
            }
        };
        self.column_check(
            CheckCode::ForbidRemovedColumn,
            column,
            Expectation::Present,
            current,
            Some(baseline),
            status,
            message,
        )
    }

    fn forbid_added_check(
        &self,
        baseline: &ReportSide,
        current: &ReportSide,
        baseline_names: &HashSet<&str>,
        column: &str,
    ) -> Check {
        let status = if baseline_names.contains(column) {
            CheckStatus::Passed
        } else {
            self.absence_status(
                baseline,
                column,
                NotEvaluated::SettingChanged {
                    setting: "columns".to_string(),
                },
            )
        };
        let message = match &status {
            CheckStatus::Passed => "the column is one the baseline profiled",
            CheckStatus::Failed => {
                "the report profiled a column the baseline's source did not have"
            }
            CheckStatus::NotEvaluated(NotEvaluated::SettingChanged { .. }) => {
                "the baseline's column selection left this column out"
            }
            CheckStatus::NotEvaluated(NotEvaluated::SettingUnrecorded { .. }) => {
                "the baseline does not record which columns it selected, so this column cannot be \
                 shown to be new"
            }
            CheckStatus::NotEvaluated(_) => {
                "this column is not in the records the baseline read, and the records it did not \
                 read may hold it"
            }
        };
        self.column_check(
            CheckCode::ForbidAddedColumn,
            column,
            Expectation::InBaseline,
            current,
            Some(baseline),
            status,
            message,
        )
    }

    /// The closed-schema rule's statement about the source columns the report
    /// did not profile: never decided, since nothing profiled shows them.
    fn unprofiled_columns_check(
        &self,
        baseline: &ReportSide,
        current: &ReportSide,
        reason: NotEvaluated,
    ) -> Check {
        let message = match &reason {
            NotEvaluated::ColumnNotProfiled => {
                "the run profiled a column selection, which may have left out a source column \
                 the baseline lacked"
            }
            NotEvaluated::SettingUnrecorded { .. } => {
                "the report does not record which columns it selected, so its columns may not \
                 be all of its source's"
            }
            _ => {
                "the report read part of its source, and the records it did not read may hold \
                 a column the baseline lacked"
            }
        };
        Check {
            column: None,
            ..self.column_check(
                CheckCode::ForbidAddedColumn,
                "",
                Expectation::InBaseline,
                current,
                Some(baseline),
                CheckStatus::NotEvaluated(reason),
                message,
            )
        }
    }

    /// A rule about which columns a report has. Its evidence is the column
    /// set's coverage of each side's source, not a number's.
    #[allow(clippy::too_many_arguments)]
    fn column_check(
        &self,
        code: CheckCode,
        column: &str,
        expected: Expectation,
        current: &ReportSide,
        baseline: Option<&ReportSide>,
        status: CheckStatus,
        message: &str,
    ) -> Check {
        Check {
            code,
            column: Some(column.to_string()),
            dimension: None,
            finding: None,
            findings: Vec::new(),
            expected,
            observed: None,
            scope: self.scope,
            evidence: current.schema_evidence(),
            bounds: None,
            baseline: baseline.map(|baseline| BaselineReading {
                observed: None,
                evidence: baseline.schema_evidence(),
                change: None,
                bounds: None,
            }),
            status,
            message: message.to_string(),
        }
    }

    /// Decide a change between two averages or ratios. Under
    /// [`PolicyScope::FullSource`] a side computed over part of its source
    /// bounds nothing about the rest, so either side's gap leaves the rule
    /// unevaluated, the evaluated report's named first.
    fn decide_change(&self, evidence: &Sides<Evidence>, satisfied: bool) -> CheckStatus {
        match (self.scope, evidence.b.gap().or(evidence.a.gap())) {
            (PolicyScope::FullSource, Some(gap)) => {
                CheckStatus::NotEvaluated(NotEvaluated::EvidenceIncomplete { gap })
            }
            _ if satisfied => CheckStatus::Passed,
            _ => CheckStatus::Failed,
        }
    }
}

/// The schema changes between the two sides, at most one per column, in the
/// delta's column order: the baseline's columns, then the added ones. A
/// change is reported only where both sides recorded what it reads; a flat
/// summary without a column's counts reports neither change for it, and one
/// without its type no type change. Column order is not compared, since no
/// rule reads it.
fn schema_diagnostics(
    baseline: &ReportSide,
    current: &ReportSide,
    delta: &ReportDelta,
) -> Vec<Diagnostic> {
    let diagnostic = |code, column: &str, data_type, message: &str| Diagnostic {
        code,
        column: column.to_string(),
        data_type,
        setting: None,
        message: message.to_string(),
    };
    // Whether each side's identifier hint names the column, `None` when the
    // side did not record its hints.
    let identified = |side: &ReportSide, column: &str| {
        side.identifier_order()
            .map(|names| names.iter().any(|name| name == column))
    };
    // Built once, so a wide schema is not rescanned per column.
    fn names(names: &[String]) -> HashSet<&str> {
        names.iter().map(String::as_str).collect()
    }
    fn by_name(side: &ReportSide) -> HashMap<&str, &ColumnSide> {
        side.columns()
            .iter()
            .map(|column| (column.name.as_str(), column))
            .collect()
    }
    let (added, removed) = (names(&delta.schema.added), names(&delta.schema.removed));
    let (baseline_columns, current_columns) = (by_name(baseline), by_name(current));
    delta
        .columns
        .iter()
        .filter_map(|change| {
            let column = change.name.as_str();
            // The side without the column decides whether its source lacks
            // it: a selection that includes the column would have profiled it
            // or refused to run, so the column is not in what that side read.
            let unselected = |side: &ReportSide| match side.column_selection() {
                Some(selection) if selection.includes(column) => None,
                Some(_) => Some(SettingDifference::SettingChanged {
                    setting: "columns".to_string(),
                }),
                None => Some(SettingDifference::SettingUnrecorded {
                    setting: "columns".to_string(),
                }),
            };
            if added.contains(column) {
                let setting = unselected(baseline);
                let message = match setting {
                    None => {
                        "the report profiled a column the baseline did not find in its source, \
                         so the baseline has nothing to measure its change from"
                    }
                    Some(_) => {
                        "the report profiled a column the baseline has no profile for, and the \
                         baseline's column selection is not shown to include it"
                    }
                };
                return Some(Diagnostic {
                    setting,
                    ..diagnostic(DiagnosticCode::ColumnAdded, column, None, message)
                });
            }
            if removed.contains(column) {
                let setting = unselected(current);
                let message = match setting {
                    None => "the baseline profiled a column the report did not find in its source",
                    Some(_) => {
                        "the baseline profiled a column the report has no profile for, and the \
                         report's column selection is not shown to include it"
                    }
                };
                return Some(Diagnostic {
                    setting,
                    ..diagnostic(DiagnosticCode::ColumnRemoved, column, None, message)
                });
            }
            let (a, b) = (
                *baseline_columns.get(column)?,
                *current_columns.get(column)?,
            );
            if a.populated()? && b.all_null()? {
                return Some(diagnostic(
                    DiagnosticCode::ColumnAllNull,
                    column,
                    None,
                    "this column held values in the baseline, and every value read for it in \
                     the report is null",
                ));
            }
            match (&a.data_type, &b.data_type) {
                (Some(type_a), Some(type_b))
                    if type_a != type_b && a.populated()? && b.populated()? =>
                {
                    let mut retyped = diagnostic(
                        DiagnosticCode::ColumnTypeChanged,
                        column,
                        Some(Sides {
                            a: type_a.clone(),
                            b: type_b.clone(),
                        }),
                        "this column holds values on both sides, and its inferred type differs",
                    );
                    // Only the identifier hint gives a column the identifier
                    // type, so the hint can explain a change only to or from
                    // it.
                    if *type_a == DataType::Identifier || *type_b == DataType::Identifier {
                        let setting = "identifier_columns".to_string();
                        retyped.setting =
                            match (identified(baseline, column), identified(current, column)) {
                                (Some(a), Some(b)) if a == b => None,
                                (Some(_), Some(_)) => {
                                    Some(SettingDifference::SettingChanged { setting })
                                }
                                _ => Some(SettingDifference::SettingUnrecorded { setting }),
                            };
                        if retyped.setting.is_some() {
                            retyped.message = "this column holds values on both sides, and its \
                                               inferred type differs; the identifier hint, \
                                               which sets the type, is not shown to be the \
                                               same on both sides"
                                .to_string();
                        }
                    }
                    Some(retyped)
                }
                _ => None,
            }
        })
        .collect()
}

/// A delta's columns by name, built once per evaluation so a rule over every
/// shared column does not rescan the schema for each one.
/// Which way a row count rule bounds the change from the baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowCountRule {
    Drop,
    Growth,
}

/// What the distinct drop rule reads about one column on one side.
#[derive(Debug, Clone, Copy)]
struct DistinctReading {
    /// Distinct non-null values, `None` when not counted or not recorded.
    count: Option<usize>,
    /// Whether the count is an estimate, `None` when not recorded.
    approximate: Option<bool>,
    /// Non-null values read, `None` when the counts were not recorded.
    non_null: Option<usize>,
}

impl DistinctReading {
    fn of(column: &ColumnSide) -> Self {
        Self {
            count: column.unique_count,
            approximate: column.unique_count_is_approximate,
            non_null: column
                .counts
                .map(|(total, nulls)| total.saturating_sub(nulls)),
        }
    }
}

/// `(b - a) / a` in percent, at the serialized precision. `a` is positive.
fn relative_change(a: f64, b: f64) -> f64 {
    rounded_2((b - a) / a * 100.0)
}

struct ColumnIndex<'a> {
    changes: HashMap<&'a str, &'a ColumnChange>,
    /// Columns the evaluated report profiled.
    in_report: HashSet<&'a str>,
    /// Columns the baseline profiled.
    in_baseline: HashSet<&'a str>,
}

impl<'a> ColumnIndex<'a> {
    fn new(delta: &'a ReportDelta) -> Self {
        let schema = &delta.schema;
        let names = |lists: [&'a [String]; 2]| {
            lists
                .into_iter()
                .flatten()
                .map(String::as_str)
                .collect::<HashSet<_>>()
        };
        Self {
            changes: delta
                .columns
                .iter()
                .map(|change| (change.name.as_str(), change))
                .collect(),
            in_report: names([&schema.common, &schema.added]),
            in_baseline: names([&schema.common, &schema.removed]),
        }
    }
}

/// The message of a `forbid_finding` check whose rule could not look.
const FINDING_NOT_EVALUATED: &str =
    "the finding's rule could not look at the whole report, and found no finding";

/// What a finding is about, which decides how far a partial scan answers a
/// `forbid_finding` check under [`PolicyScope::FullSource`].
///
/// Exhaustive on purpose: a new findings code has to say which it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FindingReach {
    /// How the source was read: the scan itself, or the delimiter detection
    /// over its first records. Decided whatever part of the source was read.
    Read,
    /// Something rows witness, which more rows cannot retract: duplicates, a
    /// repeated key, a future date, a skipped or ragged record. A finding
    /// fails a partial scan; its absence passes only a complete one.
    Witness,
    /// A share of a column's values, which more rows can move either way.
    /// Decided only on a complete scan.
    Share,
}

impl FindingReach {
    fn of(code: FindingCode) -> Self {
        match code {
            FindingCode::PartialScan
            | FindingCode::DelimiterAmbiguous
            | FindingCode::HeaderNotSplit => Self::Read,
            FindingCode::DuplicateRows
            | FindingCode::FutureDates
            | FindingCode::KeyDuplicates
            | FindingCode::RaggedRows
            | FindingCode::RecordsSkipped
            | FindingCode::TemporalOrderViolations
            | FindingCode::UnterminatedQuote => Self::Witness,
            FindingCode::AllNull
            | FindingCode::ConstantColumn
            | FindingCode::LocaleNumbers
            | FindingCode::MixedTypes
            | FindingCode::NullHeavy
            | FindingCode::SensitivePattern => Self::Share,
        }
    }
}

/// Why a side's profiled columns may not be all of its source's columns, or
/// `None` when they are: every column was selected and the column set covers
/// the source. The selection is named ahead of the coverage, since a
/// projection leaves columns out however much of the source was read.
fn unenumerated_columns(side: &ReportSide) -> Option<NotEvaluated> {
    match side.column_selection() {
        None => Some(NotEvaluated::SettingUnrecorded {
            setting: "columns".to_string(),
        }),
        Some(ColumnSelection::All) => side
            .schema_evidence()
            .gap()
            .map(|gap| NotEvaluated::EvidenceIncomplete { gap }),
        Some(_) => Some(NotEvaluated::ColumnNotProfiled),
    }
}

fn column_names(side: &ReportSide) -> HashSet<&str> {
    side.columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

fn baseline_reading(
    observed: Option<f64>,
    evidence: Evidence,
    change: Option<f64>,
) -> BaselineReading {
    BaselineReading {
        observed: observed.map(MetricValue::Percentage),
        evidence,
        change: change.map(MetricValue::Percentage),
        bounds: None,
    }
}

/// Leave a rule about change unevaluated, with a fixed sentence naming why.
fn not_evaluated_change(mut check: Check, reason: NotEvaluated, subject: &str) -> Check {
    check.message = match &reason {
        NotEvaluated::NotAssessed if check.column.is_some() => {
            "no values were read for this column".to_string()
        }
        NotEvaluated::NotAssessed => format!("{subject} was not assessed in this report"),
        NotEvaluated::ColumnNotProfiled => "this column has no profile in the report".to_string(),
        NotEvaluated::NotInBaseline => format!("the baseline carries no value for {subject}"),
        NotEvaluated::ApproximateCount => format!(
            "{subject} is an estimate on at least one side, or not recorded as exact, and an \
             estimate falling is not a column collapsing"
        ),
        NotEvaluated::SchemaChanged => format!(
            "the two reports profiled different columns, so {subject} has a different subject \
             on each side"
        ),
        NotEvaluated::DimensionsChanged => {
            format!("{subject} averages a different set of assessed dimensions on each side")
        }
        NotEvaluated::DefinitionNotComparable { .. } => format!(
            "a measurement definition {subject} depends on is not recorded identically in both \
             reports"
        ),
        NotEvaluated::SettingChanged { .. } => format!(
            "a setting that moves {subject} on unchanged data differs between the two reports"
        ),
        NotEvaluated::SettingUnrecorded { .. } => format!(
            "a setting that moves {subject} on unchanged data is not recorded in both reports"
        ),
        NotEvaluated::QualityUnavailable { .. } => {
            "the report carries no quality assessment".to_string()
        }
        NotEvaluated::EvidenceIncomplete { .. } => evidence_message(subject),
        // Only a `forbid_finding` check, which is not a rule about change,
        // carries this reason.
        NotEvaluated::FindingNotEvaluated { .. } => FINDING_NOT_EVALUATED.to_string(),
    };
    check.status = CheckStatus::NotEvaluated(reason);
    check
}

fn evidence_message(subject: &str) -> String {
    format!(
        "{subject} was computed over part of the source on at least one side, which bounds \
         nothing about the rest"
    )
}

/// A recorded measurement definition a rule about change depends on.
#[derive(Debug, Clone, Copy)]
enum Definition {
    /// The text values counted as null, behind every null count and so every
    /// score.
    NullTokens,
    /// What a pattern match counts, behind the validity score.
    PatternMatchCount,
    /// What the range and sign rules count, behind the accuracy score.
    RangeRuleCount,
    /// What key uniqueness divides by, behind the uniqueness score.
    KeyUniquenessUnits,
    /// What the outlier ratio is measured over, behind the accuracy score.
    OutlierUnits,
    /// Which column key uniqueness may measure, behind the uniqueness score.
    KeyColumnSource,
    /// How values meet outlier fences and rule bounds, behind the accuracy
    /// score.
    AccuracyComparison,
    /// How a JSON number its `f64` does not hold is read, behind the
    /// consistency, precision, uniqueness and accuracy scores.
    JsonNumberReading,
    /// How a sampled score estimates the score over every scanned value,
    /// behind the consistency, accuracy, timeliness, validity and precision
    /// scores.
    SampledScoreEstimator,
}

impl Definition {
    fn key(self) -> &'static str {
        match self {
            Self::NullTokens => "null_tokens",
            Self::PatternMatchCount => "pattern_match_count",
            Self::RangeRuleCount => "range_rule_count",
            Self::KeyUniquenessUnits => "key_uniqueness_units",
            Self::OutlierUnits => "outlier_units",
            Self::KeyColumnSource => "key_column_source",
            Self::AccuracyComparison => "accuracy_comparison",
            Self::JsonNumberReading => "json_number_reading",
            Self::SampledScoreEstimator => "sampled_score_estimator",
        }
    }

    /// Whether both sides recorded the definition, with the same value.
    fn same(self, a: &MetricSemantics, b: &MetricSemantics) -> bool {
        fn agree<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
            a.is_some() && a == b
        }
        match self {
            Self::NullTokens => agree(&a.null_tokens, &b.null_tokens),
            Self::PatternMatchCount => agree(&a.pattern_match_count, &b.pattern_match_count),
            Self::RangeRuleCount => agree(&a.range_rule_count, &b.range_rule_count),
            Self::KeyUniquenessUnits => agree(&a.key_uniqueness_units, &b.key_uniqueness_units),
            Self::OutlierUnits => agree(&a.outlier_units, &b.outlier_units),
            Self::KeyColumnSource => agree(&a.key_column_source, &b.key_column_source),
            Self::AccuracyComparison => agree(&a.accuracy_comparison, &b.accuracy_comparison),
            Self::JsonNumberReading => agree(&a.json_number_reading, &b.json_number_reading),
            Self::SampledScoreEstimator => {
                agree(&a.sampled_score_estimator, &b.sampled_score_estimator)
            }
        }
    }
}

/// The definitions a score reads. Text length and the shape-moment estimator
/// feed no score.
fn score_definitions(dimension: Option<QualityDimension>) -> &'static [Definition] {
    match dimension {
        None => &[
            Definition::NullTokens,
            Definition::PatternMatchCount,
            Definition::RangeRuleCount,
            Definition::KeyUniquenessUnits,
            Definition::OutlierUnits,
            Definition::KeyColumnSource,
            Definition::AccuracyComparison,
            Definition::JsonNumberReading,
            Definition::SampledScoreEstimator,
        ],
        Some(QualityDimension::Validity) => &[
            Definition::NullTokens,
            Definition::PatternMatchCount,
            Definition::SampledScoreEstimator,
        ],
        Some(QualityDimension::Accuracy) => &[
            Definition::NullTokens,
            Definition::RangeRuleCount,
            Definition::OutlierUnits,
            Definition::AccuracyComparison,
            Definition::JsonNumberReading,
            Definition::SampledScoreEstimator,
        ],
        Some(QualityDimension::Uniqueness) => &[
            Definition::NullTokens,
            Definition::KeyUniquenessUnits,
            Definition::KeyColumnSource,
            Definition::JsonNumberReading,
        ],
        Some(QualityDimension::Consistency | QualityDimension::Precision) => &[
            Definition::NullTokens,
            Definition::JsonNumberReading,
            Definition::SampledScoreEstimator,
        ],
        Some(QualityDimension::Timeliness) => {
            &[Definition::NullTokens, Definition::SampledScoreEstimator]
        }
        Some(_) => &[Definition::NullTokens],
    }
}

/// The first definition the two sides do not both record identically.
fn definition_gap(
    a: Option<&MetricSemantics>,
    b: Option<&MetricSemantics>,
    definitions: &[Definition],
) -> Option<NotEvaluated> {
    definitions
        .iter()
        .find(|definition| !a.zip(b).is_some_and(|(a, b)| definition.same(a, b)))
        .map(|definition| NotEvaluated::DefinitionNotComparable {
            definition: definition.key().to_string(),
        })
}

/// Reads one hint list out of a report's recorded hints.
type HintColumns = fn(&RecordedHints) -> &Vec<String>;

/// How one setting compares across the two sides.
#[derive(Debug, Clone, Copy)]
enum Agreement {
    Same,
    Changed,
    Unrecorded,
}

fn agreement<T: PartialEq + ?Sized>(a: Option<&T>, b: Option<&T>) -> Agreement {
    match (a, b) {
        (Some(a), Some(b)) if a == b => Agreement::Same,
        (Some(_), Some(_)) => Agreement::Changed,
        _ => Agreement::Unrecorded,
    }
}

/// [`NotEvaluated::SettingChanged`] on `metric_packs` when one side ran
/// pattern detection and the other did not, and the score the rule reads
/// moved because of it. Validity scores the detected patterns, so a side
/// without the `patterns` pack assesses no validity, and its overall score
/// averages the other dimensions. The rule reads the packs only where that
/// shows: the validity score, and the overall score when validity is
/// assessed on one side only. No other score reads a pack: without
/// `quality` a side has no score at all, and `statistics` moves none.
///
/// Checked before the reasons that name the effect (`not_assessed`,
/// `not_in_baseline`, `dimensions_changed`), so the rule names the cause.
/// A side that did not record its packs (written before 0.14) leaves those
/// reasons to decide, as they did before: two sides that both assessed
/// validity both ran pattern detection, so the packs need no record there.
fn pack_gap(delta: &ReportDelta, dimension: Option<QualityDimension>) -> Option<NotEvaluated> {
    let packs = &delta.configuration.metric_packs;
    let patterns = |packs: &Vec<MetricPack>| packs.contains(&MetricPack::Patterns);
    let differ = packs.a.as_ref().map(patterns)? != packs.b.as_ref().map(patterns)?;
    let validity_moved = || {
        delta
            .dimensions
            .iter()
            .find(|change| change.dimension == QualityDimension::Validity)
            .is_some_and(|change| change.score.a.is_some() != change.score.b.is_some())
    };
    let reads = match dimension {
        Some(dimension) => dimension == QualityDimension::Validity,
        None => validity_moved(),
    };
    (differ && reads).then(|| NotEvaluated::SettingChanged {
        setting: "metric_packs".to_string(),
    })
}

/// The first setting a score reads that the two sides do not both record
/// identically. The overall score reads every setting. A dimension reads the
/// identifier hint, which retypes its columns and names the key uniqueness
/// reads, except completeness, which counts nulls whatever the type. Accuracy
/// also reads the positive hint, timeliness the temporal ones and its clock,
/// and validity the locale, which ranks the detected patterns it scores. The
/// clock is read only when `reads_clock` says the score depends on it (see
/// [`score_reads_clock`]): a side without a timeliness score records no
/// clock, and an average without timeliness does not read one.
fn setting_gap(
    configuration: &ConfigurationComparison,
    identifiers: &Sides<Option<&[String]>>,
    dimension: Option<QualityDimension>,
    clock_rule: TimelinessClockRule,
    reads_clock: bool,
) -> Option<NotEvaluated> {
    let hints = &configuration.semantic_hints;
    let reads = |dimensions: &[QualityDimension]| {
        dimension.is_none_or(|dimension| dimensions.contains(&dimension))
    };
    let hint = |columns: HintColumns| {
        agreement(hints.a.as_ref().map(columns), hints.b.as_ref().map(columns))
    };
    let recorded = |same: Option<bool>| match same {
        Some(true) => Agreement::Same,
        Some(false) => Agreement::Changed,
        None => Agreement::Unrecorded,
    };
    // Both sides record the pairs themselves from 0.14. A side written before
    // records only the columns the pairs named, not which pairs with which,
    // so there only two empty pair lists are known to agree.
    let pairs = match configuration.temporal_pairs.same {
        Some(same) => recorded(Some(same)),
        None => match hint(|hints| &hints.temporal_pair_columns) {
            Agreement::Same
                if hints
                    .a
                    .as_ref()
                    .is_some_and(|hints| !hints.temporal_pair_columns.is_empty()) =>
            {
                Agreement::Unrecorded
            }
            other => other,
        },
    };
    let settings = [
        (
            "score_weights",
            dimension.is_none(),
            recorded(configuration.score_weights.same),
        ),
        (
            "identifier_columns",
            dimension != Some(QualityDimension::Completeness),
            // In the order given: the first one present is the key.
            agreement(identifiers.a, identifiers.b),
        ),
        (
            "positive_columns",
            reads(&[QualityDimension::Accuracy]),
            hint(|hints| &hints.positive_columns),
        ),
        (
            "temporal_columns",
            reads(&[QualityDimension::Timeliness]),
            hint(|hints| &hints.temporal_columns),
        ),
        (
            "temporal_pairs",
            reads(&[QualityDimension::Timeliness]),
            pairs,
        ),
        (
            "pattern_locale",
            reads(&[QualityDimension::Validity]),
            recorded(configuration.pattern_locale.same),
        ),
        (
            "timeliness_clock",
            reads_clock && reads(&[QualityDimension::Timeliness]),
            clock_agreement(
                clock_rule,
                configuration.timeliness_clock.a.as_ref(),
                configuration.timeliness_clock.b.as_ref(),
            ),
        ),
    ];
    settings
        .into_iter()
        .filter(|(_, read, _)| *read)
        .find_map(|(setting, _, agreement)| match agreement {
            Agreement::Same => None,
            Agreement::Changed => Some(NotEvaluated::SettingChanged {
                setting: setting.to_string(),
            }),
            Agreement::Unrecorded => Some(NotEvaluated::SettingUnrecorded {
                setting: setting.to_string(),
            }),
        })
}

/// Whether the score a rule reads depends on the timeliness clock: both sides
/// carry a timeliness score and, for the overall score, timeliness carries
/// weight in its average. A dimension weighted `0.0` still reports its score
/// but moves no average. The overall rule compares the weights before the
/// clock, so the evaluated report's weight stands for both sides; an
/// unrecorded weight counts as one, and leaves the rule unevaluated on
/// `score_weights` first.
fn score_reads_clock(delta: &ReportDelta, dimension: Option<QualityDimension>) -> bool {
    let scored = delta
        .dimensions
        .iter()
        .find(|change| change.dimension == QualityDimension::Timeliness)
        .is_some_and(|change| change.score.a.is_some() && change.score.b.is_some());
    let weighted = dimension.is_some()
        || delta
            .configuration
            .score_weights
            .b
            .as_ref()
            .is_none_or(|weights| weights.timeliness > 0.0);
    scored && weighted
}

/// How the two recorded timeliness clocks compare under the policy's rule.
/// The time zone and the maximum age define what "today" and "stale" mean,
/// so they must match under either rule; the reference date may differ only
/// between two rolling clocks under [`TimelinessClockRule::Rolling`]. The
/// mode alone moves no score, so two clocks at the same date agree whichever
/// way each date was chosen.
fn clock_agreement(
    rule: TimelinessClockRule,
    a: Option<&TimelinessClock>,
    b: Option<&TimelinessClock>,
) -> Agreement {
    let (Some(a), Some(b)) = (a, b) else {
        return Agreement::Unrecorded;
    };
    let rolling = |clock: &TimelinessClock| clock.mode == TimelinessClockMode::Rolling;
    let date_free = rule == TimelinessClockRule::Rolling && rolling(a) && rolling(b);
    let same_basis = a.timezone == b.timezone && a.max_data_age_years == b.max_data_age_years;
    if same_basis && (date_free || a.reference_date == b.reference_date) {
        Agreement::Same
    } else {
        Agreement::Changed
    }
}

fn check_percentage(
    code: CheckCode,
    subject: Option<String>,
    value: f64,
) -> Result<(), PolicyError> {
    if value.is_finite() && (0.0..=PERCENTAGE_MAX).contains(&value) {
        return Ok(());
    }
    Err(PolicyError::ThresholdOutOfRange {
        code,
        subject,
        value,
    })
}

/// Quality first, then dimensions in their declared order, each at most once.
fn required_metrics_in_order(requested: &[RequiredMetric]) -> Vec<RequiredMetric> {
    let mut ordered = Vec::new();
    if requested.contains(&RequiredMetric::Quality) {
        ordered.push(RequiredMetric::Quality);
    }
    for dimension in QualityDimension::all() {
        if requested.contains(&RequiredMetric::Dimension(dimension)) {
            ordered.push(RequiredMetric::Dimension(dimension));
        }
    }
    ordered
}

fn verdict_of(checks: &[Check]) -> Verdict {
    if checks.iter().any(Check::is_violation) {
        Verdict::Fail
    } else if checks.iter().any(|check| check.not_evaluated().is_some()) {
        Verdict::Inconclusive
    } else {
        Verdict::Pass
    }
}

/// Fill in a check that had no quality assessment to read, naming the reason
/// the report recorded. Without that, "you did not ask for this" and "this
/// broke" reach a gate as the same absence.
fn unavailable_quality(mut check: Check, report: &ProfileReport) -> Check {
    let quality_status = quality_status_name(&report.quality_status).to_string();
    check.message = match &report.quality_status {
        QualityAnalysisStatus::NotRequested => {
            "quality metrics were not requested for this run".to_string()
        }
        QualityAnalysisStatus::NoData => {
            "quality was requested but no sample was available to measure".to_string()
        }
        QualityAnalysisStatus::WithheldByProjection => {
            "quality was withheld: the requested dimensions measure whole rows and only \
             some columns were profiled"
                .to_string()
        }
        QualityAnalysisStatus::Failed { .. } => "the quality computation failed".to_string(),
        QualityAnalysisStatus::Unrecorded | QualityAnalysisStatus::Computed => {
            "the report carries no quality assessment".to_string()
        }
    };
    check.status = CheckStatus::NotEvaluated(NotEvaluated::QualityUnavailable { quality_status });
    check
}

fn aggregate_message(status: &CheckStatus, subject: &str) -> String {
    match status {
        CheckStatus::Passed => format!("{subject} meets the required minimum"),
        CheckStatus::Failed => format!("{subject} is below the required minimum"),
        CheckStatus::NotEvaluated(_) => format!(
            "{subject} was computed over part of the source, which bounds nothing about \
             the rest"
        ),
    }
}

fn bounded_message(status: &CheckStatus, subject: &str) -> String {
    match status {
        CheckStatus::Passed => format!(
            "{subject} was computed over a sample, and its whole-source interval meets the \
             required minimum"
        ),
        CheckStatus::Failed => format!(
            "{subject} was computed over a sample, and its whole-source interval is below the \
             required minimum"
        ),
        CheckStatus::NotEvaluated(_) => format!(
            "{subject} was computed over a sample, and the required minimum lies within its \
             whole-source interval"
        ),
    }
}

/// How a full-source score drop rule reads the change between two scores.
enum ChangeReading {
    /// Decided on the two point scores.
    Points,
    /// Decided on the interval the two sides' recorded intervals allow.
    Bounded(BoundedChange),
    /// A side's evidence settles nothing about its whole source.
    Blocked(EvidenceGap),
}

/// A score change as an interval, and the side intervals it came from (`None`
/// for a side whose score is a point).
struct BoundedChange {
    lower: f64,
    upper: f64,
    a: Option<CheckBounds>,
    b: Option<CheckBounds>,
}

/// The interval a check decides on, from the report's bounds.
fn check_bounds(bounds: &ScoreBounds, interval: Option<ScoreInterval>) -> Option<CheckBounds> {
    interval.map(|interval| CheckBounds {
        lower: interval.lower,
        upper: interval.upper,
        confidence_level: bounds.confidence_level,
    })
}

/// The `state` tag the report serializes for its quality status.
pub(crate) fn quality_status_name(status: &QualityAnalysisStatus) -> &'static str {
    match status {
        QualityAnalysisStatus::Computed => "computed",
        QualityAnalysisStatus::NotRequested => "not_requested",
        QualityAnalysisStatus::NoData => "no_data",
        QualityAnalysisStatus::WithheldByProjection => "withheld_by_projection",
        QualityAnalysisStatus::Failed { .. } => "failed",
        QualityAnalysisStatus::Unrecorded => "unrecorded",
    }
}

/// Share of a column's values that are null, or `None` when no value was read.
/// Mirrors the Python binding's `ColumnProfile.null_percentage`.
fn null_percentage(profile: &ColumnProfile) -> Option<f64> {
    (profile.total_count > 0)
        .then(|| profile.null_count as f64 / profile.total_count as f64 * PERCENTAGE_MAX)
}

/// What a column's date statistics say about its newest date.
enum NewestDate {
    /// The newest date, from `max_datetime`, and the order the column's
    /// slash dates were read in.
    Date {
        date: chrono::NaiveDate,
        order: Option<SlashDateOrder>,
    },
    /// None was recorded: the column is not typed as a date, no value parsed
    /// as one, or statistics were not computed.
    Absent,
    /// `max_datetime` holds text that is not a `YYYY-MM-DD` date, which the
    /// profiler never writes, so the document was written or edited
    /// elsewhere. Named apart from [`NewestDate::Absent`] so the check says
    /// which happened instead of guessing a date.
    Malformed,
}

fn newest_date(profile: &ColumnProfile) -> NewestDate {
    match &profile.stats {
        ColumnStats::DateTime(stats) if !stats.max_datetime.is_empty() => {
            match chrono::NaiveDate::parse_from_str(&stats.max_datetime, "%Y-%m-%d") {
                Ok(date) => NewestDate::Date {
                    date,
                    order: stats.slash_date_order,
                },
                Err(_) => NewestDate::Malformed,
            }
        }
        _ => NewestDate::Absent,
    }
}

/// Whether more values could change how the slash dates already read are
/// read. An order no value decided (`assumed_day_first`) becomes month-first
/// once a value only parses month-first, and a month-first order becomes
/// `mixed`, which reads the ambiguous values day-first, once a value only
/// parses day-first. A day-first or mixed order keeps every value read where
/// it is, and a column without slash dates has no order to change.
fn slash_order_settled(order: Option<SlashDateOrder>) -> bool {
    !matches!(
        order,
        Some(SlashDateOrder::AssumedDayFirst | SlashDateOrder::MonthFirst)
    )
}

/// Whether a column's date statistics saw every non-null value read. They
/// are computed from the values the profiler retained, the same values its
/// `type_homogeneity` classifies, so a classified count short of the non-null
/// count says the retained sample left some out. A report without the
/// classification does not record which happened.
fn date_sample_evidence(profile: &ColumnProfile) -> Evidence {
    let non_null = profile.total_count.saturating_sub(profile.null_count);
    match &profile.type_homogeneity {
        Some(forms) if forms.classified_count() >= non_null => Evidence::Complete,
        Some(_) => Evidence::Incomplete {
            reason: EvidenceGap::QualitySampled,
        },
        None => Evidence::Incomplete {
            reason: EvidenceGap::CoverageUnrecorded,
        },
    }
}

/// How much of the source the scan itself covered.
///
/// Truncation is named ahead of sampling and sampling ahead of skipped
/// records, so the gap reported is the largest one.
pub(crate) fn scan_evidence(execution: &ExecutionMetadata) -> Evidence {
    scan_coverage(
        !execution.source_exhausted || execution.truncation_reason.is_some(),
        execution.sampling_applied,
        execution.error_count > 0,
    )
}

/// [`scan_evidence`] over the three recorded facts it reads, for a report
/// delta reading them from a flat summary.
pub(crate) fn scan_coverage(truncated: bool, sampled: bool, records_skipped: bool) -> Evidence {
    let reason = if truncated {
        Some(EvidenceGap::Truncated)
    } else if sampled {
        Some(EvidenceGap::Sampled)
    } else if records_skipped {
        Some(EvidenceGap::RecordsSkipped)
    } else {
        None
    };
    match reason {
        Some(reason) => Evidence::Incomplete { reason },
        None => Evidence::Complete,
    }
}

/// Which part of the assessment a check reads, and so whose provenance
/// decides whether its number covers every scanned row.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Provenance {
    /// The aggregate over every dimension.
    Overall,
    /// One dimension, through whichever components it is built from.
    Dimension(QualityDimension),
    /// One named component, as recorded in the assessment's confidence.
    Component(&'static str),
}

/// The component labels a dimension is built from.
///
/// Uniqueness is the one dimension with two, and they can differ in
/// provenance: a full-stream row tracker counts duplicates over every row
/// while the key scan reads the retained sample.
pub(crate) fn dimension_components(dimension: QualityDimension) -> &'static [&'static str] {
    match dimension {
        QualityDimension::Completeness => &["completeness"],
        QualityDimension::Consistency => &["consistency"],
        QualityDimension::Uniqueness => &["key_uniqueness", "duplicate_rows"],
        QualityDimension::Accuracy => &["accuracy"],
        QualityDimension::Timeliness => &["timeliness"],
        QualityDimension::Validity => &["validity"],
        QualityDimension::Precision => &["precision"],
    }
}

/// Whether `label` names a component of a dimension this build knows.
fn is_known(label: &str) -> bool {
    QualityDimension::all()
        .into_iter()
        .flat_map(dimension_components)
        .any(|component| *component == label)
}

/// Whether the number a check reads came from every scanned row or from a
/// retained sample of them.
///
/// Resolved per component, so a check about one dimension is not downgraded by
/// another dimension's sampling.
fn quality_evidence(quality: Option<&QualityAssessment>, provenance: Provenance) -> Evidence {
    let Some(quality) = quality else {
        // No assessment to read; the check that called this reports the
        // absence itself, and there is no number for evidence to describe.
        return Evidence::Complete;
    };
    sampled_component_evidence(
        quality.sampled_dimensions().as_deref(),
        &quality.unlisted_components(),
        &quality.metrics.assessed_dimensions(),
        provenance,
    )
}

/// [`quality_evidence`] over the recorded facts it reads: the sampled
/// component labels (`None` when the report did not record them), the
/// components computed with signal that neither list names
/// ([`QualityAssessment::unlisted_components`]), and the dimensions the
/// overall score weighs. A report delta reads the same facts
/// from a flat summary, which has no [`QualityAssessment`] behind it, and
/// findings read one component through it, so the gate, the delta and
/// findings resolve coverage by one rule.
pub(crate) fn sampled_component_evidence(
    sampled: Option<&[String]>,
    unlisted: &[String],
    assessed: &[QualityDimension],
    provenance: Provenance,
) -> Evidence {
    let Some(sampled) = sampled else {
        return Evidence::Incomplete {
            reason: EvidenceGap::CoverageUnrecorded,
        };
    };
    // A sampled label this build does not know belongs to a dimension a
    // later release added. Its weight is in the stored overall score, and
    // its dimension was dropped when the report was read, so nothing here
    // rules out that it reaches the score: the score is not certified.
    if matches!(provenance, Provenance::Overall) && !sampled.iter().all(|label| is_known(label)) {
        return Evidence::Incomplete {
            reason: EvidenceGap::CoverageUnrecorded,
        };
    }
    // The components the number reads. The overall score is a weighted
    // average over the *assessed* dimensions, so a sampled dimension the
    // weights exclude does not reach it. Reporting the aggregate as sampled
    // because of one would withhold a verdict the number does not depend on.
    let reads: Vec<&str> = match provenance {
        Provenance::Overall => assessed
            .iter()
            .copied()
            .flat_map(dimension_components)
            .copied()
            .collect(),
        Provenance::Dimension(dimension) => dimension_components(dimension).to_vec(),
        Provenance::Component(component) => vec![component],
    };
    let any_in = |labels: &[String]| {
        reads
            .iter()
            .any(|component| labels.iter().any(|label| label == component))
    };
    // A component with signal that neither list names has no recorded
    // coverage, and unknown coverage is a larger gap than a sample.
    if any_in(unlisted) {
        return Evidence::Incomplete {
            reason: EvidenceGap::CoverageUnrecorded,
        };
    }
    if any_in(sampled) {
        Evidence::Incomplete {
            reason: EvidenceGap::QualitySampled,
        }
    } else {
        Evidence::Complete
    }
}

/// Every public type in this module is `#[non_exhaustive]` (#894), and so is
/// every enum variant with named fields, so the gate can gain variants,
/// struct fields and variant fields (#749) without breaking callers. Tuple
/// variants such as `CheckStatus::NotEvaluated(_)` are not marked: growing
/// one changes its positional shape, which is a redesign, not an extension.
///
/// The attribute only binds outside this crate, which is where doctests
/// compile. Each block below builds one struct with update syntax from a
/// supplied value. That is rejected only for a non-exhaustive struct from
/// another crate, so a field added later cannot make a block fail for a
/// different reason. The enums and their variants are checked by
/// `tests::every_public_type_is_non_exhaustive`, because no `match` or
/// pattern stays independent of the variants and fields they gain.
///
/// Stable rustdoc does not check the error code of a `compile_fail` block, so
/// any error would pass one. `tests/public_api_facade.rs` names the fields
/// used here in code that must compile, so a renamed field fails there instead
/// of passing here for the wrong reason.
///
/// ```compile_fail
/// use dataprof_runtime::CheckBounds;
/// fn f(base: CheckBounds) -> CheckBounds {
///     CheckBounds { lower: 0.0, ..base }
/// }
/// ```
///
/// ```compile_fail
/// use dataprof_runtime::Check;
/// fn f(base: Check) -> Check {
///     Check { message: String::new(), ..base }
/// }
/// ```
///
/// ```compile_fail
/// use dataprof_runtime::GateResult;
/// fn f(base: GateResult) -> GateResult {
///     GateResult { checks: Vec::new(), ..base }
/// }
/// ```
#[cfg(doctest)]
struct NonExhaustiveGuards;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dataprof_core::{
        ColumnStats, DataSource, DataType, FileFormat, QualityScoreWeights, TruncationReason,
    };
    use dataprof_metrics::{QualityMetrics, UniquenessMetrics};
    use serde_json::{Value, json};

    use super::*;

    /// The source-level half of `NonExhaustiveGuards` (#894): every `pub` type
    /// declared in this module carries `#[non_exhaustive]`, and so does every
    /// variant with named fields inside a `pub enum`. Reading the source keeps
    /// the check independent of the variants and fields the types gain, and
    /// covers a type or variant added later without a list to update. The
    /// findings module shares the gate's evidence and takes the same rule
    /// (#909).
    #[test]
    fn every_public_type_is_non_exhaustive() {
        assert_non_exhaustive(include_str!("quality_gate.rs"), 15, 6);
        assert_non_exhaustive(include_str!("findings.rs"), 10, 2);
    }

    /// `source` declares at least `types` public types and `variants`
    /// variants with named fields, all of them `#[non_exhaustive]`.
    fn assert_non_exhaustive(source: &str, types: usize, variants_with_fields: usize) {
        let lines: Vec<&str> = source.lines().collect();
        let marked = |i: usize| {
            lines[..i]
                .iter()
                .rev()
                .map(|l| l.trim())
                .take_while(|l| l.starts_with("#[") || l.starts_with("///"))
                .any(|l| l == "#[non_exhaustive]")
        };
        let identifier = |s: &str| -> String {
            s.chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect()
        };
        let mut declared = Vec::new();
        let mut variants = Vec::new();
        let mut unmarked = Vec::new();
        let mut in_enum: Option<String> = None;
        for (i, line) in lines.iter().enumerate() {
            if let Some(enum_name) = &in_enum {
                if *line == "}" {
                    in_enum = None;
                } else if let Some(rest) = line.strip_prefix("    ")
                    && rest.starts_with(|c: char| c.is_ascii_uppercase())
                    && rest.contains(" {")
                {
                    let variant = format!("{enum_name}::{}", identifier(rest));
                    if !marked(i) {
                        unmarked.push(variant.clone());
                    }
                    variants.push(variant);
                }
                continue;
            }
            let Some(rest) = line
                .strip_prefix("pub enum ")
                .or_else(|| line.strip_prefix("pub struct "))
            else {
                continue;
            };
            let name = identifier(rest);
            if !marked(i) {
                unmarked.push(name.clone());
            }
            if line.starts_with("pub enum ") {
                in_enum = Some(name.clone());
            }
            declared.push(name);
        }
        // Guard the premise: the scan found the module's types and the
        // variants with named fields at all.
        assert!(declared.len() >= types, "{declared:?}");
        assert!(variants.len() >= variants_with_fields, "{variants:?}");
        assert!(unmarked.is_empty(), "not #[non_exhaustive]: {unmarked:?}");
    }
    use crate::ReportAssembler;

    fn source() -> DataSource {
        DataSource::File {
            path: "drop.csv".to_string(),
            format: FileFormat::Csv,
            size_bytes: 256,
            modified_at: None,
            parquet_metadata: None,
            csv_metadata: None,
        }
    }

    fn column(name: &str, total: usize, nulls: usize) -> ColumnProfile {
        ColumnProfile {
            name: name.to_string(),
            data_type: DataType::String,
            null_count: nulls,
            total_count: total,
            unique_count: None,
            unique_count_is_approximate: None,
            unique_count_lower_bound: None,
            invalid_count: None,
            type_homogeneity: None,
            locale_number_count: None,
            native_type: None,
            stats: ColumnStats::None,
            patterns: None,
        }
    }

    /// Two columns: `id` clean, `note` one-fifth null.
    fn sample_data() -> HashMap<String, Vec<String>> {
        let ids = (1..=5).map(|n| n.to_string()).collect::<Vec<_>>();
        let notes = vec!["a", "b", "c", "d", ""]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        HashMap::from([("id".to_string(), ids), ("note".to_string(), notes)])
    }

    fn complete_report() -> ProfileReport {
        ReportAssembler::new(source(), ExecutionMetadata::new(5, 2, 10))
            .columns(vec![column("id", 5, 0), column("note", 5, 1)])
            .with_quality_data(sample_data())
            .build()
    }

    fn truncated_report() -> ProfileReport {
        ReportAssembler::new(
            source(),
            ExecutionMetadata::new(5, 2, 10).with_truncation(TruncationReason::MaxRows(5)),
        )
        .columns(vec![column("id", 5, 0), column("note", 5, 1)])
        .with_quality_data(sample_data())
        .build()
    }

    fn no_quality_report() -> ProfileReport {
        ReportAssembler::new(source(), ExecutionMetadata::new(5, 2, 10))
            .columns(vec![column("id", 5, 0), column("note", 5, 1)])
            .with_quality_data(sample_data())
            .skip_quality()
            .build()
    }

    /// A report whose uniqueness dimension witnessed `duplicates` duplicate
    /// rows, truncated so the evidence behind it is partial.
    fn duplicate_report(duplicates: usize, approximate: bool) -> ProfileReport {
        let metrics = QualityMetrics {
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: duplicates,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
                rows_checked: 5,
                key_column: None,
                key_values_checked: None,
                duplicate_rows_approximate: approximate,
            }),
            ..QualityMetrics::default()
        };
        ProfileReport::new(
            source(),
            vec![column("id", 5, 0)],
            ExecutionMetadata::new(5, 1, 10).with_truncation(TruncationReason::MaxRows(5)),
            Some(QualityAssessment::exact(metrics)),
        )
    }

    fn check_for(result: &GateResult, code: CheckCode) -> &Check {
        result
            .checks
            .iter()
            .find(|check| check.code == code)
            .unwrap_or_else(|| panic!("no {code} check in {:?}", result.checks))
    }

    fn column_check<'a>(result: &'a GateResult, name: &str) -> &'a Check {
        result
            .checks
            .iter()
            .find(|check| check.column.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("no check for column {name}"))
    }

    /// A report whose quality sample is smaller than the scan: the assembler
    /// bifurcates, and the dimensions computed from the reservoir are recorded
    /// as sampled while the ones from exact counters are not.
    ///
    /// This is the ordinary large-file shape, not an edge case. It is also the
    /// one an execution-metadata-only gate gets wrong: the source *was*
    /// exhausted and no row sampler ran, so `source_exhausted` and
    /// `sampling_applied` both say the scan covered everything.
    fn reservoir_report() -> ProfileReport {
        ReportAssembler::new(source(), ExecutionMetadata::new(100, 2, 10))
            .columns(vec![column("id", 100, 0), column("note", 100, 20)])
            .with_quality_data(sample_data())
            .build()
    }

    #[test]
    fn a_reservoir_backed_score_is_decided_on_its_whole_source_interval() {
        let report = reservoir_report();
        assert!(report.execution.source_exhausted);
        assert!(!report.execution.sampling_applied);
        let quality = report.quality.as_ref().expect("quality was computed");
        let sampled = quality
            .sampled_dimensions()
            .expect("a profiling run records its provenance");
        assert!(
            !sampled.is_empty(),
            "the assembler did not bifurcate; this test no longer reaches the \
             case it guards"
        );
        let interval = quality
            .score_bounds()
            .expect("a sampled assessment carries bounds")
            .overall_score
            .expect("every sampled dimension here is bounded");
        assert!(
            interval.lower < interval.upper && interval.upper < 100.0,
            "a five-value sample leaves room on both sides: {interval:?}"
        );
        let decide = |min: f64| {
            let result = QualityPolicy::new()
                .min_quality_score(min)
                .evaluate(&report)
                .unwrap();
            // The scan itself was complete; only the metric's own evidence is
            // not, and a decision on the interval does not pretend otherwise.
            assert_eq!(result.evidence, Evidence::Complete);
            let check = check_for(&result, CheckCode::MinQualityScore).clone();
            assert_eq!(
                check.evidence,
                Evidence::Incomplete {
                    reason: EvidenceGap::QualitySampled
                }
            );
            assert_eq!(
                check.bounds.map(|bounds| (bounds.lower, bounds.upper)),
                Some((interval.lower, interval.upper))
            );
            (result.verdict, check)
        };

        let (verdict, _) = decide(interval.lower);
        assert_eq!(verdict, Verdict::Pass);

        let (verdict, check) = decide((interval.lower + interval.upper) / 2.0);
        assert_eq!(verdict, Verdict::Inconclusive);
        assert_eq!(
            check.not_evaluated(),
            Some(&NotEvaluated::EvidenceIncomplete {
                gap: EvidenceGap::QualitySampled
            })
        );

        let (verdict, _) = decide(interval.upper + 0.01);
        assert_eq!(verdict, Verdict::Fail);
    }

    /// A threshold equal to the value a check shows is met, and a report that
    /// was saved and loaded reaches the same verdict (#1085). The live report
    /// holds raw scores, the loaded one their two-decimal serialization, so a
    /// raw comparison failed `observed 66.67 >= 66.67` live and passed it
    /// loaded. Every (report, rule) that goes wrong is collected first.
    #[test]
    fn a_threshold_at_the_shown_value_passes_live_and_after_a_round_trip() {
        // One value in three missing: completeness and null share are thirds.
        let thirds = ReportAssembler::new(source(), ExecutionMetadata::new(3, 1, 10))
            .columns(vec![column("x", 3, 1)])
            .with_quality_data(HashMap::from([(
                "x".to_string(),
                vec!["1".to_string(), "2".to_string(), String::new()],
            )]))
            .build();
        let mut wrong = Vec::new();
        for (label, report) in [
            ("thirds", thirds),
            ("reservoir", reservoir_report()),
            ("complete", complete_report()),
        ] {
            let loaded: ProfileReport =
                serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
            let quality = report.quality.as_ref().expect("quality was computed");
            let mut policies = Vec::new();
            // A sampled report's overall rule decides on its interval: the
            // lower-bound case below covers it.
            if quality
                .score_bounds()
                .and_then(|bounds| bounds.overall_score)
                .is_none()
            {
                policies.push((
                    "overall".to_string(),
                    QualityPolicy::new()
                        .min_quality_score(rounded_2(report.quality_score().unwrap())),
                ));
            }
            for (name, score) in &quality.scores().dimension_scores {
                let Some(score) = score else { continue };
                // On a sampled report the full-source rule decides on the
                // interval, so its threshold is the shown lower bound.
                let shown = quality
                    .score_bounds()
                    .and_then(|bounds| bounds.dimension_scores.get(name).copied().flatten())
                    .map_or(*score, |interval| interval.lower);
                let dimension: QualityDimension = name.parse().unwrap();
                policies.push((
                    name.clone(),
                    QualityPolicy::new().min_dimension_score(dimension, rounded_2(shown)),
                ));
            }
            for column in &report.column_profiles {
                let shown = rounded_2(null_percentage(column).unwrap());
                policies.push((
                    format!("null {}", column.name),
                    QualityPolicy::new().max_null_percentage(column.name.clone(), shown),
                ));
            }
            if let Some(interval) = quality
                .score_bounds()
                .and_then(|bounds| bounds.overall_score)
            {
                policies.push((
                    "overall lower bound".to_string(),
                    QualityPolicy::new().min_quality_score(rounded_2(interval.lower)),
                ));
            }
            // A minimum written with more decimals than a check shows is
            // compared as shown: `expected` reads 66.67 for 66.674, so a score
            // shown as 66.67 meets it, as an allowance does for a change.
            if label == "thirds" {
                policies.push((
                    "completeness at a third decimal".to_string(),
                    QualityPolicy::new()
                        .min_dimension_score(QualityDimension::Completeness, 66.674),
                ));
            }
            for (rule, policy) in policies {
                let live = policy.evaluate(&report).unwrap().verdict;
                let reloaded = policy.evaluate(&loaded).unwrap().verdict;
                if live != Verdict::Pass || reloaded != Verdict::Pass {
                    wrong.push(format!(
                        "{label} {rule}: live {live:?}, loaded {reloaded:?}"
                    ));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// The interval half of #1085. A bound that rounds up when saved, such as
    /// 79.426 shown as 79.43, decided a minimum of 79.43 one way live and the
    /// other way loaded: inconclusive then pass at the lower bound, fail then
    /// inconclusive at the upper one. The verdict must not change on reload.
    #[test]
    fn a_bound_that_rounds_up_decides_the_same_live_and_loaded() {
        let mut report = reservoir_report();
        let quality = report.quality.clone().expect("quality was computed");
        let mut bounds = quality
            .score_bounds()
            .expect("a sampled report is bounded")
            .clone();
        let interval = bounds
            .overall_score
            .as_mut()
            .expect("the overall score is bounded");
        // Just below each shown value, so saving rounds both up.
        interval.lower = rounded_2(interval.lower) - 0.004;
        interval.upper = rounded_2(interval.upper) - 0.004;
        let (lower, upper) = (interval.lower, interval.upper);
        assert!(rounded_2(lower) > lower && rounded_2(upper) > upper && lower < upper);
        report.quality = Some(quality.with_score_bounds(Some(bounds)));
        let loaded: ProfileReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();

        let mut wrong = Vec::new();
        for (label, min) in [("lower", rounded_2(lower)), ("upper", rounded_2(upper))] {
            let policy = QualityPolicy::new().min_quality_score(min);
            let live = policy.evaluate(&report).unwrap().verdict;
            let reloaded = policy.evaluate(&loaded).unwrap().verdict;
            if live != reloaded {
                wrong.push(format!(
                    "min at the shown {label} bound {min}: live {live:?}, loaded {reloaded:?}"
                ));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[test]
    fn an_observed_scope_decides_on_the_sampled_score_itself() {
        let report = reservoir_report();
        let score = report.quality_score().unwrap();
        let result = QualityPolicy::new()
            .min_quality_score(score)
            .scope(PolicyScope::Observed)
            .evaluate(&report)
            .unwrap();
        let check = check_for(&result, CheckCode::MinQualityScore);
        assert_eq!(result.verdict, Verdict::Pass);
        assert_eq!(check.bounds, None);
    }

    #[test]
    fn a_sampled_score_without_recorded_bounds_stays_unevaluated() {
        let mut report = reservoir_report();
        let quality = report.quality.take().unwrap();
        report.quality = Some(QualityAssessment::new(quality.metrics, quality.confidence));

        let result = QualityPolicy::new()
            .min_quality_score(1.0)
            .evaluate(&report)
            .unwrap();
        assert_eq!(result.verdict, Verdict::Inconclusive);
        assert_eq!(check_for(&result, CheckCode::MinQualityScore).bounds, None);
    }

    /// Provenance is resolved per component, so a dimension computed from
    /// exact counters is still decidable when another one was sampled.
    #[test]
    fn an_exactly_counted_dimension_survives_another_dimensions_sampling() {
        let report = reservoir_report();
        let sampled = report
            .quality
            .as_ref()
            .expect("quality was computed")
            .sampled_dimensions()
            .expect("a profiling run records its provenance");
        assert!(
            !sampled.iter().any(|label| label == "completeness"),
            "completeness is supposed to come from exact column counters"
        );

        let result = QualityPolicy::new()
            .min_dimension_score(QualityDimension::Completeness, 1.0)
            .min_dimension_score(QualityDimension::Consistency, 1.0)
            .evaluate(&report)
            .unwrap();

        assert_eq!(
            result
                .checks
                .iter()
                .find(|check| check.dimension == Some(QualityDimension::Completeness))
                .map(|check| check.evidence),
            Some(Evidence::Complete)
        );
        assert_eq!(
            result
                .checks
                .iter()
                .find(|check| check.dimension == Some(QualityDimension::Consistency))
                .map(|check| check.evidence),
            Some(Evidence::Incomplete {
                reason: EvidenceGap::QualitySampled
            })
        );
    }

    /// A report read back from a document written before dataprof recorded
    /// provenance does not say whether its numbers cover every scanned row.
    ///
    /// The compat path used to answer `Exact` for those, which is a claim the
    /// document never made: a full-source policy would then rest a verdict on
    /// numbers that may have come from a sample. The Python reload path
    /// reports the same gap, so the two agree on legacy input.
    #[test]
    fn a_legacy_document_without_recorded_coverage_is_not_read_as_a_full_scan() {
        let document = serde_json::to_value(complete_report()).expect("serializes");
        let mut legacy = document.clone();
        // Pre-0.10 documents carried the metrics flat, with no confidence.
        legacy["quality"] = document["quality"]["metrics"].clone();
        let report: ProfileReport = serde_json::from_value(legacy).expect("legacy document loads");

        assert_eq!(
            report
                .quality
                .as_ref()
                .expect("the metrics survived")
                .sampled_dimensions(),
            None
        );

        let result = QualityPolicy::new()
            .min_quality_score(1.0)
            .evaluate(&report)
            .unwrap();
        assert_eq!(result.verdict, Verdict::Inconclusive);
        let check = check_for(&result, CheckCode::MinQualityScore);
        assert_eq!(
            check.evidence,
            Evidence::Incomplete {
                reason: EvidenceGap::CoverageUnrecorded
            }
        );

        // Asked about what the metrics measured, the same report is decidable.
        let observed = QualityPolicy::new()
            .min_quality_score(1.0)
            .scope(PolicyScope::Observed)
            .evaluate(&report)
            .unwrap();
        assert_eq!(observed.verdict, Verdict::Pass);
    }

    /// Weights say what reaches the aggregate, not what was measured.
    ///
    /// The overall score renormalizes over the assessed dimensions, so a
    /// sampled dimension the weights exclude cannot move it, and reporting the
    /// aggregate as sampled because of one would withhold a verdict the number
    /// does not depend on. The excluded dimension keeps its own provenance.
    #[test]
    fn a_zero_weighted_sampled_dimension_does_not_taint_the_overall_score() {
        let mut report = reservoir_report();
        let quality = report.quality.as_mut().expect("quality was computed");
        let sampled = quality
            .sampled_dimensions()
            .expect("a profiling run records its provenance");
        assert!(
            sampled.iter().any(|label| label == "consistency"),
            "consistency is supposed to come from the reservoir"
        );
        // Completeness is the one dimension here computed from exact column
        // counters, so weighting only it leaves an aggregate that no sampled
        // component reaches.
        quality.metrics.score_weights = QualityScoreWeights {
            completeness: 1.0,
            consistency: 0.0,
            uniqueness: 0.0,
            accuracy: 0.0,
            timeliness: 0.0,
            validity: 0.0,
            precision: 0.0,
        };

        let result = QualityPolicy::new()
            .min_quality_score(1.0)
            .min_dimension_score(QualityDimension::Consistency, 1.0)
            .evaluate(&report)
            .unwrap();

        // The aggregate no longer depends on the sampled dimension.
        assert_eq!(
            check_for(&result, CheckCode::MinQualityScore).evidence,
            Evidence::Complete
        );
        // The dimension itself still reports where its number came from.
        assert_eq!(
            check_for(&result, CheckCode::MinDimensionScore).evidence,
            Evidence::Incomplete {
                reason: EvidenceGap::QualitySampled
            }
        );
    }

    /// A dimension excluded from the aggregate by a zero weight is still
    /// measured, and a gate reading its score still needs its provenance.
    ///
    /// Confidence used to be downgraded to `NotAssessed` whenever the
    /// *weighted* set was empty, which erased the sampling record for every
    /// dimension at once.
    #[test]
    fn zero_weights_do_not_erase_the_sampling_record() {
        let mut report = reservoir_report();
        let quality = report.quality.as_mut().expect("quality was computed");
        quality.metrics.score_weights = QualityScoreWeights {
            completeness: 0.0,
            consistency: 0.0,
            uniqueness: 0.0,
            accuracy: 0.0,
            timeliness: 0.0,
            validity: 0.0,
            precision: 0.0,
        };
        let reassessed =
            QualityAssessment::new(quality.metrics.clone(), quality.confidence.clone());

        assert!(reassessed.metrics.assessed_dimensions().is_empty());
        assert!(
            reassessed
                .sampled_dimensions()
                .expect("provenance survives")
                .iter()
                .any(|label| label == "consistency"),
            "a measured dimension kept no record of coming from a sample"
        );
    }

    #[test]
    fn met_requirements_pass() {
        let result = QualityPolicy::new()
            .min_quality_score(1.0)
            .max_null_percentage_any(50.0)
            .evaluate(&complete_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Pass);
        assert!(result.passed());
        assert_eq!(result.violations().count(), 0);
        assert_eq!(result.unevaluated().count(), 0);
    }

    #[test]
    fn violated_requirement_fails() {
        let result = QualityPolicy::new()
            .max_null_percentage("note", 10.0)
            .evaluate(&complete_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Fail);
        assert!(!result.passed());
        let violation = result.violations().next().expect("one violation");
        assert_eq!(violation.code, CheckCode::MaxNullPercentage);
        assert_eq!(violation.column.as_deref(), Some("note"));
        assert_eq!(violation.observed, Some(MetricValue::Percentage(20.0)));
        assert_eq!(
            violation.expected,
            Expectation::AtMost {
                value: MetricValue::Percentage(10.0)
            }
        );
    }

    /// The named limit decides its own column; the wildcard covers the rest.
    #[test]
    fn named_limit_overrides_the_wildcard() {
        let result = QualityPolicy::new()
            .max_null_percentage("note", 50.0)
            .max_null_percentage_any(0.0)
            .evaluate(&complete_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Pass);
        assert_eq!(result.checks.len(), 2);
        assert_eq!(
            column_check(&result, "note").expected,
            Expectation::AtMost {
                value: MetricValue::Percentage(50.0)
            }
        );
        assert_eq!(
            column_check(&result, "id").expected,
            Expectation::AtMost {
                value: MetricValue::Percentage(0.0)
            }
        );
    }

    /// Thresholds are percentages. A caller who writes the 0..1 ratio instead
    /// would otherwise get a gate that silently never fires.
    #[test]
    fn thresholds_are_on_the_percentage_scale() {
        let result = QualityPolicy::new()
            .min_quality_score(0.9)
            .evaluate(&complete_report())
            .unwrap();
        assert_eq!(result.verdict, Verdict::Pass);

        let err = QualityPolicy::new()
            .min_quality_score(101.0)
            .evaluate(&complete_report())
            .unwrap_err();
        assert_eq!(
            err,
            PolicyError::ThresholdOutOfRange {
                code: CheckCode::MinQualityScore,
                subject: None,
                value: 101.0,
            }
        );
        assert!(
            QualityPolicy::new()
                .max_null_percentage("id", f64::NAN)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn empty_policy_is_rejected_rather_than_passing_everything() {
        assert_eq!(
            QualityPolicy::new()
                .evaluate(&complete_report())
                .unwrap_err(),
            PolicyError::NoRequirements
        );
    }

    /// A metric that was never analyzed is not zero and not a pass. The check
    /// names the recorded reason, so a pipeline can tell a misconfiguration
    /// from an incident.
    #[test]
    fn unanalyzed_quality_is_inconclusive_not_a_pass() {
        let result = QualityPolicy::new()
            .min_quality_score(90.0)
            .evaluate(&no_quality_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Inconclusive);
        assert!(!result.passed());
        let check = check_for(&result, CheckCode::MinQualityScore);
        assert_eq!(check.observed, None);
        assert_eq!(
            check.not_evaluated(),
            Some(&NotEvaluated::QualityUnavailable {
                quality_status: "not_requested".to_string(),
            })
        );
    }

    /// `require_quality` is how a caller says the absence itself is a failure.
    #[test]
    fn require_quality_turns_absence_into_a_violation() {
        let result = QualityPolicy::new()
            .require_quality()
            .min_quality_score(90.0)
            .evaluate(&no_quality_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Fail);
        let required = check_for(&result, CheckCode::RequireMetric);
        assert!(required.is_violation());
        assert_eq!(required.expected, Expectation::Analyzed);
        assert_eq!(required.observed, None);
    }

    #[test]
    fn require_dimension_fails_when_the_dimension_had_nothing_to_assess() {
        let result = QualityPolicy::new()
            .require_dimension(QualityDimension::Timeliness)
            .evaluate(&complete_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Fail);
        assert_eq!(
            check_for(&result, CheckCode::RequireMetric).dimension,
            Some(QualityDimension::Timeliness)
        );
    }

    /// A dimension that was computed but had nothing to assess is reported as
    /// such, separately from quality never having been requested.
    #[test]
    fn unassessed_dimension_is_distinct_from_unrequested_quality() {
        let result = QualityPolicy::new()
            .min_dimension_score(QualityDimension::Timeliness, 90.0)
            .evaluate(&complete_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Inconclusive);
        assert_eq!(
            check_for(&result, CheckCode::MinDimensionScore).not_evaluated(),
            Some(&NotEvaluated::NotAssessed)
        );
    }

    /// A column the report does not profile is not decided either way: the
    /// report does not record whether it was projected away or absent.
    #[test]
    fn unprofiled_column_is_not_evaluated() {
        let result = QualityPolicy::new()
            .max_null_percentage("absent", 0.0)
            .evaluate(&complete_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Inconclusive);
        assert_eq!(
            column_check(&result, "absent").not_evaluated(),
            Some(&NotEvaluated::ColumnNotProfiled)
        );
    }

    /// The whole point of the scope: a ratio measured over a prefix of the
    /// source says nothing about the rows that were never read.
    #[test]
    fn a_ratio_over_a_partial_scan_cannot_pass_a_full_source_policy() {
        let report = truncated_report();
        let result = QualityPolicy::new()
            .max_null_percentage_any(50.0)
            .evaluate(&report)
            .unwrap();

        assert_eq!(result.verdict, Verdict::Inconclusive);
        assert_eq!(
            result.evidence,
            Evidence::Incomplete {
                reason: EvidenceGap::Truncated
            }
        );
        let check = column_check(&result, "note");
        assert_eq!(
            check.not_evaluated(),
            Some(&NotEvaluated::EvidenceIncomplete {
                gap: EvidenceGap::Truncated
            })
        );
        // The number was still read; it is the generalization that is refused.
        assert_eq!(check.observed, Some(MetricValue::Percentage(20.0)));

        // Asked about the rows that were actually read, the same report passes.
        let observed = QualityPolicy::new()
            .max_null_percentage_any(50.0)
            .scope(PolicyScope::Observed)
            .evaluate(&report)
            .unwrap();
        assert_eq!(observed.verdict, Verdict::Pass);
    }

    /// Duplicates already seen do not stop being duplicates when more rows are
    /// read, so a partial scan can still fail a full-source requirement.
    #[test]
    fn a_witnessed_duplicate_fails_despite_a_partial_scan() {
        let result = QualityPolicy::new()
            .max_duplicate_rows(0)
            .evaluate(&duplicate_report(2, false))
            .unwrap();

        assert_eq!(result.verdict, Verdict::Fail);
        let check = check_for(&result, CheckCode::MaxDuplicateRows);
        assert_eq!(check.observed, Some(MetricValue::Count(2)));
        assert_eq!(
            check.evidence,
            Evidence::Incomplete {
                reason: EvidenceGap::Truncated
            }
        );
    }

    /// A clean prefix is not a clean source.
    #[test]
    fn no_duplicate_in_a_partial_scan_is_not_a_pass() {
        let result = QualityPolicy::new()
            .max_duplicate_rows(0)
            .evaluate(&duplicate_report(0, false))
            .unwrap();

        assert_eq!(result.verdict, Verdict::Inconclusive);
        assert_eq!(
            check_for(&result, CheckCode::MaxDuplicateRows).not_evaluated(),
            Some(&NotEvaluated::EvidenceIncomplete {
                gap: EvidenceGap::Truncated
            })
        );
    }

    /// An estimated count is not a duplicate anyone saw, so it witnesses
    /// nothing and settles neither direction.
    #[test]
    fn an_estimated_duplicate_count_settles_neither_direction() {
        let result = QualityPolicy::new()
            .max_duplicate_rows(0)
            .evaluate(&duplicate_report(2, true))
            .unwrap();

        assert_eq!(result.verdict, Verdict::Inconclusive);
        let check = check_for(&result, CheckCode::MaxDuplicateRows);
        assert_eq!(
            check.not_evaluated(),
            Some(&NotEvaluated::EvidenceIncomplete {
                gap: EvidenceGap::Truncated
            })
        );
        // The gap is the estimate, not unread rows: the message said "the rows
        // that were not read" on a source read to the end (#819).
        assert_eq!(
            check.message,
            "the duplicate-row count is an estimate, which can neither witness \
             duplicates above the allowance nor rule them out"
        );
    }

    /// One null in 20,001 rows shows as a 0.00% null percentage, so a zero
    /// percentage limit passes it; the count ceiling decides on the count
    /// (#1119).
    #[test]
    fn a_null_count_ceiling_is_decided_on_the_exact_count() {
        let report = ReportAssembler::new(source(), ExecutionMetadata::new(20_001, 2, 10))
            .columns(vec![column("key", 20_001, 1), column("clean", 20_001, 0)])
            .build();

        let percentage = QualityPolicy::new()
            .max_null_percentage("key", 0.0)
            .evaluate(&report)
            .unwrap();
        assert_eq!(percentage.verdict, Verdict::Pass);

        let count = QualityPolicy::new()
            .max_null_count("key", 0)
            .max_null_count_any(0)
            .evaluate(&report)
            .unwrap();
        assert_eq!(count.verdict, Verdict::Fail);
        let key = column_check(&count, "key");
        assert_eq!(key.code, CheckCode::MaxNullCount);
        assert_eq!(key.observed, Some(MetricValue::Count(1)));
        assert_eq!(key.status, CheckStatus::Failed);
        assert_eq!(column_check(&count, "clean").status, CheckStatus::Passed);
    }

    /// Nulls already counted stay counted, so a partial scan that saw more
    /// than allowed fails a full-source rule; one that saw no more than
    /// allowed decides nothing about the rows it did not read.
    #[test]
    fn witnessed_nulls_fail_a_partial_scan_and_a_clean_prefix_does_not_pass() {
        let result = QualityPolicy::new()
            .max_null_count_any(0)
            .evaluate(&truncated_report())
            .unwrap();

        assert_eq!(result.verdict, Verdict::Fail);
        assert_eq!(column_check(&result, "note").status, CheckStatus::Failed);
        assert_eq!(
            column_check(&result, "id").not_evaluated(),
            Some(&NotEvaluated::EvidenceIncomplete {
                gap: EvidenceGap::Truncated
            })
        );

        let observed = QualityPolicy::new()
            .max_null_count("note", 1)
            .scope(PolicyScope::Observed)
            .evaluate(&truncated_report())
            .unwrap();
        assert_eq!(observed.verdict, Verdict::Pass);
    }

    /// A column with no values read has no null percentage, but it has a null
    /// count, zero: a full scan of an empty source decides the count rule.
    #[test]
    fn a_column_with_no_values_read_has_a_null_count_of_zero() {
        let report = ReportAssembler::new(source(), ExecutionMetadata::new(0, 1, 10))
            .columns(vec![column("key", 0, 0)])
            .build();

        let count = QualityPolicy::new()
            .max_null_count("key", 0)
            .evaluate(&report)
            .unwrap();
        assert_eq!(count.verdict, Verdict::Pass);
        assert_eq!(
            column_check(&count, "key").observed,
            Some(MetricValue::Count(0))
        );

        let percentage = QualityPolicy::new()
            .max_null_percentage("key", 0.0)
            .evaluate(&report)
            .unwrap();
        assert_eq!(
            column_check(&percentage, "key").not_evaluated(),
            Some(&NotEvaluated::NotAssessed)
        );
    }

    /// A named column's ceiling replaces the wildcard for that column, and a
    /// named column the report did not profile is not read as zero nulls.
    #[test]
    fn a_named_null_count_overrides_the_wildcard_and_absence_is_unevaluated() {
        let result = QualityPolicy::new()
            .max_null_count("note", 1)
            .max_null_count("absent", 0)
            .max_null_count_any(0)
            .evaluate(&complete_report())
            .unwrap();

        let checks: Vec<_> = result
            .checks
            .iter()
            .map(|check| (check.column.clone().unwrap(), check.expected))
            .collect();
        assert_eq!(
            checks,
            vec![
                (
                    "absent".to_string(),
                    Expectation::AtMost {
                        value: MetricValue::Count(0)
                    }
                ),
                (
                    "note".to_string(),
                    Expectation::AtMost {
                        value: MetricValue::Count(1)
                    }
                ),
                (
                    "id".to_string(),
                    Expectation::AtMost {
                        value: MetricValue::Count(0)
                    }
                ),
            ]
        );
        assert_eq!(
            column_check(&result, "absent").not_evaluated(),
            Some(&NotEvaluated::ColumnNotProfiled)
        );
        assert_eq!(column_check(&result, "note").status, CheckStatus::Passed);
        assert_eq!(result.verdict, Verdict::Inconclusive);
    }

    /// A conclusive violation outranks an unevaluated requirement: the gate
    /// has an answer even though it could not check everything.
    #[test]
    fn a_violation_outranks_an_unevaluated_check() {
        let result = QualityPolicy::new()
            .max_duplicate_rows(0)
            .min_dimension_score(QualityDimension::Timeliness, 90.0)
            .evaluate(&duplicate_report(2, false))
            .unwrap();

        assert_eq!(result.verdict, Verdict::Fail);
        assert_eq!(result.unevaluated().count(), 1);
    }

    /// Evaluation order is fixed by the data, not by the order requirements
    /// were added, so the same policy always serializes the same document.
    #[test]
    fn evaluation_order_does_not_depend_on_insertion_order() {
        let report = complete_report();
        let forward = QualityPolicy::new()
            .max_null_percentage("note", 50.0)
            .max_null_percentage("id", 50.0)
            .evaluate(&report)
            .unwrap();
        let reverse = QualityPolicy::new()
            .max_null_percentage("id", 50.0)
            .max_null_percentage("note", 50.0)
            .evaluate(&report)
            .unwrap();

        assert_eq!(forward, reverse);
        let columns: Vec<_> = forward
            .checks
            .iter()
            .map(|check| check.column.clone().unwrap())
            .collect();
        assert_eq!(columns, vec!["id".to_string(), "note".to_string()]);
    }

    /// The serialized shape is the contract a CI system reads. Percentages
    /// carry the report's 2dp convention; counts stay whole.
    #[test]
    fn result_serializes_into_the_documented_shape() {
        let report = complete_report();
        let result = QualityPolicy::new()
            .max_null_percentage("note", 10.0)
            .evaluate(&report)
            .unwrap();

        assert_eq!(
            serde_json::to_value(&result).unwrap(),
            json!({
                "verdict": "fail",
                "scope": "full_source",
                "evidence": {"coverage": "complete"},
                "checks": [{
                    "code": "max_null_percentage",
                    "column": "note",
                    "expected": {"comparison": "at_most", "value": 10.0},
                    "observed": 20.0,
                    "scope": "full_source",
                    "evidence": {"coverage": "complete"},
                    "status": "failed",
                    "message": "this column's null percentage is above the allowance",
                }],
                // The report's build and source version, written either way.
                "generator": serde_json::to_value(&report.generator).unwrap(),
                "source_identity": null,
            })
        );
    }

    #[test]
    fn an_unevaluated_check_serializes_its_reason() {
        let result = QualityPolicy::new()
            .min_quality_score(90.0)
            .evaluate(&no_quality_report())
            .unwrap();
        let check = &serde_json::to_value(&result).unwrap()["checks"][0];

        assert_eq!(check["status"], Value::String("not_evaluated".to_string()));
        assert_eq!(
            check["reason"],
            Value::String("quality_unavailable".to_string())
        );
        assert_eq!(
            check["quality_status"],
            Value::String("not_requested".to_string())
        );
        assert!(check.get("observed").is_none());
    }
}
