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
//! # Example
//!
//! ```no_run
//! use dataprof_runtime::{ProfileReport, QualityPolicy, Verdict};
//!
//! fn gate(report: &ProfileReport) -> Result<bool, Box<dyn std::error::Error>> {
//!     let result = QualityPolicy::new()
//!         .min_quality_score(90.0)
//!         .max_null_percentage("customer_id", 0.0)
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

use std::collections::BTreeMap;
use std::fmt;

use dataprof_core::{ColumnProfile, ExecutionMetadata, QualityDimension};
use dataprof_metrics::QualityAssessment;

use crate::profile_report::{ProfileReport, QualityAnalysisStatus};

/// Scores and percentages in a report live on a 0..=100 scale, and so do the
/// thresholds compared against them.
const PERCENTAGE_MAX: f64 = 100.0;

/// A policy that cannot be evaluated as written.
///
/// Raised before any requirement is checked: an unsatisfiable threshold is a
/// configuration mistake, and reporting it as a failed gate would blame the
/// data for it.
#[derive(Debug, Clone, PartialEq)]
pub enum PolicyError {
    /// A threshold on the 0..=100 percentage scale fell outside it, or was not
    /// a finite number. Quality scores and null percentages are percentages,
    /// not 0..1 ratios.
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
                write!(
                    f,
                    "must be a percentage between 0 and {PERCENTAGE_MAX}, got {value}"
                )
            }
            Self::NoRequirements => write!(
                f,
                "policy states no requirement; a gate must check something"
            ),
        }
    }
}

impl std::error::Error for PolicyError {}

/// What the requirements in a policy are statements about.
///
/// A profiler reads what it is given. Whether that supports a claim about the
/// whole source depends on what is being claimed, so the policy says which
/// question it asks rather than letting the evaluator guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
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
    /// The report does not record how its quality numbers were obtained, so
    /// whether they cover every scanned row is unknown. Only reachable for a
    /// report read back from a document written before dataprof recorded it;
    /// a profiling run always records it. Unknown coverage is not full
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
pub enum Evidence {
    /// Every row of the source is behind the number.
    Complete,
    /// Some of the source is not, for this reason.
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
    fn and(self, other: Self) -> Self {
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
pub enum CheckCode {
    /// The overall quality score must be at least the threshold.
    MinQualityScore,
    /// One ISO 25012 dimension score must be at least the threshold.
    MinDimensionScore,
    /// A column's null percentage must be at most the threshold.
    MaxNullPercentage,
    /// The duplicate-row count must be at most the threshold.
    MaxDuplicateRows,
    /// A metric must have been analyzed at all.
    RequireMetric,
}

impl fmt::Display for CheckCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MinQualityScore => write!(f, "min_quality_score"),
            Self::MinDimensionScore => write!(f, "min_dimension_score"),
            Self::MaxNullPercentage => write!(f, "max_null_percentage"),
            Self::MaxDuplicateRows => write!(f, "max_duplicate_rows"),
            Self::RequireMetric => write!(f, "require_metric"),
        }
    }
}

/// A number a check read, or a threshold it compared against.
///
/// Percentages follow the report's 2dp serialization convention; counts are
/// whole numbers and are not widened into rounded floats.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum MetricValue {
    /// A count of rows or values.
    Count(usize),
    /// A 0..=100 percentage or score.
    Percentage(#[serde(serialize_with = "dataprof_core::serde_helpers::round_2")] f64),
}

impl fmt::Display for MetricValue {
    /// For logs and messages, not for the serialized contract: percentages
    /// print at the report's 2dp convention, counts as whole numbers.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(count) => write!(f, "{count}"),
            Self::Percentage(value) => write!(f, "{value:.2}"),
        }
    }
}

/// The constraint a check applied.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(tag = "comparison", rename_all = "snake_case")]
pub enum Expectation {
    /// The observed value must be at least `value`.
    AtLeast {
        /// The threshold.
        value: MetricValue,
    },
    /// The observed value must be at most `value`.
    AtMost {
        /// The threshold.
        value: MetricValue,
    },
    /// The metric must have been analyzed. No number is compared.
    Analyzed,
}

impl fmt::Display for Expectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtLeast { value } => write!(f, "at least {value}"),
            Self::AtMost { value } => write!(f, "at most {value}"),
            Self::Analyzed => write!(f, "analyzed"),
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
pub enum NotEvaluated {
    /// The report carries no quality assessment, for this recorded reason.
    QualityUnavailable {
        /// The report's `quality_status` state, verbatim.
        quality_status: String,
    },
    /// Quality was computed, but this dimension had nothing to assess — no
    /// numeric values, no dates, no confidently detected pattern — or it was
    /// withheld because the run profiled a subset of columns.
    NotAssessed,
    /// The report holds no profile for the named column. It was projected
    /// away, or it is not in the source; a report does not record which, so
    /// the requirement is not decided either way.
    ColumnNotProfiled,
    /// The requirement asks about the full source, the evidence does not reach
    /// that far, and nothing observed settles it regardless.
    EvidenceIncomplete {
        /// The gap between the evidence and the requirement's scope.
        gap: EvidenceGap,
    },
}

/// What happened to one requirement.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
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
pub struct Check {
    /// Which requirement this is.
    pub code: CheckCode,
    /// The column it concerns, when it concerns one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// The quality dimension it concerns, when it concerns one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<QualityDimension>,
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

/// The overall outcome of a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
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
pub struct QualityPolicy {
    min_quality_score: Option<f64>,
    min_dimension_scores: BTreeMap<String, (QualityDimension, f64)>,
    max_null_percentage: BTreeMap<String, f64>,
    max_null_percentage_any: Option<f64>,
    max_duplicate_rows: Option<usize>,
    require_metrics: Vec<RequiredMetric>,
    scope: PolicyScope,
}

impl QualityPolicy {
    /// An empty policy. It states no requirement, so evaluating it as-is is a
    /// [`PolicyError::NoRequirements`].
    pub fn new() -> Self {
        Self::default()
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

    /// Require the report to count at most `max` duplicate rows.
    pub fn max_duplicate_rows(mut self, max: usize) -> Self {
        self.max_duplicate_rows = Some(max);
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
        if self.is_empty() {
            return Err(PolicyError::NoRequirements);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.min_quality_score.is_none()
            && self.min_dimension_scores.is_empty()
            && self.max_null_percentage.is_empty()
            && self.max_null_percentage_any.is_none()
            && self.max_duplicate_rows.is_none()
            && self.require_metrics.is_empty()
    }

    /// Evaluate the policy against a report.
    ///
    /// Returns the structured outcome. The report is not modified, nothing is
    /// printed, and the process is not exited: what to do about a failing gate
    /// is the caller's decision.
    pub fn evaluate(&self, report: &ProfileReport) -> Result<GateResult, PolicyError> {
        self.validate()?;

        let scan = scan_evidence(&report.execution);
        let mut checks = Vec::new();

        for metric in required_metrics_in_order(&self.require_metrics) {
            checks.push(self.require_metric_check(report, metric));
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
        if let Some(max) = self.max_duplicate_rows {
            checks.push(self.duplicate_rows_check(report, scan, max));
        }

        Ok(GateResult {
            verdict: verdict_of(&checks),
            scope: self.scope,
            evidence: scan,
            checks,
        })
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
            expected: Expectation::Analyzed,
            observed: None,
            scope: self.scope,
            evidence: Evidence::Complete,
            status: if analyzed {
                CheckStatus::Passed
            } else {
                CheckStatus::Failed
            },
            message: message.to_string(),
        }
    }

    fn quality_score_check(&self, report: &ProfileReport, scan: Evidence, min: f64) -> Check {
        let mut check = Check {
            code: CheckCode::MinQualityScore,
            column: None,
            dimension: None,
            expected: Expectation::AtLeast {
                value: MetricValue::Percentage(min),
            },
            observed: None,
            scope: self.scope,
            evidence: scan.and(quality_evidence(
                report.quality.as_ref(),
                Provenance::Overall,
            )),
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
        check.status = self.decide_aggregate(check.evidence, score >= min);
        check.message = aggregate_message(&check.status, "the overall quality score");
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
            expected: Expectation::AtLeast {
                value: MetricValue::Percentage(min),
            },
            observed: None,
            scope: self.scope,
            evidence: scan.and(quality_evidence(
                report.quality.as_ref(),
                Provenance::Dimension(dimension),
            )),
            status: CheckStatus::Passed,
            message: String::new(),
        };
        let Some(quality) = report.quality.as_ref() else {
            return unavailable_quality(check, report);
        };
        let Some(score) = quality.metrics.dimension_score(dimension) else {
            check.status = CheckStatus::NotEvaluated(NotEvaluated::NotAssessed);
            check.message = "this dimension had nothing to assess in this run".to_string();
            return check;
        };
        check.observed = Some(MetricValue::Percentage(score));
        check.status = self.decide_aggregate(check.evidence, score >= min);
        check.message = aggregate_message(&check.status, "this dimension's score");
        check
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
            expected: Expectation::AtMost {
                value: MetricValue::Percentage(max),
            },
            observed: None,
            scope: self.scope,
            evidence: scan,
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
        check.status = self.decide_aggregate(check.evidence, percentage <= max);
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

    /// A duplicate count is the one requirement here that an incomplete scan
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
            expected: Expectation::AtMost {
                value: MetricValue::Count(max),
            },
            observed: None,
            scope: self.scope,
            evidence: scan.and(quality_evidence(
                report.quality.as_ref(),
                Provenance::Component("duplicate_rows"),
            )),
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
            CheckStatus::NotEvaluated(_) if estimated => {
                "the duplicate-row count is estimated, so it witnesses nothing about the \
                 rows that were not read"
            }
            CheckStatus::NotEvaluated(_) => {
                "no duplicate above the allowance was observed, and the rows that were not \
                 read may hold more"
            }
        }
        .to_string();
        check
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

/// The `state` tag the report serializes for its quality status.
fn quality_status_name(status: &QualityAnalysisStatus) -> &'static str {
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

/// How much of the source the scan itself covered.
///
/// Truncation is named ahead of sampling and sampling ahead of skipped
/// records, so the gap reported is the largest one.
fn scan_evidence(execution: &ExecutionMetadata) -> Evidence {
    let reason = if !execution.source_exhausted || execution.truncation_reason.is_some() {
        Some(EvidenceGap::Truncated)
    } else if execution.sampling_applied {
        Some(EvidenceGap::Sampled)
    } else if execution.error_count > 0 {
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
enum Provenance {
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
fn dimension_components(dimension: QualityDimension) -> &'static [&'static str] {
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
    let Some(sampled) = quality.sampled_dimensions() else {
        return Evidence::Incomplete {
            reason: EvidenceGap::CoverageUnrecorded,
        };
    };
    let contains = |component: &str| sampled.iter().any(|label| label == component);
    let is_sampled = match provenance {
        // The overall score is a weighted average over the *assessed*
        // dimensions, so a sampled dimension the weights exclude does not
        // reach it. Reporting the aggregate as sampled because of one would
        // withhold a verdict the number does not depend on.
        Provenance::Overall => quality
            .metrics
            .assessed_dimensions()
            .into_iter()
            .flat_map(dimension_components)
            .any(|component| contains(component)),
        Provenance::Dimension(dimension) => dimension_components(dimension)
            .iter()
            .copied()
            .any(contains),
        Provenance::Component(component) => contains(component),
    };
    if is_sampled {
        Evidence::Incomplete {
            reason: EvidenceGap::QualitySampled,
        }
    } else {
        Evidence::Complete
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dataprof_core::{
        ColumnStats, DataSource, DataType, FileFormat, QualityScoreWeights, TruncationReason,
    };
    use dataprof_metrics::{QualityMetrics, UniquenessMetrics};
    use serde_json::{Value, json};

    use super::*;
    use crate::ReportAssembler;

    fn source() -> DataSource {
        DataSource::File {
            path: "drop.csv".to_string(),
            format: FileFormat::Csv,
            size_bytes: 256,
            modified_at: None,
            parquet_metadata: None,
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
            invalid_count: None,
            type_homogeneity: None,
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
    fn a_reservoir_backed_score_does_not_claim_full_source_coverage() {
        let report = reservoir_report();
        assert!(report.execution.source_exhausted);
        assert!(!report.execution.sampling_applied);
        let sampled = report
            .quality
            .as_ref()
            .expect("quality was computed")
            .sampled_dimensions()
            .expect("a profiling run records its provenance");
        assert!(
            !sampled.is_empty(),
            "the assembler did not bifurcate; this test no longer reaches the \
             case it guards"
        );

        let result = QualityPolicy::new()
            .min_quality_score(1.0)
            .evaluate(&report)
            .unwrap();

        assert_eq!(result.verdict, Verdict::Inconclusive);
        // The scan itself was complete; only the metric's own evidence is not.
        assert_eq!(result.evidence, Evidence::Complete);
        let check = check_for(&result, CheckCode::MinQualityScore);
        assert_eq!(
            check.evidence,
            Evidence::Incomplete {
                reason: EvidenceGap::QualitySampled
            }
        );
        assert_eq!(
            check.not_evaluated(),
            Some(&NotEvaluated::EvidenceIncomplete {
                gap: EvidenceGap::QualitySampled
            })
        );
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
        assert_eq!(
            check_for(&result, CheckCode::MaxDuplicateRows).not_evaluated(),
            Some(&NotEvaluated::EvidenceIncomplete {
                gap: EvidenceGap::Truncated
            })
        );
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
        let result = QualityPolicy::new()
            .max_null_percentage("note", 10.0)
            .evaluate(&complete_report())
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
