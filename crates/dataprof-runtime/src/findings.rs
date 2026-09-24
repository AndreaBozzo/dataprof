//! Structured, prioritized findings over a [`ProfileReport`] (#375).
//!
//! A report carries every metric, and says nothing about which of them
//! deserves attention. Answering that by hand means inventing thresholds at
//! every call site. [`FindingPolicy::evaluate`] (or [`ProfileReport::findings`]
//! with the default thresholds) turns the metrics a report already holds into
//! a short, deterministic list of [`Finding`]s.
//!
//! Findings are interpretation, not data cleaning: each one names what was
//! observed and the evidence behind it, and none carries a raw cell value.
//!
//! # Computed, not stored
//!
//! Findings are derived from the report on demand and are not part of the
//! serialized report. A report read back from its document yields the same
//! findings as the report that wrote it, and changing a threshold does not
//! require re-profiling or rewriting stored reports.
//!
//! # Absence is not a clean result
//!
//! A rule whose input the report does not carry, such as patterns that were
//! never detected or a quality assessment that was not requested, produces no
//! finding, and is listed in [`FindingsResult::not_evaluated`] with the reason.
//! An empty `findings` list means "looked, found nothing" only for the rules
//! that are not listed there.
//!
//! # Scope
//!
//! Findings describe the rows the report read. When the scan stopped early or
//! sampled, a [`FindingCode::PartialScan`] finding says so; the other findings
//! are not withheld, because what was observed is still true of those rows.
//! Use a [`QualityPolicy`](crate::QualityPolicy) for pass/fail decisions about
//! the whole source.

use std::collections::BTreeMap;
use std::fmt;

use dataprof_core::serde_helpers::rounded_2;
use dataprof_core::{ColumnProfile, DataType, PatternCategory};
use dataprof_metrics::QualityAssessment;

use crate::profile_report::ProfileReport;
use crate::quality_gate::quality_status_name;

/// Default for [`FindingPolicy::null_heavy_percentage`]. Matches the
/// `null-heavy` flag in the Python binding's `to_llm_context()`.
pub const DEFAULT_NULL_HEAVY_PERCENTAGE: f64 = 20.0;

/// Default for [`FindingPolicy::mixed_types_percentage`]. Matches the
/// `mixed types` flag in the Python binding's `to_llm_context()`.
pub const DEFAULT_MIXED_TYPES_PERCENTAGE: f64 = 5.0;

/// Pattern confidence a detection needs before a finding reports it: the
/// threshold validity scoring and the report summaries already use.
const MIN_PATTERN_CONFIDENCE: f64 = 0.5;

/// Identifier patterns that name a person. The rest of the identifier
/// category (UUIDs, product codes, VAT numbers) identifies things.
const PERSONAL_IDENTIFIER_PATTERNS: [&str; 2] = ["Codice Fiscale (IT)", "SSN (US)"];

/// How much attention a finding asks for.
///
/// Declared order is priority order: warnings sort first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Severity {
    /// Likely a defect in the data or in how it was read.
    Warning,
    /// Worth knowing when reading the report; often intended.
    Info,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Warning => write!(f, "warning"),
            Self::Info => write!(f, "info"),
        }
    }
}

/// Stable identifier for the rule a finding came from.
///
/// Part of the contract: a consumer matches on these, so they do not change
/// when the prose does. Declared in alphabetical order, which is the order
/// findings of equal severity sort in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FindingCode {
    /// Every value of a column is null.
    AllNull,
    /// Every non-null value of a column is the same one.
    ConstantColumn,
    /// The source holds exact duplicate rows.
    DuplicateRows,
    /// Date values lie after the time the report was produced.
    FutureDates,
    /// A column's values split across lexical types.
    MixedTypes,
    /// A column's null share is at or above the policy threshold.
    NullHeavy,
    /// The scan stopped early or sampled rows.
    PartialScan,
    /// Rows had a different field count from the header and were recovered.
    RaggedRows,
    /// Errors were counted while reading the source.
    RecordsSkipped,
    /// A column matches a pattern for personal or financial data.
    SensitivePattern,
    /// Start dates fall after their paired end dates.
    TemporalOrderViolations,
}

impl FindingCode {
    /// The severity every finding with this code carries.
    pub fn severity(self) -> Severity {
        match self {
            Self::AllNull
            | Self::DuplicateRows
            | Self::FutureDates
            | Self::MixedTypes
            | Self::NullHeavy
            | Self::RaggedRows
            | Self::RecordsSkipped
            | Self::TemporalOrderViolations => Severity::Warning,
            Self::ConstantColumn | Self::PartialScan | Self::SensitivePattern => Severity::Info,
        }
    }

    /// A fixed sentence naming what the rule observed.
    ///
    /// Deliberately free of numbers, as the gate's messages are: the evidence
    /// carries those, so the Rust and Python layers cannot drift on rounding
    /// inside a string.
    fn summary(self) -> &'static str {
        match self {
            Self::AllNull => "every value in this column is null",
            Self::ConstantColumn => "every non-null value in this column is the same",
            Self::DuplicateRows => "the source contains exact duplicate rows",
            Self::FutureDates => "some date values lie in the future",
            Self::MixedTypes => "this column's values are split across lexical types",
            Self::NullHeavy => "this column's null share is at or above the threshold",
            Self::PartialScan => {
                "only part of the source was read; findings describe the rows that were"
            }
            Self::RaggedRows => {
                "some rows had a different field count from the header and were recovered"
            }
            Self::RecordsSkipped => "errors were counted while reading the source",
            Self::SensitivePattern => {
                "values in this column match a pattern for personal or financial data"
            }
            Self::TemporalOrderViolations => "some start dates fall after their paired end dates",
        }
    }
}

impl fmt::Display for FindingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::AllNull => "all_null",
            Self::ConstantColumn => "constant_column",
            Self::DuplicateRows => "duplicate_rows",
            Self::FutureDates => "future_dates",
            Self::MixedTypes => "mixed_types",
            Self::NullHeavy => "null_heavy",
            Self::PartialScan => "partial_scan",
            Self::RaggedRows => "ragged_rows",
            Self::RecordsSkipped => "records_skipped",
            Self::SensitivePattern => "sensitive_pattern",
            Self::TemporalOrderViolations => "temporal_order_violations",
        };
        write!(f, "{name}")
    }
}

/// One value a finding read from the report, or a threshold it applied.
///
/// Percentages follow the report's 2dp serialization convention; counts are
/// whole numbers and are not widened into rounded floats.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum EvidenceValue {
    /// A count of rows or values.
    Count(usize),
    /// A 0..=100 percentage.
    Percentage(#[serde(serialize_with = "dataprof_core::serde_helpers::round_2")] f64),
    /// A name from the report: a lexical type, a pattern, a reason.
    Text(String),
    /// A yes/no property of the evidence.
    Flag(bool),
}

/// Something in the report that deserves attention.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct Finding {
    /// Which rule produced it.
    pub code: FindingCode,
    /// How much attention it asks for. Always `code.severity()`.
    pub severity: Severity,
    /// The column it concerns, absent for a finding about the whole report.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// The metric values and thresholds that caused it, by name. Never a raw
    /// cell value.
    pub evidence: BTreeMap<String, EvidenceValue>,
    /// A fixed sentence naming what was observed.
    pub summary: String,
}

/// Why a rule produced no finding without having looked.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[non_exhaustive]
pub enum NotEvaluatedReason {
    /// The report carries no quality assessment, for this recorded reason.
    QualityUnavailable {
        /// The report's `quality_status` state, verbatim.
        quality_status: String,
    },
    /// Quality was computed, but the dimension the rule reads had nothing to
    /// assess in this run.
    NotAssessed,
    /// The number the rule reads is an estimate, which witnesses nothing: an
    /// estimated duplicate count is not evidence that a duplicate exists.
    Estimated,
    /// The count the rule reads is zero, but it came from the retained quality
    /// sample rather than every scanned row, so it rules nothing out for the
    /// rows the sample left behind. A nonzero count is still reported.
    Sampled,
    /// The report does not record what the rule needs to tell a zero from an
    /// unmeasured value: a document written before dataprof recorded it
    /// (ragged rows before 0.10, quality sample coverage before 0.12).
    Unrecorded,
    /// The metric the rule reads was not computed for these columns: the
    /// metric pack was not selected, or the column type does not carry it.
    NotComputed,
    /// These columns had no values for the rule to look at.
    NoValues,
}

impl NotEvaluatedReason {
    /// Position in the declared order, which is the order entries sort in.
    fn rank(&self) -> u8 {
        match self {
            Self::QualityUnavailable { .. } => 0,
            Self::NotAssessed => 1,
            Self::Estimated => 2,
            Self::Sampled => 3,
            Self::Unrecorded => 4,
            Self::NotComputed => 5,
            Self::NoValues => 6,
        }
    }
}

/// A rule that could not be evaluated, and why.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct UnevaluatedRule {
    /// The rule.
    pub code: FindingCode,
    /// Why it was not evaluated.
    #[serde(flatten)]
    pub reason: NotEvaluatedReason,
    /// For a column rule, the columns it could not look at, in report order.
    /// Empty for a rule about the whole report.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

/// What [`FindingPolicy::evaluate`] found.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[non_exhaustive]
pub struct FindingsResult {
    /// Every finding, most severe first. Within a severity: by code, then
    /// report-level before column-level, then by column position in the
    /// report, then by pattern name.
    pub findings: Vec<Finding>,
    /// Every rule that could not look, by code then reason. A rule listed
    /// here for some columns was still evaluated for the others.
    pub not_evaluated: Vec<UnevaluatedRule>,
}

/// A finding threshold that cannot be applied as written.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FindingPolicyError {
    /// A threshold fell outside `(0, 100]` at the 2dp precision it is applied
    /// at, or was not a finite number. Percentages are on the report's 0..100
    /// scale, not 0..1 ratios, and a zero threshold would report every column.
    ThresholdOutOfRange {
        /// The setting that carried it.
        setting: &'static str,
        /// The value as supplied.
        value: f64,
    },
}

impl fmt::Display for FindingPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ThresholdOutOfRange { setting, value } => write!(
                f,
                "{setting} must be a percentage above 0 and at most 100 at two decimal \
                 places, got {value}"
            ),
        }
    }
}

impl std::error::Error for FindingPolicyError {}

/// The thresholds findings apply.
///
/// Separate from the profiler's configuration on purpose: findings interpret
/// a finished report, including one read back from disk that no profiler
/// configuration accompanies.
#[derive(Debug, Clone, PartialEq)]
pub struct FindingPolicy {
    null_heavy_percentage: f64,
    mixed_types_percentage: f64,
}

impl Default for FindingPolicy {
    fn default() -> Self {
        Self {
            null_heavy_percentage: DEFAULT_NULL_HEAVY_PERCENTAGE,
            mixed_types_percentage: DEFAULT_MIXED_TYPES_PERCENTAGE,
        }
    }
}

impl FindingPolicy {
    /// The default thresholds.
    pub fn new() -> Self {
        Self::default()
    }

    /// Report a column as `null_heavy` when its null percentage, at the
    /// report's 2dp precision, is at least `percentage`. The threshold is
    /// applied at 2dp too, so the evidence states exactly what was compared.
    pub fn null_heavy_percentage(mut self, percentage: f64) -> Self {
        self.null_heavy_percentage = percentage;
        self
    }

    /// Report a column as `mixed_types` when the values outside its dominant
    /// lexical type make up at least `percentage` of those classified, both
    /// at 2dp.
    pub fn mixed_types_percentage(mut self, percentage: f64) -> Self {
        self.mixed_types_percentage = percentage;
        self
    }

    /// Check that every threshold can be applied, without needing a report.
    pub fn validate(&self) -> Result<(), FindingPolicyError> {
        for (setting, value) in [
            ("null_heavy_percentage", self.null_heavy_percentage),
            ("mixed_types_percentage", self.mixed_types_percentage),
        ] {
            let applied = rounded_2(value);
            if !(value.is_finite() && applied > 0.0 && applied <= 100.0) {
                return Err(FindingPolicyError::ThresholdOutOfRange { setting, value });
            }
        }
        Ok(())
    }

    /// Derive the findings a report supports.
    ///
    /// The report is not modified and nothing is printed.
    pub fn evaluate(&self, report: &ProfileReport) -> Result<FindingsResult, FindingPolicyError> {
        self.validate()?;
        Ok(self.collect(report))
    }

    /// The thresholds as applied: at the precision a percentage is compared
    /// and reported at.
    fn null_heavy_threshold(&self) -> f64 {
        rounded_2(self.null_heavy_percentage)
    }

    fn mixed_types_threshold(&self) -> f64 {
        rounded_2(self.mixed_types_percentage)
    }

    fn collect(&self, report: &ProfileReport) -> FindingsResult {
        let mut collector = Collector::default();
        scan_findings(report, &mut collector);
        quality_findings(report, &mut collector);
        for (index, column) in report.column_profiles.iter().enumerate() {
            self.column_findings(index, column, &mut collector);
        }
        collector.finish()
    }

    fn column_findings(&self, index: usize, column: &ColumnProfile, out: &mut Collector) {
        let name = column.name.as_str();
        let non_null = column.total_count.saturating_sub(column.null_count);

        if column.total_count == 0 {
            out.skip(FindingCode::AllNull, NotEvaluatedReason::NoValues, name);
            out.skip(FindingCode::NullHeavy, NotEvaluatedReason::NoValues, name);
        } else if non_null == 0 {
            out.column(
                FindingCode::AllNull,
                index,
                name,
                [
                    ("null_count", EvidenceValue::Count(column.null_count)),
                    ("total_count", EvidenceValue::Count(column.total_count)),
                ],
            );
        } else {
            // Compared at the serialized precision, so a report read back from
            // its document lands on the same side of the threshold.
            let null_percentage =
                rounded_2(column.null_count as f64 / column.total_count as f64 * 100.0);
            if null_percentage >= self.null_heavy_threshold() {
                out.column(
                    FindingCode::NullHeavy,
                    index,
                    name,
                    [
                        ("null_percentage", percentage(null_percentage)),
                        ("threshold", percentage(self.null_heavy_threshold())),
                    ],
                );
            }
        }

        match column.unique_count {
            None => out.skip(
                FindingCode::ConstantColumn,
                NotEvaluatedReason::NotComputed,
                name,
            ),
            Some(_) if non_null == 0 => out.skip(
                FindingCode::ConstantColumn,
                NotEvaluatedReason::NoValues,
                name,
            ),
            // One value seen once is not a constant, just a single value.
            Some(1) if non_null > 1 => out.column(
                FindingCode::ConstantColumn,
                index,
                name,
                [
                    ("unique_count", EvidenceValue::Count(1)),
                    ("non_null_count", EvidenceValue::Count(non_null)),
                ],
            ),
            Some(_) => {}
        }

        self.mixed_types(index, column, non_null, out);
        sensitive_patterns(index, column, non_null, out);
    }

    fn mixed_types(
        &self,
        index: usize,
        column: &ColumnProfile,
        non_null: usize,
        out: &mut Collector,
    ) {
        let name = column.name.as_str();
        // Mixing forms ("A1", "123") is what an identifier scheme does, not a
        // defect: the same exemption the consistency dimension makes.
        if column.data_type == DataType::Identifier {
            return;
        }
        let Some(homogeneity) = column.type_homogeneity.as_ref() else {
            out.skip(
                FindingCode::MixedTypes,
                NotEvaluatedReason::NotComputed,
                name,
            );
            return;
        };
        let classified = homogeneity.classified_count();
        let Some((dominant, dominant_count)) = homogeneity.dominant() else {
            out.skip(FindingCode::MixedTypes, NotEvaluatedReason::NoValues, name);
            return;
        };
        let outside = classified - dominant_count;
        if outside == 0 {
            return;
        }
        let outside_percentage = rounded_2(outside as f64 / classified as f64 * 100.0);
        if outside_percentage < self.mixed_types_threshold() {
            return;
        }
        out.column(
            FindingCode::MixedTypes,
            index,
            name,
            [
                ("dominant_type", EvidenceValue::Text(dominant.to_string())),
                (
                    "dominant_percentage",
                    percentage(dominant_count as f64 / classified as f64 * 100.0),
                ),
                ("threshold", percentage(self.mixed_types_threshold())),
                // Shares are counted over the values the profiler retained; a
                // classified count short of the non-null count says they were
                // sampled.
                ("classified_count", EvidenceValue::Count(classified)),
                ("non_null_count", EvidenceValue::Count(non_null)),
            ],
        );
    }
}

impl ProfileReport {
    /// The findings this report supports under the default thresholds.
    ///
    /// Shorthand for `FindingPolicy::default().evaluate(self)`, which cannot
    /// fail because the defaults are in range.
    pub fn findings(&self) -> FindingsResult {
        FindingPolicy::default().collect(self)
    }
}

fn scan_findings(report: &ProfileReport, out: &mut Collector) {
    let execution = &report.execution;
    let partial = if !execution.source_exhausted || execution.truncation_reason.is_some() {
        Some("truncated")
    } else if execution.sampling_applied {
        Some("sampled")
    } else {
        None
    };
    if let Some(reason) = partial {
        out.report(
            FindingCode::PartialScan,
            [
                ("reason", EvidenceValue::Text(reason.to_string())),
                (
                    "rows_processed",
                    EvidenceValue::Count(execution.rows_processed),
                ),
            ],
        );
    }
    if report.schema_version == 0 {
        // Ragged rows were first counted by the release that introduced
        // schema versioning (0.10, #452). An older document reads back with a
        // defaulted zero that was never measured.
        out.skip_report(FindingCode::RaggedRows, NotEvaluatedReason::Unrecorded);
    } else if execution.ragged_row_count > 0 {
        out.report(
            FindingCode::RaggedRows,
            [
                (
                    "ragged_row_count",
                    EvidenceValue::Count(execution.ragged_row_count),
                ),
                (
                    "rows_processed",
                    EvidenceValue::Count(execution.rows_processed),
                ),
            ],
        );
    }
    if execution.error_count > 0 {
        out.report(
            FindingCode::RecordsSkipped,
            [("error_count", EvidenceValue::Count(execution.error_count))],
        );
    }
}

fn quality_findings(report: &ProfileReport, out: &mut Collector) {
    const QUALITY_RULES: [FindingCode; 3] = [
        FindingCode::DuplicateRows,
        FindingCode::FutureDates,
        FindingCode::TemporalOrderViolations,
    ];
    let Some(quality) = report.quality.as_ref() else {
        let quality_status = quality_status_name(&report.quality_status).to_string();
        for code in QUALITY_RULES {
            out.skip_report(
                code,
                NotEvaluatedReason::QualityUnavailable {
                    quality_status: quality_status.clone(),
                },
            );
        }
        return;
    };

    match quality
        .metrics
        .uniqueness
        .as_ref()
        .filter(|uniqueness| uniqueness.rows_checked > 0)
    {
        None => out.skip_report(FindingCode::DuplicateRows, NotEvaluatedReason::NotAssessed),
        Some(uniqueness) if uniqueness.duplicate_rows_approximate => {
            out.skip_report(FindingCode::DuplicateRows, NotEvaluatedReason::Estimated)
        }
        Some(uniqueness) if uniqueness.duplicate_rows > 0 => out.report(
            FindingCode::DuplicateRows,
            [
                (
                    "duplicate_rows",
                    EvidenceValue::Count(uniqueness.duplicate_rows),
                ),
                (
                    "rows_checked",
                    EvidenceValue::Count(uniqueness.rows_checked),
                ),
            ],
        ),
        Some(_) => skip_unless_complete(out, FindingCode::DuplicateRows, quality, "duplicate_rows"),
    }

    let timeliness = quality.metrics.timeliness.as_ref();
    match timeliness.filter(|timeliness| timeliness.date_values_checked > 0) {
        None => out.skip_report(FindingCode::FutureDates, NotEvaluatedReason::NotAssessed),
        Some(timeliness) if timeliness.future_dates_count > 0 => out.report(
            FindingCode::FutureDates,
            [
                (
                    "future_dates_count",
                    EvidenceValue::Count(timeliness.future_dates_count),
                ),
                (
                    "date_values_checked",
                    EvidenceValue::Count(timeliness.date_values_checked),
                ),
            ],
        ),
        Some(_) => skip_unless_complete(out, FindingCode::FutureDates, quality, "timeliness"),
    }
    match timeliness.filter(|timeliness| timeliness.temporal_pairs_checked > 0) {
        None => out.skip_report(
            FindingCode::TemporalOrderViolations,
            NotEvaluatedReason::NotAssessed,
        ),
        Some(timeliness) if timeliness.temporal_violations > 0 => out.report(
            FindingCode::TemporalOrderViolations,
            [
                (
                    "temporal_violations",
                    EvidenceValue::Count(timeliness.temporal_violations),
                ),
                (
                    "temporal_pairs_checked",
                    EvidenceValue::Count(timeliness.temporal_pairs_checked),
                ),
            ],
        ),
        Some(_) => skip_unless_complete(
            out,
            FindingCode::TemporalOrderViolations,
            quality,
            "timeliness",
        ),
    }
}

/// A zero count is a clean result only when it covers every scanned row.
///
/// A count from the retained quality sample, or from a report that does not
/// record where its counts came from, rules nothing out for the rows it did
/// not see, so the rule is listed rather than read as clean. A nonzero count
/// never reaches here: rows already witnessed stay witnessed. The component
/// labels are the ones the quality gate resolves provenance by.
fn skip_unless_complete(
    out: &mut Collector,
    code: FindingCode,
    quality: &QualityAssessment,
    component: &str,
) {
    match quality.sampled_dimensions() {
        None => out.skip_report(code, NotEvaluatedReason::Unrecorded),
        Some(sampled) if sampled.iter().any(|label| label == component) => {
            out.skip_report(code, NotEvaluatedReason::Sampled)
        }
        Some(_) => {}
    }
}

/// Whether a detected pattern names personal or financial data.
fn is_sensitive(category: &PatternCategory, name: &str) -> bool {
    matches!(
        category,
        PatternCategory::Contact | PatternCategory::Financial
    ) || PERSONAL_IDENTIFIER_PATTERNS.contains(&name)
}

fn sensitive_patterns(index: usize, column: &ColumnProfile, non_null: usize, out: &mut Collector) {
    let Some(patterns) = column.patterns.as_ref() else {
        out.skip(
            FindingCode::SensitivePattern,
            NotEvaluatedReason::NotComputed,
            &column.name,
        );
        return;
    };
    if non_null == 0 {
        // Detection ran over nothing, which is no evidence either way.
        out.skip(
            FindingCode::SensitivePattern,
            NotEvaluatedReason::NoValues,
            &column.name,
        );
        return;
    }
    for pattern in patterns {
        if pattern.confidence < MIN_PATTERN_CONFIDENCE
            || !is_sensitive(&pattern.category, &pattern.name)
        {
            continue;
        }
        out.push(
            Finding {
                code: FindingCode::SensitivePattern,
                severity: FindingCode::SensitivePattern.severity(),
                column: Some(column.name.clone()),
                evidence: evidence([
                    ("pattern", EvidenceValue::Text(pattern.name.clone())),
                    (
                        "category",
                        EvidenceValue::Text(pattern.category.to_string()),
                    ),
                    ("match_percentage", percentage(pattern.match_percentage)),
                ]),
                summary: FindingCode::SensitivePattern.summary().to_string(),
            },
            Some(index),
            pattern.name.clone(),
        );
    }
}

/// Percentage evidence at the precision it serializes with, so a finding
/// compares equal to one rebuilt from its own document, and to what the
/// Python layer holds.
fn percentage(value: f64) -> EvidenceValue {
    EvidenceValue::Percentage(rounded_2(value))
}

fn evidence<const N: usize>(
    entries: [(&'static str, EvidenceValue); N],
) -> BTreeMap<String, EvidenceValue> {
    entries
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect()
}

/// A finding with the keys it sorts by.
struct Ranked {
    finding: Finding,
    /// `None` for a report-level finding, which sorts before column findings.
    column_index: Option<usize>,
    /// Breaks ties between findings of one code on one column.
    detail: String,
}

#[derive(Default)]
struct Collector {
    findings: Vec<Ranked>,
    not_evaluated: Vec<UnevaluatedRule>,
}

impl Collector {
    fn push(&mut self, finding: Finding, column_index: Option<usize>, detail: String) {
        self.findings.push(Ranked {
            finding,
            column_index,
            detail,
        });
    }

    fn report<const N: usize>(
        &mut self,
        code: FindingCode,
        entries: [(&'static str, EvidenceValue); N],
    ) {
        self.push(
            Finding {
                code,
                severity: code.severity(),
                column: None,
                evidence: evidence(entries),
                summary: code.summary().to_string(),
            },
            None,
            String::new(),
        );
    }

    fn column<const N: usize>(
        &mut self,
        code: FindingCode,
        index: usize,
        name: &str,
        entries: [(&'static str, EvidenceValue); N],
    ) {
        self.push(
            Finding {
                code,
                severity: code.severity(),
                column: Some(name.to_string()),
                evidence: evidence(entries),
                summary: code.summary().to_string(),
            },
            Some(index),
            String::new(),
        );
    }

    fn skip_report(&mut self, code: FindingCode, reason: NotEvaluatedReason) {
        self.not_evaluated.push(UnevaluatedRule {
            code,
            reason,
            columns: Vec::new(),
        });
    }

    /// Record a column a rule could not look at, joining the entry for the
    /// same rule and reason so each pair is listed once.
    fn skip(&mut self, code: FindingCode, reason: NotEvaluatedReason, column: &str) {
        match self
            .not_evaluated
            .iter_mut()
            .find(|entry| entry.code == code && entry.reason == reason)
        {
            Some(entry) => entry.columns.push(column.to_string()),
            None => self.not_evaluated.push(UnevaluatedRule {
                code,
                reason,
                columns: vec![column.to_string()],
            }),
        }
    }

    fn finish(mut self) -> FindingsResult {
        self.findings.sort_by(|left, right| {
            (left.finding.severity, left.finding.code)
                .cmp(&(right.finding.severity, right.finding.code))
                .then_with(|| left.column_index.cmp(&right.column_index))
                .then_with(|| left.detail.cmp(&right.detail))
        });
        // Stable, so the columns inside one entry keep report order.
        self.not_evaluated.sort_by(|left, right| {
            (left.code, left.reason.rank()).cmp(&(right.code, right.reason.rank()))
        });
        FindingsResult {
            findings: self
                .findings
                .into_iter()
                .map(|ranked| ranked.finding)
                .collect(),
            not_evaluated: self.not_evaluated,
        }
    }
}

#[cfg(test)]
mod tests {
    use dataprof_core::{
        ColumnStats, DataSource, ExecutionMetadata, FileFormat, Pattern, TruncationReason,
        TypeHomogeneity,
    };
    use dataprof_metrics::{
        MetricConfidence, QualityAssessment, QualityMetrics, TimelinessMetrics, UniquenessMetrics,
    };

    use super::*;

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

    fn pattern(name: &str, category: PatternCategory, confidence: f64) -> Pattern {
        Pattern {
            name: name.to_string(),
            regex: String::new(),
            match_count: 10,
            match_percentage: 100.0,
            category,
            confidence,
        }
    }

    fn report(columns: Vec<ColumnProfile>, execution: ExecutionMetadata) -> ProfileReport {
        ProfileReport::new(source(), columns, execution, None)
    }

    fn codes(result: &FindingsResult) -> Vec<(FindingCode, Option<&str>)> {
        result
            .findings
            .iter()
            .map(|finding| (finding.code, finding.column.as_deref()))
            .collect()
    }

    #[test]
    fn an_estimated_duplicate_count_witnesses_nothing() {
        let metrics = QualityMetrics {
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 1_200,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
                rows_checked: 5_000_000,
                key_column: None,
                duplicate_rows_approximate: true,
            }),
            ..QualityMetrics::default()
        };
        let report = ProfileReport::new(
            source(),
            vec![],
            ExecutionMetadata::new(5_000_000, 0, 10),
            Some(QualityAssessment::exact(metrics)),
        );

        let result = report.findings();

        assert!(
            !codes(&result).contains(&(FindingCode::DuplicateRows, None)),
            "an HLL estimate reported as a duplicate: {:?}",
            result.findings
        );
        assert!(result.not_evaluated.contains(&UnevaluatedRule {
            code: FindingCode::DuplicateRows,
            reason: NotEvaluatedReason::Estimated,
            columns: vec![],
        }));
    }

    #[test]
    fn the_threshold_is_compared_at_the_serialized_precision() {
        // 4,999 of 25,000 is 19.996%, which the report serializes as 20.0. A
        // full-precision comparison would miss it here and report it once the
        // report was read back from its document.
        let report = report(
            vec![column("near", 25_000, 4_999)],
            ExecutionMetadata::new(25_000, 1, 10),
        );

        let finding = report
            .findings()
            .findings
            .into_iter()
            .find(|finding| finding.code == FindingCode::NullHeavy)
            .expect("reported at the rounded value");
        assert_eq!(
            finding.evidence["null_percentage"],
            EvidenceValue::Percentage(20.0)
        );
    }

    #[test]
    fn only_confident_personal_or_financial_patterns_are_sensitive() {
        let mut profile = column("mixed_ids", 10, 0);
        profile.patterns = Some(vec![
            pattern("UUID", PatternCategory::Identifier, 0.9),
            pattern("SSN (US)", PatternCategory::Identifier, 0.9),
            pattern("IBAN", PatternCategory::Financial, 0.4),
            pattern("Email", PatternCategory::Contact, 0.8),
            pattern("IPv4", PatternCategory::Network, 0.9),
        ]);
        let result = report(vec![profile], ExecutionMetadata::new(10, 1, 10)).findings();

        let reported: Vec<_> = result
            .findings
            .iter()
            .filter(|finding| finding.code == FindingCode::SensitivePattern)
            .map(|finding| finding.evidence["pattern"].clone())
            .collect();
        // Ordered by pattern name within the column, not by detection order.
        assert_eq!(
            reported,
            vec![
                EvidenceValue::Text("Email".to_string()),
                EvidenceValue::Text("SSN (US)".to_string()),
            ]
        );
    }

    #[test]
    fn evidence_holds_the_value_it_serializes() {
        // A percentage kept at full precision in memory and rounded only on
        // the way out would make two findings that serialize identically
        // compare unequal, depending on who wrote the document they came from.
        let mut profile = column("email", 3, 0);
        profile.type_homogeneity = Some(TypeHomogeneity {
            numeric: 1,
            date: 0,
            boolean: 0,
            text: 2,
        });
        let mut email = pattern("Email", PatternCategory::Contact, 0.8);
        email.match_percentage = 200.0 / 3.0;
        profile.patterns = Some(vec![email]);
        let result = report(vec![profile], ExecutionMetadata::new(3, 1, 10)).findings();

        let serialized: FindingsResult = {
            let value = serde_json::to_value(&result).expect("serializes");
            let mut rebuilt = result.clone();
            for (finding, document) in rebuilt
                .findings
                .iter_mut()
                .zip(value["findings"].as_array().unwrap())
            {
                for (name, evidence) in finding.evidence.iter_mut() {
                    if let EvidenceValue::Percentage(_) = evidence {
                        *evidence =
                            EvidenceValue::Percentage(document["evidence"][name].as_f64().unwrap());
                    }
                }
            }
            rebuilt
        };
        assert_eq!(result, serialized);
    }

    fn quality_report(future_dates: usize, confidence: MetricConfidence) -> ProfileReport {
        let metrics = QualityMetrics {
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 0,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
                rows_checked: 60_000,
                key_column: None,
                duplicate_rows_approximate: false,
            }),
            timeliness: Some(TimelinessMetrics {
                future_dates_count: future_dates,
                stale_data_ratio: 0.0,
                temporal_violations: 0,
                invalid_date_values: 0,
                date_values_checked: 10_000,
                temporal_pairs_checked: 10_000,
            }),
            ..QualityMetrics::default()
        };
        ProfileReport::new(
            source(),
            vec![],
            ExecutionMetadata::new(60_000, 0, 10),
            Some(QualityAssessment::new(metrics, confidence)),
        )
    }

    fn skipped(result: &FindingsResult, code: FindingCode) -> Option<&NotEvaluatedReason> {
        result
            .not_evaluated
            .iter()
            .find(|entry| entry.code == code)
            .map(|entry| &entry.reason)
    }

    #[test]
    fn a_zero_count_from_the_quality_sample_rules_nothing_out() {
        // The ordinary large-file shape: every row scanned, timeliness computed
        // over the retained reservoir, duplicates over the full stream.
        let sampled = || MetricConfidence::Mixed {
            exact_dimensions: vec!["duplicate_rows".to_string()],
            sampled_dimensions: vec!["timeliness".to_string()],
            sample_size: 10_000,
        };

        let clean = quality_report(0, sampled()).findings();
        assert_eq!(
            skipped(&clean, FindingCode::FutureDates),
            Some(&NotEvaluatedReason::Sampled)
        );
        assert_eq!(
            skipped(&clean, FindingCode::TemporalOrderViolations),
            Some(&NotEvaluatedReason::Sampled)
        );
        // Counted over every row, so its zero is a clean result.
        assert_eq!(skipped(&clean, FindingCode::DuplicateRows), None);

        // A witnessed future date stays witnessed, sample or not.
        let witnessed = quality_report(3, sampled()).findings();
        assert!(codes(&witnessed).contains(&(FindingCode::FutureDates, None)));
        assert_eq!(skipped(&witnessed, FindingCode::FutureDates), None);
    }

    #[test]
    fn unrecorded_coverage_does_not_read_as_clean() {
        let result = quality_report(0, MetricConfidence::Unrecorded).findings();
        for code in [
            FindingCode::DuplicateRows,
            FindingCode::FutureDates,
            FindingCode::TemporalOrderViolations,
        ] {
            assert_eq!(
                skipped(&result, code),
                Some(&NotEvaluatedReason::Unrecorded),
                "{code}"
            );
        }
    }

    #[test]
    fn a_report_from_before_ragged_counting_does_not_read_as_clean() {
        let mut legacy = report(vec![], ExecutionMetadata::new(5, 0, 10));
        legacy.schema_version = 0;
        assert_eq!(
            skipped(&legacy.findings(), FindingCode::RaggedRows),
            Some(&NotEvaluatedReason::Unrecorded)
        );

        let current = report(vec![], ExecutionMetadata::new(5, 0, 10)).findings();
        assert_eq!(skipped(&current, FindingCode::RaggedRows), None);
    }

    #[test]
    fn thresholds_apply_at_the_precision_they_are_reported_at() {
        // 20.004 is applied as 20.00, so a 20.00% column is reported and the
        // evidence names the threshold that was compared.
        let result = FindingPolicy::new()
            .null_heavy_percentage(20.004)
            .evaluate(&report(
                vec![column("fifth", 5, 1)],
                ExecutionMetadata::new(5, 1, 10),
            ))
            .expect("in range");
        let finding = result
            .findings
            .iter()
            .find(|finding| finding.code == FindingCode::NullHeavy)
            .expect("reported at the applied threshold");
        assert_eq!(
            finding.evidence["threshold"],
            EvidenceValue::Percentage(20.0)
        );

        // A threshold that rounds to zero would report every column.
        for value in [1e-9, 0.004] {
            assert!(
                FindingPolicy::new()
                    .null_heavy_percentage(value)
                    .validate()
                    .is_err(),
                "{value} was accepted"
            );
        }
        assert!(
            FindingPolicy::new()
                .null_heavy_percentage(0.005)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn a_nested_column_is_assessed_by_its_counts_only() {
        // Containers carry counts and nothing else (#637); the value-level
        // rules say they did not look rather than reporting a clean column.
        let mut nested = column("payload", 4, 4);
        nested.data_type = DataType::Nested;
        let result = report(vec![nested], ExecutionMetadata::new(4, 1, 10)).findings();

        assert_eq!(
            codes(&result),
            vec![(FindingCode::AllNull, Some("payload"))]
        );
        for code in [
            FindingCode::ConstantColumn,
            FindingCode::MixedTypes,
            FindingCode::SensitivePattern,
        ] {
            assert!(
                result.not_evaluated.contains(&UnevaluatedRule {
                    code,
                    reason: NotEvaluatedReason::NotComputed,
                    columns: vec!["payload".to_string()],
                }),
                "{code} was not listed: {:?}",
                result.not_evaluated
            );
        }
    }

    #[test]
    fn a_sampled_scan_names_its_reason_and_truncation_wins() {
        let sampled = report(
            vec![],
            ExecutionMetadata::new(100, 0, 10).with_sampling(0.1),
        );
        let both = report(
            vec![],
            ExecutionMetadata::new(100, 0, 10)
                .with_sampling(0.1)
                .with_truncation(TruncationReason::MaxRows(100)),
        );

        for (report, expected) in [(sampled, "sampled"), (both, "truncated")] {
            let finding = report
                .findings()
                .findings
                .into_iter()
                .find(|finding| finding.code == FindingCode::PartialScan)
                .expect("a partial scan is disclosed");
            assert_eq!(
                finding.evidence["reason"],
                EvidenceValue::Text(expected.to_string())
            );
        }
    }

    #[test]
    fn warnings_sort_before_info_then_by_code_then_column_position() {
        let mut constant = column("z_constant", 5, 0);
        constant.unique_count = Some(1);
        let mut mixed = column("a_mixed", 5, 0);
        mixed.type_homogeneity = Some(TypeHomogeneity {
            numeric: 3,
            date: 0,
            boolean: 0,
            text: 2,
        });
        let result = report(
            vec![
                constant,
                column("sparse", 5, 4),
                mixed,
                column("empty", 5, 5),
            ],
            ExecutionMetadata::new(5, 4, 10).with_ragged_row_count(1),
        )
        .findings();

        assert_eq!(
            codes(&result),
            vec![
                (FindingCode::AllNull, Some("empty")),
                (FindingCode::MixedTypes, Some("a_mixed")),
                (FindingCode::NullHeavy, Some("sparse")),
                (FindingCode::RaggedRows, None),
                (FindingCode::ConstantColumn, Some("z_constant")),
            ]
        );
        assert!(
            result
                .findings
                .iter()
                .all(|finding| finding.severity == finding.code.severity())
        );
    }
}
