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
use dataprof_core::{
    DataSource, DataType, DelimiterDetection, FileFormat, KeyColumnSource, PatternCategory,
    TypeHomogeneity,
};

use crate::profile_report::ProfileReport;
use crate::quality_gate::{
    Evidence, EvidenceGap, Provenance, quality_status_name, sampled_component_evidence,
};

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
    /// Delimiter detection found a second candidate that read the sampled CSV
    /// records as well as the one it chose.
    DelimiterAmbiguous,
    /// The source holds exact duplicate rows.
    DuplicateRows,
    /// Date values lie after the time the report was produced.
    FutureDates,
    /// No delimiter detection tries splits the CSV header, so the source was
    /// read as one column, while a character outside them splits the header
    /// and every sampled record alike.
    HeaderNotSplit,
    /// The declared key column holds a non-null value more than once.
    KeyDuplicates,
    /// At least half of a column's text values are numbers written with a
    /// decimal comma or digit-group separators, which no numeric statistic
    /// includes.
    LocaleNumbers,
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
    /// A CSV source ended inside a quoted field.
    UnterminatedQuote,
}

impl FindingCode {
    /// Every code, in declared order. A new code is added here too: parsing
    /// a code name, and so `QualityPolicy::forbid_findings` from a binding,
    /// reads this list.
    pub const ALL: [Self; 16] = [
        Self::AllNull,
        Self::ConstantColumn,
        Self::DelimiterAmbiguous,
        Self::DuplicateRows,
        Self::FutureDates,
        Self::HeaderNotSplit,
        Self::KeyDuplicates,
        Self::LocaleNumbers,
        Self::MixedTypes,
        Self::NullHeavy,
        Self::PartialScan,
        Self::RaggedRows,
        Self::RecordsSkipped,
        Self::SensitivePattern,
        Self::TemporalOrderViolations,
        Self::UnterminatedQuote,
    ];

    /// The severity every finding with this code carries.
    pub fn severity(self) -> Severity {
        match self {
            Self::AllNull
            | Self::DelimiterAmbiguous
            | Self::DuplicateRows
            | Self::FutureDates
            | Self::HeaderNotSplit
            | Self::KeyDuplicates
            | Self::LocaleNumbers
            | Self::MixedTypes
            | Self::NullHeavy
            | Self::RaggedRows
            | Self::RecordsSkipped
            | Self::TemporalOrderViolations
            | Self::UnterminatedQuote => Severity::Warning,
            Self::ConstantColumn | Self::PartialScan | Self::SensitivePattern => Severity::Info,
        }
    }

    /// A fixed sentence naming what the rule observed.
    ///
    /// Deliberately free of numbers, as the gate's messages are: the evidence
    /// carries those, at the precision it serializes with.
    fn summary(self) -> &'static str {
        match self {
            Self::AllNull => "every value in this column is null",
            Self::ConstantColumn => "every non-null value in this column is the same",
            Self::DelimiterAmbiguous => {
                "another delimiter reads the sampled CSV records as well as the one detection \
                 chose, so the columns may be split on the wrong one; set csv_delimiter to choose"
            }
            Self::DuplicateRows => "the source contains exact duplicate rows",
            Self::FutureDates => "some date values lie in the future",
            Self::HeaderNotSplit => {
                "no delimiter detection tries (comma, semicolon, tab, pipe) splits the CSV \
                 header, so the source was read as one column, but another character splits the \
                 header and every sampled record alike; set csv_delimiter to read it as a table"
            }
            Self::KeyDuplicates => {
                "some non-null values of the declared key column occur more than once"
            }
            Self::LocaleNumbers => {
                "at least half of the text values in this column are numbers written with a \
                 decimal comma or digit grouping, so no numeric statistic includes them"
            }
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
            Self::UnterminatedQuote => {
                "the source ends inside a quoted field, so its last record may hold several rows"
            }
        }
    }
}

impl fmt::Display for FindingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::AllNull => "all_null",
            Self::ConstantColumn => "constant_column",
            Self::DelimiterAmbiguous => "delimiter_ambiguous",
            Self::DuplicateRows => "duplicate_rows",
            Self::FutureDates => "future_dates",
            Self::HeaderNotSplit => "header_not_split",
            Self::KeyDuplicates => "key_duplicates",
            Self::LocaleNumbers => "locale_numbers",
            Self::MixedTypes => "mixed_types",
            Self::NullHeavy => "null_heavy",
            Self::PartialScan => "partial_scan",
            Self::RaggedRows => "ragged_rows",
            Self::RecordsSkipped => "records_skipped",
            Self::SensitivePattern => "sensitive_pattern",
            Self::TemporalOrderViolations => "temporal_order_violations",
            Self::UnterminatedQuote => "unterminated_quote",
        };
        write!(f, "{name}")
    }
}

impl std::str::FromStr for FindingCode {
    type Err = String;

    /// Parse a code from its serialized name, such as `locale_numbers`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|code| code.to_string() == s)
            .ok_or_else(|| {
                let valid: Vec<String> = Self::ALL.iter().map(ToString::to_string).collect();
                format!(
                    "Unknown finding code: {s}. Valid codes: {}",
                    valid.join(", ")
                )
            })
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
    #[non_exhaustive]
    QualityUnavailable {
        /// The report's `quality_status` state, verbatim.
        quality_status: String,
    },
    /// Quality was computed, but the dimension the rule reads had nothing to
    /// assess in this run.
    NotAssessed,
    /// The number the rule reads is an estimate, which witnesses nothing: an
    /// estimated duplicate or distinct count is not evidence that a duplicate
    /// exists.
    Estimated,
    /// The count the rule reads is zero, but it came from the retained quality
    /// sample rather than every scanned row, so it rules nothing out for the
    /// rows the sample left behind. A nonzero count is still reported.
    Sampled,
    /// The report does not record what the rule needs to tell a zero from an
    /// unmeasured value: a document written before dataprof recorded it
    /// (ragged rows before 0.10, quality sample coverage before 0.12, how the
    /// key column was chosen before 0.13).
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

/// The facts of a report the findings rules read, and nothing else (#909).
///
/// [`FindingPolicy::evaluate`] builds one from a [`ProfileReport`]. A report a
/// binding loaded from a flat summary has no `ProfileReport` behind it: the
/// binding writes these facts from the summary as JSON and reads them with
/// [`FindingsInput::from_json`], so both kinds of report go through one set
/// of rules. A count the summary does not carry reads as zero, as the rules
/// read an absent count.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[non_exhaustive]
pub struct FindingsInput {
    /// 0 for a document written before 0.10, which did not count ragged rows.
    schema_version: u32,
    rows_processed: usize,
    source_exhausted: bool,
    /// Whether the report records a truncation reason.
    truncated: bool,
    sampling_applied: bool,
    ragged_row_count: usize,
    unterminated_quote: bool,
    error_count: usize,
    /// What delimiter detection saw, when the report records that it ran.
    /// A flat summary records no source metadata, so it reads as absent.
    #[serde(default)]
    delimiter_detection: Option<DelimiterDetection>,
    /// A CSV file or bytes source whose report predates the CSV source
    /// metadata (#1100), so the delimiter rules cannot tell a clear choice
    /// from an unrecorded one.
    #[serde(default)]
    csv_source_unrecorded: bool,
    /// How the report chose the column `key_uniqueness` measured. Absent for
    /// a report written before 0.13, whose key may be a guess from column
    /// names (#1065).
    #[serde(default)]
    key_column_source: Option<KeyColumnSource>,
    /// The report's `quality_status` state, read when `quality` is absent.
    quality_status: String,
    quality: Option<QualityFacts>,
    columns: Vec<ColumnFacts>,
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
struct QualityFacts {
    uniqueness: Option<UniquenessFacts>,
    timeliness: Option<TimelinessFacts>,
    /// The sampled component labels, `None` when the report did not record
    /// them.
    sampled_dimensions: Option<Vec<String>>,
    /// Components with signal that neither recorded list names. Empty for a
    /// flat summary written before 0.14, which records no exact list (#1027).
    unlisted_components: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
struct UniquenessFacts {
    #[serde(default)]
    rows_checked: usize,
    #[serde(default)]
    duplicate_rows: usize,
    #[serde(default)]
    duplicate_rows_approximate: bool,
    /// The column `key_uniqueness` measured, absent when no key was measured.
    #[serde(default)]
    key_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
struct TimelinessFacts {
    #[serde(default)]
    date_values_checked: usize,
    #[serde(default)]
    future_dates_count: usize,
    #[serde(default)]
    temporal_pairs_checked: usize,
    #[serde(default)]
    temporal_violations: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
struct ColumnFacts {
    name: String,
    /// Typed as an identifier, which `mixed_types` and `locale_numbers` exempt.
    identifier: bool,
    total_count: usize,
    null_count: usize,
    unique_count: Option<usize>,
    /// Whether `unique_count` is an estimate; absent when not recorded.
    #[serde(default)]
    unique_count_is_approximate: Option<bool>,
    type_homogeneity: Option<TypeHomogeneity>,
    locale_number_count: Option<usize>,
    patterns: Option<Vec<PatternFacts>>,
}

#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
struct PatternFacts {
    name: String,
    /// The category as the report names it; a summary from a later release
    /// may name one this build does not know.
    category: String,
    confidence: f64,
    match_percentage: f64,
}

impl FindingsInput {
    /// Read the facts a binding wrote for a report it holds no
    /// [`ProfileReport`] for.
    pub fn from_json(text: &str) -> serde_json::Result<Self> {
        serde_json::from_str(text)
    }

    /// Whether the rule for `code` read some column's retained value sample
    /// rather than all of its values.
    ///
    /// `mixed_types`, `locale_numbers` and `sensitive_pattern` read a
    /// column's lexical forms, locale numbers and patterns, which the
    /// profiler takes from the same retained sample. A classified count short
    /// of the column's non-null count says the sample did not hold every
    /// value, even on a scan that read the whole source. Columns a rule
    /// exempts (identifiers, for the first two) are not counted. Every other
    /// code reads exact counters, or records its sampling itself.
    pub(crate) fn reads_a_partial_sample(&self, code: FindingCode) -> bool {
        let exempts_identifiers = match code {
            FindingCode::MixedTypes | FindingCode::LocaleNumbers => true,
            FindingCode::SensitivePattern => false,
            _ => return false,
        };
        self.columns
            .iter()
            .filter(|column| !(exempts_identifiers && column.identifier))
            .any(|column| {
                let non_null = column.total_count.saturating_sub(column.null_count);
                column
                    .type_homogeneity
                    .as_ref()
                    .is_some_and(|forms| forms.classified_count() < non_null)
            })
    }
}

impl From<&ProfileReport> for FindingsInput {
    fn from(report: &ProfileReport) -> Self {
        let execution = &report.execution;
        Self {
            schema_version: report.schema_version,
            rows_processed: execution.rows_processed,
            source_exhausted: execution.source_exhausted,
            truncated: execution.truncation_reason.is_some(),
            sampling_applied: execution.sampling_applied,
            ragged_row_count: execution.ragged_row_count,
            unterminated_quote: execution.unterminated_quote == Some(true),
            error_count: execution.error_count,
            delimiter_detection: report
                .data_source
                .csv_metadata()
                .and_then(|metadata| metadata.delimiter_detection.clone()),
            csv_source_unrecorded: csv_source_unrecorded(&report.data_source),
            key_column_source: report
                .metric_semantics
                .as_ref()
                .and_then(|semantics| semantics.key_column_source),
            quality_status: quality_status_name(&report.quality_status).to_string(),
            quality: report.quality.as_ref().map(|quality| QualityFacts {
                uniqueness: quality
                    .metrics
                    .uniqueness
                    .as_ref()
                    .map(|uniqueness| UniquenessFacts {
                        rows_checked: uniqueness.rows_checked,
                        duplicate_rows: uniqueness.duplicate_rows,
                        duplicate_rows_approximate: uniqueness.duplicate_rows_approximate,
                        key_column: uniqueness.key_column.clone(),
                    }),
                timeliness: quality
                    .metrics
                    .timeliness
                    .as_ref()
                    .map(|timeliness| TimelinessFacts {
                        date_values_checked: timeliness.date_values_checked,
                        future_dates_count: timeliness.future_dates_count,
                        temporal_pairs_checked: timeliness.temporal_pairs_checked,
                        temporal_violations: timeliness.temporal_violations,
                    }),
                sampled_dimensions: quality.sampled_dimensions(),
                unlisted_components: quality.unlisted_components(),
            }),
            columns: report
                .column_profiles
                .iter()
                .map(|column| ColumnFacts {
                    name: column.name.clone(),
                    identifier: column.data_type == DataType::Identifier,
                    total_count: column.total_count,
                    null_count: column.null_count,
                    unique_count: column.unique_count,
                    unique_count_is_approximate: column.unique_count_is_approximate,
                    type_homogeneity: column.type_homogeneity,
                    locale_number_count: column.locale_number_count,
                    patterns: column.patterns.as_ref().map(|patterns| {
                        patterns
                            .iter()
                            .map(|pattern| PatternFacts {
                                name: pattern.name.clone(),
                                category: pattern.category.to_string(),
                                confidence: pattern.confidence,
                                match_percentage: pattern.match_percentage,
                            })
                            .collect()
                    }),
                })
                .collect(),
        }
    }
}

/// A finding threshold that cannot be applied as written.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FindingPolicyError {
    /// A threshold fell outside `(0, 100]` at the 2dp precision it is applied
    /// at, or was not a finite number. Percentages are on the report's 0..100
    /// scale, not 0..1 ratios, and a zero threshold would report every column.
    #[non_exhaustive]
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
#[non_exhaustive]
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
        self.evaluate_input(&FindingsInput::from(report))
    }

    /// [`evaluate`](Self::evaluate) over the facts of a report, for a report
    /// a binding holds no [`ProfileReport`] for.
    pub fn evaluate_input(
        &self,
        input: &FindingsInput,
    ) -> Result<FindingsResult, FindingPolicyError> {
        self.validate()?;
        Ok(self.collect(input))
    }

    /// The thresholds as applied: at the precision a percentage is compared
    /// and reported at.
    fn null_heavy_threshold(&self) -> f64 {
        rounded_2(self.null_heavy_percentage)
    }

    fn mixed_types_threshold(&self) -> f64 {
        rounded_2(self.mixed_types_percentage)
    }

    pub(crate) fn collect(&self, input: &FindingsInput) -> FindingsResult {
        let mut collector = Collector::default();
        scan_findings(input, &mut collector);
        quality_findings(input, &mut collector);
        for (index, column) in input.columns.iter().enumerate() {
            self.column_findings(index, column, &mut collector);
        }
        collector.finish()
    }

    fn column_findings(&self, index: usize, column: &ColumnFacts, out: &mut Collector) {
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

        locale_numbers(index, column, out);
        self.mixed_types(index, column, non_null, out);
        sensitive_patterns(index, column, non_null, out);
    }

    fn mixed_types(
        &self,
        index: usize,
        column: &ColumnFacts,
        non_null: usize,
        out: &mut Collector,
    ) {
        let name = column.name.as_str();
        // Mixing forms ("A1", "123") is what an identifier scheme does, not a
        // defect: the same exemption the consistency dimension makes.
        if column.identifier {
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

/// Report a column at least half of whose text values are numbers written in
/// a locale format (#433).
///
/// Measured against the text values rather than the whole column, so it
/// covers both ways these numbers go missing: a column of `1.234,56` typed
/// `string` with no numeric statistics, and a numeric column whose
/// decimal-comma values were counted as invalid. At least half, so that a free
/// text column holding a stray `1,5` is not reported.
fn locale_numbers(index: usize, column: &ColumnFacts, out: &mut Collector) {
    let name = column.name.as_str();
    // The exemption `mixed_types` makes: `1.234.567` in an identifier
    // scheme is a code, not an amount.
    if column.identifier {
        return;
    }
    let Some(homogeneity) = column.type_homogeneity.as_ref() else {
        out.skip(
            FindingCode::LocaleNumbers,
            NotEvaluatedReason::NotComputed,
            name,
        );
        return;
    };
    // Classified, but by a release that did not count these yet.
    let Some(locale_numbers) = column.locale_number_count else {
        out.skip(
            FindingCode::LocaleNumbers,
            NotEvaluatedReason::Unrecorded,
            name,
        );
        return;
    };
    let classified = homogeneity.classified_count();
    if classified == 0 {
        out.skip(
            FindingCode::LocaleNumbers,
            NotEvaluatedReason::NoValues,
            name,
        );
        return;
    }
    let text = homogeneity.text;
    if locale_numbers == 0 || locale_numbers * 2 < text {
        return;
    }
    out.column(
        FindingCode::LocaleNumbers,
        index,
        name,
        [
            ("locale_number_count", EvidenceValue::Count(locale_numbers)),
            ("text_count", EvidenceValue::Count(text)),
            // Counted over the values the profiler retained, as in
            // `mixed_types`.
            ("classified_count", EvidenceValue::Count(classified)),
            (
                "non_null_count",
                EvidenceValue::Count(column.total_count.saturating_sub(column.null_count)),
            ),
        ],
    );
}

impl ProfileReport {
    /// The findings this report supports under the default thresholds.
    ///
    /// Shorthand for `FindingPolicy::default().evaluate(self)`, which cannot
    /// fail because the defaults are in range.
    pub fn findings(&self) -> FindingsResult {
        FindingPolicy::default().collect(&FindingsInput::from(self))
    }
}

fn scan_findings(input: &FindingsInput, out: &mut Collector) {
    let partial = if !input.source_exhausted || input.truncated {
        Some("truncated")
    } else if input.sampling_applied {
        Some("sampled")
    } else {
        None
    };
    if let Some(reason) = partial {
        out.report(
            FindingCode::PartialScan,
            [
                ("reason", EvidenceValue::Text(reason.to_string())),
                ("rows_processed", EvidenceValue::Count(input.rows_processed)),
            ],
        );
    }
    if input.schema_version == 0 {
        // Ragged rows were first counted by the release that introduced
        // schema versioning (0.10, #452). An older document reads back with a
        // defaulted zero that was never measured.
        out.skip_report(FindingCode::RaggedRows, NotEvaluatedReason::Unrecorded);
    } else if input.ragged_row_count > 0 {
        out.report(
            FindingCode::RaggedRows,
            [
                (
                    "ragged_row_count",
                    EvidenceValue::Count(input.ragged_row_count),
                ),
                ("rows_processed", EvidenceValue::Count(input.rows_processed)),
            ],
        );
    }
    if input.unterminated_quote {
        out.report(
            FindingCode::UnterminatedQuote,
            [("rows_processed", EvidenceValue::Count(input.rows_processed))],
        );
    }
    if input.error_count > 0 {
        out.report(
            FindingCode::RecordsSkipped,
            [("error_count", EvidenceValue::Count(input.error_count))],
        );
    }
    if input.csv_source_unrecorded {
        out.skip_report(
            FindingCode::DelimiterAmbiguous,
            NotEvaluatedReason::Unrecorded,
        );
        out.skip_report(FindingCode::HeaderNotSplit, NotEvaluatedReason::Unrecorded);
    }
    // A delimiter the caller set records no detection, so neither rule asks
    // about a choice nobody made.
    if let Some(detection) = &input.delimiter_detection {
        if let Some(runner_up) = &detection.runner_up {
            out.report(
                FindingCode::DelimiterAmbiguous,
                [
                    (
                        "delimiter",
                        EvidenceValue::Text(detection.delimiter.clone()),
                    ),
                    ("runner_up", EvidenceValue::Text(runner_up.clone())),
                ],
            );
        }
        // A one-column source is often intended, so an unsplit header alone
        // is no finding: only a character outside the candidates that splits
        // the header and every sampled record alike makes the reading doubtful.
        if let Some(unlisted) = &detection.unlisted_delimiter {
            out.report(
                FindingCode::HeaderNotSplit,
                [("unlisted_delimiter", EvidenceValue::Text(unlisted.clone()))],
            );
        }
    }
}

/// Whether `source` is a CSV source that records no CSV source metadata: a
/// report written before 0.14 recorded it. A stream written before 0.14
/// records no format either (#1042), so it cannot be told apart.
fn csv_source_unrecorded(source: &DataSource) -> bool {
    match source {
        DataSource::File { format, .. }
        | DataSource::Bytes { format, .. }
        | DataSource::Stream {
            format: Some(format),
            ..
        } => matches!(format, FileFormat::Csv) && source.csv_metadata().is_none(),
        _ => false,
    }
}

fn quality_findings(input: &FindingsInput, out: &mut Collector) {
    const QUALITY_RULES: [FindingCode; 4] = [
        FindingCode::DuplicateRows,
        FindingCode::FutureDates,
        FindingCode::KeyDuplicates,
        FindingCode::TemporalOrderViolations,
    ];
    let Some(quality) = input.quality.as_ref() else {
        for code in QUALITY_RULES {
            out.skip_report(
                code,
                NotEvaluatedReason::QualityUnavailable {
                    quality_status: input.quality_status.clone(),
                },
            );
        }
        return;
    };

    match quality
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

    key_duplicates(input, quality, out);

    let timeliness = quality.timeliness.as_ref();
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

/// Report repeated values in the declared key column (#1068).
///
/// Reads the key the uniqueness dimension measured, which is only ever a
/// declared one (#1065), and the key column's own distinct count over its
/// non-null values: a null key is completeness's finding, not a repeat. Whole
/// duplicate rows are `duplicate_rows`; this finds the same key on rows that
/// differ elsewhere, which no row comparison sees.
///
/// The rule needs an exact distinct count. An estimate witnesses no repeat,
/// and a report that does not record whether its count was exact, or how its
/// key was chosen, is listed rather than judged.
fn key_duplicates(input: &FindingsInput, quality: &QualityFacts, out: &mut Collector) {
    const CODE: FindingCode = FindingCode::KeyDuplicates;
    let Some(key) = quality
        .uniqueness
        .as_ref()
        .and_then(|uniqueness| uniqueness.key_column.as_deref())
    else {
        out.skip_report(CODE, NotEvaluatedReason::NotAssessed);
        return;
    };
    // Before 0.13 an undeclared key was the first id-like column name, so
    // the column may be a foreign key that repeats by design.
    let Some(source) = input.key_column_source else {
        out.skip(CODE, NotEvaluatedReason::Unrecorded, key);
        return;
    };
    let Some((index, column)) = input
        .columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.name == key)
    else {
        out.skip(CODE, NotEvaluatedReason::NotComputed, key);
        return;
    };
    let Some(unique_count) = column.unique_count else {
        out.skip(CODE, NotEvaluatedReason::NotComputed, key);
        return;
    };
    let values = column.total_count.saturating_sub(column.null_count);
    if values == 0 {
        out.skip(CODE, NotEvaluatedReason::NoValues, key);
        return;
    }
    let coverage = sampled_component_evidence(
        quality.sampled_dimensions.as_deref(),
        &quality.unlisted_components,
        &[],
        Provenance::Component("key_uniqueness"),
    );
    // The key component is filed as sampled exactly when the key's distinct
    // count is an estimate.
    let estimated = column.unique_count_is_approximate == Some(true)
        || matches!(
            coverage,
            Evidence::Incomplete {
                reason: EvidenceGap::QualitySampled
            }
        );
    if estimated {
        out.skip(CODE, NotEvaluatedReason::Estimated, key);
        return;
    }
    let exact =
        column.unique_count_is_approximate == Some(false) || matches!(coverage, Evidence::Complete);
    if !exact {
        out.skip(CODE, NotEvaluatedReason::Unrecorded, key);
        return;
    }
    let repeated = values.saturating_sub(unique_count);
    if repeated == 0 {
        return;
    }
    let source = match source {
        KeyColumnSource::Declared => "declared",
    };
    out.column(
        CODE,
        index,
        key,
        [
            ("key_values_checked", EvidenceValue::Count(values)),
            ("repeated_values", EvidenceValue::Count(repeated)),
            ("key_column_source", EvidenceValue::Text(source.to_string())),
        ],
    );
}

/// A zero count is a clean result only when it covers every scanned row.
///
/// A count from the retained quality sample, or from a report that does not
/// record where its counts came from, rules nothing out for the rows it did
/// not see, so the rule is listed rather than read as clean. A nonzero count
/// never reaches here: rows already witnessed stay witnessed. Coverage is
/// resolved by the quality gate's rule, so a finding and a check never
/// disagree about one component.
fn skip_unless_complete(
    out: &mut Collector,
    code: FindingCode,
    quality: &QualityFacts,
    component: &'static str,
) {
    // A component's coverage does not depend on the dimensions the overall
    // score weighs, so none are passed.
    let coverage = sampled_component_evidence(
        quality.sampled_dimensions.as_deref(),
        &quality.unlisted_components,
        &[],
        Provenance::Component(component),
    );
    match coverage {
        Evidence::Complete => {}
        Evidence::Incomplete {
            reason: EvidenceGap::QualitySampled,
        } => out.skip_report(code, NotEvaluatedReason::Sampled),
        Evidence::Incomplete { .. } => out.skip_report(code, NotEvaluatedReason::Unrecorded),
    }
}

/// Whether a detected pattern names personal or financial data.
fn is_sensitive(category: &str, name: &str) -> bool {
    [PatternCategory::Contact, PatternCategory::Financial]
        .iter()
        .any(|sensitive| sensitive.to_string() == category)
        || PERSONAL_IDENTIFIER_PATTERNS.contains(&name)
}

fn sensitive_patterns(index: usize, column: &ColumnFacts, non_null: usize, out: &mut Collector) {
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
                    ("category", EvidenceValue::Text(pattern.category.clone())),
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
/// compares equal to one rebuilt from its own document.
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
        ColumnProfile, ColumnStats, CsvSourceMetadata, DataSource, ExecutionMetadata, FileFormat,
        Pattern, TruncationReason, TypeHomogeneity,
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
                key_values_checked: None,
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
                key_values_checked: None,
                duplicate_rows_approximate: false,
            }),
            timeliness: Some(TimelinessMetrics {
                future_dates_count: future_dates,
                stale_data_ratio: 0.0,
                temporal_violations: 0,
                invalid_date_values: 0,
                date_values_checked: 10_000,
                temporal_pairs_checked: 10_000,
                clock: None,
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

    /// A CSV report from before the CSV source metadata cannot say whether
    /// detection was clear, so both delimiter rules are listed as unrecorded
    /// rather than passed. A recorded clear detection, a configured delimiter
    /// and a non-CSV source list neither (#1100).
    #[test]
    fn a_csv_report_without_source_metadata_does_not_read_as_a_clear_delimiter() {
        let rules = [FindingCode::DelimiterAmbiguous, FindingCode::HeaderNotSplit];
        let with_source = |data_source: DataSource| {
            ProfileReport::new(data_source, vec![], ExecutionMetadata::new(5, 0, 10), None)
                .findings()
        };
        let mut wrong = Vec::new();

        let legacy = with_source(source());
        for code in rules {
            if skipped(&legacy, code) != Some(&NotEvaluatedReason::Unrecorded) {
                wrong.push(format!("legacy csv: {code} not listed as unrecorded"));
            }
        }

        let json = DataSource::File {
            path: "drop.json".to_string(),
            format: FileFormat::Json,
            size_bytes: 256,
            modified_at: None,
            parquet_metadata: None,
            csv_metadata: None,
        };
        let recorded = [
            (
                "clear detection",
                source().with_csv_metadata(CsvSourceMetadata::detected(DelimiterDetection::new(
                    b',', 3, None,
                ))),
            ),
            (
                "configured",
                source().with_csv_metadata(CsvSourceMetadata::configured(b';')),
            ),
            ("json", json),
        ];
        for (case, data_source) in recorded {
            let result = with_source(data_source);
            for code in rules {
                if skipped(&result, code).is_some()
                    || result.findings.iter().any(|finding| finding.code == code)
                {
                    wrong.push(format!("{case}: {code} skipped or reported"));
                }
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// A classified column of `numeric` numbers and `text` text values,
    /// `locale` of them locale-formatted numbers.
    fn classified(name: &str, numeric: usize, text: usize, locale: Option<usize>) -> ColumnProfile {
        let mut profile = column(name, numeric + text, 0);
        profile.type_homogeneity = Some(TypeHomogeneity {
            numeric,
            date: 0,
            boolean: 0,
            text,
        });
        profile.locale_number_count = locale;
        profile
    }

    fn locale_number_columns(result: &FindingsResult) -> Vec<&str> {
        result
            .findings
            .iter()
            .filter(|finding| finding.code == FindingCode::LocaleNumbers)
            .filter_map(|finding| finding.column.as_deref())
            .collect()
    }

    #[test]
    fn locale_numbers_are_reported_from_half_of_the_text_values() {
        let result = report(
            vec![
                classified("half", 0, 4, Some(2)),
                classified("under_half", 0, 5, Some(2)),
                // A float column whose decimal-comma values were invalid.
                classified("float_with_commas", 9, 1, Some(1)),
                classified("clean", 0, 4, Some(0)),
            ],
            ExecutionMetadata::new(10, 0, 10),
        )
        .findings();

        assert_eq!(
            locale_number_columns(&result),
            ["half", "float_with_commas"]
        );
        let half = &result.findings[0];
        assert_eq!(half.code, FindingCode::LocaleNumbers);
        assert_eq!(
            half.evidence,
            BTreeMap::from([
                ("classified_count".to_string(), EvidenceValue::Count(4)),
                ("locale_number_count".to_string(), EvidenceValue::Count(2)),
                ("non_null_count".to_string(), EvidenceValue::Count(4)),
                ("text_count".to_string(), EvidenceValue::Count(4)),
            ])
        );
        assert_eq!(skipped(&result, FindingCode::LocaleNumbers), None);
    }

    #[test]
    fn an_identifier_column_is_exempt_from_locale_numbers() {
        let mut codes = classified("codes", 0, 4, Some(4));
        codes.data_type = DataType::Identifier;

        let result = report(vec![codes], ExecutionMetadata::new(4, 0, 10)).findings();

        assert!(locale_number_columns(&result).is_empty());
        assert_eq!(skipped(&result, FindingCode::LocaleNumbers), None);
    }

    #[test]
    fn a_report_from_before_locale_number_counting_does_not_read_as_clean() {
        // Classified by a release that did not count locale numbers yet.
        let result = report(
            vec![classified("legacy", 0, 4, None)],
            ExecutionMetadata::new(4, 0, 10),
        )
        .findings();
        assert_eq!(
            skipped(&result, FindingCode::LocaleNumbers),
            Some(&NotEvaluatedReason::Unrecorded)
        );

        // Not classified at all: nothing was computed to count from.
        let result = report(
            vec![column("unclassified", 4, 0)],
            ExecutionMetadata::new(4, 0, 10),
        )
        .findings();
        assert_eq!(
            skipped(&result, FindingCode::LocaleNumbers),
            Some(&NotEvaluatedReason::NotComputed)
        );
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

    /// A report whose declared key `order_id` holds ten non-null values and
    /// one null, `unique` of them distinct.
    fn key_report(
        unique: usize,
        approximate: Option<bool>,
        confidence: MetricConfidence,
    ) -> ProfileReport {
        let values = 10;
        let mut key = column("order_id", values + 1, 1);
        key.data_type = DataType::Identifier;
        key.unique_count = Some(unique);
        key.unique_count_is_approximate = approximate;
        let metrics = QualityMetrics {
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 0,
                key_uniqueness: unique as f64 / values as f64 * 100.0,
                high_cardinality_warning: false,
                rows_checked: values + 1,
                key_column: Some("order_id".to_string()),
                key_values_checked: Some(values),
                duplicate_rows_approximate: false,
            }),
            ..QualityMetrics::default()
        };
        ProfileReport::new(
            source(),
            vec![column("amount", values + 1, 0), key],
            ExecutionMetadata::new(values + 1, 0, 10),
            Some(QualityAssessment::new(metrics, confidence)),
        )
    }

    fn key_sampled() -> MetricConfidence {
        MetricConfidence::Mixed {
            exact_dimensions: vec!["duplicate_rows".to_string()],
            sampled_dimensions: vec!["key_uniqueness".to_string()],
            sample_size: 10,
        }
    }

    /// What the key rule did: its finding's evidence, or its skip reason.
    fn key_outcome(result: &FindingsResult) -> String {
        let found: Vec<_> = result
            .findings
            .iter()
            .filter(|finding| finding.code == FindingCode::KeyDuplicates)
            .collect();
        match (
            found.as_slice(),
            skipped(result, FindingCode::KeyDuplicates),
        ) {
            ([finding], None) => format!(
                "found on {:?}: {}",
                finding.column,
                serde_json::to_string(&finding.evidence).unwrap()
            ),
            ([], Some(reason)) => format!("skipped: {}", serde_json::to_string(reason).unwrap()),
            ([], None) => "clean".to_string(),
            _ => format!("inconsistent: {found:?}"),
        }
    }

    #[test]
    fn a_repeated_declared_key_is_reported_only_from_an_exact_count() {
        let found = r#"found on Some("order_id"): {"key_column_source":"declared","key_values_checked":10,"repeated_values":2}"#;
        let skip = |reason: &str| format!(r#"skipped: {{"reason":"{reason}"}}"#);
        let mut legacy_key = key_report(8, Some(false), MetricConfidence::Exact);
        legacy_key
            .metric_semantics
            .as_mut()
            .expect("a current report records its semantics")
            .key_column_source = None;
        let mut no_key = key_report(8, Some(false), MetricConfidence::Exact);
        no_key
            .quality
            .as_mut()
            .and_then(|quality| quality.metrics.uniqueness.as_mut())
            .expect("uniqueness assessed")
            .key_column = None;
        let mut all_null_key = key_report(0, Some(false), MetricConfidence::Exact);
        all_null_key.column_profiles[1].null_count = 11;

        let cases = [
            (
                "exact count with repeats",
                key_report(8, Some(false), MetricConfidence::Exact),
                found.to_string(),
            ),
            (
                // One null and ten distinct values: a null is not a repeat.
                "exact count, distinct, one null",
                key_report(10, Some(false), MetricConfidence::Exact),
                "clean".to_string(),
            ),
            (
                "estimated distinct count",
                key_report(8, Some(true), MetricConfidence::Exact),
                skip("estimated"),
            ),
            (
                "key component filed as sampled",
                key_report(8, Some(false), key_sampled()),
                skip("estimated"),
            ),
            (
                "exactness recorded on the column only",
                key_report(8, Some(false), MetricConfidence::Unrecorded),
                found.to_string(),
            ),
            (
                "exactness recorded in the coverage only",
                key_report(8, None, MetricConfidence::Exact),
                found.to_string(),
            ),
            (
                "exactness recorded nowhere",
                key_report(8, None, MetricConfidence::Unrecorded),
                skip("unrecorded"),
            ),
            ("key chosen before 0.13", legacy_key, skip("unrecorded")),
            ("no key measured", no_key, skip("not_assessed")),
            ("key without values", all_null_key, skip("no_values")),
        ];
        let mut wrong = Vec::new();
        for (case, report, expected) in cases {
            let outcome = key_outcome(&report.findings());
            if outcome != expected {
                wrong.push(format!("{case}: {outcome}, expected {expected}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
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
