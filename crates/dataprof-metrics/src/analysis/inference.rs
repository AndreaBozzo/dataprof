use regex::Regex;
use std::collections::BTreeMap;
use std::sync::LazyLock;

use dataprof_core::{LexicalClass, TypeHomogeneity};

use crate::analysis::metrics::accuracy::{
    RULE_REGION_COUNT, classify_rule_region_f64, classify_rule_region_i64, classify_rule_region_u64,
};
use crate::analysis::metrics::consistency::has_encoding_issue;
use crate::analysis::metrics::precision::decimal_scale;
use crate::analysis::metrics::utils::extract_year;
use crate::types::DataType;

// Date grammar shared by type inference and the date patterns in `patterns.rs`,
// so a value is a date to both or to neither.
pub(crate) const DATE_ISO_PATTERN: &str = r"^\d{4}-\d{2}-\d{2}$";
pub(crate) const DATE_SLASH_PATTERN: &str = r"^\d{2}/\d{2}/\d{4}$";
pub(crate) const DATE_DASH_PATTERN: &str = r"^\d{2}-\d{2}-\d{4}$";
pub(crate) const DATE_YEAR_SLASH_PATTERN: &str = r"^\d{4}/\d{2}/\d{2}$";
pub(crate) const DATE_DOT_PATTERN: &str = r"^\d{2}\.\d{2}\.\d{4}$";
pub(crate) const ISO_DATETIME_PATTERN: &str =
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)?$";
/// [`ISO_DATETIME_PATTERN`] with a space for the `T`. Type inference only:
/// the `DateTime (ISO)` pattern has never matched the spaced form.
const SPACED_ISO_DATETIME_PATTERN: &str =
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)?$";

// Pre-compile regex patterns for better performance
// These patterns are compiled once at startup instead of on every column analysis
//
// NOTE: Some patterns (^\d{2}/\d{2}/\d{4}$) are ambiguous between DD/MM/YYYY and MM/DD/YYYY.
// The datetime parsing module assumes European format (DD/MM/YYYY) by default.
// See datetime.rs documentation for details on date format handling.
static DATE_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(DATE_ISO_PATTERN)
            .expect("BUG: Invalid hardcoded regex pattern for ISO 8601 date"),
        Regex::new(DATE_SLASH_PATTERN)
            .expect("BUG: Invalid hardcoded regex pattern for DD/MM/YYYY date"),
        Regex::new(DATE_DASH_PATTERN)
            .expect("BUG: Invalid hardcoded regex pattern for DD-MM-YYYY date"),
        Regex::new(DATE_YEAR_SLASH_PATTERN)
            .expect("BUG: Invalid hardcoded regex pattern for YYYY/MM/DD date"),
        Regex::new(DATE_DOT_PATTERN)
            .expect("BUG: Invalid hardcoded regex pattern for DD.MM.YYYY date"),
        // RFC 3339: the `T` form with an optional fractional part and an
        // optional `Z`/`±HH:MM` offset. The offset alternatives are exactly the
        // ones `chrono::DateTime::parse_from_rfc3339` accepts, so a value this
        // pattern calls a date is a value the parser can turn into an instant.
        //
        // The offset is range-bound, not `\d{2}:\d{2}`: chrono rejects `+24:00`
        // and `+00:60` as out of range, so a looser pattern would type the
        // column `Date` and then count every value as `invalid_date_values` —
        // the exact mismatch this pattern exists to avoid. The ISO 8601 basic
        // form (`+0200`) is absent for the same reason.
        Regex::new(ISO_DATETIME_PATTERN)
            .expect("BUG: Invalid hardcoded regex pattern for ISO datetime"),
        // The same form with a space for the `T`, which RFC 3339 permits and
        // `parse_from_rfc3339` accepts: Python's `str()` of a datetime, pandas
        // timestamps and SQL exports write it, with an offset when the value
        // is timezone-aware (#1335). Without one the parser reads it naive.
        Regex::new(SPACED_ISO_DATETIME_PATTERN)
            .expect("BUG: Invalid hardcoded regex pattern for spaced ISO datetime"),
        Regex::new(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$")
            .expect("BUG: Invalid hardcoded regex pattern for DD/MM/YYYY datetime"),
    ]
});

/// A number written with a decimal comma or with digit-group separators: the
/// forms locale-formatted exports carry and plain numeric parsing rejects.
///
/// Digits are ASCII on purpose: `\d` is Unicode-aware in this crate and would
/// accept digits no numeric parser here reads. Space-like grouping requires a
/// decimal part, because `333 123 456` is more often a phone number or a code
/// than an integer. The pattern alone also accepts `1.234`, which plain parsing
/// reads as a number; [`is_locale_number_token`] keeps only text values.
static LOCALE_NUMBER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"^[+-]?(?:",
        // 10,50 and 1,234: a decimal comma, or one comma-grouped thousand.
        r"[0-9]+,[0-9]+",
        // 1.234,56 and 1.234.567: dot grouping, optional comma decimals.
        r"|[0-9]{1,3}(?:\.[0-9]{3})+(?:,[0-9]+)?",
        // 1,234.56 and 1,234,567: comma grouping, optional dot decimals.
        r"|[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?",
        // 12,34,567.89: Indian grouping, pairs above the first thousand.
        r"|[0-9]{1,2}(?:,[0-9]{2})+,[0-9]{3}(?:\.[0-9]+)?",
        // 1'234.56: apostrophe grouping, either decimal mark.
        r"|[0-9]{1,3}(?:['\u{2019}][0-9]{3})+(?:[.,][0-9]+)?",
        // 1 234,56: space, no-break or narrow no-break space grouping.
        r"|[0-9]{1,3}(?:[ \u{A0}\u{202F}][0-9]{3})+[.,][0-9]+",
        r")$",
    ))
    .expect("BUG: Invalid hardcoded regex pattern for locale-formatted numbers")
});

/// Type a column of text values.
///
/// [`LexicalTally`] over `data`: the engines that stream a column count the
/// same tally one value at a time, so a column is typed the same way whether
/// its values arrive at once or in a stream.
pub fn infer_type(data: &[String]) -> DataType {
    LexicalTally::from_values(data).data_type()
}

/// Inline bins for common decimal scales (`0..16`), avoiding heap allocation
/// for nearly every column.
const INLINE_DECIMAL_SCALES: usize = 16;

/// Histogram of effective decimal scales observed on finite numeric tokens.
///
/// Scales of 16 and above are kept in a map with no cap, so the dominant
/// scale is exact and does not depend on merge order. Scientific notation
/// such as `0e-17`, `0e-18`, ... parses as a finite `0.0` with any scale, so
/// the map can hold one entry per distinct value at worst: no more than the
/// column's exact distinct set, and counted in [`LexicalTally::heap_bytes`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DecimalScaleHistogram {
    small: [usize; INLINE_DECIMAL_SCALES],
    overflow: BTreeMap<usize, usize>,
}

impl DecimalScaleHistogram {
    fn record(&mut self, scale: usize) {
        if scale < INLINE_DECIMAL_SCALES {
            self.small[scale] += 1;
        } else {
            *self.overflow.entry(scale).or_default() += 1;
        }
    }

    fn merge(&mut self, other: &Self) {
        for (dst, src) in self.small.iter_mut().zip(other.small.iter()) {
            *dst += *src;
        }
        for (&scale, &count) in &other.overflow {
            *self.overflow.entry(scale).or_default() += count;
        }
    }

    /// `(total_values, dominant_scale_count)` over finite numeric values.
    fn summary(&self) -> (usize, usize) {
        let mut total = 0;
        let mut dominant = 0usize;
        for &count in self.small.iter().chain(self.overflow.values()) {
            total += count;
            if count > dominant {
                dominant = count;
            }
        }
        (total, dominant)
    }

    fn heap_bytes(&self) -> usize {
        if self.overflow.is_empty() {
            0
        } else {
            self.overflow.len() * (std::mem::size_of::<usize>() * 2 + 16)
        }
    }
}

/// Per-column quality counters fed beside type inference over every scanned
/// non-null value (#788 steps 2–3).
///
/// Tracks the four `DATE_FORMAT_REGEXES` date formats, decimal separator usage,
/// encoding issues, the decimal scale histogram for precision, the six accuracy
/// rule-bound regions, and raw temporal-hint matches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ValueQualityCounters {
    /// Counts per `DATE_FORMAT_REGEXES` entry (`DATE_REGEXES[0..4]`):
    /// `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`, `YYYY/MM/DD`.
    date_formats: [usize; 4],
    /// Values containing both `.` and `,`.
    both_separators: usize,
    /// Values containing exactly one `.` and no `,`.
    single_dot: usize,
    /// Values containing exactly one `,` and no `.`.
    single_comma: usize,
    /// Values containing U+FFFD or a mojibake artifact.
    encoding_issues: usize,
    /// Effective decimal scales of finite numeric tokens.
    decimal_scales: DecimalScaleHistogram,
    /// Counts of rule-checkable numeric values across the six real-line regions
    /// partitioned by `{0, 100, 150, 1900, 2100}`. Their sum is also the
    /// `positive_columns` hint match count.
    rule_regions: [usize; RULE_REGION_COUNT],
    /// Non-null values whose original (untrimmed) text parses via
    /// [`extract_year`]: the `temporal_columns` / `temporal_pairs` hint match
    /// count and date-column parsed count.
    temporal_hint_matches: usize,
}

impl ValueQualityCounters {
    fn record_separators_and_encoding(&mut self, value: &str) {
        let mut dots = 0usize;
        let mut commas = 0usize;
        let mut non_ascii = false;
        for &byte in value.as_bytes() {
            match byte {
                b'.' => dots += 1,
                b',' => commas += 1,
                0x80.. => non_ascii = true,
                _ => {}
            }
        }
        if dots > 0 && commas > 0 {
            self.both_separators += 1;
        } else if dots == 1 {
            self.single_dot += 1;
        } else if commas == 1 {
            self.single_comma += 1;
        }
        if non_ascii && has_encoding_issue(value) {
            self.encoding_issues += 1;
        }
    }

    fn merge(&mut self, other: &Self) {
        for (dst, src) in self.date_formats.iter_mut().zip(other.date_formats.iter()) {
            *dst += *src;
        }
        self.both_separators += other.both_separators;
        self.single_dot += other.single_dot;
        self.single_comma += other.single_comma;
        self.encoding_issues += other.encoding_issues;
        self.decimal_scales.merge(&other.decimal_scales);
        for (dst, src) in self.rule_regions.iter_mut().zip(other.rule_regions.iter()) {
            *dst += *src;
        }
        self.temporal_hint_matches += other.temporal_hint_matches;
    }

    fn format_violations(&self) -> usize {
        let date_sum: usize = self.date_formats.iter().sum();
        let date_max = self.date_formats.iter().copied().max().unwrap_or(0);
        let mixed_dates = date_sum - date_max;

        let mixed_separators = if self.single_dot > 0 && self.single_comma > 0 {
            self.single_dot.min(self.single_comma)
        } else {
            0
        };

        mixed_dates + self.both_separators + mixed_separators
    }

    fn heap_bytes(&self) -> usize {
        self.decimal_scales.heap_bytes()
    }
}

/// The lexical facts about a column's non-null text values that type
/// inference, `type_homogeneity`, `locale_number_count`, consistency and
/// precision read, counted one value at a time (#788).
///
/// Every engine that reads a column as text feeds each value it reads to
/// [`Self::record`], so the type, shares, consistency and precision cover
/// every value read rather than a retained sample: one zero-padded code,
/// minority format, encoding issue or precision outlier past the sample gives
/// the same result from a file and from memory. Parts read separately combine
/// with [`Self::merge`] in any order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LexicalTally {
    /// Each value's [`LexicalClass`]: the `type_homogeneity` a profile reports.
    forms: TypeHomogeneity,
    /// Numeric values that are integers in the signed or unsigned 64-bit range.
    integers: usize,
    /// Numeric values written with a leading zero, such as `007` (#814).
    zero_padded: usize,
    /// Numeric values that are an eight-digit `YYYYMMDD` calendar date.
    compact_dates: usize,
    /// Date values in a form [`is_inferred_date_token`] types a column by.
    inferred_dates: usize,
    /// Text values that are locale-formatted numbers.
    locale_numbers: usize,
    /// Exact consistency and precision counters over the same values.
    quality: ValueQualityCounters,
}

impl LexicalTally {
    pub fn new() -> Self {
        Self::default()
    }

    /// The tally of every value in `values`.
    pub fn from_values<S: AsRef<str>>(values: &[S]) -> Self {
        let mut tally = Self::new();
        for value in values {
            tally.record(value.as_ref());
        }
        tally
    }

    /// Count one value. Surrounding whitespace is ignored, and a null-like
    /// value is not counted: it is absence, not a lexical form.
    pub fn record(&mut self, value: &str) {
        let trimmed = value.trim();
        if is_null_like_token(trimmed) {
            return;
        }
        let Classified {
            class,
            numeric_token,
            inferred_date,
            date_format,
        } = classify(trimmed);
        self.forms.record(class);
        match class {
            LexicalClass::Numeric => {
                match numeric_token {
                    NumericToken::IntegerI64(n) => {
                        self.integers += 1;
                        // Integer tokens are finite ASCII digits with optional sign
                        // (never `.`, `,`, or non-ASCII) and effective scale 0.
                        self.quality.decimal_scales.record(0);
                        self.quality.rule_regions[classify_rule_region_i64(n)] += 1;
                    }
                    NumericToken::IntegerU64 => {
                        self.integers += 1;
                        self.quality.decimal_scales.record(0);
                        self.quality.rule_regions[classify_rule_region_u64()] += 1;
                    }
                    NumericToken::Float(number) => {
                        // Valid `f64` tokens are strictly ASCII with at most one `.`
                        // and never `,`.
                        if trimmed.as_bytes().contains(&b'.') {
                            self.quality.single_dot += 1;
                        }
                        if number.is_finite()
                            && let Some(scale) = decimal_scale(trimmed)
                        {
                            self.quality.decimal_scales.record(scale);
                        }
                        if let Some(region) = classify_rule_region_f64(trimmed, number) {
                            self.quality.rule_regions[region] += 1;
                        }
                    }
                    NumericToken::None => unreachable!("Numeric class always carries a token"),
                }
                if is_zero_padded_digits(trimmed) {
                    self.zero_padded += 1;
                }
                if is_compact_date_token(trimmed) {
                    self.compact_dates += 1;
                }
            }
            // The inferred forms are a subset of the date class: a value in
            // them has a separator, so it never parses as a number first.
            LexicalClass::Date => {
                self.inferred_dates += usize::from(inferred_date);
                if let Some(index) = date_format {
                    self.quality.date_formats[usize::from(index)] += 1;
                    // ASCII `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`, `YYYY/MM/DD`
                    // contain only digits, `-`, and `/`.
                    if !trimmed.is_ascii() {
                        self.quality.record_separators_and_encoding(trimmed);
                    }
                } else {
                    self.quality.record_separators_and_encoding(trimmed);
                }
                if extract_year(value).is_some() {
                    self.quality.temporal_hint_matches += 1;
                }
            }
            // Strict booleans (`true`/`false`) are ASCII letters with no
            // separators, encoding artifacts, or date shape.
            LexicalClass::Boolean => {}
            LexicalClass::Text => {
                if may_be_locale_number(trimmed) && LOCALE_NUMBER_REGEX.is_match(trimmed) {
                    self.locale_numbers += 1;
                }
                self.quality.record_separators_and_encoding(trimmed);
                if extract_year(value).is_some() {
                    self.quality.temporal_hint_matches += 1;
                }
            }
        }
    }

    /// Add the counts of a part of the same column read separately.
    pub fn merge(&mut self, other: &Self) {
        let TypeHomogeneity {
            numeric,
            date,
            boolean,
            text,
        } = other.forms;
        self.forms.numeric += numeric;
        self.forms.date += date;
        self.forms.boolean += boolean;
        self.forms.text += text;
        self.integers += other.integers;
        self.zero_padded += other.zero_padded;
        self.compact_dates += other.compact_dates;
        self.inferred_dates += other.inferred_dates;
        self.locale_numbers += other.locale_numbers;
        self.quality.merge(&other.quality);
    }

    /// Heap memory held by the tally's overflow decimal-scale map.
    pub fn heap_bytes(&self) -> usize {
        self.quality.heap_bytes()
    }

    /// Non-null values recorded in this tally.
    pub(crate) fn values_checked(&self) -> usize {
        self.forms.classified_count()
    }

    /// Values matching `is_integer_token`.
    pub(crate) fn integer_count(&self) -> usize {
        self.integers
    }

    /// Whether the column's recorded values satisfy the digit-code share rule
    /// (`holds_digit_codes` over every value).
    pub(crate) fn holds_digit_codes(&self) -> bool {
        digit_code_rule(self.forms.numeric, self.zero_padded, self.compact_dates)
    }

    /// Mixed date formats and decimal-separator violations counted over every
    /// recorded value.
    pub(crate) fn format_violations(&self) -> usize {
        self.quality.format_violations()
    }

    /// Values with U+FFFD or mojibake artifacts counted over every recorded
    /// value.
    pub(crate) fn encoding_issues(&self) -> usize {
        self.quality.encoding_issues
    }

    /// `(finite_numeric_values, dominant_scale_count)` for precision.
    pub(crate) fn decimal_scale_summary(&self) -> (usize, usize) {
        self.quality.decimal_scales.summary()
    }

    /// Counts of rule-checkable numeric values across the six accuracy
    /// rule-bound regions (#788 step 3).
    pub(crate) fn rule_regions(&self) -> [usize; RULE_REGION_COUNT] {
        self.quality.rule_regions
    }

    /// Non-null values accepted by [`crate::analysis::metrics::accuracy::rule_value`]:
    /// the `positive_columns` hint match count and numeric rule denominator.
    pub(crate) fn rule_numeric_count(&self) -> usize {
        self.quality.rule_regions.iter().sum()
    }

    /// Non-null values whose original (untrimmed) text parses as a calendar
    /// date (`value_matches_hint(_, Temporal)`): the `temporal_columns` /
    /// `temporal_pairs` hint match count and date-column parsed count.
    pub fn temporal_match_count(&self) -> usize {
        self.quality.temporal_hint_matches
    }

    /// The counts by lexical class, which a profile reports as
    /// `type_homogeneity`.
    pub fn type_homogeneity(&self) -> TypeHomogeneity {
        self.forms
    }

    /// Of the values counted as text, how many are locale-formatted numbers
    /// ([`is_locale_number_token`]), which a profile reports as
    /// `locale_number_count`.
    pub fn locale_number_count(&self) -> usize {
        self.locale_numbers
    }

    /// Of the values counted as numeric, how many keep a leading zero, which
    /// a profile reports as `zero_padded_count`.
    pub fn zero_padded_count(&self) -> usize {
        self.zero_padded
    }

    /// The column's type, from the shares of the values counted.
    ///
    /// Numbers come first, unless the values are digit codes; then booleans
    /// (at least 90%) and dates (more than 70%), which no value can be both of,
    /// so their order does not matter. Pure `0`/`1` columns are integers.
    pub fn data_type(&self) -> DataType {
        let counted = self.forms.classified_count();
        if counted == 0 {
            return DataType::String;
        }
        let share = |count: usize| count as f64 / counted as f64;
        // Codes written in digits are text: typed numeric, they get a mean (#814).
        let digit_codes = digit_code_rule(self.forms.numeric, self.zero_padded, self.compact_dates);
        if !digit_codes && self.integers == counted {
            return DataType::Integer;
        }
        // Infinity is a numeric lexical form even though it cannot enter
        // finite statistics, so it counts here, and the profile's
        // `invalid_count` discloses the unusable value. More than 80% tolerates
        // a few non-numeric values such as "N/A".
        if !digit_codes && share(self.forms.numeric) > 0.8 {
            return DataType::Float;
        }
        if share(self.forms.boolean) >= 0.9 {
            return DataType::Boolean;
        }
        // Supported date forms count together, so a column mixing them is
        // still a date column.
        if share(self.inferred_dates) > 0.7 {
            return DataType::Date;
        }
        DataType::String
    }
}

/// Whether a value carries one of the date forms `infer_type` counts towards
/// typing a column as [`DataType::Date`].
///
/// Deliberately narrower than `is_date_token`: this set decides the *type*, so
/// widening it changes which columns are dates.
///
/// The streaming engines type their columns from sampled strings and call this
/// directly; the buffered paths reach it through [`infer_type`]. Both decide
/// "is this a date" on the same set, so a file cannot be a date column through
/// one engine and a text column through the other.
pub fn is_inferred_date_token(value: &str) -> bool {
    may_be_date_token(value) && DATE_REGEXES.iter().any(|regex| regex.is_match(value))
}

/// A cheap necessary condition for every date form, so the regular
/// expressions run only on values that could match one (#788 counts every
/// value). Every pattern in [`DATE_REGEXES`] and in the validation set starts
/// with `\d`, which is Unicode-aware here and lies inside
/// [`char::is_numeric`], and the shortest form, `1/2/2024`, has eight
/// characters, so at least eight bytes.
fn may_be_date_token(value: &str) -> bool {
    value.len() >= 8 && value.chars().next().is_some_and(char::is_numeric)
}

/// Whether a value has the lexical form of a date in any format dataprof
/// recognizes.
///
/// The union of the forms [`is_inferred_date_token`] scores when typing a column
/// and the forms `is_valid_date_format` accepts when validating one. Neither set
/// contains the other — inference alone misses `1/2/2024`, validation alone
/// misses dotted dates and both datetime forms — and a value that only one of
/// them recognizes is still a date rather than free text.
///
/// Classifying against only one set silently reunites dates with junk: a column
/// of 70% ISO datetimes and 30% junk falls short of the inference threshold, and
/// if the datetimes then fail the date test too, every value lands in the text
/// class and the column reports a perfect consistency score. Reconciling the two
/// sets into one is tracked separately; until then this union is what "looks like
/// a date" means for classification.
pub(crate) fn is_date_token(value: &str) -> bool {
    may_be_date_token(value)
        && (is_inferred_date_token(value)
            || crate::analysis::metrics::utils::is_valid_date_format(value))
}

/// Whether trimmed, non-null `values` are codes written in digits rather than
/// quantities, so a numeric type would give them a mean (#814).
///
/// Two shapes qualify:
///
/// - At least [`ZERO_PADDED_CODE_PERCENT`] of the numeric values keep a
///   leading zero, such as `007` or a postal code `08001`. No quantity is
///   written that way, but one stray code among many quantities does not make
///   them codes (#788), so the rule is a share. `0` alone is a quantity and
///   does not count.
/// - Every numeric value is an eight-digit `YYYYMMDD` calendar date, such as
///   `20240115`. Recognizing these as dates is #815; until then they are text.
///
/// Both inference paths, [`infer_type`] and the streaming one, ask this before
/// typing a column numeric, so every engine and input path agrees. A column a
/// source declares numeric (an Arrow integer type) is not re-inferred: it has
/// no leading zeros to see.
pub fn holds_digit_codes<'a>(values: impl IntoIterator<Item = &'a str>) -> bool {
    let mut numeric = 0usize;
    let mut zero_padded = 0usize;
    let mut compact_dates = 0usize;
    for value in values {
        if is_integer_token(value) || value.parse::<f64>().is_ok() {
            numeric += 1;
            if is_zero_padded_digits(value) {
                zero_padded += 1;
            }
            if is_compact_date_token(value) {
                compact_dates += 1;
            }
        }
    }
    digit_code_rule(numeric, zero_padded, compact_dates)
}

/// The share of a column's numeric values, in percent, that must keep a
/// leading zero for the column to be digit codes (#788).
///
/// Postal codes, phone numbers and product codes keep a leading zero on a
/// few percent of their values or more: US ZIP codes of the north-eastern
/// states, Italian CAPs of Lazio, most Italian landlines. A quantity column
/// holds one only as a stray: a single `007` among 40,000 integers is
/// 0.0025%.
pub const ZERO_PADDED_CODE_PERCENT: usize = 1;

/// The digit-code rule of [`holds_digit_codes`] over counts: at least
/// [`ZERO_PADDED_CODE_PERCENT`] of the numeric values zero-padded, or every
/// numeric value a compact date. [`LexicalTally`] applies it to the counts it
/// keeps, so the two can never disagree. Compared in integers, so a share at
/// the threshold is not decided by rounding.
fn digit_code_rule(numeric: usize, zero_padded: usize, compact_dates: usize) -> bool {
    let codes = zero_padded > 0
        && zero_padded.saturating_mul(100) >= numeric.saturating_mul(ZERO_PADDED_CODE_PERCENT);
    codes || (numeric > 0 && compact_dates == numeric)
}

/// `0` followed by one or more ASCII digits, and nothing else.
fn is_zero_padded_digits(value: &str) -> bool {
    value.len() > 1 && value.starts_with('0') && value.bytes().all(|b| b.is_ascii_digit())
}

/// An eight-digit `YYYYMMDD` value naming a real calendar day in 1800-2199.
///
/// The year window keeps arbitrary eight-digit numbers out: `10101010` is a
/// valid date in year 1010, and no data this profiler reads dates from then.
fn is_compact_date_token(value: &str) -> bool {
    let digits = value.as_bytes();
    if digits.len() != 8 || !digits.iter().all(u8::is_ascii_digit) {
        return false;
    }
    let number = |range: std::ops::Range<usize>| {
        digits[range]
            .iter()
            .fold(0u32, |n, digit| n * 10 + u32::from(digit - b'0'))
    };
    let year = number(0..4);
    (1800..=2199).contains(&year)
        && chrono::NaiveDate::from_ymd_opt(year as i32, number(4..6), number(6..8)).is_some()
}

/// Return whether a token is an integer representable by dataprof's signed or
/// unsigned 64-bit integer contract.
///
/// Keep inference and quality validation on this shared predicate: values above
/// `i64::MAX` are valid integer tokens even though they require `u64` to parse.
pub fn is_integer_token(value: &str) -> bool {
    value.parse::<i64>().is_ok() || value.parse::<u64>().is_ok()
}

pub use dataprof_core::is_null_like_token;

pub fn parse_strict_boolean_token(value: &str) -> Option<bool> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        Some(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

/// Classify one trimmed, non-null value into its [`LexicalClass`].
///
/// The precedence matches the order [`infer_type`] tries types on whole columns,
/// so a column that just missed a type threshold reports the same share of
/// matching values that it reported while it still had that type. Integers and
/// fractions share [`LexicalClass::Numeric`] deliberately: `["1.5", "2", "3"]`
/// is one numeric column, not a two-class mixture.
///
/// Dates use `is_date_token`, the union of the forms inference and validation
/// each recognize. Using only the validation set would drop ISO datetimes and
/// dotted dates into `Text` alongside genuine junk.
pub fn lexical_class(value: &str) -> LexicalClass {
    classify(value).class
}

/// The parsed numeric value remembered from [`classify`], so
/// [`LexicalTally::record`] does not parse the same token a second time for
/// precision or accuracy rule regions.
#[derive(Debug, Clone, Copy)]
enum NumericToken {
    None,
    IntegerI64(i64),
    IntegerU64,
    Float(f64),
}

/// One value's class and the facts the tally needs beside it, from a single
/// pass: an integer is parsed once, and an inferred date form matched once.
struct Classified {
    class: LexicalClass,
    /// Parsed numeric token when `class == LexicalClass::Numeric`.
    numeric_token: NumericToken,
    /// A date value in a form [`is_inferred_date_token`] accepts.
    inferred_date: bool,
    /// Index into `DATE_REGEXES[0..4]` (identical to `DATE_FORMAT_REGEXES`)
    /// when matched.
    date_format: Option<u8>,
}

fn classify(value: &str) -> Classified {
    if let Ok(n) = value.parse::<i64>() {
        return Classified {
            class: LexicalClass::Numeric,
            numeric_token: NumericToken::IntegerI64(n),
            inferred_date: false,
            date_format: None,
        };
    }
    if value.parse::<u64>().is_ok() {
        return Classified {
            class: LexicalClass::Numeric,
            numeric_token: NumericToken::IntegerU64,
            inferred_date: false,
            date_format: None,
        };
    }
    if let Ok(number) = value.parse::<f64>() {
        return Classified {
            class: LexicalClass::Numeric,
            numeric_token: NumericToken::Float(number),
            inferred_date: false,
            date_format: None,
        };
    }
    // `is_date_token`, with the inferred forms tried first and remembered.
    let inferred_date_index = if may_be_date_token(value) {
        DATE_REGEXES.iter().position(|regex| regex.is_match(value))
    } else {
        None
    };
    let inferred_date = inferred_date_index.is_some();
    let date_format = inferred_date_index.and_then(|idx| (idx < 4).then_some(idx as u8));
    let class = if inferred_date
        || (may_be_date_token(value)
            && crate::analysis::metrics::utils::is_valid_date_format(value))
    {
        LexicalClass::Date
    } else if parse_strict_boolean_token(value).is_some() {
        LexicalClass::Boolean
    } else {
        LexicalClass::Text
    };
    Classified {
        class,
        numeric_token: NumericToken::None,
        inferred_date,
        date_format,
    }
}

/// A cheap necessary condition for [`LOCALE_NUMBER_REGEX`]: an ASCII digit
/// first, after an optional sign.
fn may_be_locale_number(value: &str) -> bool {
    let bytes = value.as_bytes();
    let first = match bytes.first() {
        Some(b'+' | b'-') => bytes.get(1),
        other => other,
    };
    first.is_some_and(u8::is_ascii_digit)
}

/// Distribute `values` across the lexical classes, skipping null-like tokens.
///
/// This is the single classifier behind both the consistency score of a column
/// with no inferred type and the `type_homogeneity` a profile reports, so the
/// two can never disagree about what a column holds.
pub fn classify_lexical_forms<S: AsRef<str>>(values: &[S]) -> TypeHomogeneity {
    let mut homogeneity = TypeHomogeneity::default();
    for value in values {
        let trimmed = value.as_ref().trim();
        if !is_null_like_token(trimmed) {
            homogeneity.record(lexical_class(trimmed));
        }
    }
    homogeneity
}

/// Whether a trimmed value is a number written with a decimal comma or with
/// digit-group separators, such as `10,50`, `1.234,56`, `1,234.56`,
/// `1'234.56` or `1 234,56` (#433).
///
/// Only [`LexicalClass::Text`] values qualify, so every value this accepts is
/// one [`classify_lexical_forms`] counted as text, and one no statistic read
/// as a number. Which convention a value uses is not decided here: `1,234` is
/// a thousand in one locale and a fraction in another, and either way it was
/// not read as a number.
pub fn is_locale_number_token(value: &str) -> bool {
    // The regex first: almost no value matches it, and `lexical_class` parses
    // and runs the date patterns.
    may_be_locale_number(value)
        && LOCALE_NUMBER_REGEX.is_match(value)
        && lexical_class(value) == LexicalClass::Text
}

/// Count the non-null `values` that [`is_locale_number_token`] accepts.
///
/// Classified over the same values as [`classify_lexical_forms`], so the count
/// is a subset of that result's `text` count.
pub fn count_locale_numbers<S: AsRef<str>>(values: &[S]) -> usize {
    values
        .iter()
        .map(|value| value.as_ref().trim())
        .filter(|trimmed| !is_null_like_token(trimmed) && is_locale_number_token(trimmed))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn each_value_lands_in_exactly_one_class() {
        for (value, expected) in [
            ("42", LexicalClass::Numeric),
            ("-1.5", LexicalClass::Numeric),
            (u64::MAX.to_string().as_str(), LexicalClass::Numeric),
            ("2024-01-15", LexicalClass::Date),
            ("2024-01-15T10:30:00", LexicalClass::Date),
            ("15.01.2024", LexicalClass::Date),
            ("true", LexicalClass::Boolean),
            ("FALSE", LexicalClass::Boolean),
            ("junk0", LexicalClass::Text),
            ("N/A", LexicalClass::Text),
        ] {
            assert_eq!(lexical_class(value), expected, "{value}");
        }
    }

    #[test]
    fn digit_codes_are_text_and_quantities_stay_numeric() {
        let strings = |values: &[&str]| values.iter().map(|v| v.to_string()).collect::<Vec<_>>();

        // #814: a leading zero anywhere, or every value a YYYYMMDD date.
        for codes in [
            &["00123", "00456", "01234", "09999"][..],
            &["28013", "08001", "41001"],
            &["20240115", "20240216", "19991231"],
            &["0612345678", "3471234567"],
            &["007", "12", "N/A"],
            &["20240115", "", "null"],
        ] {
            assert_eq!(infer_type(&strings(codes)), DataType::String, "{codes:?}");
        }

        for (quantities, expected) in [
            (&["0", "1", "10", "250"][..], DataType::Integer),
            (&["12345678", "20240115"], DataType::Integer),
            (&["20241315", "20240230"], DataType::Integer),
            (&["17991231", "22000101"], DataType::Integer),
            (&["0.5", "0.25", "12"], DataType::Float),
            (&["-01", "-02", "3"], DataType::Integer),
        ] {
            assert_eq!(infer_type(&strings(quantities)), expected, "{quantities:?}");
        }
    }

    #[test]
    fn a_tally_merged_from_parts_is_the_tally_of_the_whole() {
        // Zero-padded codes past the share in the second part type the whole
        // column as codes, however the column was split (#788).
        let values = [
            "12",
            " 34 ",
            "null",
            "1.234,56",
            "2024-01-15",
            "true",
            "007",
            "20240115",
            "junk",
            "",
        ];
        let whole = LexicalTally::from_values(&values);
        for split in 0..=values.len() {
            let (left, right) = values.split_at(split);
            let mut merged = LexicalTally::from_values(right);
            merged.merge(&LexicalTally::from_values(left));
            assert_eq!(merged, whole, "split at {split}");
        }
        assert_eq!(whole.type_homogeneity(), classify_lexical_forms(&values));
        assert_eq!(whole.locale_number_count(), count_locale_numbers(&values));
        assert_eq!(whole.data_type(), DataType::String);

        let mut quantities = LexicalTally::from_values(&["12", "34"]);
        assert_eq!(quantities.data_type(), DataType::Integer);
        quantities.merge(&LexicalTally::from_values(&["007"]));
        assert_eq!(quantities.data_type(), DataType::String);
        assert_eq!(quantities.zero_padded_count(), 1);
    }

    #[test]
    fn zero_padded_values_make_codes_from_one_percent_of_the_numbers() {
        // #788: one stray code among quantities no longer retypes the column;
        // a share of them still does. 99 quantities and one code is exactly
        // 1%; 100 quantities and one code is just under.
        let quantities = |count: usize| (1..=count).map(|n| n.to_string());
        let at: Vec<String> = quantities(99).chain(["007".to_string()]).collect();
        let under: Vec<String> = quantities(100).chain(["007".to_string()]).collect();
        let tally = LexicalTally::from_values(&at);
        assert_eq!(
            (tally.data_type(), tally.zero_padded_count()),
            (DataType::String, 1)
        );
        let tally = LexicalTally::from_values(&under);
        assert_eq!(
            (tally.data_type(), tally.zero_padded_count()),
            (DataType::Integer, 1)
        );
        // The sample-reading predicate applies the same rule.
        assert!(holds_digit_codes(at.iter().map(String::as_str)));
        assert!(!holds_digit_codes(under.iter().map(String::as_str)));
        // Text values do not count towards the share: half the numbers are codes.
        assert_eq!(
            infer_type(&["007".into(), "12".into(), "N/A".into()]),
            DataType::String
        );
    }

    #[test]
    fn classification_counts_only_non_null_values() {
        // Null-like tokens are absence, not a lexical form: counting them would
        // make a sparse numeric column look mixed.
        let values = ["1", "2", "", "null", "NaN", "junk", "2024-01-15"].map(String::from);

        let counts = classify_lexical_forms(&values);

        assert_eq!(counts.numeric, 2);
        assert_eq!(counts.date, 1);
        assert_eq!(counts.text, 1);
        assert_eq!(counts.classified_count(), 4);
    }

    #[test]
    fn classification_matches_the_consistency_score_it_shares_a_classifier_with() {
        // 800 integers and 200 junk values: the shape from #544. The dominant
        // share and the consistency score of the same column are one number,
        // computed once, so the flag and the score can never disagree.
        let values: Vec<String> = (0..800)
            .map(|i| (1000 + i).to_string())
            .chain((0..200).map(|i| format!("junk{i}")))
            .collect();

        let counts = classify_lexical_forms(&values);

        assert_eq!(counts.dominant(), Some((LexicalClass::Numeric, 800)));
        assert_eq!(counts.dominant_share(), Some(0.8));
    }

    #[test]
    fn a_column_of_only_nulls_is_classified_and_holds_nothing() {
        let values = ["", "   ", "NULL"].map(String::from);

        // Not the same as "never classified": the caller can tell the two apart
        // because one is `Some` and the other is absent from the profile.
        assert_eq!(classify_lexical_forms(&values), TypeHomogeneity::default());
    }

    #[test]
    fn locale_formatted_numbers_are_recognized_and_nothing_else_is() {
        for value in [
            "10,50",
            "-0,5",
            "+3,14",
            "1,234",
            "1.234,56",
            "1.234.567",
            "12.345.678,9",
            "1,234.56",
            "1,234,567",
            "12,34,567",
            "1,00,000.50",
            "1'234.56",
            "1\u{2019}234,5",
            "1 234,56",
            "1\u{A0}234,56",
            "1\u{202F}234.5",
        ] {
            assert!(is_locale_number_token(value), "{value:?} not recognized");
            assert_eq!(lexical_class(value), LexicalClass::Text, "{value:?}");
        }

        for value in [
            // Read as numbers already: not text, so not a missed number.
            "1.234",
            "10.50",
            "1234",
            "-7",
            "1e3",
            // Dates and other dotted or grouped forms.
            "15.01.2024",
            "2024-01-15",
            "1.2.3",
            "192.168.1.1",
            // Space grouping without decimals is a phone number or a code as
            // often as it is an integer.
            "333 123 456",
            "1 234",
            // Malformed grouping and separators without digits.
            "1,2,3",
            "1,23,4",
            "123,45,678",
            "1.23,4",
            ",5",
            "5,",
            "1,,5",
            // Not ASCII digits.
            "\u{0661},\u{0665}",
            // Units, currencies and other text around a number are out of
            // scope for this recognizer.
            "10,50 EUR",
            "\u{20AC} 10,50",
            "10,5%",
            "junk",
        ] {
            assert!(!is_locale_number_token(value), "{value:?} recognized");
        }
    }

    #[test]
    fn locale_numbers_are_counted_among_the_text_values_only() {
        let values = [
            "1.234,56",
            " 10,50 ",
            "",
            "null",
            "1.234",
            "12",
            "junk",
            "15.01.2024",
        ]
        .map(String::from);

        assert_eq!(count_locale_numbers(&values), 2);
        // The same values `classify_lexical_forms` puts in `text`, and no more.
        assert_eq!(classify_lexical_forms(&values).text, 3);
    }

    #[test]
    fn test_infer_integer() {
        let data = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_float() {
        let data = vec!["1.5".to_string(), "2.3".to_string(), "3.7".to_string()];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_mixed_numeric_as_float() {
        // Mix of integers and floats should be detected as Float
        let data = vec!["1".to_string(), "2.5".to_string(), "3".to_string()];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_unsigned_integer_beyond_i64() {
        let data = vec![
            u64::MAX.to_string(),
            (u64::MAX - 1).to_string(),
            (i64::MAX as u64 + 1).to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_non_finite_numeric_tokens_as_float() {
        let data = vec![
            "1.0".to_string(),
            "Infinity".to_string(),
            "-inf".to_string(),
            "2.0".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_date_iso() {
        let data = vec![
            "2023-01-15".to_string(),
            "2023-02-20".to_string(),
            "2023-03-25".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_european_slash() {
        let data = vec![
            "15/01/2023".to_string(),
            "20/02/2023".to_string(),
            "25/03/2023".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_european_dash() {
        let data = vec![
            "15-01-2023".to_string(),
            "20-02-2023".to_string(),
            "25-03-2023".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_european_dot() {
        let data = vec![
            "15.01.2023".to_string(),
            "20.02.2023".to_string(),
            "25.03.2023".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_threshold() {
        // 71.4% dates (5 out of 7), should still be detected as Date (threshold > 70%)
        let data = vec![
            "2023-01-15".to_string(),
            "2023-02-20".to_string(),
            "2023-03-25".to_string(),
            "2023-04-30".to_string(),
            "2023-05-15".to_string(),
            "not a date".to_string(),
            "also not".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_mixed_date_formats() {
        let data = vec![
            "2024-01-15".to_string(),
            "15/01/2024".to_string(),
            "2024-01-16".to_string(),
            "16-01-2024".to_string(),
            "2024-01-17".to_string(),
            "2024-01-18".to_string(),
            "2024/01/19".to_string(),
            "19/01/2024".to_string(),
            "".to_string(),
            "2024-01-20".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_iso_datetime() {
        let data = vec![
            "2024-01-15T10:00:00".to_string(),
            "2024-01-15T10:15:00".to_string(),
            "2024-01-15T10:30:00".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_string() {
        let data = vec!["hello".to_string(), "world".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_empty_data() {
        let data: Vec<String> = vec![];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_all_empty_strings() {
        let data = vec!["".to_string(), "".to_string(), "".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_whitespace_handling() {
        // Integers with leading/trailing whitespace
        let data = vec![" 1 ".to_string(), "  2".to_string(), "3  ".to_string()];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_whitespace_only_strings() {
        let data = vec!["  ".to_string(), "\t".to_string(), " \n ".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_dates_with_whitespace() {
        let data = vec![
            " 2023-01-15 ".to_string(),
            "  2023-02-20".to_string(),
            "2023-03-25  ".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_floats_with_whitespace() {
        let data = vec![
            " 1.5 ".to_string(),
            "  2.3".to_string(),
            "3.7  ".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_mixed_non_numeric() {
        // Mix of different non-numeric types should be String
        let data = vec![
            "hello".to_string(),
            "123abc".to_string(),
            "2023".to_string(), // This looks like a year but alone is an integer
        ];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_boolean_lowercase() {
        let data = vec![
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_titlecase() {
        let data = vec!["True".to_string(), "False".to_string(), "True".to_string()];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_uppercase() {
        let data = vec!["TRUE".to_string(), "FALSE".to_string(), "TRUE".to_string()];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_yes_no() {
        let data = vec!["yes".to_string(), "no".to_string(), "yes".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_boolean_mixed_case() {
        let data = vec![
            "True".to_string(),
            "false".to_string(),
            "TRUE".to_string(),
            "False".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_with_whitespace() {
        let data = vec![
            " true ".to_string(),
            "  false".to_string(),
            "true  ".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_threshold() {
        // 90% threshold: 9 of 10 are boolean → should detect
        let data = vec![
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "maybe".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_not_boolean_below_threshold() {
        // Only 50% are boolean → should be String
        let data = vec![
            "true".to_string(),
            "false".to_string(),
            "hello".to_string(),
            "world".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_pure_01_stays_integer() {
        // Pure 0/1 columns should remain Integer, not Boolean
        let data = vec![
            "0".to_string(),
            "1".to_string(),
            "0".to_string(),
            "1".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_boolean_with_null_like_tokens() {
        let data = vec![
            "true".to_string(),
            "FALSE".to_string(),
            "null".to_string(),
            "NULL".to_string(),
            "nan".to_string(),
            "NaN".to_string(),
            "".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_date_regex_patterns_are_valid() {
        // Validate that all hardcoded regex patterns compile successfully
        // This test will fail at initialization if any pattern is invalid
        assert_eq!(DATE_REGEXES.len(), 8);
    }

    #[test]
    fn test_is_inferred_date_token() {
        // Test various date formats
        assert!(is_inferred_date_token("2024-01-15"));
        assert!(is_inferred_date_token("15/01/2024"));
        assert!(is_inferred_date_token("15-01-2024"));
        assert!(is_inferred_date_token("2024/01/15"));
        assert!(is_inferred_date_token("15.01.2024"));
        assert!(is_inferred_date_token("2024-01-15T10:30:00"));

        // Test non-dates
        assert!(!is_inferred_date_token("not a date"));
        assert!(!is_inferred_date_token("12345"));
        assert!(!is_inferred_date_token("user@example.com"));
    }

    /// The streaming engines type their columns through `is_inferred_date_token`
    /// and the buffered ones through `infer_type`. Both read the same set, so
    /// the forms one accepts are exactly the forms the other accepts.
    #[test]
    fn inferred_date_token_accepts_offsets_and_rejects_what_chrono_cannot_parse() {
        for value in [
            // Forms the streaming engines' old private list missed, so a column
            // of them used to be a date through the buffered engines and text
            // through streaming.
            "2024-01-15T10:30:00Z",
            "2024-01-15T10:30:00+02:00",
            "2024-01-15T10:30:00.500-05:00",
            "2024-01-15 10:30:00",
            "15/01/2024 10:30:00",
            // Forms both lists always shared.
            "2024-01-15",
            "15.01.2024",
        ] {
            assert!(
                is_inferred_date_token(value),
                "{value:?} should be an inferred date"
            );
        }

        for value in [
            // The old list's ISO datetime pattern was unanchored, so it typed
            // these as dates and then failed to parse a single one of them.
            "2024-01-15T10:",
            "2024-01-15T10:30",
            "2024-01-15T10:30:00 (UTC)",
            // The ISO 8601 basic offset, which chrono's RFC 3339 parser rejects.
            "2024-01-15T10:30:00+0200",
            // Offsets chrono rejects as out of range. A grammar that took these
            // would type the column `Date` and then fail to parse a value of it.
            "2024-01-15T10:30:00+24:00",
            "2024-01-15T10:30:00+99:99",
            "2024-01-15T10:30:00-00:60",
            "not a date",
        ] {
            assert!(
                !is_inferred_date_token(value),
                "{value:?} should not be an inferred date"
            );
        }
    }

    /// Every date pattern dataprof recognizes, paired with an example of it.
    ///
    /// Keyed by regex source rather than by example, because the two sets
    /// overlap: the lenient `^\d{1,2}/\d{1,2}/\d{4}$` also matches `15/01/2024`,
    /// so a table checked only by "some example matches this pattern" stays green
    /// when a pattern is added or its example deleted. Pinning the pattern set
    /// makes any change to either set fail here until this table is updated.
    ///
    /// The four patterns the two sets share appear once, so this is the union.
    const DATE_FORM_EXAMPLES: [(&str, &str); 11] = [
        (r"^\d{4}-\d{2}-\d{2}$", "2024-01-15"),
        (r"^\d{2}/\d{2}/\d{4}$", "15/01/2024"),
        (r"^\d{2}-\d{2}-\d{4}$", "15-01-2024"),
        (r"^\d{4}/\d{2}/\d{2}$", "2024/01/15"),
        (r"^\d{2}\.\d{2}\.\d{4}$", "15.01.2024"),
        (
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)?$",
            "2024-01-15T10:30:00",
        ),
        (
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)?$",
            "2024-01-15 10:30:00.5+02:00",
        ),
        (
            r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$",
            "15/01/2024 10:30:00",
        ),
        (r"^\d{1,2}/\d{1,2}/\d{4}$", "1/2/2024"),
        (r"^\d{4}-\d{1,2}-\d{1,2}$", "2024-1-5"),
        (r"^\d{1,2}-\d{1,2}-\d{4}$", "1-2-2024"),
    ];

    #[test]
    fn is_date_token_accepts_every_form_either_regex_set_recognizes() {
        // The two sets exist for different jobs — one types a column, the other
        // validates its values — and neither contains the other. When they were
        // used independently a column of clean ISO datetimes was typed `Date` on
        // one set and then failed every value against the other, scoring 0%
        // consistency. `is_date_token` is the union both jobs now share.
        let validation = &crate::analysis::metrics::utils::DATE_VALIDATION_REGEXES;
        let recognized: BTreeSet<&str> = DATE_REGEXES
            .iter()
            .chain(validation.iter())
            .map(|regex| regex.as_str())
            .collect();
        let covered: BTreeSet<&str> = DATE_FORM_EXAMPLES
            .iter()
            .map(|(pattern, _)| *pattern)
            .collect();

        // Adding, removing, or editing a pattern in either set fails here until
        // this table is updated, so no date form can arrive without an example.
        assert_eq!(
            recognized, covered,
            "the recognized date patterns and the examples below have diverged"
        );

        for (pattern, example) in DATE_FORM_EXAMPLES {
            let regex = DATE_REGEXES
                .iter()
                .chain(validation.iter())
                .find(|regex| regex.as_str() == pattern)
                .expect("checked by the set equality above");
            assert!(
                regex.is_match(example),
                "{example:?} is not an example of {pattern}"
            );
            assert!(
                is_date_token(example),
                "{example:?} is a recognized date form but is_date_token rejects it"
            );
        }
    }

    #[test]
    fn the_pre_filters_never_change_an_answer() {
        // The cheap checks in front of the regular expressions (#788) must
        // only skip values no pattern matches: Unicode digits included, since
        // `\d` is Unicode-aware here.
        let unfiltered_inferred = |v: &str| DATE_REGEXES.iter().any(|r| r.is_match(v));
        let unfiltered_date = |v: &str| {
            unfiltered_inferred(v)
                || crate::analysis::metrics::utils::DATE_VALIDATION_REGEXES
                    .iter()
                    .any(|r| r.is_match(v))
        };
        let unfiltered_locale = |v: &str| {
            LOCALE_NUMBER_REGEX.is_match(v)
                && (if is_integer_token(v) || v.parse::<f64>().is_ok() {
                    false
                } else {
                    !unfiltered_date(v) && parse_strict_boolean_token(v).is_none()
                })
        };
        let mut values: Vec<String> = DATE_FORM_EXAMPLES
            .iter()
            .map(|(_, example)| example.to_string())
            .collect();
        values.extend(
            [
                "1/2/2024",
                "1/2/202",
                "\u{0662}\u{0660}\u{0662}\u{0664}-\u{0660}\u{0661}-\u{0661}\u{0665}",
                "\u{FF12}\u{FF10}\u{FF12}\u{FF14}-01-15",
                "x2024-01-15",
                "2024-01-15x",
                "user 12",
                "1.234,56",
                "+1.234,56",
                "-10,50",
                "+",
                "-",
                "",
                "10,50 EUR",
                "\u{20AC}10,50",
                "1 234,56",
                "true",
                "007",
            ]
            .map(String::from),
        );
        for value in &values {
            let v = value.as_str();
            assert_eq!(is_inferred_date_token(v), unfiltered_inferred(v), "{v:?}");
            assert_eq!(is_date_token(v), unfiltered_date(v), "{v:?}");
            assert_eq!(is_locale_number_token(v), unfiltered_locale(v), "{v:?}");
        }
    }

    #[test]
    fn a_value_that_is_not_a_date_is_not_a_date_token() {
        // The union must not become a predicate that accepts anything; a date
        // column full of malformed values still has to lose consistency.
        for value in [
            "not-a-date",
            "2024",
            "15/01",
            "2024-13-45x",
            "",
            "junk1",
            "10:30:00",
        ] {
            assert!(!is_date_token(value), "{value:?} was accepted as a date");
        }
    }

    #[test]
    fn tally_merge_matches_whole_for_consistency_and_precision_counters() {
        let values = [
            "42",
            "007",
            "20240115",
            "12.30",
            "12.345",
            "1.5e-20",
            "1.5e-20",
            "inf",
            "NaN",
            "NA",
            "2024-03-15",
            "15/03/2024",
            "15-03-2024",
            "2024/03/15",
            "2024-03-15T12:00:00.5Z",
            "true",
            "false",
            "1,234.56",
            "10,50",
            "10.50",
            "Caf\u{00c3}\u{00a9}",
            "bad\u{FFFD}utf8",
            "plain text",
            "",
        ];
        let whole = LexicalTally::from_values(&values);
        for split in [0, 1, 5, 11, 17, values.len()] {
            let mut left = LexicalTally::from_values(&values[..split]);
            let right = LexicalTally::from_values(&values[split..]);
            left.merge(&right);
            assert_eq!(left, whole, "split at {split}");
            assert_eq!(left.format_violations(), whole.format_violations());
            assert_eq!(left.encoding_issues(), whole.encoding_issues());
            assert_eq!(left.decimal_scale_summary(), whole.decimal_scale_summary());
        }
    }

    #[test]
    fn many_distinct_large_scales_keep_an_exact_mode_in_any_merge_order() {
        // 1,024 distinct scales above the inline bins, then 2,000 values at
        // one more scale: that last scale is the mode however the values
        // are split and merged.
        let spread: Vec<String> = (0..1024)
            .map(|i| format!("0e-{}", INLINE_DECIMAL_SCALES + i))
            .collect();
        let repeated = vec![format!("0e-{}", INLINE_DECIMAL_SCALES + 1024); 2000];
        let whole = LexicalTally::from_values(&[spread.clone(), repeated.clone()].concat());
        assert_eq!(whole.decimal_scale_summary(), (3024, 2000));

        let mut forward = LexicalTally::from_values(&spread);
        forward.merge(&LexicalTally::from_values(&repeated));
        let mut reverse = LexicalTally::from_values(&repeated);
        reverse.merge(&LexicalTally::from_values(&spread));
        assert_eq!(forward.decimal_scale_summary(), (3024, 2000));
        assert_eq!(reverse.decimal_scale_summary(), (3024, 2000));
        assert!(whole.heap_bytes() > 0);
    }

    #[test]
    fn an_extreme_exponent_does_not_overflow_the_decimal_scale() {
        let mut tally = LexicalTally::new();
        tally.record("0e-2147483648");
        tally.record("0e2147483647");
        assert_eq!(tally.decimal_scale_summary(), (2, 1));
    }

    #[test]
    fn tally_merge_matches_whole_for_accuracy_rules_and_hint_bindings() {
        let values = [
            "-1e-700",
            "-5",
            " 0 ",
            "50",
            "100",
            "100.000000000000000001",
            "150",
            "1000",
            "1900",
            "2100",
            "2100.000000000000000001",
            "1e400",
            "inf",
            "NaN",
            "2024-06-15",
            " 2024-06-15",
            "not-a-date",
            "",
        ];
        let whole = LexicalTally::from_values(&values);
        // Leading whitespace is accepted for both numeric rules and temporal
        // dates, matching `DateTimeAccumulator` (#788 step 4).
        assert_eq!(whole.rule_numeric_count(), 12);
        assert_eq!(whole.temporal_match_count(), 2);
        assert_eq!(whole.rule_regions(), [2, 3, 2, 1, 2, 2]);
        for split in [0, 1, 4, 9, 14, values.len()] {
            let mut left = LexicalTally::from_values(&values[..split]);
            let right = LexicalTally::from_values(&values[split..]);
            left.merge(&right);
            assert_eq!(left, whole, "split at {split}");
            assert_eq!(left.rule_regions(), whole.rule_regions());
            assert_eq!(left.rule_numeric_count(), whole.rule_numeric_count());
            assert_eq!(left.temporal_match_count(), whole.temporal_match_count());
        }
    }
}
