use regex::{Regex, RegexSet};
use std::sync::LazyLock;

use crate::analysis::validators;
use crate::types::{Pattern, PatternCategory};

/// Internal pattern definition with metadata for overlap resolution and confidence scoring.
struct PatternDef {
    name: &'static str,
    regex: Regex,
    category: PatternCategory,
    /// 0-255: higher = more structurally specific regex (fewer false positives).
    specificity: u8,
    /// ISO 3166-1 alpha-2 locale, or None for universal patterns.
    locale: Option<&'static str>,
    /// Per-pattern minimum match percentage to report (replaces uniform 5% threshold).
    min_threshold: f64,
    /// Optional semantic validator run on regex-matched values.
    /// When present, the validator pass rate feeds into the confidence formula.
    validator: Option<fn(&str) -> bool>,
}

// Pre-compile pattern definitions with metadata for better detection accuracy.
// Specificity values encode how structurally unique each regex is:
//   95 = very specific (Codice Fiscale: letter/digit/letter alternation)
//   30 = very broad (File paths, bare digit sequences)
// Per-pattern thresholds compensate for broad regexes that produce false positives.
static PATTERN_DEFS: LazyLock<Vec<PatternDef>> = LazyLock::new(|| {
    vec![
        PatternDef {
            name: "Email",
            regex: Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
                .expect("BUG: Invalid regex for Email"),
            category: PatternCategory::Contact,
            specificity: 80,
            locale: None,
            min_threshold: 3.0,
            validator: None,
        },
        PatternDef {
            name: "Phone (US)",
            regex: Regex::new(r"^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$")
                .expect("BUG: Invalid regex for Phone (US)"),
            category: PatternCategory::Contact,
            specificity: 70,
            locale: Some("US"),
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "Phone (IT)",
            regex: Regex::new(r"^(?:\+39|0039)[-.\s]?(?:0[0-9]{1,3}|3[0-9]{2})[-.\s]?[0-9]{5,8}$")
                .expect("BUG: Invalid regex for Phone (IT)"),
            category: PatternCategory::Contact,
            specificity: 70,
            locale: Some("IT"),
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "URL",
            regex: Regex::new(r"^(?:https?|ftps?)://[^\s/$.?#].[^\s]*$")
                .expect("BUG: Invalid regex for URL"),
            category: PatternCategory::Network,
            specificity: 70,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "UUID",
            regex: Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
                .expect("BUG: Invalid regex for UUID"),
            category: PatternCategory::Identifier,
            specificity: 85,
            locale: None,
            min_threshold: 3.0,
            validator: None,
        },
        PatternDef {
            name: "IPv4",
            regex: Regex::new(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
                .expect("BUG: Invalid regex for IPv4"),
            category: PatternCategory::Network,
            specificity: 65,
            locale: None,
            min_threshold: 3.0,
            validator: None,
        },
        PatternDef {
            name: "IPv6",
            // Pre-filter: hex groups with at least one colon, including :: compression.
            // The validator (Ipv6Addr::from_str) does the real validation.
            regex: Regex::new(r"^[0-9a-fA-F]*:[0-9a-fA-F:.]*$")
                .expect("BUG: Invalid regex for IPv6"),
            category: PatternCategory::Network,
            specificity: 75,
            locale: None,
            min_threshold: 3.0,
            validator: Some(validators::validate_ipv6),
        },
        PatternDef {
            name: "MAC Address",
            regex: Regex::new(r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$")
                .expect("BUG: Invalid regex for MAC Address"),
            category: PatternCategory::Network,
            specificity: 80,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "Geographic Coordinates",
            regex: Regex::new(r"^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$")
                .expect("BUG: Invalid regex for Geographic Coordinates"),
            category: PatternCategory::Geographic,
            specificity: 75,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "IBAN",
            regex: Regex::new(r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$")
                .expect("BUG: Invalid regex for IBAN"),
            category: PatternCategory::Financial,
            specificity: 90,
            locale: None,
            min_threshold: 5.0,
            validator: Some(validators::validate_iban),
        },
        PatternDef {
            name: "Codice Fiscale (IT)",
            regex: Regex::new(r"^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$")
                .expect("BUG: Invalid regex for Codice Fiscale"),
            category: PatternCategory::Identifier,
            specificity: 95,
            locale: Some("IT"),
            min_threshold: 5.0,
            validator: Some(validators::validate_codice_fiscale),
        },
        PatternDef {
            name: "P.IVA (IT)",
            regex: Regex::new(r"^\d{11}$")
                .expect("BUG: Invalid regex for P.IVA"),
            category: PatternCategory::Identifier,
            specificity: 40,
            locale: Some("IT"),
            min_threshold: 25.0,
            validator: Some(validators::validate_piva_it),
        },
        PatternDef {
            name: "CAP (IT)",
            regex: Regex::new(r"^\d{5}$")
                .expect("BUG: Invalid regex for CAP"),
            category: PatternCategory::Geographic,
            specificity: 35,
            locale: Some("IT"),
            min_threshold: 20.0,
            validator: Some(validators::validate_cap_it),
        },
        PatternDef {
            name: "ZIP Code (US)",
            regex: Regex::new(r"^\d{5}(-\d{4})?$")
                .expect("BUG: Invalid regex for ZIP Code"),
            category: PatternCategory::Geographic,
            specificity: 35,
            locale: Some("US"),
            min_threshold: 15.0,
            validator: None,
        },
        PatternDef {
            name: "File Path (Unix)",
            regex: Regex::new(r"^(/[^/\x00]+)+/?$")
                .expect("BUG: Invalid regex for Unix File Path"),
            category: PatternCategory::FilePath,
            specificity: 30,
            locale: None,
            min_threshold: 10.0,
            validator: None,
        },
        PatternDef {
            name: "File Path (Windows)",
            regex: Regex::new(r##"^[A-Z]:\\(?:[^\\/:*?"<>|\r\n]+\\)*[^\\/:*?"<>|\r\n]*$"##)
                .expect("BUG: Invalid regex for Windows File Path"),
            category: PatternCategory::FilePath,
            specificity: 30,
            locale: None,
            min_threshold: 10.0,
            validator: None,
        },
        // ---- Phase 3 additions ----
        PatternDef {
            name: "Credit Card",
            regex: Regex::new(r"^[0-9]{4}[\s-]?[0-9]{4}[\s-]?[0-9]{4}[\s-]?[0-9]{1,4}$")
                .expect("BUG: Invalid regex for Credit Card"),
            category: PatternCategory::Financial,
            specificity: 60,
            locale: None,
            min_threshold: 10.0,
            validator: Some(validators::validate_credit_card),
        },
        PatternDef {
            name: "SSN (US)",
            regex: Regex::new(r"^\d{3}-?\d{2}-?\d{4}$")
                .expect("BUG: Invalid regex for SSN (US)"),
            category: PatternCategory::Identifier,
            specificity: 70,
            locale: Some("US"),
            min_threshold: 10.0,
            validator: Some(validators::validate_ssn_us),
        },
        PatternDef {
            name: "UK Postcode",
            regex: Regex::new(r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$")
                .expect("BUG: Invalid regex for UK Postcode"),
            category: PatternCategory::Geographic,
            specificity: 50,
            locale: Some("GB"),
            min_threshold: 15.0,
            validator: None,
        },
        PatternDef {
            name: "German PLZ",
            regex: Regex::new(r"^\d{5}$")
                .expect("BUG: Invalid regex for German PLZ"),
            category: PatternCategory::Geographic,
            specificity: 30,
            locale: Some("DE"),
            min_threshold: 20.0,
            validator: None,
        },
        PatternDef {
            name: "Canadian Postal Code",
            regex: Regex::new(r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$")
                .expect("BUG: Invalid regex for Canadian Postal Code"),
            category: PatternCategory::Geographic,
            specificity: 50,
            locale: Some("CA"),
            min_threshold: 15.0,
            validator: None,
        },
        PatternDef {
            name: "French Code Postal",
            regex: Regex::new(r"^\d{5}$")
                .expect("BUG: Invalid regex for French Code Postal"),
            category: PatternCategory::Geographic,
            specificity: 30,
            locale: Some("FR"),
            min_threshold: 20.0,
            validator: None,
        },
        PatternDef {
            name: "Hex Color",
            regex: Regex::new(r"^#[0-9a-fA-F]{6}$")
                .expect("BUG: Invalid regex for Hex Color"),
            category: PatternCategory::Other,
            specificity: 60,
            locale: None,
            min_threshold: 10.0,
            validator: None,
        },
        PatternDef {
            name: "SWIFT/BIC",
            regex: Regex::new(r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$")
                .expect("BUG: Invalid regex for SWIFT/BIC"),
            category: PatternCategory::Financial,
            specificity: 75,
            locale: None,
            min_threshold: 10.0,
            validator: None,
        },
        PatternDef {
            name: "Currency",
            regex: Regex::new(r"^[$€£¥₹]\s?-?\d{1,3}([,.\s]\d{3})*([.,]\d{1,2})?$|^-?\d{1,3}([,.\s]\d{3})*([.,]\d{1,2})?\s?[$€£¥₹]$")
                .expect("BUG: Invalid regex for Currency"),
            category: PatternCategory::Other,
            specificity: 40,
            locale: None,
            min_threshold: 15.0,
            validator: None,
        },
        PatternDef {
            name: "Percentage",
            regex: Regex::new(r"^-?\d+([.,]\d+)?\s?%$")
                .expect("BUG: Invalid regex for Percentage"),
            category: PatternCategory::Other,
            specificity: 35,
            locale: None,
            min_threshold: 15.0,
            validator: None,
        },
        // ---- IoT / coded identifier patterns ----
        PatternDef {
            name: "Alphanumeric Code",
            regex: Regex::new(r"^[A-Z]{2,}[_-]\d{2,}$")
                .expect("BUG: Invalid regex for Alphanumeric Code"),
            category: PatternCategory::Identifier,
            specificity: 15,
            locale: None,
            min_threshold: 30.0,
            validator: None,
        },
        PatternDef {
            name: "Scientific Notation",
            regex: Regex::new(r"^[-+]?\d+(\.\d+)?[eE][-+]?\d+$")
                .expect("BUG: Invalid regex for Scientific Notation"),
            category: PatternCategory::Other,
            specificity: 20,
            locale: None,
            min_threshold: 20.0,
            validator: None,
        },
        PatternDef {
            name: "Labeled Identifier",
            regex: Regex::new(r"^[A-Za-z]+[_-][A-Za-z0-9]+$")
                .expect("BUG: Invalid regex for Labeled Identifier"),
            category: PatternCategory::Identifier,
            specificity: 10,
            locale: None,
            min_threshold: 35.0,
            validator: None,
        },
        // ---- Date patterns (unified from DATE_PATTERN_REGEXES) ----
        PatternDef {
            name: "Date (ISO)",
            regex: Regex::new(r"^\d{4}-\d{2}-\d{2}$")
                .expect("BUG: Invalid regex for Date (ISO)"),
            category: PatternCategory::Other,
            specificity: 50,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "Date (EU slash)",
            regex: Regex::new(r"^\d{2}/\d{2}/\d{4}$")
                .expect("BUG: Invalid regex for Date (EU slash)"),
            category: PatternCategory::Other,
            specificity: 50,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "Date (EU dash)",
            regex: Regex::new(r"^\d{2}-\d{2}-\d{4}$")
                .expect("BUG: Invalid regex for Date (EU dash)"),
            category: PatternCategory::Other,
            specificity: 50,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "Date (YYYY/MM/DD)",
            regex: Regex::new(r"^\d{4}/\d{2}/\d{2}$")
                .expect("BUG: Invalid regex for Date (YYYY/MM/DD)"),
            category: PatternCategory::Other,
            specificity: 50,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "Date (EU dot)",
            regex: Regex::new(r"^\d{2}\.\d{2}\.\d{4}$")
                .expect("BUG: Invalid regex for Date (EU dot)"),
            category: PatternCategory::Other,
            specificity: 50,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
        PatternDef {
            name: "DateTime (ISO)",
            regex: Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:")
                .expect("BUG: Invalid regex for DateTime (ISO)"),
            category: PatternCategory::Other,
            specificity: 55,
            locale: None,
            min_threshold: 5.0,
            validator: None,
        },
    ]
});

/// Pre-compiled RegexSet for single-pass pre-filtering.
/// For each input value, `REGEX_SET.matches(value)` returns all pattern indices
/// that match, avoiding redundant regex evaluation for patterns that can't match.
static REGEX_SET: LazyLock<RegexSet> = LazyLock::new(|| {
    let patterns: Vec<&str> = PATTERN_DEFS.iter().map(|d| d.regex.as_str()).collect();
    RegexSet::new(&patterns).expect("BUG: Failed to compile RegexSet from PATTERN_DEFS")
});

// Pre-compile date pattern regexes for type inference
// Previously this was duplicated across 5 engine files without pre-compilation
static DATE_PATTERN_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"^\d{4}-\d{2}-\d{2}$").expect("BUG: Invalid date pattern YYYY-MM-DD"),
        Regex::new(r"^\d{2}/\d{2}/\d{4}$").expect("BUG: Invalid date pattern DD/MM/YYYY"),
        Regex::new(r"^\d{2}-\d{2}-\d{4}$").expect("BUG: Invalid date pattern DD-MM-YYYY"),
        Regex::new(r"^\d{4}/\d{2}/\d{2}$").expect("BUG: Invalid date pattern YYYY/MM/DD"),
        Regex::new(r"^\d{2}\.\d{2}\.\d{4}$").expect("BUG: Invalid date pattern DD.MM.YYYY"),
        Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:").expect("BUG: Invalid date pattern ISO datetime"),
    ]
});

/// Intermediate match result used during overlap resolution.
struct PatternMatch<'a> {
    def: &'a PatternDef,
    match_count: usize,
    match_percentage: f64,
    /// Per-row match bitmap for overlap computation.
    matched_rows: Vec<bool>,
    /// Fraction of regex-matched values that also pass the semantic validator.
    /// 1.0 when no validator is defined.
    validator_pass_rate: f64,
}

/// Compute confidence score from pattern specificity, match rate, and validator pass rate.
///
/// Formula: `clamp((specificity / 100) × clamp(match_percentage / 50, 0.0, 1.0) × validator_pass_rate, 0.0, 1.0)`
///
/// The validator_pass_rate penalizes patterns where the regex matches but the values
/// fail semantic validation (e.g. 11-digit numbers that aren't valid P.IVA).
fn compute_confidence(specificity: u8, match_percentage: f64, validator_pass_rate: f64) -> f64 {
    let base = specificity as f64 / 100.0;
    let match_factor = (match_percentage / 50.0).clamp(0.0, 1.0);
    (base * match_factor * validator_pass_rate).clamp(0.0, 1.0)
}

/// Detect common data patterns in a column.
///
/// Analyzes string data to identify common patterns like emails, phone numbers,
/// URLs, etc. Uses pre-compiled regex patterns with per-pattern thresholds and
/// specificity-based overlap resolution to reduce false positives.
///
/// # Arguments
/// * `data` - Slice of string values to analyze
/// * `locale` - Optional ISO 3166-1 alpha-2 locale (e.g. "IT", "US", "GB").
///   When set, locale-matching patterns receive a confidence boost and
///   non-matching locale patterns are suppressed (unless match rate is very high).
///
/// # Returns
/// Vector of detected patterns sorted by confidence descending, after overlap
/// suppression and locale filtering.
pub fn detect_patterns(data: &[String], locale: Option<&str>) -> Vec<Pattern> {
    // Filter empty and whitespace-only strings for robust analysis
    let non_empty: Vec<&str> = data
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if non_empty.is_empty() {
        return Vec::new();
    }

    let n = non_empty.len();
    let num_patterns = PATTERN_DEFS.len();

    // Phase 1: Single-pass RegexSet pre-filter + per-row bitmaps
    //
    // For each value, `REGEX_SET.matches()` returns all pattern indices that
    // match in a single pass, avoiding redundant regex evaluation. We build
    // per-pattern match bitmaps from these results.
    let mut match_bitmaps: Vec<Vec<bool>> = vec![vec![false; n]; num_patterns];

    for (row_idx, value) in non_empty.iter().enumerate() {
        let matches = REGEX_SET.matches(value);
        for pat_idx in matches.iter() {
            match_bitmaps[pat_idx][row_idx] = true;
        }
    }

    let mut candidates: Vec<PatternMatch<'_>> = Vec::new();

    for (pat_idx, def) in PATTERN_DEFS.iter().enumerate() {
        let matched_rows = &match_bitmaps[pat_idx];
        let match_count = matched_rows.iter().filter(|&&m| m).count();
        let match_percentage = (match_count as f64 / n as f64) * 100.0;

        // Apply per-pattern threshold (replaces uniform >5% check)
        if match_percentage > def.min_threshold {
            // Run semantic validator on regex-matched values (if defined)
            let validator_pass_rate = match def.validator {
                Some(validate) => {
                    if match_count == 0 {
                        1.0
                    } else {
                        let passed = non_empty
                            .iter()
                            .zip(matched_rows.iter())
                            .filter(|&(s, matched)| *matched && validate(s))
                            .count();
                        passed as f64 / match_count as f64
                    }
                }
                None => 1.0,
            };

            candidates.push(PatternMatch {
                def,
                match_count,
                match_percentage,
                matched_rows: std::mem::take(&mut match_bitmaps[pat_idx]),
                validator_pass_rate,
            });
        }
    }

    // Phase 2: Specificity-based overlap suppression
    // When a more-specific pattern explains >=80% of a less-specific pattern's
    // matches, the less-specific one is noise and gets suppressed.
    // Equal-specificity patterns are never suppressed (resolved by locale in Phase 4).
    let mut suppressed = vec![false; candidates.len()];

    // Sort indices by specificity descending for pairwise comparison
    let mut indices: Vec<usize> = (0..candidates.len()).collect();
    indices.sort_by(|&a, &b| {
        candidates[b]
            .def
            .specificity
            .cmp(&candidates[a].def.specificity)
    });

    for i in 0..indices.len() {
        if suppressed[indices[i]] {
            continue;
        }
        let a_idx = indices[i];
        let a_spec = candidates[a_idx].def.specificity;

        for &b_idx in &indices[(i + 1)..] {
            if suppressed[b_idx] {
                continue;
            }
            let b_spec = candidates[b_idx].def.specificity;

            // Only suppress when specificity strictly differs
            if a_spec <= b_spec {
                continue;
            }

            let b_count = candidates[b_idx].match_count;
            if b_count == 0 {
                continue;
            }

            // Count how many of B's matches are also matched by A
            let overlap_count = candidates[a_idx]
                .matched_rows
                .iter()
                .zip(candidates[b_idx].matched_rows.iter())
                .filter(|&(a, b)| *a && *b)
                .count();

            let overlap_ratio = overlap_count as f64 / b_count as f64;
            if overlap_ratio >= 0.80 {
                suppressed[b_idx] = true;
            }
        }
    }

    // Phase 3: Build results with locale-adjusted confidence
    let mut results: Vec<Pattern> = candidates
        .iter()
        .enumerate()
        .filter(|(i, _)| !suppressed[*i])
        .filter_map(|(_, pm)| {
            let mut confidence = compute_confidence(
                pm.def.specificity,
                pm.match_percentage,
                pm.validator_pass_rate,
            );

            // Locale adjustments (Phase 4)
            if let Some(configured_locale) = locale {
                match pm.def.locale {
                    Some(pattern_locale) if pattern_locale == configured_locale => {
                        // Boost locale-matching patterns (cap at 1.0)
                        confidence = (confidence * 1.2).min(1.0);
                    }
                    Some(_)
                        // Suppress non-matching locale patterns with low confidence,
                        // unless the match rate is very high (data speaks for itself)
                        if confidence < 0.5 && pm.match_percentage <= 80.0 => {
                            return None;
                        }
                    Some(_) => {}
                    None => {} // Universal patterns — no adjustment
                }
            }

            Some(Pattern {
                name: pm.def.name.to_string(),
                regex: pm.def.regex.as_str().to_string(),
                match_count: pm.match_count,
                match_percentage: pm.match_percentage,
                category: pm.def.category.clone(),
                confidence,
            })
        })
        .collect();

    results.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    results
}

/// Check if a string value looks like a date
///
/// Uses pre-compiled regex patterns to detect common date formats.
/// This function consolidates date detection logic that was previously
/// duplicated across multiple engine files.
///
/// # Arguments
/// * `value` - String value to check
///
/// # Returns
/// `true` if the value matches any common date pattern
///
/// # Supported Formats
/// - YYYY-MM-DD (ISO 8601)
/// - DD/MM/YYYY (European slash)
/// - DD-MM-YYYY (European dash)
/// - YYYY/MM/DD (Asian format)
/// - DD.MM.YYYY (European dot)
/// - ISO datetime (YYYY-MM-DDTHH:...)
pub fn looks_like_date(value: &str) -> bool {
    DATE_PATTERN_REGEXES.iter().any(|re| re.is_match(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Pattern detection tests ----

    #[test]
    fn test_detect_email_pattern() {
        let data = vec![
            "user@example.com".to_string(),
            "admin@test.org".to_string(),
            "contact@company.com".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Email");
        assert_eq!(patterns[0].match_count, 3);
        assert_eq!(patterns[0].match_percentage, 100.0);
        assert_eq!(patterns[0].category, PatternCategory::Contact);
        assert!(patterns[0].confidence > 0.0);
    }

    #[test]
    fn test_detect_us_phone_pattern() {
        let data = vec![
            "(555) 123-4567".to_string(),
            "555-123-4567".to_string(),
            "5551234567".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Phone (US)");
        assert_eq!(patterns[0].category, PatternCategory::Contact);
    }

    #[test]
    fn test_detect_url_pattern() {
        let data = vec![
            "https://example.com".to_string(),
            "http://test.org/path".to_string(),
            "https://github.com/user/repo".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "URL");
        assert_eq!(patterns[0].match_percentage, 100.0);
        assert_eq!(patterns[0].category, PatternCategory::Network);
    }

    #[test]
    fn test_detect_uuid_pattern() {
        let data = vec![
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_string(),
            "123e4567-e89b-12d3-a456-426614174000".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "UUID");
        assert_eq!(patterns[0].category, PatternCategory::Identifier);
    }

    #[test]
    fn test_detect_no_patterns() {
        let data = vec![
            "random text".to_string(),
            "some string".to_string(),
            "no pattern".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_detect_mixed_data() {
        let data = vec![
            "user@example.com".to_string(),
            "random text".to_string(),
            "admin@test.org".to_string(),
            "more text".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        // 50% emails should be detected (threshold 3%)
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Email");
        assert_eq!(patterns[0].match_count, 2);
        assert_eq!(patterns[0].match_percentage, 50.0);
    }

    #[test]
    fn test_email_threshold_at_3_percent() {
        // 1 out of 40 = 2.5% - should NOT be detected (Email threshold = 3%)
        let mut data = vec!["user@example.com".to_string()];
        for _ in 0..39 {
            data.push("random text".to_string());
        }
        let patterns = detect_patterns(&data, None);
        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_email_threshold_above_3_percent() {
        // 2 out of 20 = 10% - should be detected (Email threshold = 3%)
        let mut data = vec!["user@example.com".to_string(), "admin@test.org".to_string()];
        for _ in 0..18 {
            data.push("random text".to_string());
        }
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].match_percentage, 10.0);
    }

    #[test]
    fn test_empty_data() {
        let data: Vec<String> = vec![];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_all_empty_strings() {
        let data = vec!["".to_string(), "".to_string(), "".to_string()];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_whitespace_handling() {
        let data = vec![
            " user@example.com ".to_string(),
            "  admin@test.org".to_string(),
            "contact@company.com  ".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Email");
        assert_eq!(patterns[0].match_count, 3);
    }

    #[test]
    fn test_whitespace_only_strings() {
        let data = vec!["  ".to_string(), "\t".to_string(), " \n ".to_string()];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_detect_ipv4_pattern() {
        let data = vec![
            "192.168.1.1".to_string(),
            "10.0.0.1".to_string(),
            "255.255.255.0".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "IPv4");
        assert_eq!(patterns[0].match_count, 3);
        assert_eq!(patterns[0].match_percentage, 100.0);
        assert_eq!(patterns[0].category, PatternCategory::Network);
    }

    #[test]
    fn test_detect_ipv6_pattern() {
        let data = vec![
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string(),
            "fe80:0000:0000:0000:0204:61ff:fe9d:f156".to_string(),
            "2001:0db8:0001:0000:0000:0ab9:C0A8:0102".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "IPv6");
        assert_eq!(patterns[0].category, PatternCategory::Network);
    }

    #[test]
    fn test_detect_mac_address_pattern() {
        let data = vec![
            "00:1B:44:11:3A:B7".to_string(),
            "A0-B1-C2-D3-E4-F5".to_string(),
            "aa:bb:cc:dd:ee:ff".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "MAC Address");
        assert_eq!(patterns[0].category, PatternCategory::Network);
    }

    #[test]
    fn test_detect_coordinates_pattern() {
        let data = vec![
            "41.9028, 12.4964".to_string(),   // Rome
            "40.7128, -74.0060".to_string(),  // New York
            "-33.8688, 151.2093".to_string(), // Sydney
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Geographic Coordinates");
        assert_eq!(patterns[0].category, PatternCategory::Geographic);
    }

    #[test]
    fn test_detect_iban_pattern() {
        let data = vec![
            "IT60X0542811101000000123456".to_string(),
            "GB82WEST12345698765432".to_string(),
            "DE89370400440532013000".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "IBAN");
        assert_eq!(patterns[0].category, PatternCategory::Financial);
    }

    #[test]
    fn test_detect_codice_fiscale_pattern() {
        let data = vec![
            "RSSMRA85M01H501Q".to_string(),
            "BNCGVN90A01F205O".to_string(),
            "VRDLCU75D15L219V".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Codice Fiscale (IT)");
        assert_eq!(patterns[0].category, PatternCategory::Identifier);
    }

    #[test]
    fn test_detect_piva_pattern() {
        // 100% 11-digit numbers — well above 25% threshold
        let data = vec![
            "12345678901".to_string(),
            "98765432109".to_string(),
            "11111111111".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert!(!patterns.is_empty());
        assert!(patterns.iter().any(|p| p.name == "P.IVA (IT)"));
    }

    #[test]
    fn test_piva_below_threshold() {
        // P.IVA threshold is 25%. 4 out of 20 = 20% — should NOT be detected.
        let mut data: Vec<String> = (0..4).map(|i| format!("{:011}", i)).collect();
        for _ in 0..16 {
            data.push("random text".to_string());
        }
        let patterns = detect_patterns(&data, None);
        assert!(!patterns.iter().any(|p| p.name == "P.IVA (IT)"));
    }

    #[test]
    fn test_piva_above_threshold() {
        // 6 out of 20 = 30% — above 25% threshold
        let mut data: Vec<String> = (0..6).map(|i| format!("{:011}", i)).collect();
        for _ in 0..14 {
            data.push("random text".to_string());
        }
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "P.IVA (IT)"));
    }

    #[test]
    fn test_detect_cap_pattern() {
        let data = vec![
            "00118".to_string(),
            "20121".to_string(),
            "10100".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        // Both CAP and ZIP have specificity 35 (equal), so neither suppresses the other
        assert!(!patterns.is_empty());
        assert!(
            patterns
                .iter()
                .any(|p| p.name == "CAP (IT)" || p.name == "ZIP Code (US)")
        );
    }

    #[test]
    fn test_detect_zip_code_pattern() {
        let data = vec![
            "12345".to_string(),
            "90210-1234".to_string(),
            "10001".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert!(!patterns.is_empty());
        // ZIP Code (US) should match — the extended format (90210-1234) only matches ZIP, not CAP
        assert!(patterns.iter().any(|p| p.name == "ZIP Code (US)"));
    }

    #[test]
    fn test_detect_unix_file_path_pattern() {
        let data = vec![
            "/home/user/documents".to_string(),
            "/var/log/system.log".to_string(),
            "/etc/config/app.conf".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "File Path (Unix)");
        assert_eq!(patterns[0].category, PatternCategory::FilePath);
    }

    #[test]
    fn test_detect_windows_file_path_pattern() {
        let data = vec![
            "C:\\Users\\Admin\\Documents".to_string(),
            "D:\\Projects\\myapp\\src".to_string(),
            "E:\\Data\\file.txt".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "File Path (Windows)");
        assert_eq!(patterns[0].category, PatternCategory::FilePath);
    }

    // ---- Overlap suppression tests ----

    #[test]
    fn test_overlap_suppression_high_specificity_wins() {
        // All data matches both IPv4 (specificity 65) and could match a broad
        // pattern. The more-specific pattern should survive.
        let data = vec![
            "192.168.1.1".to_string(),
            "10.0.0.1".to_string(),
            "172.16.0.1".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "IPv4");
    }

    #[test]
    fn test_equal_specificity_no_suppression() {
        // CAP (IT) and ZIP Code (US) both have specificity 35.
        // Pure 5-digit data should return both (no suppression at equal specificity).
        let data: Vec<String> = (10000..10020).map(|n| n.to_string()).collect();
        let patterns = detect_patterns(&data, None);
        let cap = patterns.iter().any(|p| p.name == "CAP (IT)");
        let zip = patterns.iter().any(|p| p.name == "ZIP Code (US)");
        assert!(
            cap && zip,
            "Both CAP and ZIP should survive at equal specificity"
        );
    }

    // ---- Confidence scoring tests ----

    #[test]
    fn test_confidence_increases_with_match_rate() {
        // Higher match rate → higher confidence for the same pattern
        let data_low: Vec<String> = {
            let mut d: Vec<String> = (0..3).map(|_| "user@example.com".to_string()).collect();
            for _ in 0..27 {
                d.push("random text".to_string());
            }
            d
        };
        let data_high = vec![
            "user@example.com".to_string(),
            "admin@test.org".to_string(),
            "hello@world.com".to_string(),
        ];

        let p_low = detect_patterns(&data_low, None);
        let p_high = detect_patterns(&data_high, None);

        assert_eq!(p_low.len(), 1);
        assert_eq!(p_high.len(), 1);
        assert!(
            p_high[0].confidence >= p_low[0].confidence,
            "100% match should have >= confidence than 10% match"
        );
    }

    #[test]
    fn test_confidence_sorted_descending() {
        // Mix different pattern types — results should be sorted by confidence
        let mut data = vec![
            "user@example.com".to_string(),
            "admin@test.org".to_string(),
            "/home/user/docs".to_string(),
            "/var/log/app.log".to_string(),
            "/etc/hosts".to_string(),
            "random text".to_string(),
        ];
        // Add more file paths to ensure it passes the 10% threshold
        for _ in 0..10 {
            data.push("/usr/local/bin/tool".to_string());
        }
        let patterns = detect_patterns(&data, None);

        for w in patterns.windows(2) {
            assert!(
                w[0].confidence >= w[1].confidence,
                "Patterns should be sorted by confidence descending: {} ({}) vs {} ({})",
                w[0].name,
                w[0].confidence,
                w[1].name,
                w[1].confidence
            );
        }
    }

    #[test]
    fn test_confidence_range() {
        let data = vec![
            "user@example.com".to_string(),
            "admin@test.org".to_string(),
            "hello@world.com".to_string(),
        ];
        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert!(patterns[0].confidence > 0.0 && patterns[0].confidence <= 1.0);
    }

    #[test]
    fn test_confidence_stays_low_for_singleton_like_matches() {
        let mut data = vec!["user@example.com".to_string()];
        for _ in 0..29 {
            data.push("random text".to_string());
        }

        let patterns = detect_patterns(&data, None);

        assert_eq!(patterns.len(), 1);
        assert!(
            patterns[0].confidence < 0.1,
            "singleton-like matches should not look highly confident"
        );
    }

    // ---- Validator integration tests ----

    #[test]
    fn test_validator_reduces_confidence_for_invalid_ibans() {
        // Mix of valid and invalid IBANs — all match the regex, but validator
        // pass rate < 1.0 should reduce confidence compared to all-valid.
        let all_valid = vec![
            "GB82WEST12345698765432".to_string(),
            "DE89370400440532013000".to_string(),
            "FR7630006000011234567890189".to_string(),
        ];
        let mixed = vec![
            "GB82WEST12345698765432".to_string(),      // Valid
            "DE89370400440532013001".to_string(),      // Invalid checksum
            "FR7630006000011234567890180".to_string(), // Invalid checksum
        ];

        let p_valid = detect_patterns(&all_valid, None);
        let p_mixed = detect_patterns(&mixed, None);

        assert_eq!(p_valid.len(), 1);
        assert_eq!(p_mixed.len(), 1);
        assert_eq!(p_valid[0].name, "IBAN");
        assert_eq!(p_mixed[0].name, "IBAN");
        assert!(
            p_valid[0].confidence > p_mixed[0].confidence,
            "All-valid IBANs ({:.3}) should have higher confidence than mixed ({:.3})",
            p_valid[0].confidence,
            p_mixed[0].confidence,
        );
    }

    #[test]
    fn test_validator_cap_filters_out_of_range() {
        // All match ^\d{5}$ but some are outside CAP range (00010-98168)
        // This tests that the validator pass rate reduces confidence.
        let valid_caps = vec![
            "00118".to_string(), // Roma
            "20121".to_string(), // Milano
            "80100".to_string(), // Napoli
        ];
        let invalid_caps = vec![
            "99999".to_string(), // Out of range
            "99998".to_string(), // Out of range
            "99997".to_string(), // Out of range
        ];

        let p_valid = detect_patterns(&valid_caps, None);
        let p_invalid = detect_patterns(&invalid_caps, None);

        let cap_valid = p_valid.iter().find(|p| p.name == "CAP (IT)");
        let cap_invalid = p_invalid.iter().find(|p| p.name == "CAP (IT)");

        // Valid CAPs should have higher confidence than invalid ones
        if let (Some(v), Some(iv)) = (cap_valid, cap_invalid) {
            assert!(
                v.confidence > iv.confidence,
                "Valid CAPs ({:.3}) should have higher confidence than invalid ({:.3})",
                v.confidence,
                iv.confidence,
            );
        }
        // At minimum, valid CAPs should be detected
        assert!(cap_valid.is_some(), "Valid CAPs should be detected");
    }

    #[test]
    fn test_validator_piva_check_digit() {
        // Valid P.IVA: 12345678903 (computed check digit)
        // Invalid: 12345678901 (wrong check digit)
        let valid = vec![
            "12345678903".to_string(),
            "12345678903".to_string(),
            "12345678903".to_string(),
            "00000000000".to_string(),
        ];
        let invalid = vec![
            "12345678901".to_string(),
            "12345678902".to_string(),
            "99999999999".to_string(),
            "11111111111".to_string(),
        ];

        let p_valid = detect_patterns(&valid, None);
        let p_invalid = detect_patterns(&invalid, None);

        let piva_valid = p_valid.iter().find(|p| p.name == "P.IVA (IT)");
        let piva_invalid = p_invalid.iter().find(|p| p.name == "P.IVA (IT)");

        assert!(piva_valid.is_some(), "Valid P.IVA should be detected");
        // Both should be detected (regex matches), but valid should have higher confidence
        if let (Some(v), Some(iv)) = (piva_valid, piva_invalid) {
            assert!(
                v.confidence > iv.confidence,
                "Valid P.IVA ({:.3}) should have higher confidence than invalid ({:.3})",
                v.confidence,
                iv.confidence,
            );
        }
    }

    // ---- Phase 3 pattern tests ----

    #[test]
    fn test_detect_credit_card_pattern() {
        let data = vec![
            "4532015112830366".to_string(),
            "5425233430109903".to_string(),
            "4111111111111111".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "Credit Card"));
    }

    #[test]
    fn test_detect_ssn_pattern() {
        let data = vec![
            "123-45-6789".to_string(),
            "234-56-7890".to_string(),
            "345-67-8901".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "SSN (US)"));
    }

    #[test]
    fn test_detect_uk_postcode_pattern() {
        let data = vec![
            "SW1A 1AA".to_string(),
            "EC1A 1BB".to_string(),
            "W1J 7NT".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "UK Postcode"));
    }

    #[test]
    fn test_detect_canadian_postal_pattern() {
        let data = vec![
            "K1A 0B1".to_string(),
            "V6B 3K9".to_string(),
            "M5V 2T6".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "Canadian Postal Code"));
    }

    #[test]
    fn test_detect_hex_color_pattern() {
        let data = vec![
            "#FF5733".to_string(),
            "#00FF00".to_string(),
            "#0A0B0C".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "Hex Color"));
    }

    #[test]
    fn test_detect_swift_bic_pattern() {
        let data = vec![
            "DEUTDEFF".to_string(),
            "BNPAFRPP".to_string(),
            "CHASUS33XXX".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "SWIFT/BIC"));
    }

    #[test]
    fn test_detect_percentage_pattern() {
        let data = vec!["12.5%".to_string(), "99%".to_string(), "0.1%".to_string()];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "Percentage"));
    }

    #[test]
    fn test_detect_currency_pattern() {
        let data = vec![
            "$1,234.56".to_string(),
            "$99.99".to_string(),
            "$1,000".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "Currency"));
    }

    #[test]
    fn test_detect_scientific_notation_pattern() {
        let data = vec![
            "1.23e+04".to_string(),
            "5.67E-03".to_string(),
            "-9.81e+00".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "Scientific Notation"));
    }

    #[test]
    fn test_detect_ipv6_compressed() {
        // Phase 3 fix: IPv6 with :: compression should now be detected
        let data = vec![
            "::1".to_string(),
            "fe80::1".to_string(),
            "2001:db8::1".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(
            patterns.iter().any(|p| p.name == "IPv6"),
            "Compressed IPv6 addresses should be detected"
        );
    }

    #[test]
    fn test_detect_date_iso_pattern() {
        let data = vec![
            "2024-01-15".to_string(),
            "2023-12-25".to_string(),
            "2022-06-01".to_string(),
        ];
        let patterns = detect_patterns(&data, None);
        assert!(patterns.iter().any(|p| p.name == "Date (ISO)"));
    }

    // ---- Date detection tests (looks_like_date helper) ----

    #[test]
    fn test_looks_like_date_function() {
        // Test various date formats
        assert!(looks_like_date("2024-01-15"));
        assert!(looks_like_date("15/01/2024"));
        assert!(looks_like_date("15-01-2024"));
        assert!(looks_like_date("2024/01/15"));
        assert!(looks_like_date("15.01.2024"));
        assert!(looks_like_date("2024-01-15T10:30:00"));

        // Test non-dates
        assert!(!looks_like_date("not a date"));
        assert!(!looks_like_date("12345"));
        assert!(!looks_like_date("user@example.com"));
    }

    #[test]
    fn test_date_pattern_regexes_precompiled() {
        assert_eq!(DATE_PATTERN_REGEXES.len(), 6);
    }

    #[test]
    fn test_pattern_defs_compiled() {
        assert_eq!(PATTERN_DEFS.len(), 35);
    }

    // ---- RegexSet tests ----

    #[test]
    fn test_regex_set_matches_all_patterns() {
        // Ensure the RegexSet has the same number of patterns as PATTERN_DEFS
        assert_eq!(REGEX_SET.len(), PATTERN_DEFS.len());
    }

    #[test]
    fn test_regex_set_prefilter_matches_individual() {
        // Verify RegexSet matches agree with individual regex matches
        let test_values = vec![
            "user@example.com",
            "192.168.1.1",
            "2001:db8::1",
            "IT60X0542811101000000123456",
            "RSSMRA85M01H501Q",
            "12345",
            "hello world",
            "4111111111111111",
            "$1,234.56",
        ];

        for value in &test_values {
            let set_matches: Vec<usize> = REGEX_SET.matches(value).iter().collect();
            for (idx, def) in PATTERN_DEFS.iter().enumerate() {
                let individual = def.regex.is_match(value);
                let in_set = set_matches.contains(&idx);
                assert_eq!(
                    individual, in_set,
                    "Mismatch for pattern '{}' on value '{}': individual={}, set={}",
                    def.name, value, individual, in_set
                );
            }
        }
    }

    // ---- Locale tests ----

    #[test]
    fn test_locale_it_boosts_cap_confidence() {
        let data: Vec<String> = (20100..=20200).map(|n| format!("{:05}", n)).collect();
        let without_locale = detect_patterns(&data, None);
        let with_locale = detect_patterns(&data, Some("IT"));

        let cap_no_locale = without_locale.iter().find(|p| p.name == "CAP (IT)");
        let cap_it_locale = with_locale.iter().find(|p| p.name == "CAP (IT)");

        assert!(
            cap_no_locale.is_some(),
            "CAP should be detected without locale"
        );
        assert!(
            cap_it_locale.is_some(),
            "CAP should be detected with IT locale"
        );
        assert!(
            cap_it_locale.unwrap().confidence > cap_no_locale.unwrap().confidence,
            "IT locale should boost CAP confidence"
        );
    }

    #[test]
    fn test_locale_us_suppresses_non_matching_low_match_rate() {
        // With US locale, low-confidence IT patterns should be suppressed
        // when match rate is not overwhelmingly high
        let mut data: Vec<String> = (20100..=20130).map(|n| format!("{:05}", n)).collect();
        // Add non-matching rows to bring match percentage below 80%
        for i in 0..100 {
            data.push(format!("text_{}", i));
        }
        let with_us = detect_patterns(&data, Some("US"));

        // CAP (IT) has low specificity (35) and locale=IT.
        // With US locale and low match rate, it should be suppressed
        let cap_us = with_us.iter().find(|p| p.name == "CAP (IT)");
        assert!(
            cap_us.is_none(),
            "CAP (IT) should be suppressed with US locale when match rate is low"
        );
    }

    #[test]
    fn test_locale_high_match_rate_keeps_non_matching() {
        // Even with non-matching locale, very high match rate (>80%) keeps the pattern
        let data: Vec<String> = (20100..=20200).map(|n| format!("{:05}", n)).collect();
        let with_us = detect_patterns(&data, Some("US"));

        // CAP (IT) has 100% match rate, so it survives despite US locale
        let cap_us = with_us.iter().find(|p| p.name == "CAP (IT)");
        assert!(
            cap_us.is_some(),
            "CAP (IT) should survive with US locale when match rate >80% (data speaks for itself)"
        );
    }

    #[test]
    fn test_locale_none_keeps_all() {
        // Without locale, both CAP and ZIP should be present (equal specificity)
        let data: Vec<String> = (10001..=10050).map(|n| format!("{}", n)).collect();
        let patterns = detect_patterns(&data, None);

        // At least one 5-digit pattern should survive
        let has_five_digit = patterns.iter().any(|p| {
            p.name == "CAP (IT)"
                || p.name == "ZIP Code (US)"
                || p.name == "German PLZ"
                || p.name == "French Code Postal"
        });
        assert!(
            has_five_digit,
            "Some 5-digit pattern should be detected without locale"
        );
    }

    // ---- End-to-end tests ----

    #[test]
    fn test_e2e_mixed_column_patterns() {
        // Simulate a column with mostly emails and some noise
        let mut data: Vec<String> = (0..100).map(|i| format!("user{}@example.com", i)).collect();
        data.push("not_an_email".to_string());
        data.push("also not".to_string());

        let patterns = detect_patterns(&data, None);

        assert!(!patterns.is_empty());
        assert_eq!(patterns[0].name, "Email", "Email should be the top pattern");
        assert!(
            patterns[0].confidence > 0.5,
            "Email confidence should be high"
        );
        assert_eq!(patterns[0].category, PatternCategory::Contact);
    }

    #[test]
    fn test_e2e_all_pattern_categories_present() {
        // Verify that the pattern system covers all expected categories
        let categories: Vec<&PatternCategory> = PATTERN_DEFS.iter().map(|d| &d.category).collect();
        assert!(categories.contains(&&PatternCategory::Contact));
        assert!(categories.contains(&&PatternCategory::Identifier));
        assert!(categories.contains(&&PatternCategory::Network));
        assert!(categories.contains(&&PatternCategory::Geographic));
        assert!(categories.contains(&&PatternCategory::Financial));
        assert!(categories.contains(&&PatternCategory::FilePath));
        assert!(categories.contains(&&PatternCategory::Other));
    }

    #[test]
    fn test_e2e_confidence_never_exceeds_one() {
        let data: Vec<String> = (0..200).map(|i| format!("user{}@example.com", i)).collect();
        let patterns = detect_patterns(&data, Some("US"));

        for p in &patterns {
            assert!(
                p.confidence <= 1.0,
                "Confidence for {} should not exceed 1.0, got {}",
                p.name,
                p.confidence
            );
        }
    }
}
