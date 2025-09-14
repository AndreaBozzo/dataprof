use regex::Regex;

use crate::types::Pattern;

pub fn detect_patterns(data: &[String]) -> Vec<Pattern> {
    let mut patterns = Vec::new();
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();

    if non_empty.is_empty() {
        return patterns;
    }

    // Common patterns to check
    let pattern_checks = [
        ("Email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
        (
            "Phone (US)",
            r"^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$",
        ),
        (
            "Phone (IT)",
            r"^\+39|0039|39?[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{5,10}$",
        ),
        ("URL", r"^https?://[^\s/$.?#].[^\s]*$"),
        (
            "UUID",
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        ),
    ];

    for (name, pattern_str) in &pattern_checks {
        if let Ok(regex) = Regex::new(pattern_str) {
            let matches = non_empty.iter().filter(|s| regex.is_match(s)).count();
            let percentage = (matches as f64 / non_empty.len() as f64) * 100.0;

            if percentage > 5.0 {
                // Only show patterns with >5% matches
                patterns.push(Pattern {
                    name: name.to_string(),
                    regex: pattern_str.to_string(),
                    match_count: matches,
                    match_percentage: percentage,
                });
            }
        }
    }

    patterns
}
