//! The closed set of locales the pattern catalogue carries detectors for.
//!
//! A locale is *strict*: setting one suppresses patterns belonging to any other
//! locale. That strictness is right for a tag the catalogue knows, and wrong for
//! one it does not — an unrecognised tag used to be accepted and behave as
//! "suppress every locale-specific pattern", so `locale="it-IT"` returned no
//! patterns where `locale="IT"` returned a confident match, with nothing in the
//! report saying the tag was not understood. Parsing a tag into [`Locale`]
//! before it can be stored keeps that failure out of the type: the common
//! spellings normalise, and anything left over is an error naming the set.

/// A locale the pattern catalogue carries locale-specific detectors for.
///
/// Parse a user-supplied tag with [`str::parse`] or
/// [`Locale::parse_optional`]; the variants are also usable directly
/// (`Locale::It`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Locale {
    /// Canada
    Ca,
    /// Germany
    De,
    /// France
    Fr,
    /// United Kingdom
    Gb,
    /// Italy
    It,
    /// United States
    Us,
}

impl Locale {
    /// Every supported locale, in the order error messages list them.
    pub fn all() -> Vec<Self> {
        vec![Self::Ca, Self::De, Self::Fr, Self::Gb, Self::It, Self::Us]
    }

    /// The ISO 3166-1 alpha-2 code, as the pattern catalogue spells it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ca => "CA",
            Self::De => "DE",
            Self::Fr => "FR",
            Self::Gb => "GB",
            Self::It => "IT",
            Self::Us => "US",
        }
    }

    /// The ISO 3166-1 alpha-3 code, accepted as an alternative spelling.
    fn alpha3(&self) -> &'static str {
        match self {
            Self::Ca => "CAN",
            Self::De => "DEU",
            Self::Fr => "FRA",
            Self::Gb => "GBR",
            Self::It => "ITA",
            Self::Us => "USA",
        }
    }

    /// Parse an optional tag, where absent and blank both mean "no locale".
    ///
    /// This is the boundary helper for callers who receive a tag from a user or
    /// another language: `None` and `""` are the same request (rank patterns
    /// without a locale preference), and every other value must resolve.
    ///
    /// ```
    /// use dataprof_core::Locale;
    ///
    /// assert_eq!(Locale::parse_optional(None), Ok(None));
    /// assert_eq!(Locale::parse_optional(Some("  ")), Ok(None));
    /// assert_eq!(Locale::parse_optional(Some("it-IT")), Ok(Some(Locale::It)));
    /// assert!(Locale::parse_optional(Some("XX")).is_err());
    /// ```
    pub fn parse_optional(tag: Option<&str>) -> Result<Option<Self>, String> {
        match tag.map(str::trim) {
            None | Some("") => Ok(None),
            Some(tag) => tag.parse().map(Some),
        }
    }

    /// Resolve a normalised tag, or `None` when it names no supported locale.
    ///
    /// A single subtag is read as a region code (`"IT"`, `"it"`, `"ITA"`). With
    /// more than one, the region is the last subtag, so both the BCP 47 and the
    /// POSIX spellings resolve (`"it-IT"`, `"it_IT"`, `"en-GB"`). A tag naming a
    /// region the catalogue has no patterns for (`"de-CH"`) does not fall back
    /// to its language subtag: it names a locale dataprof does not support, and
    /// answering with Germany's patterns would be a guess.
    fn resolve(tag: &str) -> Option<Self> {
        let region = tag
            .split(['-', '_'])
            .rfind(|subtag| !subtag.is_empty())?
            .to_ascii_uppercase();

        Self::all()
            .into_iter()
            .find(|locale| locale.as_str() == region || locale.alpha3() == region)
    }
}

impl std::str::FromStr for Locale {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::resolve(s).ok_or_else(|| {
            let supported = Self::all()
                .iter()
                .map(|locale| locale.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "Unknown locale: '{s}'. Supported locales: {supported} (ISO 3166-1 alpha-2). \
                 'it', 'ITA' and 'it-IT' are accepted spellings of 'IT'."
            )
        })
    }
}

impl std::fmt::Display for Locale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spellings_of_the_same_locale_resolve_alike() {
        for tag in ["IT", "it", "It", " it ", "ITA", "ita", "it-IT", "it_IT"] {
            assert_eq!(
                Locale::parse_optional(Some(tag)),
                Ok(Some(Locale::It)),
                "tag {tag:?} did not resolve to IT"
            );
        }
    }

    #[test]
    fn the_region_subtag_decides_not_the_language() {
        // en-GB and en-US share a language and select different catalogues.
        assert_eq!(Locale::parse_optional(Some("en-GB")), Ok(Some(Locale::Gb)));
        assert_eq!(Locale::parse_optional(Some("en-US")), Ok(Some(Locale::Us)));
        assert_eq!(Locale::parse_optional(Some("fr-CA")), Ok(Some(Locale::Ca)));
    }

    #[test]
    fn absent_and_blank_mean_no_locale() {
        assert_eq!(Locale::parse_optional(None), Ok(None));
        assert_eq!(Locale::parse_optional(Some("")), Ok(None));
        assert_eq!(Locale::parse_optional(Some("   ")), Ok(None));
    }

    #[test]
    fn an_unsupported_region_is_an_error_not_a_fallback() {
        // Switzerland has no catalogue; resolving de-CH to Germany would be a
        // guess, and accepting it silently is the bug this type exists for.
        for tag in ["XX", "de-CH", "es", "en", "ZZZZ", "it-IT-u-ca-gregory"] {
            assert!(
                tag.parse::<Locale>().is_err(),
                "tag {tag:?} was accepted as a locale"
            );
        }
    }

    #[test]
    fn the_error_names_the_supported_set() {
        let error = "it-IT-u-ca-gregory".parse::<Locale>().unwrap_err();
        assert!(error.contains("it-IT-u-ca-gregory"), "{error}");
        for locale in Locale::all() {
            assert!(error.contains(locale.as_str()), "{error}");
        }
    }

    #[test]
    fn display_round_trips_through_parse() {
        for locale in Locale::all() {
            assert_eq!(locale.to_string().parse::<Locale>(), Ok(locale));
        }
    }
}
