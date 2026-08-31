//! The unit every reported text length is measured in.
//!
//! `TextStats::min_length`, `max_length` and `avg_length` count **Unicode
//! scalar values**, not UTF-8 bytes and not grapheme clusters.
//!
//! Bytes were the accidental unit until 0.12: every accumulator reached for
//! Rust's `str::len()`, so `"東京"` reported a length of 6 and `"🙂"` a length
//! of 4 under a field named `max_length` (#627). The counts were consistent
//! across every engine, and consistently surprising for anything but ASCII.
//!
//! Scalar values are the least surprising interpretation that costs no
//! dependency. Grapheme clusters are closer to what a reader would call a
//! character — `"e\u{0301}"` is one grapheme and two scalars — but segmenting
//! them correctly needs a Unicode segmentation table and a policy for which
//! version of it, which is a larger commitment than a profiler's length field
//! justifies. Encoded size is a property of the encoding rather than the value,
//! and is available at the source level; it is deliberately not reported under
//! a name like "length".
//!
//! ASCII is unaffected: for ASCII text all three units agree.

/// Number of Unicode scalar values in `value`.
///
/// This is the single definition of "length" behind every text statistic, so
/// the CSV, JSON, Parquet, Arrow and in-memory paths cannot drift apart on what
/// they are counting.
#[inline]
pub fn char_len(value: &str) -> usize {
    // `Chars::count` counts non-continuation bytes rather than decoding each
    // scalar, so this stays close to `len()` in cost.
    value.chars().count()
}

#[cfg(test)]
mod tests {
    use super::char_len;

    #[test]
    fn ascii_is_unchanged() {
        for value in ["", "a", "hello", "1234567890"] {
            assert_eq!(char_len(value), value.len(), "{value:?}");
        }
    }

    #[test]
    fn multibyte_scalars_count_once() {
        // The issue's reproduction: UTF-8 widths are 1, 2, 6, 4.
        assert_eq!(char_len("a"), 1);
        assert_eq!(char_len("é"), 1);
        assert_eq!(char_len("東京"), 2);
        assert_eq!(char_len("🙂"), 1);
    }

    #[test]
    fn a_combining_sequence_counts_each_scalar() {
        // Two scalars, one grapheme. Documented rather than segmented: the
        // precomposed and decomposed spellings of the same word differ in
        // length, which is a property of scalar counting, not a defect.
        let precomposed = "\u{00e9}";
        let decomposed = "e\u{0301}";
        assert_eq!(char_len(precomposed), 1);
        assert_eq!(char_len(decomposed), 2);
        // They differ under the old byte unit too, by a different amount, so
        // neither unit makes the two spellings compare equal.
        assert_eq!((precomposed.len(), decomposed.len()), (2, 3));
    }

    #[test]
    fn an_emoji_sequence_counts_each_scalar() {
        // A ZWJ family is one grapheme and seven scalars.
        assert_eq!(char_len("👨‍👩‍👧‍👦"), 7);
    }
}
