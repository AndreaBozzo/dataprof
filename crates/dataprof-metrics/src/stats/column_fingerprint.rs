use chrono::{Datelike, NaiveDate, NaiveDateTime};
use dataprof_core::{LexicalClass, SlashDateOrder};
use twox_hash::XxHash3_128;

use super::super::datetime::resolve_slash_order;
use super::{
    CellFingerprint, CellText, DATE, DECIMAL, FLOAT, NAIVE, NULL_TAG, TEXT, WIDE_DECIMAL,
    WideDecimal, date_bytes, encode_classified_text, encode_float, encode_text, timestamp,
};

/// The canonical multiset of a column's scalar values. Nulls are added from
/// the caller's count, including padding for columns discovered mid-stream.
#[derive(Clone, Debug, Default)]
pub struct ColumnFingerprint {
    cells: CellFingerprint,
    encoded: Vec<u8>,
    unavailable: bool,
    day_first: bool,
    month_first: bool,
    /// The sum of (month-first hash minus day-first hash) for ambiguous dates.
    /// A later row or another partition can decide how to read them.
    month_first_delta: u128,
}

impl ColumnFingerprint {
    pub fn update_text(&mut self, value: &str) {
        self.update_text_value(value, None, false);
    }

    /// [`Self::update_text`] for a value the caller has already classified
    /// with `LexicalTally::record`, which returns `None` for a null-like
    /// value. Encodes the same bytes without classifying the value again.
    pub fn update_classified_text(&mut self, value: &str, class: Option<LexicalClass>) {
        match class {
            Some(class) => self.update_text_value(value, Some(class), false),
            None => self.update_text(value),
        }
    }

    /// A known decimal cannot fall back to a binary float's representation.
    pub fn update_decimal_text(&mut self, value: &str) {
        self.update_text_value(value, None, true);
    }

    /// Encode a floating-point value at the precision used by the profiler.
    pub fn update_float(&mut self, value: f64) {
        if !self.unavailable {
            self.encoded.clear();
            encode_float(value, format_args!("{value}"), &mut self.encoded);
            self.update_encoded();
        }
    }

    fn update_text_value(
        &mut self,
        value: &str,
        class: Option<LexicalClass>,
        declared_decimal: bool,
    ) {
        if self.unavailable {
            return;
        }
        self.encoded.clear();
        match class {
            Some(class) => encode_classified_text(value, class, &mut self.encoded),
            None => encode_text(Some(value), &mut self.encoded),
        }
        let tag = self.encoded[0];
        // A float writer may use %.17g instead of the shortest spelling
        // (#783, #1182). Reuse the binary float encoder for that same value.
        // A declared decimal always keeps its literal precision.
        if !declared_decimal
            && (tag == WIDE_DECIMAL || (tag == DECIMAL && self.encoded.len() >= 18))
            && let Some(float) = padded_float(value.trim())
        {
            self.update_float(float);
            return;
        }
        // A float encoding of a decimal literal can also occur when the
        // exponent cannot be represented even by the wide-decimal encoder.
        if tag == FLOAT {
            let trimmed = value.trim();
            if (declared_decimal && WideDecimal::read(trimmed).is_some())
                || (trimmed.contains(['e', 'E']) && WideDecimal::read(trimmed).is_none())
            {
                self.withhold();
                return;
            }
        }
        self.update_encoded();
        if matches!(tag, DATE | NAIVE | TEXT) {
            let trimmed = value.trim_start();
            if trimmed.as_bytes().first().is_some_and(u8::is_ascii_digit) && trimmed.contains('/') {
                self.update_slash_date(trimmed.trim_end(), tag);
            }
        }
    }

    fn update_slash_date(&mut self, value: &str, tag: u8) {
        match resolve_slash_order(&[value]) {
            Some(SlashDateOrder::DayFirst) => self.day_first = true,
            Some(SlashDateOrder::MonthFirst) => self.month_first = true,
            // The date reader accepts forms beyond the lexical grammar (e.g.
            // a short year). They can decide the order, but their pinned
            // encoding remains text.
            Some(SlashDateOrder::AssumedDayFirst) if matches!(tag, DATE | NAIVE) => {
                let day_first = XxHash3_128::oneshot(&self.encoded);
                self.encoded.clear();
                if tag == DATE {
                    let date = NaiveDate::parse_from_str(value, "%m/%d/%Y")
                        .expect("ambiguous slash date also parses month-first");
                    date_bytes(date.year(), date.month(), date.day(), &mut self.encoded);
                } else {
                    let time = NaiveDateTime::parse_from_str(value, "%m/%d/%Y %H:%M:%S")
                        .expect("ambiguous slash datetime also parses month-first")
                        .and_utc();
                    timestamp(
                        time.timestamp(),
                        time.timestamp_subsec_nanos(),
                        NAIVE,
                        &mut self.encoded,
                    );
                }
                let month_first = XxHash3_128::oneshot(&self.encoded);
                self.month_first_delta = self
                    .month_first_delta
                    .wrapping_add(month_first.wrapping_sub(day_first));
            }
            _ => {}
        }
    }

    /// Read a scalar array after the caller decodes dictionary or run-end
    /// encoding. String arrays use `update_text` so slash order is tracked.
    #[cfg(feature = "arrow")]
    pub fn update_arrow(
        &mut self,
        array: &dyn arrow_array::Array,
    ) -> Result<(), arrow_schema::ArrowError> {
        if !super::supports_arrow_type(array.data_type()) {
            self.withhold();
        }
        if self.unavailable || matches!(array.data_type(), arrow_schema::DataType::Null) {
            return Ok(());
        }
        for index in 0..array.len() {
            if array.is_valid(index) {
                self.encoded.clear();
                super::encode_arrow(array, index, &mut self.encoded)?;
                self.update_encoded();
                if self.unavailable {
                    break;
                }
            }
        }
        Ok(())
    }

    fn update_encoded(&mut self) {
        match self.encoded[0] {
            NULL_TAG => {} // folded in from the column's null count at finish
            WIDE_DECIMAL => self.withhold(),
            _ => self.cells.update(&self.encoded),
        }
    }

    /// The read did not supply canonical scalar values for this column.
    pub fn withhold(&mut self) {
        self.unavailable = true;
    }

    pub fn merge(&mut self, other: &Self) {
        self.cells.merge(other.cells);
        self.unavailable |= other.unavailable;
        self.day_first |= other.day_first;
        self.month_first |= other.month_first;
        self.month_first_delta = self.month_first_delta.wrapping_add(other.month_first_delta);
    }

    pub fn heap_bytes(&self) -> usize {
        self.encoded.capacity()
    }

    pub fn finish(&self, total_count: usize, null_count: usize) -> Option<String> {
        if self.unavailable || (self.day_first && self.month_first) {
            return None;
        }
        let mut cells = self.cells;
        cells.add_nulls(null_count as u64);
        let (count, mut sum) = cells.finish();
        if self.month_first {
            sum = sum.wrapping_add(self.month_first_delta);
        }
        // Refuse to publish a value if the route only supplied part of a column.
        (count == total_count as u64).then(|| format!("{sum:032x}"))
    }
}

/// Recover a float only if its digits are what the float writer produced.
/// Subnormals can have extra digits at shorter precisions too. A literal
/// that underflows to zero retains its own encoding.
fn padded_float(value: &str) -> Option<f64> {
    let literal = WideDecimal::read(value)?;
    let digits = literal.digits().count();
    if literal.scale <= 0 || !(1..=17).contains(&digits) {
        return None;
    }
    let float = value.parse::<f64>().expect("encoded numeric literal");
    if !float.is_finite() || float == 0.0 || (float.is_normal() && digits != 17) {
        return None;
    }
    let mut written = CellText::new();
    written.write(format_args!("{:.*e}", digits - 1, float));
    WideDecimal::read(written.as_str())
        .is_some_and(|rendered| literal.same_value(&rendered))
        .then_some(float)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(values: &[&str]) -> ColumnFingerprint {
        let mut result = ColumnFingerprint::default();
        for value in values {
            result.update_text(value);
        }
        result
    }

    #[test]
    fn column_fingerprint_resolves_slash_order_across_merges() {
        let ambiguous = column(&["01/02/2024", "03/04/2024 12:15:00"]);
        let month_first = column(&["12/31/2024"]);
        let day_first = column(&["31/12/2024"]);
        let expected = column(&["2024-01-02", "2024-03-04T12:15:00", "2024-12-31"]);
        let mut merged = ambiguous.clone();
        merged.merge(&month_first);
        assert_eq!(merged.finish(3, 0), expected.finish(3, 0));
        assert_ne!(merged.finish(3, 0), None);
        merged.merge(&day_first);
        assert_eq!(merged.finish(4, 0), None);
        assert_eq!(column(&["31/12/2024", "12/31/24"]).finish(2, 0), None);
    }

    #[test]
    fn column_fingerprint_withholds_unheld_decimals() {
        let wide = "1234567890123456789012345678901234567891";
        assert_eq!(column(&[wide]).finish(1, 0), None);
        assert_eq!(column(&["1e999999999999999999999"]).finish(1, 0), None);
        assert_ne!(column(&["1e100"]).finish(1, 0), None);
        let mut decimal = ColumnFingerprint::default();
        decimal.update_decimal_text("1e100");
        assert_eq!(decimal.finish(1, 0), None);
    }

    #[test]
    fn column_fingerprint_accepts_padded_float_spelling() {
        let shortest = column(&["5e-324", "1.5e-320", "0.1"]).finish(3, 0);
        let padded = column(&[
            "4.9406564584124654e-324",
            "1.4999833007740245e-320",
            "0.1000000000000000100",
        ])
        .finish(3, 0);
        assert!(shortest.is_some());
        assert_eq!(padded, shortest);
        let mut declared = ColumnFingerprint::default();
        declared.update_decimal_text("0.10000000000000001");
        assert_ne!(declared.finish(1, 0), column(&["0.1"]).finish(1, 0));
        assert_ne!(
            column(&["1.2345678e-320"]).finish(1, 0),
            column(&["1.2347e-320"]).finish(1, 0),
        );
    }
}
