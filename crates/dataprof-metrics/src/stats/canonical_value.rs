//! Canonical cell encodings and an order-independent fingerprint.
//!
//! Encodings start with a tag. Integers, decimals and the shortest decimal
//! representation of finite floats share a tag when held by [`ExactDecimal`].
//! Text and binary retain their original bytes. Null has an empty payload.
//! Multibyte integers are big endian. The fingerprint is for accidental
//! changes in the input.

use std::{fmt, io::Write};

#[cfg(feature = "arrow")]
use arrow_array::{Array, types::*};
#[cfg(feature = "arrow")]
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::Datelike;
#[cfg(feature = "arrow")]
use chrono::NaiveDate;
use dataprof_core::{LexicalClass, is_null_like_token};
use twox_hash::XxHash3_128;

use super::{ExactDecimal, datetime::parse_local_date_or_datetime};
use crate::analysis::inference::{holds_digit_codes, lexical_class, parse_strict_boolean_token};

/// Tag for null cells, including a floating-point NaN.
pub const NULL_TAG: u8 = 0x00;
const DECIMAL: u8 = 0x01;
const WIDE_DECIMAL: u8 = 0x02;
const FLOAT: u8 = 0x03;
const BOOLEAN: u8 = 0x04;
const DATE: u8 = 0x05;
const ZONED: u8 = 0x06;
const NAIVE: u8 = 0x07;
const TEXT: u8 = 0x08;
#[cfg(feature = "arrow")]
const BINARY: u8 = 0x09;
const INVALID_DATE: u8 = 0xfe;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// Append a text cell using the inference classifier. Null markers and digit
/// codes follow inference. An ambiguous slash date uses day first, as in the
/// date statistics fallback. A date-shaped token with an invalid calendar
/// value retains its original bytes under a separate tag.
///
/// Reuse the buffer across cells to amortize its capacity growth.
pub fn encode_text(value: Option<&str>, output: &mut Vec<u8>) {
    let Some(raw) = value.filter(|value| !is_null_like_token(value)) else {
        output.push(NULL_TAG);
        return;
    };
    let value = raw.trim();
    match lexical_class(value) {
        LexicalClass::Numeric if !holds_digit_codes([value]) => encode_number(value, output),
        LexicalClass::Boolean => {
            let boolean = parse_strict_boolean_token(value).expect("classified as boolean");
            output.extend_from_slice(&[BOOLEAN, u8::from(boolean)]);
        }
        LexicalClass::Date => {
            if let Ok(zoned) = chrono::DateTime::parse_from_rfc3339(value) {
                timestamp(
                    zoned.timestamp(),
                    zoned.timestamp_subsec_nanos(),
                    ZONED,
                    output,
                );
            } else if let Some((date, time)) = parse_local_date_or_datetime(value) {
                if let Some(time) = time {
                    let time = time.and_utc();
                    timestamp(
                        time.timestamp(),
                        time.timestamp_subsec_nanos(),
                        NAIVE,
                        output,
                    );
                } else {
                    date_bytes(date.year(), date.month(), date.day(), output);
                }
            } else {
                tagged(INVALID_DATE, raw.as_bytes(), output);
            }
        }
        LexicalClass::Numeric | LexicalClass::Text => tagged(TEXT, raw.as_bytes(), output),
    }
}

#[cfg(feature = "arrow")]
/// Whether this Arrow type has a canonical scalar encoding. Dictionary and
/// run-end arrays can be decoded to their value type before encoding.
pub fn supports_arrow_type(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        data_type,
        Null | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Decimal32(..)
            | Decimal64(..)
            | Decimal128(..)
            | Decimal256(..)
            | Boolean
            | Date32
            | Date64
            | Timestamp(..)
            | Utf8
            | LargeUtf8
            | Utf8View
            | Binary
            | LargeBinary
            | BinaryView
            | FixedSizeBinary(_)
    )
}

#[cfg(feature = "arrow")]
/// Append the value at `index` in a typed Arrow array. String arrays use
/// Arrow's declared string type and retain the supplied bytes, with the
/// profiler's usual null markers. Text ingestion routes that store raw cells
/// in a string array use [`encode_text`].
pub fn encode_arrow(
    array: &dyn Array,
    index: usize,
    output: &mut Vec<u8>,
) -> Result<(), ArrowError> {
    use DataType::*;
    use arrow_array::cast::AsArray;
    use arrow_array::*;

    if index >= array.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "cell index {index} is outside an array of length {}",
            array.len()
        )));
    }
    if array.is_null(index) || matches!(array.data_type(), Null) {
        output.push(NULL_TAG);
        return Ok(());
    }
    macro_rules! primitive {
        ($ty:ty) => {
            array.as_primitive::<$ty>().value(index)
        };
    }
    macro_rules! integer {
        ($ty:ty) => {
            decimal(ExactDecimal::integer(i128::from(primitive!($ty))), output)
        };
    }
    macro_rules! scaled {
        ($ty:ty, $scale:expr) => {{
            let mut text = CellText::new();
            text.write(format_args!("{}e{}", primitive!($ty), -i16::from(*$scale)));
            encode_decimal_literal(text.as_str(), output);
        }};
    }
    macro_rules! bytes {
        ($ty:ty, $tag:expr) => {{
            let values = array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("array matches its type");
            tagged($tag, values.value(index).as_ref(), output);
        }};
    }

    match array.data_type() {
        Int8 => integer!(Int8Type),
        Int16 => integer!(Int16Type),
        Int32 => integer!(Int32Type),
        Int64 => integer!(Int64Type),
        UInt8 => integer!(UInt8Type),
        UInt16 => integer!(UInt16Type),
        UInt32 => integer!(UInt32Type),
        UInt64 => integer!(UInt64Type),
        Float16 => {
            let value = primitive!(Float16Type).to_f32();
            encode_float(f64::from(value), format_args!("{value}"), output);
        }
        Float32 => {
            let value = primitive!(Float32Type);
            encode_float(f64::from(value), format_args!("{value}"), output);
        }
        Float64 => {
            let value = primitive!(Float64Type);
            encode_float(value, format_args!("{value}"), output);
        }
        Decimal32(_, scale) => scaled!(Decimal32Type, scale),
        Decimal64(_, scale) => scaled!(Decimal64Type, scale),
        Decimal128(_, scale) => scaled!(Decimal128Type, scale),
        Decimal256(_, scale) => scaled!(Decimal256Type, scale),
        Boolean => output.extend_from_slice(&[BOOLEAN, u8::from(array.as_boolean().value(index))]),
        Date32 => date_from_days(i64::from(primitive!(Date32Type)), output),
        Date64 => date_from_days(primitive!(Date64Type).div_euclid(86_400_000), output),
        Timestamp(unit, zone) => {
            let (value, nanos_per_unit) = match unit {
                TimeUnit::Second => (primitive!(TimestampSecondType), NANOS_PER_SECOND),
                TimeUnit::Millisecond => (primitive!(TimestampMillisecondType), 1_000_000),
                TimeUnit::Microsecond => (primitive!(TimestampMicrosecondType), 1_000),
                TimeUnit::Nanosecond => (primitive!(TimestampNanosecondType), 1),
            };
            let tag = if zone.is_some() { ZONED } else { NAIVE };
            tagged(
                tag,
                &(i128::from(value) * nanos_per_unit).to_be_bytes(),
                output,
            );
        }
        Utf8 => encode_string(array.as_string::<i32>().value(index), output),
        LargeUtf8 => encode_string(array.as_string::<i64>().value(index), output),
        Utf8View => encode_string(array.as_string_view().value(index), output),
        Binary => bytes!(BinaryArray, BINARY),
        LargeBinary => bytes!(LargeBinaryArray, BINARY),
        BinaryView => bytes!(BinaryViewArray, BINARY),
        FixedSizeBinary(_) => bytes!(FixedSizeBinaryArray, BINARY),
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "no canonical cell encoding for {other}"
            )));
        }
    }
    Ok(())
}

fn tagged(tag: u8, payload: &[u8], output: &mut Vec<u8>) {
    output.push(tag);
    output.extend_from_slice(payload);
}

#[cfg(feature = "arrow")]
fn encode_string(value: &str, output: &mut Vec<u8>) {
    if is_null_like_token(value) {
        output.push(NULL_TAG);
    } else {
        tagged(TEXT, value.as_bytes(), output);
    }
}

fn decimal(value: ExactDecimal, output: &mut Vec<u8>) {
    output.push(DECIMAL);
    value.write_canonical(output);
}

fn encode_float(value: f64, display: fmt::Arguments<'_>, output: &mut Vec<u8>) {
    if value.is_nan() {
        output.push(NULL_TAG);
    } else if value == 0.0 {
        tagged(DECIMAL, b"0", output);
    } else {
        let mut text = CellText::new();
        text.write(display);
        if let Some(exact) = ExactDecimal::from_text(text.as_str()) {
            decimal(exact, output);
        } else {
            tagged(FLOAT, &value.to_bits().to_be_bytes(), output);
        }
    }
}

fn encode_number(value: &str, output: &mut Vec<u8>) {
    if let Some(exact) = ExactDecimal::from_text(value) {
        decimal(exact, output);
        return;
    }
    let float = value.parse::<f64>().expect("classified as numeric");
    let Some(wide) = WideDecimal::read(value) else {
        encode_float(float, format_args!("{float}"), output);
        return;
    };
    if let Some(exact) = wide.exact() {
        decimal(exact, output);
        return;
    }
    let mut display = CellText::new();
    display.write(format_args!("{float}"));
    if WideDecimal::read(display.as_str()).is_some_and(|shortest| wide.same_value(&shortest)) {
        encode_float(float, format_args!("{float}"), output);
    } else {
        wide.write(output);
    }
}

#[cfg(feature = "arrow")]
fn encode_decimal_literal(value: &str, output: &mut Vec<u8>) {
    if let Some(exact) = ExactDecimal::from_text(value) {
        decimal(exact, output);
    } else {
        let wide = WideDecimal::read(value).expect("formatted Arrow decimal");
        if let Some(exact) = wide.exact() {
            decimal(exact, output);
        } else {
            wide.write(output);
        }
    }
}

/// A decimal whose coefficient or scale may exceed ExactDecimal's range.
/// The significant digits borrow the literal and omit the decimal point
/// and leading and trailing zeroes. The scale counts from the last remaining
/// digit, and may be negative.
struct WideDecimal<'a> {
    negative: bool,
    significant: &'a str,
    scale: i64,
}

impl<'a> WideDecimal<'a> {
    fn read(value: &'a str) -> Option<Self> {
        let negative = value.starts_with('-');
        let value = value.strip_prefix(['+', '-']).unwrap_or(value);
        let (mantissa, exponent) = if let Some((mantissa, exponent)) = value.split_once(['e', 'E'])
        {
            (mantissa, exponent.parse::<i64>().ok()?)
        } else {
            (value, 0)
        };
        let nonzero = |byte: u8| matches!(byte, b'1'..=b'9');
        let Some(first) = mantissa.bytes().position(nonzero) else {
            return mantissa.contains('0').then_some(Self {
                negative: false,
                significant: "",
                scale: 0,
            });
        };
        let last = mantissa.bytes().rposition(nonzero).expect("nonzero digit");
        let dot = mantissa.find('.').unwrap_or(mantissa.len());
        let scale = if last < dot {
            last as i64 + 1 - dot as i64
        } else {
            last as i64 - dot as i64
        };
        Some(Self {
            negative,
            significant: &mantissa[first..=last],
            scale: scale.checked_sub(exponent)?,
        })
    }

    fn digits(&self) -> impl Iterator<Item = u8> + '_ {
        self.significant.bytes().filter(|byte| *byte != b'.')
    }

    fn same_value(&self, other: &Self) -> bool {
        self.negative == other.negative
            && self.scale == other.scale
            && self.digits().eq(other.digits())
    }

    fn exact(&self) -> Option<ExactDecimal> {
        let mut coefficient = 0_i128;
        for digit in self.digits() {
            let digit = i128::from(digit - b'0');
            let signed = if self.negative { -digit } else { digit };
            coefficient = coefficient.checked_mul(10)?.checked_add(signed)?;
        }
        ExactDecimal::from_coefficient(coefficient, self.scale)
    }

    fn write(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&[WIDE_DECIMAL, u8::from(self.negative)]);
        output.extend_from_slice(&self.scale.to_be_bytes());
        output.extend(self.digits());
    }
}

/// Stack space for Rust's fixed-point display of a binary float (up to 327
/// bytes), or an Arrow decimal coefficient and exponent.
struct CellText {
    bytes: [u8; 384],
    len: usize,
}

impl CellText {
    fn new() -> Self {
        Self {
            bytes: [0; 384],
            len: 0,
        }
    }

    fn write(&mut self, args: fmt::Arguments<'_>) {
        let original_len = self.bytes.len();
        let mut free = &mut self.bytes[..];
        free.write_fmt(args)
            .expect("cell rendering fits in stack buffer");
        self.len = original_len - free.len();
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len]).expect("number rendering writes ASCII")
    }
}

#[cfg(feature = "arrow")]
fn date_from_days(days: i64, output: &mut Vec<u8>) {
    // The Gregorian calendar repeats every 400 years. Keep the date passed
    // to Chrono in its range even for extreme Arrow values.
    const CYCLE_DAYS: i64 = 146_097;
    let cycle = days.div_euclid(CYCLE_DAYS);
    let date = NaiveDate::from_epoch_days(days.rem_euclid(CYCLE_DAYS) as i32)
        .expect("one Gregorian cycle from the epoch");
    date_bytes(
        i64::from(date.year()) + cycle * 400,
        date.month(),
        date.day(),
        output,
    );
}

fn date_bytes(year: impl Into<i64>, month: u32, day: u32, output: &mut Vec<u8>) {
    output.push(DATE);
    let year = year.into();
    match year {
        0..=9999 => write!(output, "{year:04}-{month:02}-{day:02}"),
        _ => write!(output, "{year:+05}-{month:02}-{day:02}"),
    }
    .expect("writing to Vec is infallible");
}

fn timestamp(seconds: i64, subsecond: u32, tag: u8, output: &mut Vec<u8>) {
    let nanos = i128::from(seconds) * NANOS_PER_SECOND + i128::from(subsecond);
    tagged(tag, &nanos.to_be_bytes(), output);
}

/// XXH3-128 of a null encoding, pinned by the cell vectors.
const NULL_HASH: u128 = 0xa6cd5e9392000f6ac44bdff4074eecdb;

/// Sum of canonical cell hashes modulo 2^128, with multiplicities.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellFingerprint {
    sum: u128,
    count: u64,
}

impl CellFingerprint {
    /// Add one cell previously appended by a canonical encoder.
    pub fn update(&mut self, encoded: &[u8]) {
        self.count += 1;
        self.sum = self.sum.wrapping_add(XxHash3_128::oneshot(encoded));
    }

    /// Add any number of nulls without hashing each one.
    pub fn add_nulls(&mut self, count: u64) {
        self.count += count;
        self.sum = self
            .sum
            .wrapping_add(NULL_HASH.wrapping_mul(u128::from(count)));
    }

    /// Add a partition of the input. Partition and row order do not matter.
    pub fn merge(&mut self, other: Self) {
        self.count += other.count;
        self.sum = self.sum.wrapping_add(other.sum);
    }

    /// The number of cells and their fingerprint.
    pub fn finish(self) -> (u64, u128) {
        (self.count, self.sum)
    }
}

#[cfg(all(test, feature = "arrow"))]
mod tests {
    use super::*;
    use arrow_array::*;

    type Decimal256Native = <Decimal256Type as ArrowPrimitiveType>::Native;

    #[test]
    fn canonical_cell_vectors() {
        struct Vector {
            text: &'static [Option<&'static str>],
            arrow: Vec<ArrayRef>,
            bytes: &'static [u8],
            hash: u128,
        }
        const WIDE: &str = "1234567890123456789012345678901234567891";
        let vectors = [
            Vector {
                text: &[None, Some("NaN"), Some(" N/A ")],
                arrow: vec![
                    std::sync::Arc::new(NullArray::new(1)),
                    std::sync::Arc::new(Float64Array::from(vec![f64::NAN])),
                    std::sync::Arc::new(StringArray::from(vec!["N/A"])),
                ],
                bytes: b"\x00",
                hash: 0xa6cd5e9392000f6ac44bdff4074eecdb,
            },
            Vector {
                text: &[Some("1"), Some("1.0"), Some("1.00"), Some(" +1 ")],
                arrow: vec![
                    std::sync::Arc::new(Int8Array::from(vec![1])),
                    std::sync::Arc::new(UInt16Array::from(vec![1])),
                    std::sync::Arc::new(Float16Array::from(vec![
                        <Float16Type as ArrowPrimitiveType>::Native::from_f32(1.0),
                    ])),
                    std::sync::Arc::new(Float32Array::from(vec![1.0])),
                    std::sync::Arc::new(
                        Decimal128Array::from(vec![100])
                            .with_precision_and_scale(4, 2)
                            .unwrap(),
                    ),
                ],
                bytes: b"\x011",
                hash: 0xa1505b6b490383043fdad128b7dfd3e3,
            },
            Vector {
                text: &[Some("1.50"), Some("1.5"), Some("15e-1")],
                arrow: vec![
                    std::sync::Arc::new(Float64Array::from(vec![1.5])),
                    std::sync::Arc::new(
                        Decimal256Array::from(vec![
                            "15000000000000000000000000000000000000000"
                                .parse::<Decimal256Native>()
                                .unwrap(),
                        ])
                        .with_precision_and_scale(60, 40)
                        .unwrap(),
                    ),
                ],
                bytes: b"\x011.5",
                hash: 0xb49421ff5ad094f517bd5aa5ae960d65,
            },
            Vector {
                text: &[Some("0"), Some("-0.0"), Some("0.000")],
                arrow: vec![std::sync::Arc::new(Float64Array::from(vec![-0.0]))],
                bytes: b"\x010",
                hash: 0xb809ea213e2301bb42847200a9c2f43c,
            },
            Vector {
                text: &[Some("-0.125")],
                arrow: vec![std::sync::Arc::new(Float32Array::from(vec![-0.125]))],
                bytes: b"\x01-0.125",
                hash: 0xd1519feba23fb66c13717cf4b5ce337f,
            },
            Vector {
                text: &[
                    Some(WIDE),
                    Some("1234567890123456789012345678901234567891.00"),
                ],
                arrow: vec![std::sync::Arc::new(
                    Decimal256Array::from(vec![WIDE.parse::<Decimal256Native>().unwrap()])
                        .with_precision_and_scale(40, 0)
                        .unwrap(),
                )],
                bytes: b"\x02\0\0\0\0\0\0\0\0\x001234567890123456789012345678901234567891",
                hash: 0x0c6947d82e4fe0f6e7e8489e5f87050e,
            },
            Vector {
                text: &[Some("inf"), Some("Infinity")],
                arrow: vec![std::sync::Arc::new(Float64Array::from(vec![f64::INFINITY]))],
                bytes: b"\x03\x7f\xf0\0\0\0\0\0\0",
                hash: 0x40d5b6e006c293130b7ba36506f24d8a,
            },
            Vector {
                text: &[Some("True"), Some("true")],
                arrow: vec![std::sync::Arc::new(BooleanArray::from(vec![true]))],
                bytes: b"\x04\x01",
                hash: 0xd96e724ebb109a113bd631e4a5582d1f,
            },
            Vector {
                text: &[Some("FALSE")],
                arrow: vec![std::sync::Arc::new(BooleanArray::from(vec![false]))],
                bytes: b"\x04\x00",
                hash: 0x4417e50c7b9324c6bf99a53001e4f61c,
            },
            Vector {
                text: &[Some("2024-02-01"), Some("01/02/2024")],
                arrow: vec![std::sync::Arc::new(Date32Array::from(vec![19754]))],
                bytes: b"\x052024-02-01",
                hash: 0xe265d468a572ec3c0539835c52250d6e,
            },
            Vector {
                text: &[Some("12/31/1969")],
                arrow: vec![std::sync::Arc::new(Date64Array::from(vec![-86_400_000]))],
                bytes: b"\x051969-12-31",
                hash: 0xb6b05531f61c6802c4008716be02fba5,
            },
            Vector {
                text: &[
                    Some("1970-01-01T00:00:00.000000001Z"),
                    Some("1970-01-01 02:00:00.000000001+02:00"),
                ],
                arrow: vec![std::sync::Arc::new(
                    TimestampNanosecondArray::from(vec![1]).with_timezone("Europe/Rome"),
                )],
                bytes: b"\x06\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01",
                hash: 0x5762a99e341c75d92ea65e7e322ec59b,
            },
            Vector {
                text: &[Some("1969-12-31T23:59:59.999999999")],
                arrow: vec![std::sync::Arc::new(TimestampNanosecondArray::from(vec![
                    -1,
                ]))],
                bytes: b"\x07\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
                hash: 0x1fc1b772424299a6e23b0102a81704ce,
            },
            Vector {
                text: &[Some(" Rome ")],
                arrow: vec![
                    std::sync::Arc::new(StringArray::from(vec![" Rome "])),
                    std::sync::Arc::new(LargeStringArray::from(vec![" Rome "])),
                    std::sync::Arc::new(StringViewArray::from(vec![" Rome "])),
                ],
                bytes: b"\x08 Rome ",
                hash: 0xe75ced3de7278cbee55feb471e73b097,
            },
            Vector {
                text: &[Some("007")],
                arrow: vec![std::sync::Arc::new(StringArray::from(vec!["007"]))],
                bytes: b"\x08007",
                hash: 0x11cfeb5b40892349a8f5ba19ea945696,
            },
            Vector {
                text: &[],
                arrow: vec![
                    std::sync::Arc::new(BinaryArray::from_vec(vec![b"\0\xff"])),
                    std::sync::Arc::new(LargeBinaryArray::from_vec(vec![b"\0\xff"])),
                    std::sync::Arc::new(BinaryViewArray::from_iter_values([b"\0\xff"])),
                    std::sync::Arc::new(
                        FixedSizeBinaryArray::try_from_iter([b"\0\xff"].into_iter()).unwrap(),
                    ),
                ],
                bytes: b"\x09\0\xff",
                hash: 0xc22451481e79b5755e1b71ed05857c1d,
            },
            Vector {
                text: &[Some("2024-02-30")],
                arrow: vec![],
                bytes: b"\xfe2024-02-30",
                hash: 0x33767ddb9d094d94e12455a8a4025ff1,
            },
        ];
        let mut actual = Vec::with_capacity(64);
        for vector in vectors {
            assert_eq!(
                XxHash3_128::oneshot(vector.bytes),
                vector.hash,
                "{:?}",
                vector.bytes
            );
            for text in vector.text {
                actual.clear();
                encode_text(*text, &mut actual);
                assert_eq!(actual, vector.bytes, "{text:?}");
            }
            for array in vector.arrow {
                actual.clear();
                encode_arrow(array.as_ref(), 0, &mut actual).unwrap();
                assert_eq!(actual, vector.bytes, "{array:?}");
            }
        }
        assert_eq!(XxHash3_128::oneshot(&[NULL_TAG]), NULL_HASH);
    }

    #[test]
    fn fingerprint_groups_rows_and_nulls() {
        fn fingerprint(values: impl IntoIterator<Item = Option<&'static str>>) -> CellFingerprint {
            let mut result = CellFingerprint::default();
            let mut encoded = Vec::new();
            for cell in values {
                encoded.clear();
                encode_text(cell, &mut encoded);
                result.update(&encoded);
            }
            result
        }

        let rows = [Some("A"), Some("1.50"), Some("A"), None, None];
        let whole = fingerprint(rows);
        assert_eq!(whole.finish().0, 5);
        assert_eq!(fingerprint(rows.into_iter().rev()), whole);
        assert_eq!(
            fingerprint([Some("A"), Some("1.5"), Some("A"), None, None]),
            whole
        );
        assert_ne!(
            fingerprint([Some("B"), Some("1.5"), Some("A"), None, None]),
            whole
        );
        assert_ne!(fingerprint([Some("A"), Some("A")]).finish().1, 0);

        let left = fingerprint([Some("A"), Some("1.5")]);
        let mut right = fingerprint([Some("A")]);
        right.add_nulls(2);
        let mut forward = left;
        forward.merge(right);
        right.merge(left);
        assert_eq!(forward, whole);
        assert_eq!(right, whole);
    }
}
