//! Whether a CSV source ended inside a quoted field.
//!
//! The `csv` crate, and `arrow-csv` which shares its `csv-core` tokenizer,
//! treat end of input inside a quoted field as the end of that field. A quote
//! that is never closed therefore swallows every following row into the last
//! field of one record, and nothing the parser returns says so (#782).
//!
//! [`QuoteTrackingReader`] sits between the source and the parser and follows
//! the same quote rules as `csv-core` with the options dataprof uses: a quote
//! opens a field only at the start of a field, a doubled quote inside a quoted
//! field is a literal quote, and CR, LF and CRLF all end a record. Only a
//! quote changes that state, so it jumps from quote to quote with `memchr`
//! and costs little on data that is mostly unquoted.

use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use dataprof_core::DataProfilerError;

const UTF8_BOM: &[u8; 3] = b"\xef\xbb\xbf";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Outside any quoted field, where a quote opens one only at a field start.
    Outside,
    /// Inside a quoted field.
    InQuoted,
    /// Inside a quoted field, having just read a quote at the end of a read:
    /// the next byte decides whether it closed the field or was the first half
    /// of an escaped quote.
    QuoteInQuoted,
}

const NOT_AT_END: u8 = 0;
const ENDED_OUTSIDE_QUOTES: u8 = 1;
const ENDED_INSIDE_QUOTES: u8 = 2;

/// Shared answer from a [`QuoteTrackingReader`], readable after the reader has
/// been moved into a parser.
#[derive(Debug, Clone, Default)]
pub struct QuoteOutcome(Arc<AtomicU8>);

impl QuoteOutcome {
    /// Whether the source ended inside a quoted field, or `None` while it has
    /// not been read to its end.
    ///
    /// A parser reads ahead of the records it returns, so the end can be
    /// reached by a scan that stopped at a row cap. Callers take the answer
    /// only for a scan that consumed its whole source.
    pub fn at_end(&self) -> Option<bool> {
        match self.0.load(Ordering::Relaxed) {
            NOT_AT_END => None,
            state => Some(state == ENDED_INSIDE_QUOTES),
        }
    }
}

/// Pass-through reader that records whether its source ended inside a quoted
/// field. See the module docs.
#[derive(Debug)]
pub struct QuoteTrackingReader<R> {
    inner: R,
    delimiter: u8,
    quote: u8,
    state: State,
    /// The last byte observed, `None` at the start of the source, which is a
    /// field start. A quote opens a field only when the byte before it ends
    /// one, so this carries that answer across reads.
    previous: Option<u8>,
    /// Bytes of a leading UTF-8 BOM matched so far, `None` once past it.
    /// `csv-core` strips the BOM, so the byte after it starts a field.
    bom_matched: Option<usize>,
    outcome: QuoteOutcome,
}

impl<R: Read> QuoteTrackingReader<R> {
    /// Track a source from its first byte, where a UTF-8 BOM is skipped.
    pub fn new(inner: R, delimiter: u8, quote: u8) -> Self {
        Self {
            inner,
            delimiter,
            quote,
            state: State::Outside,
            previous: None,
            bom_matched: Some(0),
            outcome: QuoteOutcome::default(),
        }
    }

    /// Handle to the answer, set when the source reaches its end.
    pub fn outcome(&self) -> QuoteOutcome {
        self.outcome.clone()
    }
}

impl<R> QuoteTrackingReader<R> {
    fn ends_a_field(&self, byte: u8) -> bool {
        byte == self.delimiter || byte == b'\r' || byte == b'\n'
    }

    /// Follow the quote state over the next bytes of the source, visiting only
    /// the quotes: nothing else changes it.
    fn observe(&mut self, mut bytes: &[u8]) {
        while let Some(matched) = self.bom_matched {
            let Some((&byte, rest)) = bytes.split_first() else {
                return;
            };
            if byte != UTF8_BOM[matched] {
                // A partial BOM is data, so the byte after it is mid-field.
                self.bom_matched = None;
                self.previous = matched.checked_sub(1).map(|last| UTF8_BOM[last]);
                break;
            }
            bytes = rest;
            self.bom_matched = (matched + 1 < UTF8_BOM.len()).then_some(matched + 1);
        }
        let Some(&last) = bytes.last() else {
            return;
        };

        let mut index = 0;
        if self.state == State::QuoteInQuoted {
            if bytes[0] == self.quote {
                index = 1;
                self.state = State::InQuoted;
            } else {
                self.state = State::Outside;
            }
        }
        while let Some(offset) = memchr::memchr(self.quote, &bytes[index..]) {
            let at = index + offset;
            index = at + 1;
            if self.state == State::InQuoted {
                match bytes.get(index) {
                    // A doubled quote is a literal one; the field goes on.
                    Some(&next) if next == self.quote => index += 1,
                    Some(_) => self.state = State::Outside,
                    None => self.state = State::QuoteInQuoted,
                }
            } else {
                let before = at.checked_sub(1).map_or(self.previous, |i| Some(bytes[i]));
                if before.is_none_or(|byte| self.ends_a_field(byte)) {
                    self.state = State::InQuoted;
                }
            }
        }
        self.previous = Some(last);
    }
}

impl<R: Read> Read for QuoteTrackingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        if read == 0 {
            if !buf.is_empty() {
                let state = if self.state == State::InQuoted {
                    ENDED_INSIDE_QUOTES
                } else {
                    ENDED_OUTSIDE_QUOTES
                };
                self.outcome.0.store(state, Ordering::Relaxed);
            }
        } else {
            self.observe(&buf[..read]);
        }
        Ok(read)
    }
}

/// Whether `bytes`, which start at a record boundary and run to the end of the
/// source, end inside a quoted field. `at_source_start` says whether they begin
/// at the source's first byte, where a UTF-8 BOM is skipped.
pub(crate) fn ends_inside_quotes(
    bytes: &[u8],
    delimiter: u8,
    quote: u8,
    at_source_start: bool,
) -> bool {
    let mut tracker = QuoteTrackingReader::new(io::empty(), delimiter, quote);
    if !at_source_start {
        tracker.bom_matched = None;
    }
    tracker.observe(bytes);
    tracker.state == State::InQuoted
}

/// The error strict mode (`flexible = false`) raises for a source that ended
/// inside a quoted field.
pub fn unterminated_quote_error() -> DataProfilerError {
    DataProfilerError::csv_structure(
        "The CSV source ends inside a quoted field: a quote was opened and never closed, \
         so every row after it was read into that one field.",
        "Close the quote in the source. Set csv_flexible=true to profile it anyway; the \
         report then sets execution.unterminated_quote.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracked(input: &[u8], read_size: usize) -> bool {
        struct Trickle<'a>(&'a [u8], usize);
        impl Read for Trickle<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                let n = self.0.len().min(self.1).min(buf.len());
                buf[..n].copy_from_slice(&self.0[..n]);
                self.0 = &self.0[n..];
                Ok(n)
            }
        }
        let mut reader = QuoteTrackingReader::new(Trickle(input, read_size), b',', b'"');
        let outcome = reader.outcome();
        assert_eq!(outcome.at_end(), None);
        io::copy(&mut reader, &mut io::sink()).unwrap();
        let answer = outcome.at_end().expect("read to the end");
        assert_eq!(answer, ends_inside_quotes(input, b',', b'"', true));
        answer
    }

    fn records(input: &[u8]) -> usize {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(input);
        let mut count = 0;
        for record in reader.byte_records() {
            record.unwrap();
            count += 1;
        }
        count
    }

    /// The `csv` crate's own answer. Text appended after a record terminator
    /// starts a new record, unless the input ended inside a quoted field, where
    /// it lands in that field instead.
    fn csv_ended_inside_quotes(input: &[u8]) -> bool {
        let extended = [input, b"\nZ"].concat();
        records(&extended) == records(input)
    }

    #[test]
    fn agrees_with_the_csv_crate_on_every_short_input() {
        const ALPHABET: &[u8] = b"a,\"\r\n";
        let mut checked = 0;
        let mut inside = 0;
        for len in 0..=7u32 {
            for code in 0..ALPHABET.len().pow(len) {
                let mut rest = code;
                let input: Vec<u8> = (0..len)
                    .map(|_| {
                        let byte = ALPHABET[rest % ALPHABET.len()];
                        rest /= ALPHABET.len();
                        byte
                    })
                    .collect();
                let expected = csv_ended_inside_quotes(&input);
                for read_size in [1, 2, 64] {
                    assert_eq!(
                        tracked(&input, read_size),
                        expected,
                        "{:?} read {read_size} bytes at a time",
                        String::from_utf8_lossy(&input)
                    );
                }
                checked += 1;
                inside += usize::from(expected);
            }
        }
        assert!(checked > 90_000 && inside > 10_000, "{checked} / {inside}");
    }

    #[test]
    fn a_leading_bom_is_skipped_as_csv_core_skips_it() {
        // Past the BOM the quote opens a field, so the comma inside it is data.
        // A partial BOM is data, so the quote after it is mid-field and literal.
        for input in [
            &b"\xef\xbb\xbf\"x,\"\n"[..],
            b"\xef\xbb\xbf\"open\n1,2\n",
            b"\xef\xbb\"x,\"\n",
            b"\xef\"open\n1,2\n",
        ] {
            let expected = csv_ended_inside_quotes(input);
            for read_size in [1, 2, 64] {
                assert_eq!(tracked(input, read_size), expected);
            }
        }
        assert!(!tracked(b"\xef\xbb\xbf\"x,\"\n", 1));
        assert!(tracked(b"\xef\xbb\xbf\"open\n1,2\n", 1));
    }

    #[test]
    fn the_issue_example_ends_inside_quotes() {
        let input = b"id,text\n1,\"never closed\n2,x\n3,y\n";
        assert!(csv_ended_inside_quotes(input));
        assert!(tracked(input, 4096));
        assert!(!tracked(b"id,text\n1,\"line one\nline two\"\n2,x\n", 4096));
    }
}
