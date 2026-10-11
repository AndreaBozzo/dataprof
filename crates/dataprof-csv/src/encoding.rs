//! Encoding diagnosis for text input that failed UTF-8 decoding.

use std::path::Path;

/// What [`diagnose_non_utf8`] found about a file that failed UTF-8 decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingDiagnostic {
    /// Best-effort encoding guess (e.g. `"latin-1"`, `"windows-1252"`, `"utf-16le"`).
    pub guess: &'static str,
    /// Byte offset of the first invalid UTF-8 sequence, when found within the
    /// sampled prefix. `None` means the sampled prefix was valid UTF-8 and the
    /// offending byte lies further into the file.
    pub first_invalid_byte: Option<usize>,
}

impl EncodingDiagnostic {
    /// Human-readable detail for an error message, without echoing file contents.
    pub fn detail(&self) -> String {
        // Plain ASCII only: this string ends up in an encoding-error message that
        // may be printed to a non-UTF-8 terminal, so it must never add mojibake.
        match self.first_invalid_byte {
            Some(offset) => format!(
                "looks like {}; first invalid UTF-8 byte at offset {offset}",
                self.guess
            ),
            None => format!(
                "looks like {}; contains bytes that are not valid UTF-8",
                self.guess
            ),
        }
    }
}

/// How many leading bytes [`diagnose_non_utf8`] inspects. Large enough to locate
/// the offending byte in the common case, bounded so the error path never reads
/// an entire large file.
const ENCODING_SNIFF_LIMIT: usize = 1 << 20; // 1 MiB

/// Inspect a file that failed UTF-8 decoding and guess its encoding.
///
/// Returns `None` when the sampled prefix is valid UTF-8 (so a failure that is
/// *not* an encoding problem is never mislabeled as one). BOM-prefixed UTF-16 is
/// reported directly; otherwise a byte in `0x80..=0x9F` points at windows-1252
/// and anything else at latin-1 — a best-effort guess, not a definitive verdict.
pub fn diagnose_non_utf8(path: &Path) -> Option<EncodingDiagnostic> {
    use std::io::Read;

    // decode-audit: unknown — this runs only to enrich an error the caller
    // already has. If the file cannot be reopened/read for sniffing, returning
    // None keeps that original engine error, so the failure is surfaced, not lost.
    let file = std::fs::File::open(path).ok()?;
    let mut sample = Vec::new();
    // decode-audit: unknown — as above; a read failure abandons the guess and
    // preserves the caller's original error rather than swallowing it.
    file.take(ENCODING_SNIFF_LIMIT as u64)
        .read_to_end(&mut sample)
        .ok()?;

    if sample.starts_with(&[0xFF, 0xFE]) {
        return Some(EncodingDiagnostic {
            guess: "utf-16le",
            first_invalid_byte: Some(0),
        });
    }
    if sample.starts_with(&[0xFE, 0xFF]) {
        return Some(EncodingDiagnostic {
            guess: "utf-16be",
            first_invalid_byte: Some(0),
        });
    }

    // A valid-UTF-8 prefix means the fault, if any, is beyond the sample — do not
    // claim an encoding problem we cannot see.
    let first_invalid_byte = match std::str::from_utf8(&sample) {
        Ok(_) => return None,
        Err(err) => Some(err.valid_up_to()),
    };

    let has_c1_controls = sample.iter().any(|&b| (0x80..=0x9F).contains(&b));
    let guess = if has_c1_controls {
        "windows-1252"
    } else {
        "latin-1"
    };

    Some(EncodingDiagnostic {
        guess,
        first_invalid_byte,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_bytes(bytes: &[u8]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn diagnose_non_utf8_flags_latin1() {
        // "café" in latin-1: 0xE9 (é) is a UTF-8 leading byte for a 3-byte
        // sequence, but here it is not followed by continuation bytes, so the
        // stream is invalid UTF-8 at that position.
        let f = write_bytes(b"name\ncaf\xE9\n");
        let d = diagnose_non_utf8(f.path()).expect("latin-1 should be flagged");
        assert_eq!(d.guess, "latin-1");
        assert_eq!(d.first_invalid_byte, Some(8));
    }

    #[test]
    fn diagnose_non_utf8_flags_windows1252() {
        // 0x93/0x94 are smart quotes in windows-1252 (C1 range), invalid UTF-8.
        let f = write_bytes(b"quote\n\x93hi\x94\n");
        let d = diagnose_non_utf8(f.path()).expect("windows-1252 should be flagged");
        assert_eq!(d.guess, "windows-1252");
        assert_eq!(d.first_invalid_byte, Some(6));
    }

    #[test]
    fn diagnose_non_utf8_flags_utf16_bom() {
        let f = write_bytes(&[0xFF, 0xFE, b'a', 0x00]);
        let d = diagnose_non_utf8(f.path()).expect("utf-16 BOM should be flagged");
        assert_eq!(d.guess, "utf-16le");
    }

    #[test]
    fn diagnose_non_utf8_ignores_clean_utf8() {
        let f = write_bytes("name\ncafé\n".as_bytes());
        assert!(diagnose_non_utf8(f.path()).is_none());
    }

    #[test]
    fn encoding_diagnostic_detail_never_leaks_bytes() {
        let d = EncodingDiagnostic {
            guess: "latin-1",
            first_invalid_byte: Some(8),
        };
        let detail = d.detail();
        assert!(detail.contains("latin-1"));
        assert!(detail.contains("offset 8"));
    }
}
