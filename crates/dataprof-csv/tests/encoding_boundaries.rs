//! The encoding diagnostic's bounded prefix must finish its final UTF-8 sequence (#920).

use std::io::Write;

use dataprof_csv::diagnose_non_utf8;
use tempfile::NamedTempFile;

const LIMIT: usize = 1 << 20;

fn fixture(bytes: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn diagnostic_finishes_only_the_sequence_at_the_sample_boundary() {
    let mut wrong = Vec::new();
    for character in ["é", "€", "🦀"] {
        for prefix_bytes in 1..character.len() {
            for tail in ["", "🦀"] {
                let mut bytes = vec![b'a'; LIMIT - prefix_bytes];
                bytes.extend_from_slice(character.as_bytes());
                bytes.extend_from_slice(tail.as_bytes());
                let file = fixture(&bytes);
                if let Some(diagnostic) = diagnose_non_utf8(file.path()) {
                    wrong.push(format!(
                        "{character:?}, split={prefix_bytes}, tail={tail:?}: {diagnostic:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn diagnostic_flags_truncated_eof_and_invalid_lookahead() {
    let mut bytes = vec![b'a'; LIMIT - 1];
    bytes.push(0xC3);
    for suffix in [b"".as_slice(), b"\xff"] {
        let mut input = bytes.clone();
        input.extend_from_slice(suffix);
        let file = fixture(&input);
        assert_eq!(
            diagnose_non_utf8(file.path()).unwrap().first_invalid_byte,
            Some(LIMIT - 1)
        );
    }
    let mut bytes = vec![b'a'; LIMIT];
    bytes.push(0xFF);
    assert!(diagnose_non_utf8(fixture(&bytes).path()).is_none());
}
