//! A valid boundary sequence cannot replace a later UTF-8 decoding error.
//! Python twin: test_encoding_sample_boundary.py.
use dataprof::{EngineType, Profiler};
use std::io::Write;
#[test]
fn valid_utf8_at_the_sample_boundary_preserves_the_later_error() {
    let mut file = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    let mut payload = b"value\n".to_vec();
    payload.resize(1024 * 1024 - 1, b'a');
    payload.extend_from_slice("é\n".as_bytes());
    payload.extend_from_slice(b"\xff\n");
    file.write_all(&payload).unwrap();
    file.flush().unwrap();
    let mut wrong = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let error = Profiler::new()
            .engine(engine)
            .analyze_file(file.path())
            .unwrap_err();
        let message = error.to_string().to_lowercase();
        if message.contains("latin") || message.contains("windows-1252") {
            wrong.push(format!("[{engine:?}] {error}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
