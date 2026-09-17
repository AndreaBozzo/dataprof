//! Benchmark fixture integrity is part of the published measurement protocol.

#[path = "../benches/support/mod.rs"]
mod support;

use support::{CsvFixture, DatasetSize, analyze_full_report, count_rows_phase, parse_csv_phase};

#[test]
fn fixtures_are_deterministic_isolated_and_match_measured_workloads() {
    let custom = CsvFixture::with_rows(17);
    assert_eq!(parse_csv_phase(custom.path()).unwrap(), (8, 17));
    for size in [
        DatasetSize::Tiny,
        DatasetSize::Small,
        DatasetSize::Medium,
        DatasetSize::Large,
    ] {
        let first = CsvFixture::new(size);
        let second = CsvFixture::new(size);
        assert_ne!(
            first.path(),
            second.path(),
            "{} fixture isolation",
            size.name()
        );
        let bytes = std::fs::read(first.path()).unwrap();
        assert_eq!(bytes, std::fs::read(second.path()).unwrap());
        assert_eq!(first.bytes(), bytes.len() as u64);
        assert_eq!(
            bytes.iter().filter(|&&byte| byte == b'\n').count(),
            size.rows() + 1
        );
        if matches!(size, DatasetSize::Tiny) {
            assert_eq!(parse_csv_phase(first.path()).unwrap(), (8, size.rows()));
            assert_eq!(count_rows_phase(first.path()).unwrap(), size.rows() as u64);
            assert_eq!(
                analyze_full_report(first.path())
                    .unwrap()
                    .execution
                    .rows_processed,
                size.rows()
            );
        }
        let first_path = first.path().to_path_buf();
        drop(first);
        assert!(!first_path.exists());
        assert!(second.path().exists());
    }
}
