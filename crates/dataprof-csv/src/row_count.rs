//! Shared record counts and observed row density for partial and streaming APIs.

use std::io::Read;

use dataprof_core::DataProfilerError;

/// Count logical CSV records with bounded memory, including the header.
///
/// A full scan avoids ambiguous quote state at seek offsets and prefix-density
/// bias. Byte records let a profiler retain its own decoding and row-validation
/// boundary, including when it stops before a later invalid record.
pub fn csv_record_count<R: Read>(
    reader: R,
    delimiter: u8,
    quote: u8,
) -> Result<u64, DataProfilerError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter)
        .quote(quote)
        .from_reader(reader);
    let mut record = csv::ByteRecord::new();
    let mut count = 0;
    while reader.read_byte_record(&mut record)? {
        count += 1;
    }
    Ok(count)
}

/// Extrapolate an observed population of complete records over source bytes.
///
/// For a true stream the unread row-width distribution is unknown. This is a
/// density estimate, not a statistical confidence interval. Exclude CSV header
/// bytes from both byte populations. A materialized input uses an exact count
/// instead.
pub fn observed_total_rows(total_bytes: u64, read_bytes: u64, rows: u64) -> Option<u64> {
    if read_bytes == 0 || rows == 0 {
        return None;
    }
    let estimate = (total_bytes as u128 * rows as u128).div_ceil(read_bytes as u128);
    Some(estimate.max(rows as u128).min(u64::MAX as u128) as u64)
}
