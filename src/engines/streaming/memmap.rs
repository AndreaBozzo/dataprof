use crate::core::MemoryTracker;
use crate::parsers::csv::CsvParserConfig;
use anyhow::Result;
use memmap2::Mmap;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;

/// Memory-mapped CSV reader for efficient processing of large files
pub struct MemoryMappedCsvReader {
    mmap: Mmap,
    file_size: u64,
    memory_tracker: MemoryTracker,
    resource_id: String,
}

impl MemoryMappedCsvReader {
    pub fn new(path: &Path) -> Result<Self> {
        Self::new_with_tracker(path, MemoryTracker::default())
    }

    pub fn new_with_tracker(path: &Path, memory_tracker: MemoryTracker) -> Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();

        // Safety: The file is opened read-only and we hold the File handle for the
        // lifetime of the Mmap. The file must not be concurrently modified.
        #[allow(unsafe_code)]
        let mmap = unsafe { Mmap::map(&file)? };

        let resource_id = format!("mmap_{}", path.display());

        // Track the memory mapping
        memory_tracker.track_allocation(resource_id.clone(), file_size as usize, "memory_map");

        Ok(Self {
            mmap,
            file_size,
            memory_tracker,
            resource_id,
        })
    }

    /// Get file size in bytes
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Read a chunk of the file starting at the given byte offset.
    ///
    /// Returns `(lines, actual_bytes_consumed)` where `actual_bytes_consumed`
    /// accounts for line-boundary trimming and may be less than `chunk_size`.
    pub fn read_chunk(&self, offset: u64, chunk_size: usize) -> Result<(Vec<String>, usize)> {
        let start = offset as usize;
        let end = std::cmp::min(start + chunk_size, self.mmap.len());

        if start >= self.mmap.len() {
            return Ok((Vec::new(), 0));
        }

        // Get the chunk data
        let chunk_data = &self.mmap[start..end];

        // Find line boundaries to avoid cutting lines in half.
        // Only skip a leading partial line when the chunk truly starts
        // mid-line. This happens when the preceding byte is NOT a newline
        // (and we're not at the start of the file).
        let at_eof = end == self.mmap.len();
        let starts_mid_line = start > 0 && self.mmap[start - 1] != b'\n';
        let (adjusted_chunk, bytes_consumed) =
            self.find_line_boundary(chunk_data, starts_mid_line, at_eof);

        // Parse lines from the chunk
        let cursor = Cursor::new(adjusted_chunk);
        let reader = BufReader::new(cursor);

        let mut lines = Vec::new();
        for line in reader.lines() {
            lines.push(line?);
        }

        Ok((lines, bytes_consumed))
    }

    /// Parse CSV records from memory-mapped data in chunks.
    ///
    /// Returns `(headers, records, actual_bytes_consumed)`.
    pub fn read_csv_chunk(
        &self,
        offset: u64,
        chunk_size: usize,
        has_headers: bool,
        csv_config: Option<&CsvParserConfig>,
    ) -> Result<(Option<csv::StringRecord>, Vec<csv::StringRecord>, usize)> {
        let (lines, actual_bytes) = self.read_chunk(offset, chunk_size)?;

        if lines.is_empty() {
            return Ok((None, Vec::new(), 0));
        }

        // Create a CSV reader from the chunk data
        let chunk_data = lines.join("\n");
        let mut builder = csv::ReaderBuilder::new();
        builder.has_headers(has_headers && offset == 0); // Only first chunk has headers
        if let Some(config) = csv_config {
            if let Some(delim) = config.delimiter {
                builder.delimiter(delim);
            }
            builder.flexible(config.flexible);
            builder.quote(config.quote_char);
            if config.trim_whitespace {
                builder.trim(csv::Trim::All);
            }
        }
        let mut reader = builder.from_reader(Cursor::new(chunk_data.as_bytes()));

        let headers = if has_headers && offset == 0 {
            Some(reader.headers()?.clone())
        } else {
            None
        };

        let mut records = Vec::new();
        for result in reader.records() {
            records.push(result?);
        }

        Ok((headers, records, actual_bytes))
    }

    /// Find the next line boundary to avoid cutting CSV records in half.
    ///
    /// When `at_eof` is true, any trailing data after the last newline is
    /// included because there is no subsequent chunk that will pick it up.
    ///
    /// Returns `(data_slice, bytes_consumed)` where `bytes_consumed` is the
    /// number of bytes from the original chunk that were consumed (including
    /// any skipped partial-line prefix), so the caller can correctly advance
    /// the file offset.
    fn find_line_boundary<'a>(
        &self,
        chunk: &'a [u8],
        skip_first_partial: bool,
        at_eof: bool,
    ) -> (&'a [u8], usize) {
        if chunk.is_empty() {
            return (chunk, 0);
        }

        let mut start_pos = 0;

        // If this isn't the first chunk, skip the first partial line
        if skip_first_partial {
            if let Some(first_newline) = chunk.iter().position(|&b| b == b'\n') {
                start_pos = first_newline + 1;
            } else {
                // No newline found — entire chunk is one partial line.
                // Consume all bytes so the next chunk moves past it.
                return (&chunk[chunk.len()..], chunk.len());
            }
        }

        // Find the last complete line
        let mut end_pos = chunk.len();

        // Look for the last newline, but don't include incomplete final line
        // unless we are at end-of-file (no subsequent chunk will pick it up).
        if !at_eof {
            if let Some(last_newline) = chunk[start_pos..].iter().rposition(|&b| b == b'\n') {
                end_pos = start_pos + last_newline + 1;
            } else if start_pos > 0 {
                // No complete lines in this chunk.
                // Consume up through the skipped partial so we advance past it.
                return (&chunk[chunk.len()..], start_pos);
            }
        }

        // Return the data slice and the total bytes consumed from the
        // original chunk (including the skipped prefix), so the caller's
        // offset advances past everything up to end_pos.
        (&chunk[start_pos..end_pos], end_pos)
    }

    /// Estimate the number of rows in the file by sampling
    pub fn estimate_row_count(&self) -> Result<usize> {
        const SAMPLE_SIZE: usize = 64 * 1024; // 64KB sample

        if self.file_size < SAMPLE_SIZE as u64 {
            // For small files, count all lines
            let cursor = Cursor::new(&*self.mmap);
            let reader = BufReader::new(cursor);
            return Ok(reader.lines().count());
        }

        // Sample from the beginning of the file
        let sample_data = &self.mmap[0..SAMPLE_SIZE];
        let cursor = Cursor::new(sample_data);
        let reader = BufReader::new(cursor);

        let sample_lines = reader.lines().count();
        if sample_lines == 0 {
            return Ok(0);
        }

        // Estimate based on sample
        let estimated_rows = (self.file_size * sample_lines as u64) / SAMPLE_SIZE as u64;
        Ok(estimated_rows as usize)
    }

    /// Check for memory leaks in the memory tracker
    pub fn check_memory_leaks(&self) -> String {
        self.memory_tracker.report_leaks()
    }

    /// Get memory usage statistics
    pub fn get_memory_stats(&self) -> (usize, usize, usize) {
        self.memory_tracker.get_memory_stats()
    }
}

impl Drop for MemoryMappedCsvReader {
    fn drop(&mut self) {
        // Automatically track deallocation when dropped
        self.memory_tracker.track_deallocation(&self.resource_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_memory_mapped_reader() -> Result<()> {
        // Create a test CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,New York")?;
        writeln!(temp_file, "Bob,30,London")?;
        writeln!(temp_file, "Charlie,35,Tokyo")?;
        temp_file.flush()?;

        // Test memory-mapped reader
        let reader = MemoryMappedCsvReader::new(temp_file.path())?;

        assert!(reader.file_size() > 0);

        // Read the entire file as one chunk
        let (headers, records, _bytes) = reader.read_csv_chunk(0, 1024, true, None)?;

        assert!(headers.is_some());
        assert_eq!(records.len(), 3);

        let header_record = headers.expect("Headers should be present in test data");
        assert_eq!(header_record.get(0), Some("name"));
        assert_eq!(header_record.get(1), Some("age"));
        assert_eq!(header_record.get(2), Some("city"));

        assert_eq!(records[0].get(0), Some("Alice"));
        assert_eq!(records[0].get(1), Some("25"));

        Ok(())
    }

    #[test]
    fn test_row_estimation() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "a,b,c")?;
        for i in 0..100 {
            writeln!(temp_file, "{},{},{}", i, i * 2, i * 3)?;
        }
        temp_file.flush()?;

        let reader = MemoryMappedCsvReader::new(temp_file.path())?;
        let estimated = reader.estimate_row_count()?;

        // Should estimate around 101 rows (header + 100 data rows)
        assert!(estimated > 90 && estimated < 120);

        Ok(())
    }

    /// Regression test: chunked reading must not lose rows at chunk boundaries.
    ///
    /// When the first chunk ends exactly at a newline, the next chunk starts
    /// at a complete line boundary. Previously, `skip_first_partial` would
    /// incorrectly skip this first complete line, silently dropping one row
    /// per chunk boundary that happened to align with a newline.
    #[test]
    fn test_no_row_loss_at_chunk_boundaries() -> Result<()> {
        let expected_rows = 1000;
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,name,value")?;
        for i in 0..expected_rows {
            // Variable-length rows to ensure chunk boundaries land mid-line sometimes
            let padding = "x".repeat(i % 50);
            writeln!(temp_file, "{},name_{}{},{}", i, i, padding, i * 10)?;
        }
        temp_file.flush()?;

        let reader = MemoryMappedCsvReader::new(temp_file.path())?;

        // Use a small chunk size to force many chunk boundaries
        let chunk_size = 512;
        let mut offset = 0u64;
        let mut total_records = 0;
        let mut first = true;

        loop {
            let (headers, records, bytes) =
                reader.read_csv_chunk(offset, chunk_size, first, None)?;
            if records.is_empty() && bytes == 0 {
                break;
            }
            if first && headers.is_some() {
                first = false;
            }
            total_records += records.len();
            offset += bytes as u64;
        }

        assert_eq!(
            total_records, expected_rows,
            "Expected {expected_rows} rows but got {total_records} — \
             rows lost at chunk boundaries"
        );

        Ok(())
    }
}
