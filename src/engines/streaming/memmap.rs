use crate::core::MemoryTracker;
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

        // Memory map the file
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

    /// Read a chunk of the file starting at the given byte offset
    pub fn read_chunk(&self, offset: u64, chunk_size: usize) -> Result<Vec<String>> {
        let start = offset as usize;
        let end = std::cmp::min(start + chunk_size, self.mmap.len());

        if start >= self.mmap.len() {
            return Ok(Vec::new());
        }

        // Get the chunk data
        let chunk_data = &self.mmap[start..end];

        // Find line boundaries to avoid cutting lines in half
        let adjusted_chunk = self.find_line_boundary(chunk_data, start > 0);

        // Parse lines from the chunk
        let cursor = Cursor::new(adjusted_chunk);
        let reader = BufReader::new(cursor);

        let mut lines = Vec::new();
        for line in reader.lines() {
            lines.push(line?);
        }

        Ok(lines)
    }

    /// Parse CSV records from memory-mapped data in chunks
    pub fn read_csv_chunk(
        &self,
        offset: u64,
        chunk_size: usize,
        has_headers: bool,
    ) -> Result<(Option<csv::StringRecord>, Vec<csv::StringRecord>)> {
        let lines = self.read_chunk(offset, chunk_size)?;

        if lines.is_empty() {
            return Ok((None, Vec::new()));
        }

        // Create a CSV reader from the chunk data
        let chunk_data = lines.join("\n");
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(has_headers && offset == 0) // Only first chunk has headers
            .from_reader(Cursor::new(chunk_data.as_bytes()));

        let headers = if has_headers && offset == 0 {
            Some(reader.headers()?.clone())
        } else {
            None
        };

        let mut records = Vec::new();
        for result in reader.records() {
            records.push(result?);
        }

        Ok((headers, records))
    }

    /// Find the next line boundary to avoid cutting CSV records in half
    fn find_line_boundary<'a>(&self, chunk: &'a [u8], skip_first_partial: bool) -> &'a [u8] {
        if chunk.is_empty() {
            return chunk;
        }

        let mut start_pos = 0;

        // If this isn't the first chunk, skip the first partial line
        if skip_first_partial {
            if let Some(first_newline) = chunk.iter().position(|&b| b == b'\n') {
                start_pos = first_newline + 1;
            } else {
                // No newline found, return empty chunk
                return &chunk[chunk.len()..];
            }
        }

        // Find the last complete line
        let mut end_pos = chunk.len();

        // Look for the last newline, but don't include incomplete final line
        if let Some(last_newline) = chunk[start_pos..].iter().rposition(|&b| b == b'\n') {
            end_pos = start_pos + last_newline + 1;
        } else if start_pos > 0 {
            // No complete lines in this chunk
            return &chunk[chunk.len()..];
        }

        &chunk[start_pos..end_pos]
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
        let (headers, records) = reader.read_csv_chunk(0, 1024, true)?;

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
}
