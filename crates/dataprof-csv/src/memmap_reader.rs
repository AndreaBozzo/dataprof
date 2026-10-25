use anyhow::Result;
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

use crate::CsvParserConfig;

/// Memory-mapped CSV reader for efficient processing of large files.
pub struct MemoryMappedCsvReader {
    mmap: Mmap,
    file_size: u64,
}

impl MemoryMappedCsvReader {
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();

        // Safety: The file is opened read-only and we hold the File handle for the
        // lifetime of the Mmap. The file must not be concurrently modified.
        #[allow(unsafe_code)]
        let mmap = unsafe { Mmap::map(&file)? };

        Ok(Self { mmap, file_size })
    }

    /// Get file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
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
        let start = offset as usize;
        if start >= self.mmap.len() {
            return Ok((None, Vec::new(), 0));
        }

        let quote = csv_config.map_or(b'"', |config| config.quote_char);
        let delimiter = csv_config
            .and_then(|config| config.delimiter)
            .unwrap_or(b',');
        let mut end = self.csv_record_boundary(start, chunk_size.max(1), quote, delimiter);

        // A chunk containing only leading record terminators makes the csv
        // crate synthesize an empty header at EOF. The following chunk would
        // then treat the real header as data. When the first target lands in a
        // run of blank lines, include the first non-blank logical record too so
        // header discovery has the same view as a full-file reader.
        if has_headers && offset == 0 {
            // The CSV decoder strips a leading UTF-8 BOM, so the blank lines
            // to step over start after it. Counting from byte 0 stopped at the
            // BOM, and a chunk of BOM and line end became an empty header with
            // the real one read as data (#1101).
            let bom = self.leading_bom_len();
            let header_start = bom
                + self.mmap[bom..]
                    .iter()
                    .take_while(|&&byte| matches!(byte, b'\r' | b'\n'))
                    .count();
            if end <= header_start && header_start < self.mmap.len() {
                end = self.csv_record_boundary(header_start, 1, quote, delimiter);
            }
        }
        let chunk_data = &self.mmap[start..end];
        let actual_bytes = chunk_data.len();

        let mut builder = csv::ReaderBuilder::new();
        builder.has_headers(has_headers && offset == 0);
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
        let mut reader = builder.from_reader(chunk_data);

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

    /// Whether the file ends inside a quoted field, scanning from `offset` on.
    ///
    /// `offset` must be a record boundary, such as the offset of a chunk
    /// [`Self::read_csv_chunk`] returned. Passing the last chunk's offset keeps
    /// the scan to that chunk. The CSV parser accepts a quote that is never
    /// closed as a field running to the end of the file, so this is the only
    /// sign of it.
    pub fn ends_inside_quotes(&self, offset: u64, csv_config: Option<&CsvParserConfig>) -> bool {
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.mmap.len());
        let quote = csv_config.map_or(b'"', |config| config.quote_char);
        let delimiter = csv_config
            .and_then(|config| config.delimiter)
            .unwrap_or(b',');
        crate::quote_state::ends_inside_quotes(&self.mmap[start..], delimiter, quote, start == 0)
    }

    /// Return a byte boundary that never splits an RFC 4180 record.
    ///
    /// `chunk_size` is a working-set target, not permission to hand a partial
    /// header or record to the CSV decoder. Prefer the last complete record at
    /// or before the target. If the first record itself is larger, extend just
    /// far enough to include it. The latter is unavoidable for any parser that
    /// returns a complete cell, and prevents a long header/record from being
    /// silently discarded.
    fn csv_record_boundary(
        &self,
        start: usize,
        chunk_size: usize,
        quote: u8,
        delimiter: u8,
    ) -> usize {
        let target = start.saturating_add(chunk_size).min(self.mmap.len());
        // A leading BOM is no field content: the byte after it starts the
        // first field, so a quote there opens a quoted header (#1101).
        let mut index = if start == 0 {
            self.leading_bom_len()
        } else {
            start
        };
        let mut in_quotes = false;
        let mut at_field_start = true;
        let mut last_boundary = None;

        while index < target {
            let byte = self.mmap[index];
            if in_quotes && byte == quote {
                // Two quotes inside a quoted field encode one literal quote;
                // neither ends the field.
                if self.mmap.get(index + 1) == Some(&quote) {
                    index += 2;
                    continue;
                }
                in_quotes = false;
            } else if !in_quotes && at_field_start && byte == quote {
                in_quotes = true;
                at_field_start = false;
            } else if !in_quotes && byte == delimiter {
                at_field_start = true;
            } else if !in_quotes && let Some(boundary) = self.csv_record_terminator_end(index) {
                // A CRLF whose CR is the target's last byte crosses the target
                // by one. Prefer an earlier complete record when one exists;
                // otherwise include the LF so the pair is never split.
                if boundary > target && last_boundary.is_some() {
                    break;
                }
                last_boundary = Some(boundary);
                at_field_start = true;
                index = boundary;
                continue;
            } else if !in_quotes {
                at_field_start = false;
            }
            index += 1;
        }

        // A target at EOF owns the final unterminated record as well as every
        // earlier terminated one. Returning the last separator here would
        // unnecessarily defer that final record to a second chunk and would
        // make a whole-file logical-record sample incomplete.
        if target == self.mmap.len() {
            return target;
        }

        if let Some(boundary) = last_boundary {
            return boundary;
        }

        // No complete record fit. Continue from the quote state at `target`
        // until the current logical record ends, or include the final
        // unterminated record so the CSV crate can validate it honestly.
        while index < self.mmap.len() {
            let byte = self.mmap[index];
            if in_quotes && byte == quote {
                if self.mmap.get(index + 1) == Some(&quote) {
                    index += 2;
                    continue;
                }
                in_quotes = false;
            } else if !in_quotes && at_field_start && byte == quote {
                in_quotes = true;
                at_field_start = false;
            } else if !in_quotes && byte == delimiter {
                at_field_start = true;
            } else if !in_quotes && let Some(boundary) = self.csv_record_terminator_end(index) {
                return boundary;
            } else if !in_quotes {
                at_field_start = false;
            }
            index += 1;
        }

        self.mmap.len()
    }

    /// Length of a UTF-8 BOM at the start of the file, which the CSV decoder
    /// strips; 0 when there is none.
    fn leading_bom_len(&self) -> usize {
        if self.mmap.starts_with(b"\xEF\xBB\xBF") {
            3
        } else {
            0
        }
    }

    /// End offset for the CSV crate's default CR, LF, or CRLF terminator.
    fn csv_record_terminator_end(&self, index: usize) -> Option<usize> {
        match self.mmap[index] {
            b'\r' if self.mmap.get(index + 1) == Some(&b'\n') => Some(index + 2),
            b'\r' | b'\n' => Some(index + 1),
            _ => None,
        }
    }

    /// Count logical CSV records, including the header, with a full scan.
    /// This shares the record parser with partial counting and avoids prefix bias.
    pub fn estimate_csv_record_count(&self, csv_config: Option<&CsvParserConfig>) -> Result<usize> {
        let delimiter = csv_config
            .and_then(|config| config.delimiter)
            .unwrap_or(b',');
        let quote = csv_config.map_or(b'"', |config| config.quote_char);
        let count = crate::row_count::csv_record_count(&self.mmap[..], delimiter, quote)?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_memory_mapped_reader() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,New York")?;
        writeln!(temp_file, "Bob,30,London")?;
        writeln!(temp_file, "Charlie,35,Tokyo")?;
        temp_file.flush()?;

        let reader = MemoryMappedCsvReader::new(temp_file.path())?;

        assert!(reader.file_size() > 0);

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
        let estimated = reader.estimate_csv_record_count(None)?;

        assert!(estimated > 90 && estimated < 120);

        Ok(())
    }

    #[test]
    fn row_estimation_counts_logical_csv_records() -> Result<()> {
        let cases: [(&str, &[u8], Option<CsvParserConfig>); 3] = [
            (
                "quoted multiline",
                b"id,bio\n1,\"hello\nworld\"\n2,plain",
                None,
            ),
            ("lone CR", b"id,value\r1,x\r2,y", None),
            (
                "custom delimiter",
                b"id;bio\r1;\"hello\nworld\"\r2;plain",
                Some(CsvParserConfig::default().with_delimiter(b';')),
            ),
        ];

        for (case, payload, config) in cases {
            let mut temp_file = NamedTempFile::new()?;
            temp_file.write_all(payload)?;
            temp_file.flush()?;

            let reader = MemoryMappedCsvReader::new(temp_file.path())?;
            assert_eq!(
                reader.estimate_csv_record_count(config.as_ref())?,
                3,
                "case={case}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_no_row_loss_at_chunk_boundaries() -> Result<()> {
        let expected_rows = 1000;
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,name,value")?;
        for i in 0..expected_rows {
            let padding = "x".repeat(i % 50);
            writeln!(temp_file, "{},name_{}{},{}", i, i, padding, i * 10)?;
        }
        temp_file.flush()?;

        let reader = MemoryMappedCsvReader::new(temp_file.path())?;

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
            "Expected {expected_rows} rows but got {total_records} — rows lost at chunk boundaries"
        );

        Ok(())
    }

    #[test]
    fn csv_chunk_smaller_than_header_preserves_schema_and_rows() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        write!(temp_file, "alpha,beta\n1,2\n3,4\n")?;
        temp_file.flush()?;

        let reader = MemoryMappedCsvReader::new(temp_file.path())?;
        let mut offset = 0u64;
        let mut headers = None;
        let mut records = Vec::new();

        loop {
            let (chunk_headers, chunk_records, bytes) =
                reader.read_csv_chunk(offset, 5, headers.is_none(), None)?;
            if bytes == 0 {
                break;
            }
            if headers.is_none() {
                headers = chunk_headers;
            }
            records.extend(chunk_records);
            offset += bytes as u64;
        }

        assert_eq!(
            headers.expect("header").iter().collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].iter().collect::<Vec<_>>(), ["1", "2"]);
        assert_eq!(records[1].iter().collect::<Vec<_>>(), ["3", "4"]);
        Ok(())
    }

    #[test]
    fn csv_chunk_skips_leading_blank_lines_before_header() -> Result<()> {
        let payload = b"\n\r\n\ralpha,beta\n1,2";
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(payload)?;
        temp_file.flush()?;

        let reader = MemoryMappedCsvReader::new(temp_file.path())?;
        let mut offset = 0u64;
        let mut headers = None;
        let mut records = Vec::new();

        loop {
            let (chunk_headers, chunk_records, bytes) =
                reader.read_csv_chunk(offset, 1, headers.is_none(), None)?;
            if bytes == 0 {
                break;
            }
            if headers.is_none() {
                headers = chunk_headers;
            }
            records.extend(chunk_records);
            offset += bytes as u64;
        }

        assert_eq!(
            headers.expect("header").iter().collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].iter().collect::<Vec<_>>(), ["1", "2"]);
        Ok(())
    }

    #[test]
    fn csv_chunk_preserves_multiline_records_for_every_terminator() -> Result<()> {
        let cases: [(&str, &[u8], &str); 2] = [
            (
                "crlf",
                b"id,bio\r\n1,\"hello \"\"quoted\"\"\nworld\"\r\n2,plain",
                "hello \"quoted\"\nworld",
            ),
            (
                "lone-cr",
                b"id,bio\r1,\"hello\nworld\"\r2,plain",
                "hello\nworld",
            ),
        ];

        for (terminator, payload, expected_bio) in cases {
            let mut temp_file = NamedTempFile::new()?;
            temp_file.write_all(payload)?;
            temp_file.flush()?;

            let reader = MemoryMappedCsvReader::new(temp_file.path())?;
            for chunk_size in 1..=payload.len() + 1 {
                let mut offset = 0u64;
                let mut headers = None;
                let mut records = Vec::new();

                loop {
                    let (chunk_headers, chunk_records, bytes) =
                        reader.read_csv_chunk(offset, chunk_size, headers.is_none(), None)?;
                    if bytes == 0 {
                        break;
                    }
                    if headers.is_none() {
                        headers = chunk_headers;
                    }
                    records.extend(chunk_records);
                    offset += bytes as u64;
                }

                assert_eq!(
                    headers.expect("header").iter().collect::<Vec<_>>(),
                    ["id", "bio"],
                    "terminator={terminator}, chunk_size={chunk_size}"
                );
                assert_eq!(
                    records.len(),
                    2,
                    "terminator={terminator}, chunk_size={chunk_size}"
                );
                assert_eq!(
                    records[0].get(0),
                    Some("1"),
                    "terminator={terminator}, chunk_size={chunk_size}"
                );
                assert_eq!(
                    records[0].get(1),
                    Some(expected_bio),
                    "terminator={terminator}, chunk_size={chunk_size}"
                );
                assert_eq!(
                    records[1].iter().collect::<Vec<_>>(),
                    ["2", "plain"],
                    "terminator={terminator}, chunk_size={chunk_size}"
                );
            }
        }
        Ok(())
    }
}
