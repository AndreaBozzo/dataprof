use std::io::{self, BufRead, Read};

const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// A buffered reader that removes exactly one UTF-8 BOM at byte offset zero.
///
/// Bytes inspected during construction are replayed unchanged when they are not
/// the complete UTF-8 BOM. This also makes detection reliable when the wrapped
/// reader exposes the prefix one byte at a time.
#[doc(hidden)]
#[derive(Debug)]
pub struct Utf8BomReader<R> {
    inner: R,
    prefix: [u8; UTF8_BOM.len()],
    prefix_len: usize,
    prefix_pos: usize,
    stripped: bool,
}

impl<R: Read> Utf8BomReader<R> {
    pub fn new(mut inner: R) -> io::Result<Self> {
        let mut prefix = [0; UTF8_BOM.len()];
        let mut prefix_len = 0;

        while prefix_len < prefix.len() {
            match inner.read(&mut prefix[prefix_len..]) {
                Ok(0) => break,
                Ok(read) => prefix_len += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }

        let stripped = prefix_len == UTF8_BOM.len() && prefix == UTF8_BOM;
        if stripped {
            prefix_len = 0;
        }

        Ok(Self {
            inner,
            prefix,
            prefix_len,
            prefix_pos: 0,
            stripped,
        })
    }

    /// Number of source bytes removed from the decoded view.
    pub fn stripped_len(&self) -> usize {
        usize::from(self.stripped) * UTF8_BOM.len()
    }
}

impl<R: Read> Read for Utf8BomReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.prefix_pos < self.prefix_len && !output.is_empty() {
            let available = &self.prefix[self.prefix_pos..self.prefix_len];
            let copied = available.len().min(output.len());
            output[..copied].copy_from_slice(&available[..copied]);
            self.prefix_pos += copied;
            return Ok(copied);
        }

        self.inner.read(output)
    }
}

impl<R: BufRead> BufRead for Utf8BomReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.prefix_pos < self.prefix_len {
            Ok(&self.prefix[self.prefix_pos..self.prefix_len])
        } else {
            self.inner.fill_buf()
        }
    }

    fn consume(&mut self, amount: usize) {
        let prefix_remaining = self.prefix_len.saturating_sub(self.prefix_pos);
        let prefix_consumed = amount.min(prefix_remaining);
        self.prefix_pos += prefix_consumed;
        self.inner.consume(amount - prefix_consumed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    fn read_all(data: &[u8]) -> (Vec<u8>, usize) {
        let chunked = BufReader::with_capacity(1, Cursor::new(data));
        let mut reader = Utf8BomReader::new(chunked).unwrap();
        let stripped = reader.stripped_len();
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        (output, stripped)
    }

    #[test]
    fn strips_one_leading_utf8_bom_across_small_buffers() {
        let (output, stripped) = read_all(b"\xEF\xBB\xBF{\"id\":1}");
        assert_eq!(output, b"{\"id\":1}");
        assert_eq!(stripped, 3);
    }

    #[test]
    fn preserves_incomplete_nonleading_and_second_bom_bytes() {
        for data in [
            b"\xEF\xBB".as_slice(),
            b" \xEF\xBB\xBF{}".as_slice(),
            b"\xEF\xBB\xBF\xEF\xBB\xBF{}".as_slice(),
        ] {
            let (output, _) = read_all(data);
            let expected = data.strip_prefix(&UTF8_BOM).unwrap_or(data);
            assert_eq!(output, expected);
        }
    }
}
