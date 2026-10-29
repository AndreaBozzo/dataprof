//! Recognize binary files by their signature bytes before a text reader sees
//! them (#893).
//!
//! The text readers (CSV, JSON, JSONL) are chosen by file name, so a gzip file
//! named `data.csv.gz`, or a Parquet file without a `.parquet` extension,
//! reached a text parser and failed with advice about delimiters or column
//! counts. Reading a few signature bytes first turns that into an error that
//! names what the file is and what to do with it.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::errors::DataProfilerError;

/// The formats recognized by a marker at both ends of the file. Each marker is
/// plain ASCII, so a text file may start with it; only one that also ends with
/// it is taken for the binary format.
const BOTH_ENDS_MAGIC: [(&[u8], BinarySignature); 3] = [
    (b"PAR1", BinarySignature::Parquet),
    (b"ARROW1", BinarySignature::ArrowIpc),
    (b"FEA1", BinarySignature::FeatherV1),
];

/// The longest prefix any signature needs.
const HEAD_LEN: u64 = 6;

/// A binary format recognized by its signature bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BinarySignature {
    /// gzip, which starts with `1f 8b`.
    Gzip,
    /// Zstandard, which starts with `28 b5 2f fd`.
    Zstd,
    /// A zip archive, which starts with `50 4b 03 04`. Spreadsheet formats
    /// such as `.xlsx` and `.ods` are zip archives too.
    Zip,
    /// Parquet, which carries `PAR1` at both ends of the file.
    Parquet,
    /// An Arrow IPC file (Feather V2), which carries `ARROW1` at both ends.
    /// The IPC stream format has no magic and is not recognized.
    ArrowIpc,
    /// Feather V1, which carries `FEA1` at both ends.
    FeatherV1,
}

impl BinarySignature {
    /// The signature found at the start of `head`, for the formats that a
    /// prefix identifies. Every one of them holds a byte that cannot appear
    /// there in UTF-8 text, so no text file matches.
    fn from_prefix(head: &[u8]) -> Option<Self> {
        if head.starts_with(&[0x1f, 0x8b]) {
            Some(Self::Gzip)
        } else if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            Some(Self::Zstd)
        } else if head.starts_with(&[0x50, 0x4b, 0x03, 0x04]) {
            Some(Self::Zip)
        } else {
            None
        }
    }

    /// What the file is, phrased to follow "the file is".
    pub fn description(self) -> &'static str {
        match self {
            Self::Gzip => "gzip-compressed",
            Self::Zstd => "zstd-compressed",
            Self::Zip => "a zip archive",
            Self::Parquet => "a Parquet file",
            Self::ArrowIpc => "an Arrow IPC file (Feather V2)",
            Self::FeatherV1 => "a Feather V1 file",
        }
    }

    /// The way forward for a file at `path` carrying this signature.
    pub fn suggestion(self, path: &str) -> String {
        match self {
            Self::Gzip => {
                format!("Decompress it first (e.g. `gunzip -k '{path}'`) and profile the result.")
            }
            Self::Zstd => {
                format!("Decompress it first (e.g. `zstd -d '{path}'`) and profile the result.")
            }
            Self::Zip => format!(
                "Extract the file to profile (e.g. `unzip '{path}'`) and profile it. A spreadsheet \
                 (.xlsx, .ods) is a zip archive too: export the sheet as CSV first."
            ),
            Self::Parquet => parquet_suggestion(),
            Self::ArrowIpc => format!(
                "dataprof does not read Arrow IPC files directly. Read it with pyarrow and pass \
                 the table (`pyarrow.feather.read_table('{path}')`), or stream it with \
                 `pyarrow.dataset.dataset('{path}', format=\"ipc\").scanner().to_reader()`."
            ),
            Self::FeatherV1 => format!(
                "dataprof does not read Feather files directly. Read it with pyarrow and pass \
                 the table (`pyarrow.feather.read_table('{path}')`)."
            ),
        }
    }
}

#[cfg(feature = "parquet")]
fn parquet_suggestion() -> String {
    "Read it as Parquet: give it a `.parquet` extension, or select the format \
     (`.format(FileFormat::Parquet)` in Rust, `format=\"parquet\"` in Python)."
        .to_string()
}

#[cfg(not(feature = "parquet"))]
fn parquet_suggestion() -> String {
    "This build does not read Parquet; rebuild with the `parquet` feature to profile it."
        .to_string()
}

/// The binary format the bytes of `path` carry, if any.
///
/// Reads the first six bytes, and for a `PAR1`, `ARROW1` or `FEA1` start also
/// the same number of bytes at the end: a text file can begin with any of
/// those, so the format is only recognized when the marker sits at both ends,
/// as `dataprof_parquet::is_parquet_file` requires for Parquet.
///
/// A file that cannot be opened or read answers `None`. Whether the file is
/// readable is for the reader that comes next to report, with its own path
/// context; this only decides whether a text reader should see it at all.
pub fn sniff_binary_file(path: &Path) -> Option<BinarySignature> {
    let mut file = File::open(path).ok()?;
    let mut head = Vec::with_capacity(HEAD_LEN as usize);
    (&mut file).take(HEAD_LEN).read_to_end(&mut head).ok()?;
    if let Some(signature) = BinarySignature::from_prefix(&head) {
        return Some(signature);
    }
    let (magic, signature) = BOTH_ENDS_MAGIC
        .into_iter()
        .find(|(magic, _)| head.starts_with(magic))?;
    // Both markers, without overlap.
    if file.metadata().ok()?.len() < 2 * magic.len() as u64 {
        return None;
    }
    let mut tail = vec![0u8; magic.len()];
    file.seek(SeekFrom::End(-(magic.len() as i64))).ok()?;
    file.read_exact(&mut tail).ok()?;
    (tail == magic).then_some(signature)
}

/// Fail with [`DataProfilerError::BinaryInput`] when `path` carries a binary
/// signature. Call it before a file reaches a text reader.
pub fn reject_binary_input(path: &Path) -> Result<(), DataProfilerError> {
    match sniff_binary_file(path) {
        Some(signature) => Err(DataProfilerError::binary_input(path, signature)),
        None => Ok(()),
    }
}
