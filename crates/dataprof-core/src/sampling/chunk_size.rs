use sysinfo::System;

/// How much data a streaming engine reads per chunk.
///
/// **The unit is bytes, everywhere.** Chunk size bounds how much of the source
/// is resident at once, so it is expressed in the unit that actually bounds
/// memory rather than in rows, whose width varies per dataset. Every engine and
/// binding agrees: `ChunkSize::Fixed(65_536)` and Python's `chunk_size=65536`
/// both mean 64 KiB per chunk, whether the source is a file, a byte stream, or
/// a URL.
///
/// Chunk size never changes *what* a profile contains — only the granularity at
/// which the source is read, progress is emitted, and chunk-level stop
/// conditions are evaluated.
#[derive(Debug, Clone, Default)]
pub enum ChunkSize {
    /// Fixed chunk size in **bytes**.
    Fixed(usize),

    /// Let the engine choose the chunk size (default).
    ///
    /// Each engine resolves this against what it knows: the incremental engine
    /// derives a size from its memory limit and the file size, while the async
    /// reader — whose source has no length to adapt to — uses a fixed working-set
    /// target. Unlike [`Fixed`](Self::Fixed), the resulting size is not a
    /// guarantee, so it is not something to assert against.
    #[default]
    Adaptive,

    /// Custom sizing function, given the source size in bytes and returning a
    /// chunk size in bytes. Cannot derive Debug/Clone with a function pointer.
    Custom(fn(u64) -> usize),
}

impl ChunkSize {
    /// Resolve to a concrete chunk size in bytes for a source of the given size.
    ///
    /// This is a standalone helper, not the path any engine takes: `Fixed` and
    /// `Custom` resolve exactly as an engine would, but `Adaptive` here is
    /// derived from *system* available memory, whereas an engine derives it
    /// from its own configured memory limit. Do not use this to predict what an
    /// engine will do with `Adaptive`.
    pub fn calculate(&self, file_size_bytes: u64) -> usize {
        match self {
            ChunkSize::Fixed(size) => *size,
            ChunkSize::Adaptive => self.adaptive_size(file_size_bytes),
            ChunkSize::Custom(func) => func(file_size_bytes),
        }
    }

    /// Derive a chunk size from system available memory and the source size.
    fn adaptive_size(&self, file_size_bytes: u64) -> usize {
        let mut system = System::new_all();
        system.refresh_memory();

        let available_memory = system.available_memory();

        // Use max 10% of available memory for each chunk
        let bytes_per_chunk = (available_memory / 10).max(64 * 1024 * 1024) as usize; // Min 64MB

        // Adjust based on file size
        let file_size_mb = file_size_bytes / (1024 * 1024);

        if file_size_mb > 10_000 {
            // Very large files: smaller chunks to avoid memory pressure
            bytes_per_chunk / 2
        } else {
            bytes_per_chunk
        }
    }
}
