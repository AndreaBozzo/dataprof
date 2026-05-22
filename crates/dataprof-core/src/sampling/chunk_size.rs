use sysinfo::System;

#[derive(Debug, Clone, Default)]
pub enum ChunkSize {
    /// Fixed chunk size in rows
    Fixed(usize),

    /// Adaptive based on available memory
    #[default]
    Adaptive,

    /// Custom sizing function - cannot derive Debug/Clone with function pointer
    Custom(fn(u64) -> usize),
}

impl ChunkSize {
    /// Calculate optimal chunk size based on available memory and file size
    pub fn calculate(&self, file_size_bytes: u64) -> usize {
        match self {
            ChunkSize::Fixed(size) => *size,
            ChunkSize::Adaptive => self.adaptive_size(file_size_bytes),
            ChunkSize::Custom(func) => func(file_size_bytes),
        }
    }

    fn adaptive_size(&self, file_size_bytes: u64) -> usize {
        let mut system = System::new_all();
        system.refresh_memory();

        let available_memory = system.available_memory();

        // Use max 10% of available memory for each chunk
        let memory_per_chunk = (available_memory / 10).max(64 * 1024 * 1024); // Min 64MB

        // Estimate rows per MB (rough heuristic: 1KB per row average)
        let estimated_row_size = 1024;
        let rows_per_chunk = (memory_per_chunk / estimated_row_size) as usize;

        // Adjust based on file size
        let file_size_mb = file_size_bytes / (1024 * 1024);

        if file_size_mb < 100 {
            // Small files: process all at once
            rows_per_chunk * 10
        } else if file_size_mb > 10_000 {
            // Very large files: smaller chunks to avoid memory pressure
            rows_per_chunk / 2
        } else {
            rows_per_chunk
        }
    }
}
