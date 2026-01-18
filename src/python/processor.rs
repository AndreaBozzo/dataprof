use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::analysis::analyze_csv_file;

/// Context manager for CSV file processing with automatic handling
#[pyclass]
pub struct PyCsvProcessor {
    file_handle: Option<String>,
    temp_files: Vec<String>,
    chunk_size: usize,
    processed_rows: usize,
}

#[pymethods]
impl PyCsvProcessor {
    #[new]
    fn new(chunk_size: Option<usize>) -> Self {
        PyCsvProcessor {
            file_handle: None,
            temp_files: Vec::new(),
            chunk_size: chunk_size.unwrap_or(1000),
            processed_rows: 0,
        }
    }

    /// Enter context manager
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Exit context manager with cleanup
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        // Clean up all temporary files
        for temp_file in &self.temp_files {
            let _ = std::fs::remove_file(temp_file);
        }
        self.temp_files.clear();
        self.file_handle = None;
        self.processed_rows = 0;
        Ok(false)
    }

    /// Open a CSV file for processing
    fn open_file(&mut self, path: &str) -> PyResult<()> {
        // Validate file exists and is readable
        if !std::path::Path::new(path).exists() {
            return Err(PyRuntimeError::new_err(format!("File not found: {}", path)));
        }

        self.file_handle = Some(path.to_string());
        self.processed_rows = 0;
        Ok(())
    }

    /// Process the file in chunks
    fn process_chunks(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        let file_path = self
            .file_handle
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("No file opened. Call open_file() first."))?;

        // Read and process file in chunks
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to read file: {}", e)))?;

        let lines: Vec<&str> = content.lines().collect();
        if lines.is_empty() {
            return Err(PyRuntimeError::new_err("Empty file"));
        }

        let header = lines[0];
        let data_lines = &lines[1..];

        let mut chunk_results: Vec<Py<PyAny>> = Vec::new();
        let mut chunk_number = 0;

        for chunk in data_lines.chunks(self.chunk_size) {
            chunk_number += 1;

            // Create temporary chunk file
            let chunk_content = format!("{}\n{}", header, chunk.join("\n"));
            let temp_path = format!("{}.chunk_{}.csv", file_path, chunk_number);

            std::fs::write(&temp_path, chunk_content)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to write chunk: {}", e)))?;

            self.temp_files.push(temp_path.clone());

            // Analyze the chunk
            let chunk_result = analyze_csv_file(&temp_path, None)?;
            chunk_results.push(chunk_result.into_pyobject(py)?.into());

            self.processed_rows += chunk.len();
        }

        Ok(chunk_results.into_pyobject(py)?.into())
    }

    /// Get processing statistics
    fn get_processing_info(&self, py: Python) -> PyResult<Py<PyAny>> {
        let mut info: std::collections::HashMap<&str, Py<PyAny>> = std::collections::HashMap::new();
        info.insert(
            "file_path",
            self.file_handle
                .as_ref()
                .unwrap_or(&"None".to_string())
                .clone()
                .into_pyobject(py)?
                .into(),
        );
        info.insert("chunk_size", self.chunk_size.into_pyobject(py)?.into());
        info.insert(
            "processed_rows",
            self.processed_rows.into_pyobject(py)?.into(),
        );
        info.insert(
            "temp_files_created",
            self.temp_files.len().into_pyobject(py)?.into(),
        );

        Ok(info.into_pyobject(py)?.into())
    }
}
