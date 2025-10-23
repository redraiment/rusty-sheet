use crate::error::RustySheetError;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub(crate) enum UnifiedReaderError {
    #[error("No data from remote file: '{0}'")]
    RemoteFileNoDataError(String),
}

/// A unified reader that can handle both local files and remote URLs
pub(crate) enum UnifiedReader {
    /// Local file reader
    Local(BufReader<File>),
    /// Remote URL reader (in-memory buffer)
    Remote(Cursor<Vec<u8>>),
}

impl UnifiedReader {
    /// Opens a file from either a local path or remote URL
    /// For remote URLs, uses DuckDB's read_blob with proper credential handling
    ///
    /// # Arguments
    /// * `file_name` - Path or URL to the file
    ///
    /// # Returns
    /// * `Result<UnifiedReader, RustySheetError>` - Reader for the file content
    pub(crate) fn new(file_name: &str) -> Result<UnifiedReader, RustySheetError> {
        // Check if it's a remote URL
        if Self::is_remote_url(file_name) {
            // Use DuckDB's read_blob for all remote URLs (http, https, s3, gs, hf, etc.)
            // DuckDB handles credentials and protocols automatically
            Self::read_blob_with_duckdb(file_name)
        } else {
            // Local file
            let file = File::open(file_name)?;
            Ok(UnifiedReader::Local(BufReader::new(file)))
        }
    }

    /// Checks if a file name represents a remote URL
    pub(crate) fn is_remote_url(file_name: &str) -> bool {
        if let Ok(url) = Url::parse(file_name) {
            url.scheme() != "file"
        } else {
            false
        }
    }

    /// Reads a remote file using DuckDB's read_blob functionality
    /// This handles all protocols (http, https, s3, gs, hf, etc.) with proper credential management
    fn read_blob_with_duckdb(file_name: &str) -> Result<UnifiedReader, RustySheetError> {
        // Create an in-memory DuckDB connection and read the blob directly
        let connection = duckdb::Connection::open_in_memory()?;
        // Read the blob directly using query_row - DuckDB handles all URL types and credentials
        let result: Result<Vec<u8>, _> = connection.query_row("SELECT content FROM read_blob(?)", [file_name], |row| row.get(0));
        // Close connection
        connection.close().map_err(|(_, e)| e)?;

        let bytes = result?;
        if bytes.is_empty() {
            Err(UnifiedReaderError::RemoteFileNoDataError(file_name.to_owned()))?;
        }

        // Return as in-memory cursor
        Ok(UnifiedReader::Remote(Cursor::new(bytes)))
    }
}

impl Read for UnifiedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            UnifiedReader::Local(reader) => reader.read(buf),
            UnifiedReader::Remote(reader) => reader.read(buf),
        }
    }
}

impl Seek for UnifiedReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            UnifiedReader::Local(reader) => reader.seek(pos),
            UnifiedReader::Remote(reader) => reader.seek(pos),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_remote_url() {
        // Test local files
        assert!(!UnifiedReader::is_remote_url("test.xlsx"));
        assert!(!UnifiedReader::is_remote_url("/path/to/test.xlsx"));
        assert!(!UnifiedReader::is_remote_url("./relative/test.xlsx"));

        // Test remote URLs
        assert!(UnifiedReader::is_remote_url("http://example.com/test.xlsx"));
        assert!(UnifiedReader::is_remote_url("https://example.com/test.xlsx"));
        assert!(UnifiedReader::is_remote_url("s3://bucket/test.xlsx"));
        assert!(UnifiedReader::is_remote_url("gs://bucket/test.xlsx"));

        // Test file URLs (should not be considered remote)
        assert!(!UnifiedReader::is_remote_url("file:///path/to/test.xlsx"));
    }

    #[test]
    fn test_open_local_file() {
        // Test opening a local file (Cargo.toml should exist)
        let result = UnifiedReader::new("Cargo.toml");
        assert!(result.is_ok(), "Failed to open local file: {:?}", result.err());

        // Test opening a non-existent local file
        let result = UnifiedReader::new("non_existent_file.xlsx");
        assert!(result.is_err(), "Should fail to open non-existent file");
    }
}
