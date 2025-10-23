use crate::error::RustySheetError;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};
use url::Url;

/// A unified reader that can handle both local files and remote URLs
pub enum UnifiedReader {
    /// Local file reader
    Local(BufReader<File>),
    /// Remote URL reader (in-memory buffer)
    Remote(Cursor<Vec<u8>>),
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

/// Opens a file from either a local path or remote URL
/// For remote URLs, uses DuckDB's read_blob with proper credential handling
/// 
/// # Arguments
/// * `file_name` - Path or URL to the file
/// 
/// # Returns
/// * `Result<UnifiedReader, RustySheetError>` - Reader for the file content
pub fn open_remote_file(file_name: &str) -> Result<UnifiedReader, RustySheetError> {
    // Check if it's a remote URL
    if is_remote_url(file_name) {
        // Use DuckDB's read_blob for all remote URLs (http, https, s3, gs)
        // DuckDB handles credentials and protocols automatically
        read_blob_with_duckdb(file_name)
    } else {
        // Local file
        let file = File::open(file_name)?;
        Ok(UnifiedReader::Local(BufReader::new(file)))
    }
}

/// Reads a remote file using DuckDB's read_blob functionality
/// This handles all protocols (http, https, s3, gs) with proper credential management
fn read_blob_with_duckdb(file_name: &str) -> Result<UnifiedReader, RustySheetError> {
    use duckdb::Connection;
    
    // Create an in-memory DuckDB connection and read the blob directly
    let conn = Connection::open_in_memory().map_err(|e| {
        RustySheetError::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to create DuckDB connection: {}", e),
        ))
    })?;
    
    // Read the blob directly using query_row - DuckDB handles all URL types and credentials
    let bytes: Vec<u8> = conn
        .query_row("SELECT content FROM read_blob(?)", [file_name], |row| row.get(0))
        .map_err(|e| {
            RustySheetError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read blob from '{}': {}", file_name, e),
            ))
        })?;
    
    if bytes.is_empty() {
        return Err(RustySheetError::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("No data from remote file: {}", file_name),
        )));
    }
    
    // Return as in-memory cursor
    Ok(UnifiedReader::Remote(Cursor::new(bytes)))
}

/// Checks if a file name represents a remote URL
pub fn is_remote_url(file_name: &str) -> bool {
    if let Ok(url) = Url::parse(file_name) {
        matches!(url.scheme(), "http" | "https" | "s3" | "gs")
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_remote_url() {
        // Test local files
        assert!(!is_remote_url("test.xlsx"));
        assert!(!is_remote_url("/path/to/test.xlsx"));
        assert!(!is_remote_url("./relative/test.xlsx"));
        
        // Test remote URLs
        assert!(is_remote_url("http://example.com/test.xlsx"));
        assert!(is_remote_url("https://example.com/test.xlsx"));
        assert!(is_remote_url("s3://bucket/test.xlsx"));
        assert!(is_remote_url("gs://bucket/test.xlsx"));
        
        // Test file URLs (should not be considered remote)
        assert!(!is_remote_url("file:///path/to/test.xlsx"));
    }

    #[test]
    fn test_open_local_file() {
        // Test opening a local file (Cargo.toml should exist)
        let result = open_remote_file("Cargo.toml");
        assert!(result.is_ok(), "Failed to open local file: {:?}", result.err());
        
        // Test opening a non-existent local file
        let result = open_remote_file("non_existent_file.xlsx");
        assert!(result.is_err(), "Should fail to open non-existent file");
    }
}