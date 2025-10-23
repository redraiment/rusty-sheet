use thiserror::Error;

/// Main error type for the Rusty Sheet extension.
/// Aggregates errors from various sources including standard library, dependencies, and internal modules.
#[derive(Error, Debug)]
pub(crate) enum RustySheetError {
    #[error("{0}")]
    WithContextError(String),

    #[error("{0}")]
    AnyhowError(#[from] anyhow::Error),

    // Standard library errors
    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("{0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("{0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("{0}")]
    ParseDateTimeError(#[from] chrono::ParseError),

    #[error("{0}")]
    StringEncodingError(#[from] std::str::Utf8Error),

    #[error("{0}")]
    PatternError(#[from] glob::PatternError),

    // Third-party library errors
    #[error("{0}")]
    DuckDBError(#[from] duckdb::Error),

    #[error("{0}")]
    ZipError(#[from] zip::result::ZipError),

    #[error("{0}")]
    XmlError(#[from] quick_xml::Error),

    #[error("{0}")]
    XmlEncodingError(#[from] quick_xml::encoding::EncodingError),

    #[error("{0}")]
    XmlAttributeError(#[from] quick_xml::events::attributes::AttrError),

    // Helper module errors
    #[error("{0}")]
    CfbHelperError(#[from] crate::helpers::cfb::CfbError),

    #[error("{0}")]
    XmlHelperError(#[from] crate::helpers::xml::XmlError),

    #[error("{0}")]
    Biff8HelperError(#[from] crate::helpers::biff8::Biff8Error),

    #[error("{0}")]
    Biff12HelperError(#[from] crate::helpers::biff12::Biff12Error),

    #[error("{0}")]
    UnifiedReaderError(#[from] crate::helpers::reader::UnifiedReaderError),

    // Spreadsheet module errors
    #[error("{0}")]
    SpreadsheetError(#[from] crate::spreadsheet::SpreadsheetError),

    #[error("{0}")]
    OdsError(#[from] crate::spreadsheet::ods::OdsError),

    #[error("{0}")]
    XlsError(#[from] crate::spreadsheet::xls::XlsError),

    // Database module errors
    #[error("{0}")]
    RangeError(#[from] crate::database::range::RangeError),

    #[error("{0}")]
    ColumnError(#[from] crate::database::column::ColumnError),

    // Extension module errors
    #[error("{0}")]
    ExtensionError(#[from] crate::extension::ExtensionError),
}

pub(crate) trait ResultOptionChain {
    fn ok_none_else<F>(self, f: F) -> Self
    where
        F: FnOnce() -> Self;
}

impl<T, E> ResultOptionChain for Result<Option<T>, E> {
    fn ok_none_else<F>(self, f: F) -> Self
    where
        F: FnOnce() -> Self,
    {
        match self {
            Ok(None) => f(),
            _ => self,
        }
    }
}

pub(crate) trait ResultMessage {
    fn with_prefix(self, message: &str) -> Self;
}

impl<T> ResultMessage for Result<T, RustySheetError> {
    fn with_prefix(self, message: &str) -> Self {
        self.map_err(|e| RustySheetError::WithContextError(format!("{}: {}", message, e)))
    }
}
