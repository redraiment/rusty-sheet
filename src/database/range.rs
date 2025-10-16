use crate::error::RustySheetError;
use crate::spreadsheet::reference::col_to_index;
use crate::spreadsheet::reference::row_to_index;
use regex::Regex;
use thiserror::Error;

/// Errors related to Excel-style range parsing.
#[derive(Error, Debug)]
pub(crate) enum RangeError {
    #[error("Invalid range format '{0}'")]
    FormatError(String)
}

/// Represents an Excel-style cell range with optional boundaries.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Range {
    /// Lower row bound (0-based index), None for unbounded
    pub(crate) row_lower_bound: Option<usize>,
    /// Upper row bound (0-based index), None for unbounded
    pub(crate) row_upper_bound: Option<usize>,
    /// Lower column bound (0-based index), None for unbounded
    pub(crate) col_lower_bound: Option<usize>,
    /// Upper column bound (0-based index), None for unbounded
    pub(crate) col_upper_bound: Option<usize>,
}

impl TryFrom<&str> for Range {
    type Error = RustySheetError;

    /// Parses an Excel-style range string (e.g., "A1", "B2:C5", "A", "1:10").
    /// Supports single cells, ranges, and partial ranges (columns or rows only).
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let pattern = Regex::new(r"^([A-Z]*)(\d*)(:([A-Z]*)(\d*))?$").expect("Hardcode regex pattern");
        let value = value.to_ascii_uppercase();
        let captures = pattern
            .captures(value.as_str())
            .ok_or(RangeError::FormatError(value.to_owned()))?;
        Ok(Range {
            col_lower_bound: captures
                .get(1)
                .map(|matcher| matcher.as_str())
                .and_then(col_to_index),
            row_lower_bound: captures
                .get(2)
                .map(|matcher| matcher.as_str())
                .and_then(row_to_index),
            col_upper_bound: captures
                .get(4)
                .map(|matcher| matcher.as_str())
                .and_then(col_to_index),
            row_upper_bound: captures
                .get(5)
                .map(|matcher| matcher.as_str())
                .and_then(row_to_index),
        })
    }
}

impl Default for Range {
    /// Creates an unbounded range (selects entire sheet).
    fn default() -> Self {
        Range {
            row_lower_bound: None,
            row_upper_bound: None,
            col_lower_bound: None,
            col_upper_bound: None,
        }
    }
}

