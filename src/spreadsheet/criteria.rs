use crate::database::range::Range;
use glob::Pattern;
use std::collections::HashSet;

/// Criteria for filtering and selecting data from spreadsheets.
#[derive(Clone, Debug)]
pub(crate) struct Criteria {
    /// Sheet name patterns for filtering which sheets to process.
    pub(crate) sheet_name_patterns: Option<Vec<Pattern>>,

    /// Maximum number of sheets to read.
    pub(crate) sheet_limit: Option<usize>,

    /// Data range within sheets to extract.
    pub(crate) range: Option<Range>,

    /// Maximum number of rows to read per sheet.
    pub(crate) rows_limit: Option<usize>,

    /// null literals (default: empty string)
    pub(crate) nulls: HashSet<String>,

    /// Convert parsing errors to null values instead of failing.
    pub(crate) error_as_null: bool,

    /// Skip rows where all columns are empty.
    pub(crate) skip_empty_rows: bool,

    /// Stop reading when encountering a completely empty row.
    pub(crate) end_at_empty_row: bool,
}

impl Criteria {
    /// Checks if a sheet name matches the criteria patterns.
    /// Returns true if no patterns are specified or if name matches any pattern.
    pub(crate) fn accept(&self, sheet_name: &str) -> bool {
        if let Some(patterns) = &self.sheet_name_patterns {
            for pattern in patterns {
                if pattern.matches(sheet_name) {
                    return true;
                }
            }
            false
        } else {
            true
        }
    }
}
