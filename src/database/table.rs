use crate::database::column::Column;

/// Represents a table extracted from a spreadsheet with metadata about data ranges.
#[derive(Clone, Debug)]
pub(crate) struct Table {
    /// Table/sheet name
    pub(crate) name: String,
    /// Column definitions
    pub(crate) columns: Vec<Column>,
    /// Data extraction range - row boundaries
    pub(crate) row_lower_bound: Option<usize>,
    /// Data extraction range - column lower bound
    pub(crate) col_lower_bound: usize,
    /// Data extraction range - column upper bound
    pub(crate) col_upper_bound: usize,
}
