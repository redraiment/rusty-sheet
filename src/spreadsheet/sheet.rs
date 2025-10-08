use crate::database::range::Range;
use crate::spreadsheet::cell::Cell;

/// Represents a sheet from a spreadsheet file with data organized in chunks for efficient processing.
pub(crate) struct Sheet {
    /// Source file name
    pub(crate) file_name: String,
    /// Sheet name
    pub(crate) name: String,
    /// All cells in the sheet
    pub(crate) cells: Vec<Cell>,
    /// Data chunks for efficient processing:
    /// 1. row lower index
    /// 2. row upper index
    /// 3. cells lower index
    /// 4. cells upper index
    pub(crate) chunks: Vec<(usize, usize, usize, usize)>,
    /// Current chunk starting index in cells vector
    chunk_index_lower: usize,
    /// Current chunk starting row
    pub(super) chunk_row_lower: Option<usize>,
    /// Expected data range (user-specified)
    pub(super) range: Range,
    /// Row limit for data extraction
    pub(super) limit: Option<usize>,
    /// Whether to skip empty rows
    pub(super) skip_empty_rows: bool,
    /// Actual data range (determined from cell data)
    pub(crate) row_lower_bound: Option<usize>,
    pub(crate) row_upper_bound: Option<usize>,
    pub(crate) col_lower_bound: Option<usize>,
    pub(crate) col_upper_bound: Option<usize>,
}

impl Sheet {
    /// Size of data chunks for processing efficiency
    const CHUNK_SIZE: usize = 2048;

    /// Creates a new sheet with specified parameters.
    pub(super) fn new(file_name: &str, name: &str, range: Option<Range>, limit: Option<usize>, skip_empty_rows: bool) -> Self {
        let range = range.unwrap_or_default();
        Self {
            file_name: file_name.to_owned(),
            name: name.to_owned(),
            cells: Vec::new(),
            chunks: Vec::new(),
            chunk_index_lower: 0,
            // If skipping empty rows, use first non-empty cell's row as starting row
            chunk_row_lower: range.row_lower_bound.filter(|_| !skip_empty_rows),
            row_lower_bound: None,
            row_upper_bound: None,
            col_lower_bound: None,
            col_upper_bound: None,
            range,
            limit,
            skip_empty_rows,
        }
    }

    /// Returns true if the sheet contains no cells.
    pub(super) fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Checks if a row is before the lower bound of the specified range.
    pub(super) fn before_row_lower_bound(&self, row: usize) -> bool {
        self.range.row_lower_bound
            .map(|row_lower_bound| row < row_lower_bound)
            .unwrap_or(false)
    }

    /// Checks if a row is after the upper bound of the specified range or exceeds the row limit.
    pub(super) fn after_row_upper_bound(&self, row: usize) -> bool {
        let is_out_of_bound = self.range.row_upper_bound
            .map(|row_upper_bound| row_upper_bound < row)
            .unwrap_or(false);
        let is_more_than_limit = self.row_lower_bound.zip(self.limit)
            .map(|(row_lower_bound, limit)| row_lower_bound + limit <= row)
            .unwrap_or(false);
        is_out_of_bound || is_more_than_limit
    }

    /// Checks if a column is before the lower bound of the specified range.
    pub(super) fn before_col_lower_bound(&self, col: usize) -> bool {
        self.range.col_lower_bound
            .map(|col_lower_bound| col < col_lower_bound)
            .unwrap_or(false)
    }

    /// Checks if a column is after the upper bound of the specified range.
    pub(super) fn after_col_upper_bound(&self, col: usize) -> bool {
        self.range.col_upper_bound
            .map(|col_upper_bound| col_upper_bound < col)
            .unwrap_or(false)
    }

    /// Checks if a cell at (row, col) is within the specified range and limits.
    pub(super) fn contains(&self, row: usize, col: usize) -> bool {
        !self.before_row_lower_bound(row)
            && !self.after_row_upper_bound(row)
            && !self.before_col_lower_bound(col)
            && !self.after_col_upper_bound(col)
    }

    /// Adds a cell to the sheet, updating chunk boundaries and data ranges.
    pub(super) fn push(&mut self, cell: Cell) {
        self.update_chunk(cell.row);
        self.update_bound(cell.row, cell.col);
        self.cells.push(cell);
    }

    /// Updates chunk boundaries when adding cells.
    /// Manages chunk creation for efficient data processing.
    fn update_chunk(&mut self, row: usize) {
        if self.chunk_row_lower.is_none() { // If no range specified or skipping empty rows, use first cell's row
            self.chunk_row_lower = Some(row);
        }
        if self.row_upper_bound.map(|row_upper_bound| row_upper_bound != row).unwrap_or(false) { // Row changed
            let mut chunk_row_lower = self.chunk_row_lower.unwrap();
            let chunk_row_upper = self.row_upper_bound.unwrap();
            if self.skip_empty_rows && chunk_row_upper + 1 < row { // Need to skip empty rows
                let chunk_index_upper = self.cells.len();
                self.chunks.push((
                    chunk_row_lower,
                    chunk_row_upper,
                    self.chunk_index_lower,
                    chunk_index_upper,
                ));
                self.chunk_index_lower = chunk_index_upper;
                self.chunk_row_lower = Some(row);
            } else {
                while chunk_row_lower + Self::CHUNK_SIZE < row { // Chunk full
                    self.chunks.push((
                        chunk_row_lower,
                        chunk_row_lower + Self::CHUNK_SIZE - 1,
                        self.chunk_index_lower,
                        self.cells.len(),
                    ));
                    self.chunk_index_lower = self.cells.len();
                    chunk_row_lower += Self::CHUNK_SIZE;
                }
                self.chunk_row_lower = Some(chunk_row_lower);
            }
        }
    }

    /// Updates the actual data range boundaries based on cell positions.
    fn update_bound(&mut self, row: usize, col: usize) {
        if self.row_lower_bound.is_none() { // First cell
            self.row_lower_bound = Some(row);
        }
        if self.col_lower_bound.map(|col_lower_bound| col < col_lower_bound).unwrap_or(true) {
            self.col_lower_bound = Some(col);
        }
        if self.col_upper_bound.map(|col_upper_bound| col_upper_bound < col).unwrap_or(true) {
            self.col_upper_bound = Some(col);
        }
        self.row_upper_bound = Some(row);
    }

    /// Finalizes chunk creation after all cells have been added.
    /// Creates remaining chunks to cover the entire data range.
    pub(super) fn finish(&mut self, end_at_empty_row: bool) {
        if let Some(row_upper_bound) = self.range.row_upper_bound
            .filter(|_| !self.skip_empty_rows && !end_at_empty_row)
            .or(self.row_upper_bound)
        { // Has data
            let mut chunk_row_lower = self.chunk_row_lower.unwrap();
            if self.chunk_index_lower < self.cells.len() {
                let chunk_row_upper = row_upper_bound.min(chunk_row_lower + Self::CHUNK_SIZE - 1);
                self.chunks.push((
                    chunk_row_lower,
                    chunk_row_upper,
                    self.chunk_index_lower,
                    self.cells.len(),
                ));
                chunk_row_lower = chunk_row_upper + 1;
                self.chunk_index_lower = self.cells.len();
            }
            while chunk_row_lower <= row_upper_bound {
                let chunk_row_upper = row_upper_bound.min(chunk_row_lower + Self::CHUNK_SIZE - 1);
                self.chunks.push((
                    chunk_row_lower,
                    chunk_row_upper,
                    self.chunk_index_lower,
                    self.chunk_index_lower,
                ));
                chunk_row_lower = chunk_row_upper + 1;
            }
        }
    }

    /// Retrieves a chunk of data as a 2D table of optional cell references.
    /// Returns None if the chunk index is out of bounds.
    pub(crate) fn chunk(&self, index: usize) -> Option<Vec<Vec<Option<&Cell>>>> {
        let (row_lower, row_upper, index_lower, index_upper) = self.chunks.get(index)?;
        let col_lower = self.range.col_lower_bound.or(self.col_lower_bound).unwrap();
        let col_upper = self.range.col_upper_bound.or(self.col_upper_bound).unwrap();
        let mut index = *index_lower;
        let mut table = Vec::<Vec<Option<&Cell>>>::new();
        for row in (*row_lower)..=(*row_upper) {
            let mut record = Vec::<Option<&Cell>>::new();
            for col in col_lower..=col_upper {
                if index == *index_upper {
                    record.push(None);
                } else {
                    let cell = &self.cells[index];
                    if row == cell.row && col == cell.col {
                        record.push(Some(cell));
                        index += 1;
                    } else {
                        record.push(None);
                    }
                }
            }
            table.push(record);
        }
        Some(table)
    }
}

#[cfg(test)]
mod tests {
    use crate::database::range::Range;
    use crate::spreadsheet::*;

    fn push(sheet: &mut Sheet, row: usize, col: usize) {
        sheet.push(Cell {
            row,
            col,
            kind: CellType::InlineString,
            value: "".to_owned(),
        });
    }

    #[test]
    fn sheet_initial() {
        let sheet = Sheet::new("", "", None, None, false);

        assert_eq!(sheet.row_lower_bound, None);
        assert_eq!(sheet.row_upper_bound, None);
        assert_eq!(sheet.col_lower_bound, None);
        assert_eq!(sheet.col_upper_bound, None);
    }

    #[test]
    fn sheet_update() {
        let mut sheet = Sheet::new("", "", None, None, false);
        push(&mut sheet, 1, 1);
        push(&mut sheet, 1, 3);
        push(&mut sheet, 3, 1);
        push(&mut sheet, 3, 3);
        sheet.finish(false);

        assert_eq!(sheet.cells.len(), 4);

        assert_eq!(sheet.row_lower_bound, Some(1));
        assert_eq!(sheet.row_upper_bound, Some(3));
        assert_eq!(sheet.col_lower_bound, Some(1));
        assert_eq!(sheet.col_upper_bound, Some(3));

        assert_eq!(sheet.chunks.len(), 1);
        let (row_lower, row_upper, index_lower, index_upper) = &sheet.chunks[0];
        assert_eq!(*row_lower, 1);
        assert_eq!(*row_upper, 3);
        assert_eq!(*index_lower, 0);
        assert_eq!(*index_upper, 4);
    }

    #[test]
    fn sheet_update_skip_empty_rows() {
        let mut sheet = Sheet::new("", "", None, None, true);
        push(&mut sheet, 1, 1);
        push(&mut sheet, 1, 3);
        push(&mut sheet, 3, 1);
        push(&mut sheet, 3, 3);
        sheet.finish(false);

        assert_eq!(sheet.cells.len(), 4);

        assert_eq!(sheet.row_lower_bound, Some(1));
        assert_eq!(sheet.row_upper_bound, Some(3));
        assert_eq!(sheet.col_lower_bound, Some(1));
        assert_eq!(sheet.col_upper_bound, Some(3));

        assert_eq!(sheet.chunks.len(), 2);
        let (row_lower, row_upper, index_lower, index_upper) = &sheet.chunks[0];
        assert_eq!(*row_lower, 1);
        assert_eq!(*row_upper, 1);
        assert_eq!(*index_lower, 0);
        assert_eq!(*index_upper, 2);
        let (row_lower, row_upper, index_lower, index_upper) = &sheet.chunks[1];
        assert_eq!(*row_lower, 3);
        assert_eq!(*row_upper, 3);
        assert_eq!(*index_lower, 2);
        assert_eq!(*index_upper, 4);
    }

    #[test]
    fn sheet_update_with_range() {
        let mut sheet = Sheet::new("", "", Some(Range {
            row_lower_bound: Some(0),
            row_upper_bound: Some(5),
            col_lower_bound: Some(0),
            col_upper_bound: Some(5),
        }), None, false);
        push(&mut sheet, 1, 1);
        push(&mut sheet, 1, 3);
        push(&mut sheet, 3, 1);
        push(&mut sheet, 3, 3);
        sheet.finish(false);

        assert_eq!(sheet.cells.len(), 4);

        assert_eq!(sheet.row_lower_bound, Some(1));
        assert_eq!(sheet.row_upper_bound, Some(3));
        assert_eq!(sheet.col_lower_bound, Some(1));
        assert_eq!(sheet.col_upper_bound, Some(3));

        assert_eq!(sheet.chunks.len(), 1);
        let (row_lower, row_upper, index_lower, index_upper) = &sheet.chunks[0];
        assert_eq!(*row_lower, 0);
        assert_eq!(*row_upper, 5);
        assert_eq!(*index_lower, 0);
        assert_eq!(*index_upper, 4);
    }

    #[test]
    fn sheet_update_with_trim_range() {
        let mut sheet = Sheet::new("", "", Some(Range {
            row_lower_bound: Some(0),
            row_upper_bound: Some(5),
            col_lower_bound: Some(0),
            col_upper_bound: Some(5),
        }), None, true);
        push(&mut sheet, 1, 1);
        push(&mut sheet, 1, 3);
        push(&mut sheet, 3, 1);
        push(&mut sheet, 3, 3);
        sheet.finish(false);

        assert_eq!(sheet.cells.len(), 4);

        assert_eq!(sheet.row_lower_bound, Some(1));
        assert_eq!(sheet.row_upper_bound, Some(3));
        assert_eq!(sheet.col_lower_bound, Some(1));
        assert_eq!(sheet.col_upper_bound, Some(3));

        assert_eq!(sheet.chunks.len(), 2);
        let (row_lower, row_upper, index_lower, index_upper) = &sheet.chunks[0];
        assert_eq!(*row_lower, 1);
        assert_eq!(*row_upper, 1);
        assert_eq!(*index_lower, 0);
        assert_eq!(*index_upper, 2);
        let (row_lower, row_upper, index_lower, index_upper) = &sheet.chunks[1];
        assert_eq!(*row_lower, 3);
        assert_eq!(*row_upper, 3);
        assert_eq!(*index_lower, 2);
        assert_eq!(*index_upper, 4);
    }

    #[test]
    fn sheet_update_end_at_empty_row() {
        let mut sheet = Sheet::new("", "", Some(Range {
            row_lower_bound: None,
            row_upper_bound: Some(5),
            col_lower_bound: None,
            col_upper_bound: None,
        }), None, true);
        push(&mut sheet, 1, 1);
        push(&mut sheet, 1, 3);
        push(&mut sheet, 2, 2);
        push(&mut sheet, 3, 1);
        push(&mut sheet, 3, 3);
        sheet.finish(true);

        assert_eq!(sheet.cells.len(), 5);

        assert_eq!(sheet.row_lower_bound, Some(1));
        assert_eq!(sheet.row_upper_bound, Some(3));
        assert_eq!(sheet.col_lower_bound, Some(1));
        assert_eq!(sheet.col_upper_bound, Some(3));

        assert_eq!(sheet.chunks.len(), 1);
        let (row_lower, row_upper, index_lower, index_upper) = &sheet.chunks[0];
        assert_eq!(*row_lower, 1);
        assert_eq!(*row_upper, 3);
        assert_eq!(*index_lower, 0);
        assert_eq!(*index_upper, 5);
    }
}