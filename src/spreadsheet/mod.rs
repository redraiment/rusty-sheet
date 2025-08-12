//! # Spreadsheet Processing Module
//!
//! This module provides the core functionality for reading and processing various
//! spreadsheet formats including Excel (.xlsx, .xlsm, .xlsb, .xls, .xla, .xlam)
//! and OpenDocument (.ods) files. It handles data type detection, cell value
//! extraction, and provides a unified interface for different spreadsheet formats.
use crate::spreadsheet::SpreadsheetError::{
    EmptySheet, InvalidCellValue, InvalidFileFormat, MissingHeaderColumn, MissingHeaderRow,
    UnknownCellKind,
};
use anyhow::Result;
use calamine::{
    open_workbook, Data, DataRef, DataType, Ods, OdsError, Reader, Xls, XlsError, Xlsb, XlsbError,
    Xlsx, XlsxError,
};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use duckdb::core::LogicalTypeId;
use std::cmp::min;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use thiserror::Error;

/// Custom error types for spreadsheet operations.
///
/// This enum covers all possible errors that can occur during spreadsheet
/// reading, parsing, and data extraction operations.
#[derive(Error, Debug)]
pub enum SpreadsheetError {
    /// Error in Excel 2007+ format (.xlsx, .xlsm, .xlam)
    #[error("Invalid xlsx file format: {0}")]
    InvalidXlsxFileFormat(#[from] XlsxError),

    /// Error in Excel Binary format (.xlsb)
    #[error("Invalid xlsb file format: {0}")]
    InvalidXlsbFileFormat(#[from] XlsbError),

    /// Error in legacy Excel format (.xls, .xla)
    #[error("Invalid xls file format: {0}")]
    InvalidXlsFileFormat(#[from] XlsError),

    /// Error in OpenDocument format (.ods)
    #[error("Invalid ods file format: {0}")]
    InvalidOdsFileFormat(#[from] OdsError),

    /// Unsupported or unrecognized file format
    #[error("Cannot detect file format for '{name}'")]
    InvalidFileFormat { name: String },

    /// Requested sheet not found or spreadsheet is empty
    #[error("Sheet not found or spreadsheet is empty")]
    SheetNotFound,

    /// Sheet exists but contains no data
    #[error("Empty sheet or missing data")]
    EmptySheet,

    /// Header row expected but not found
    #[error("Missing header row")]
    MissingHeaderRow,

    /// Column header is missing or invalid
    #[error("Missing column name at '{position}'")]
    MissingHeaderColumn { position: String },

    /// Invalid cell value that cannot be converted to expected type
    #[error("Invalid cell value at '{position}': {message}")]
    InvalidCellValue { position: String, message: String },

    /// Unknown or unsupported cell data type
    #[error("Invalid cell kind '{kind}'")]
    UnknownCellKind { kind: String },
}

/// Type alias for buffered file reader
pub type FileReader = BufReader<File>;

/// Wrapper enum for different spreadsheet format readers.
///
/// This enum provides a unified interface over the various spreadsheet
/// formats supported by the calamine library, abstracting away the
/// differences between formats.
pub enum Spreadsheet {
    // Cell-based readers (stream processing)
    /// Excel 2007+ format reader (.xlsx, .xlsm, .xlam)
    Xlsx(Xlsx<FileReader>),
    /// Excel Binary format reader (.xlsb)
    Xlsb(Xlsb<FileReader>),

    // Range-based readers (in-memory processing)
    /// Legacy Excel format reader (.xls, .xla)
    Xls(Xls<FileReader>),
    /// OpenDocument format reader (.ods)
    Ods(Ods<FileReader>),
}

/// Enumeration of supported cell data types.
///
/// This enum represents all the data types that can be detected and handled
/// by the spreadsheet extension, with mappings to corresponding DuckDB types.
#[derive(Copy, Clone, Debug)]
pub enum CellKind {
    /// Boolean values (true/false)
    Bool,
    /// 64-bit signed integers
    BigInt,
    /// Double precision floating point numbers
    Double,
    /// Variable-length character strings
    Varchar,
    /// Date and time values
    DateTime,
    /// Date-only values
    Date,
    /// Time-only values
    Time,
    /// Duration/interval values
    Interval,
}

impl CellKind {
    /// Returns the string representation of the cell kind.
    ///
    /// This is used for displaying type information and in the analyze_sheet output.
    ///
    /// # Returns
    ///
    /// * `&'static str` - The string representation of the type
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::BigInt => "bigint",
            Self::Double => "double",
            Self::Varchar => "text",
            Self::DateTime => "timestamp",
            Self::Date => "date",
            Self::Time => "time",
            Self::Interval => "interval",
        }
    }

    /// Parses a cell kind from its string representation.
    ///
    /// Supports various aliases for each type (case-insensitive):
    /// - Bool: "bool", "boolean"
    /// - BigInt: "int", "bigint", "integer"
    /// - Double: "float", "double", "decimal", "numeric"
    /// - Varchar: "text", "string", "varchar"
    /// - DateTime: "datetime", "timestamp"
    /// - Date: "date"
    /// - Time: "time"
    /// - Interval: "interval", "duration"
    ///
    /// # Arguments
    ///
    /// * `name` - The string representation to parse
    ///
    /// # Returns
    ///
    /// * `Result<Self, SpreadsheetError>` - The parsed cell kind or error
    pub fn parse(name: &str) -> Result<Self, SpreadsheetError> {
        match name.to_ascii_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "INT" | "BIGINT" | "INTEGER" => Ok(Self::BigInt),
            "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => Ok(Self::Double),
            "TEXT" | "STRING" | "VARCHAR" => Ok(Self::Varchar),
            "DATETIME" | "TIMESTAMP" => Ok(Self::DateTime),
            "DATE" => Ok(Self::Date),
            "TIME" => Ok(Self::Time),
            "INTERVAL" | "DURATION" => Ok(Self::Interval),
            _ => Err(UnknownCellKind {
                kind: name.to_string(),
            }),
        }
    }

    /// Converts the cell kind to the corresponding DuckDB logical type ID.
    ///
    /// # Returns
    ///
    /// * `LogicalTypeId` - The corresponding DuckDB type
    pub const fn to_logical_type_id(&self) -> LogicalTypeId {
        match self {
            Self::Bool => LogicalTypeId::Boolean,
            Self::BigInt => LogicalTypeId::Bigint,
            Self::Double => LogicalTypeId::Double,
            Self::Varchar => LogicalTypeId::Varchar,
            Self::DateTime => LogicalTypeId::Timestamp,
            Self::Date => LogicalTypeId::Date,
            Self::Time => LogicalTypeId::Time,
            Self::Interval => LogicalTypeId::Interval,
        }
    }
}

/// Represents a single cell in a spreadsheet with its position and value.
///
/// This struct encapsulates both the location information and the actual
/// data value of a spreadsheet cell, providing methods for type checking
/// and value extraction.
#[derive(Debug)]
pub struct Cell {
    /// Row index (0-based)
    pub row: usize,
    /// Column index (0-based)
    pub column: usize,
    /// The actual cell data from the spreadsheet
    pub value: Data,
}

/// Convert 1-based row & column numbers to Excel-style cell position.
///
/// # Arguments
///
/// * `row` - The 1-based row number
/// * `column` - The 1-based column number
///
/// # Returns
///
/// * `String` - Excel-style cell position in upper case
pub fn cell_position(row: usize, column: usize) -> String {
    let row = (row + 1).to_string();
    let mut column: u32 = column as u32 + 1;
    let mut position = String::from("");
    while column > 0 {
        column -= 1;
        let digit = char::from_u32(65 + column % 26).expect("Hardcode letters");
        column /= 26;
        position.insert(0, digit)
    }
    position.push_str(row.as_str());
    position
}

impl Cell {
    /// Get Excel-style cell position.
    pub fn get_position(&self) -> String {
        cell_position(self.row, self.column)
    }

    /// Checks if the cell is empty.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains no data
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Checks if the cell contains a boolean value.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains a boolean
    pub fn is_bool(&self) -> bool {
        self.value.is_bool()
    }

    /// Extracts the boolean value from the cell.
    ///
    /// # Returns
    ///
    /// * `Option<bool>` - The boolean value if present
    pub fn get_bool(&self) -> Option<bool> {
        self.value.get_bool()
    }

    /// Checks if the cell contains an integer value.
    ///
    /// This includes both native integers and floating-point numbers
    /// with no fractional part.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains an integer
    pub fn is_bigint(&self) -> bool {
        self.value.is_int()
            || (self.value.is_float() && self.value.get_float().unwrap().fract() == 0.0)
    }

    /// Extracts the integer value from the cell.
    ///
    /// # Returns
    ///
    /// * `Option<i64>` - The integer value if present
    pub fn get_bigint(&self) -> Option<i64> {
        match self.value {
            Data::Int(value) => Some(value),
            Data::Float(value) => Some(value as i64),
            _ => None,
        }
    }

    /// Checks if the cell contains a numeric value (integer or float).
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains a number
    pub fn is_double(&self) -> bool {
        self.value.is_float() || self.value.is_int()
    }

    /// Extracts the numeric value as a double from the cell.
    ///
    /// # Returns
    ///
    /// * `Option<f64>` - The numeric value if present
    pub fn get_double(&self) -> Option<f64> {
        match self.value {
            Data::Int(value) => Some(value as f64),
            Data::Float(value) => Some(value),
            _ => None,
        }
    }

    /// Checks if the cell can be converted to a string.
    ///
    /// This returns true for all non-empty, non-error cells.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell can be converted to string
    pub fn is_varchar(&self) -> bool {
        !self.is_empty() && !self.is_error()
    }

    /// Extracts the string representation of the cell value.
    ///
    /// All supported data types can be converted to strings,
    /// including formatted dates and times.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The string representation if possible
    pub fn get_varchar(&self) -> Option<String> {
        match &self.value {
            Data::Bool(value) => Some(value.to_string()),
            Data::Int(value) => Some(value.to_string()),
            Data::Float(value) => Some(value.to_string()),
            Data::String(value) => Some(value.to_owned()),
            Data::DateTime(_) => {
                // Format datetime based on its specific type
                if self.is_time() {
                    Some(self.get_time()?.to_string())
                } else if self.is_date() {
                    Some(self.get_date()?.to_string())
                } else {
                    Some(self.get_datetime()?.to_string())
                }
            }
            Data::DateTimeIso(value) => Some(value.to_owned()),
            Data::DurationIso(value) => Some(value.to_owned()),
            _ => None,
        }
    }

    /// Checks if the cell contains a datetime value.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains date/time information
    pub fn is_datetime(&self) -> bool {
        self.value.is_datetime() || self.value.is_datetime_iso()
    }

    /// Extracts the datetime value from the cell.
    ///
    /// Handles both Excel's numeric datetime format and ISO string format.
    ///
    /// # Returns
    ///
    /// * `Option<NaiveDateTime>` - The datetime value if present
    pub fn get_datetime(&self) -> Option<NaiveDateTime> {
        match &self.value {
            Data::DateTime(value) => value.as_datetime(),
            Data::DateTimeIso(value) => DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|datetime| datetime.naive_local()),
            _ => None,
        }
    }

    /// Checks if the cell contains a date-only value.
    ///
    /// This is determined by checking if the datetime value has no time component
    /// (fractional part is 0 in Excel's numeric format).
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains a date without time
    pub fn is_date(&self) -> bool {
        self.value.is_datetime() && self.value.get_datetime().unwrap().as_f64().fract() == 0.0
    }

    /// Extracts the date portion from the cell.
    ///
    /// # Returns
    ///
    /// * `Option<NaiveDate>` - The date value if present
    pub fn get_date(&self) -> Option<NaiveDate> {
        self.get_datetime().map(|datetime| datetime.date())
    }

    /// Checks if the cell contains a time-only value.
    ///
    /// This is determined by checking if the datetime value is less than or equal to 1
    /// (representing times within the first day in Excel's format).
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains time without date
    pub fn is_time(&self) -> bool {
        self.value.is_datetime() && self.value.get_datetime().unwrap().as_f64() <= 1.0
    }

    /// Extracts the time portion from the cell.
    ///
    /// # Returns
    ///
    /// * `Option<NaiveTime>` - The time value if present
    pub fn get_time(&self) -> Option<NaiveTime> {
        self.get_datetime().map(|datetime| datetime.time())
    }

    /// Checks if the cell contains an interval/duration value.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains a duration
    pub fn is_interval(&self) -> bool {
        self.value.is_duration_iso()
    }

    /// Extracts the duration value from the cell.
    ///
    /// Parses ISO 8601 duration strings and converts them to chrono Duration.
    ///
    /// # Returns
    ///
    /// * `Option<Duration>` - The duration value if present and valid
    pub fn get_interval(&self) -> Option<Duration> {
        self.value
            .get_duration_iso()
            .map(parse_duration::parse)
            .and_then(Result::ok)
            .map(Duration::from_std)
            .and_then(Result::ok)
    }

    /// Checks if the cell contains an error value.
    ///
    /// # Returns
    ///
    /// * `bool` - True if the cell contains an error
    pub fn is_error(&self) -> bool {
        self.value.is_error()
    }

    /// Extracts error information from the cell.
    ///
    /// Converts spreadsheet errors into SpreadsheetError with location context.
    ///
    /// # Returns
    ///
    /// * `Option<SpreadsheetError>` - The error with position information
    pub fn get_error(&self) -> Option<SpreadsheetError> {
        let error = self.value.get_error()?;
        Some(InvalidCellValue {
            position: self.get_position(),
            message: error.to_string(),
        })
    }
}

/// Represents a spreadsheet sheet with its data and boundaries.
///
/// This struct contains all the cell data from a sheet along with metadata
/// about its structure, boundaries, and header configuration.
#[derive(Debug)]
pub struct Sheet {
    /// Whether the sheet has a header row
    pub with_header: bool,
    /// Starting row index (0-based, inclusive)
    pub row_lower_bound: usize,
    /// Ending row index (0-based, inclusive)
    pub row_upper_bound: usize,
    /// Starting column index (0-based, inclusive)
    pub column_lower_bound: usize,
    /// Ending column index (0-based, inclusive)
    pub column_upper_bound: usize,
    /// All cells in the sheet
    pub cells: Vec<Cell>,
    /// Index mapping from (row, column) to cell vector position
    pub indexes: HashMap<(usize, usize), usize>,
}

/// Macro to convert cells-based readers to Sheet structure.
///
/// This macro handles the common pattern of reading cells from streaming
/// readers (like XLSX and XLSB) and building the Sheet data structure.
macro_rules! cells_reader_to_sheet {
    ($with_header:expr, $reader:expr) => {{
        let mut row_lower_bound = usize::MAX;
        let mut row_upper_bound = 0;
        let mut column_lower_bound = usize::MAX;
        let mut column_upper_bound = 0;
        let mut cells: Vec<Cell> = Vec::new();
        let mut indexes: HashMap<(usize, usize), usize> = HashMap::new();

        // Stream through all cells and collect them
        while let Some(cell) = $reader.next_cell()? {
            let row = cell.get_position().0 as usize;
            row_lower_bound = row_lower_bound.min(row);
            row_upper_bound = row_upper_bound.max(row);

            let column = cell.get_position().1 as usize;
            column_lower_bound = column_lower_bound.min(column);
            column_upper_bound = column_upper_bound.max(column);

            // Store position index for fast lookup
            indexes.insert((row, column), cells.len());
            cells.push(Cell {
                row,
                column,
                value: match cell.get_value() {
                    DataRef::Int(value) => Data::Int(*value),
                    DataRef::Float(value) => Data::Float(*value),
                    DataRef::String(value) => Data::String(value.to_owned()),
                    DataRef::SharedString(value) => Data::String(value.to_string()),
                    DataRef::Bool(value) => Data::Bool(*value),
                    DataRef::DateTime(value) => Data::DateTime(*value),
                    DataRef::DateTimeIso(value) => Data::DateTimeIso(value.to_owned()),
                    DataRef::DurationIso(value) => Data::DurationIso(value.to_owned()),
                    DataRef::Error(value) => Data::Error(value.to_owned()),
                    DataRef::Empty => Data::Empty,
                },
            });
        }

        if !cells.is_empty() {
            Ok(Sheet {
                with_header: $with_header,
                row_lower_bound,
                row_upper_bound,
                column_lower_bound,
                column_upper_bound,
                cells,
                indexes,
            })
        } else {
            Err(EmptySheet)
        }
    }};
}

/// Macro to extract range data from range-based readers.
///
/// This macro handles the common pattern of extracting cell data from
/// range-based readers (like XLS and ODS) that provide data in blocks.
macro_rules! extract_range {
    ($with_header:expr, $range:expr) => {
        if !$range.is_empty() {
            let start = $range
                .start()
                .map(|(row, column)| (row as usize, column as usize))
                .unwrap();
            let end = $range
                .end()
                .map(|(row, column)| (row as usize, column as usize))
                .unwrap();
            let mut cells: Vec<Cell> = Vec::new();
            let mut indexes: HashMap<(usize, usize), usize> = HashMap::new();

            // Extract all used cells from the range
            for cell in $range.used_cells() {
                let row = start.0 + cell.0;
                let column = start.1 + cell.1;
                indexes.insert((row, column), cells.len());
                cells.push(Cell {
                    row,
                    column,
                    value: cell.2.to_owned(),
                })
            }
            Ok(Sheet {
                with_header: $with_header,
                row_lower_bound: start.0,
                row_upper_bound: end.0,
                column_lower_bound: start.1,
                column_upper_bound: end.1,
                cells,
                indexes,
            })
        } else {
            Err(EmptySheet)
        }
    };
}

impl Spreadsheet {
    /// Opens a spreadsheet file and returns the appropriate reader.
    ///
    /// Automatically detects the file format based on the file extension
    /// and creates the corresponding reader type.
    ///
    /// Supported formats:
    /// - `.xlsx`, `.xlsm`, `.xlam` - Excel 2007+ format (cells reader)
    /// - `.xlsb` - Excel Binary format (cells reader)
    /// - `.xls`, `.xla` - Legacy Excel format (range reader)
    /// - `.ods` - OpenDocument format (range reader)
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the spreadsheet file
    ///
    /// # Returns
    ///
    /// * `Result<Spreadsheet, SpreadsheetError>` - The appropriate reader or error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file format is not supported
    /// - The file cannot be opened or read
    /// - The file is corrupted or invalid
    pub fn open<P>(path: P) -> Result<Spreadsheet, SpreadsheetError>
    where
        P: AsRef<Path>,
    {
        match path.as_ref().extension().and_then(OsStr::to_str) {
            // Cells-based readers for newer formats
            Some("xlsx") | Some("xlsm") | Some("xlam") => Ok(Self::Xlsx(open_workbook(path)?)),
            Some("xlsb") => Ok(Self::Xlsb(open_workbook(path)?)),
            // Range-based readers for legacy formats
            Some("xls") | Some("xla") => Ok(Self::Xls(open_workbook(path)?)),
            Some("ods") => Ok(Self::Ods(open_workbook(path)?)),
            _ => Err(InvalidFileFormat {
                name: path.as_ref().to_string_lossy().to_string(),
            }),
        }
    }

    /// Returns the names of all sheets in the spreadsheet.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - List of sheet names in the spreadsheet
    pub fn sheet_names(&self) -> Vec<String> {
        match self {
            Self::Xlsx(xlsx) => xlsx.sheet_names(),
            Self::Xlsb(xlsb) => xlsb.sheet_names(),
            Self::Xls(xls) => xls.sheet_names(),
            Self::Ods(ods) => ods.sheet_names(),
        }
    }

    /// Returns the name of the sheet at the specified index.
    ///
    /// # Arguments
    ///
    /// * `index` - Zero-based index of the sheet
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The sheet name if it exists
    pub fn sheet_name_at(&self, index: usize) -> Option<String> {
        self.sheet_names().get(index).map(|name| name.to_owned())
    }

    /// Opens a specific sheet from the spreadsheet.
    ///
    /// This method handles the differences between cells-based and range-based
    /// readers, providing a unified interface for accessing sheet data.
    ///
    /// # Arguments
    ///
    /// * `sheet_name` - Name of the sheet to open
    /// * `with_header` - Whether the first row should be treated as headers
    ///
    /// # Returns
    ///
    /// * `Result<Sheet, SpreadsheetError>` - The opened sheet or error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The specified sheet is not found
    /// - The sheet is empty or contains no data
    /// - There's an error reading the sheet data
    pub fn open_sheet(
        &mut self,
        sheet_name: &str,
        with_header: bool,
    ) -> Result<Sheet, SpreadsheetError> {
        match self {
            Self::Xlsx(xlsx) => {
                let mut reader = xlsx.worksheet_cells_reader(sheet_name)?;
                cells_reader_to_sheet!(with_header, reader)
            }
            Self::Xlsb(xlsb) => {
                let mut reader = xlsb.worksheet_cells_reader(sheet_name)?;
                cells_reader_to_sheet!(with_header, reader)
            }
            Self::Xls(xls) => {
                let range = xls.worksheet_range(sheet_name)?;
                extract_range!(with_header, range)
            }
            Self::Ods(ods) => {
                let range = ods.worksheet_range(&sheet_name)?;
                extract_range!(with_header, range)
            }
        }
    }
}

impl Sheet {
    /// Gets a cell at the specified position.
    ///
    /// Returns None if the position is outside the sheet boundaries
    /// or if no cell exists at that position.
    ///
    /// # Arguments
    ///
    /// * `row` - Row index (0-based)
    /// * `column` - Column index (0-based)
    ///
    /// # Returns
    ///
    /// * `Option<&Cell>` - Reference to the cell if it exists
    pub fn get(&self, row: usize, column: usize) -> Option<&Cell> {
        if self.row_lower_bound <= row
            && row <= self.row_upper_bound
            && self.column_lower_bound <= column
            && column <= self.column_upper_bound
        {
            self.indexes
                .get(&(row, column))
                .and_then(|index| self.cells.get(*index))
        } else {
            None
        }
    }

    /// Extracts header row information from the sheet.
    ///
    /// If headers are enabled, reads the first row and converts all values
    /// to strings for use as column names. If headers are disabled,
    /// generates default column names (column1, column2, etc.).
    ///
    /// # Returns
    ///
    /// * `Result<Vec<String>, SpreadsheetError>` - Column names or error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Headers are expected but the header row is missing
    /// - A header cell cannot be converted to a string
    /// - A header cell is missing or empty
    pub fn header(&self) -> Result<Vec<String>, SpreadsheetError> {
        if self.with_header && !self.cells.is_empty() {
            (self.column_lower_bound..=self.column_upper_bound)
                .into_iter()
                .map(|column| {
                    self.get(self.row_lower_bound, column)
                        .ok_or(MissingHeaderColumn {
                            position: cell_position(self.row_lower_bound, column),
                        })
                        .and_then(|cell| {
                            cell.get_varchar().ok_or(InvalidCellValue {
                                position: cell_position(self.row_lower_bound, column),
                                message: "cast to varchar failed".to_string(),
                            })
                        })
                })
                .collect()
        } else if !self.with_header {
            // Generate default column names
            Ok((0..=(self.column_upper_bound - self.column_lower_bound))
                .into_iter()
                .map(|index| format!("column{}", index + 1))
                .collect())
        } else {
            Err(MissingHeaderRow)
        }
    }

    /// Analyzes column data types by examining a sample of rows.
    ///
    /// This method performs automatic type inference by examining the first
    /// N rows of data (excluding headers) and determining the most appropriate
    /// data type for each column based on the cell values found.
    ///
    /// Type inference priority (most to least specific):
    /// 1. Bool - if all values are boolean
    /// 2. BigInt - if all values are integers
    /// 3. Double - if all values are numeric
    /// 4. Time - if all values are time-only
    /// 5. Date - if all values are date-only
    /// 6. DateTime - if all values are datetime
    /// 7. Interval - if all values are durations
    /// 8. Varchar - fallback for mixed or string data
    ///
    /// # Arguments
    ///
    /// * `rows` - Number of data rows to analyze for type inference
    ///
    /// # Returns
    ///
    /// * `Result<Vec<(String, CellKind)>, SpreadsheetError>` - Column analysis results
    ///
    /// # Errors
    ///
    /// Returns an error if header extraction fails.
    pub fn analyze_columns(
        &self,
        rows: usize,
    ) -> Result<Vec<(String, CellKind)>, SpreadsheetError> {
        // Calculate analysis range (skip header if present)
        let row_lower_bound = self.row_lower_bound + if self.with_header { 1 } else { 0 };
        let row_upper_bound = min(row_lower_bound + rows, self.row_upper_bound + 1);

        self.header()?
            .into_iter()
            .zip(self.column_lower_bound..=self.column_upper_bound)
            .map(|(title, column)| {
                // Collect valid (non-empty, non-error) cells for analysis
                let cells: Vec<&Cell> = (row_lower_bound..row_upper_bound)
                    .filter_map(|row| self.get(row, column))
                    .filter(|cell| cell.is_varchar())
                    .collect();

                // Infer type based on cell contents
                let kind = if cells.is_empty() {
                    // No valid data found, default to string
                    CellKind::Varchar
                } else if cells.iter().all(|cell| cell.is_bool()) {
                    CellKind::Bool
                } else if cells.iter().all(|cell| cell.is_bigint()) {
                    CellKind::BigInt
                } else if cells.iter().all(|cell| cell.is_double()) {
                    CellKind::Double
                } else if cells.iter().all(|cell| cell.is_time()) {
                    CellKind::Time
                } else if cells.iter().all(|cell| cell.is_date()) {
                    CellKind::Date
                } else if cells.iter().all(|cell| cell.is_datetime()) {
                    CellKind::DateTime
                } else if cells.iter().all(|cell| cell.is_interval()) {
                    CellKind::Interval
                } else {
                    // Mixed types or unsupported data, fallback to string
                    CellKind::Varchar
                };
                Ok((title, kind))
            })
            .collect()
    }
}
