//! # Extension Core Module
//!
//! This module provides the core functionality for the DuckDB spreadsheet extension,
//! including parameter handling, error types, and sheet parsing utilities.
use crate::bridge::ValueBridge;
use crate::extension::ExtensionError::InvalidParameter;
use crate::spreadsheet::{
    CellKind, Sheet, Spreadsheet, SpreadsheetError, SpreadsheetError::SheetNotFound,
};
use anyhow::Result;
use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::BindInfo;
use regex::Regex;
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

pub(crate) mod analyze_sheet_table_function;
pub(crate) mod read_sheet_table_function;

/// Custom error types for the extension operations.
///
/// This enum represents all possible errors that can occur during extension
/// operations, providing structured error handling with context information.
#[derive(Error, Debug)]
pub enum ExtensionError {
    /// Error occurred while reading or processing spreadsheet data
    #[error("Read spreadsheet failed: {0}")]
    SpreadsheetError(#[from] SpreadsheetError),

    /// Invalid parameter provided to a table function
    #[error("Invalid parameter '{name}': {message}")]
    InvalidParameter { name: String, message: String },
}

// Named parameter handling traits and implementations

/// Trait for handling named parameters in DuckDB table functions.
///
/// This trait provides a standardized way to define, validate, and extract
/// named parameters from DuckDB bind information.
///
/// # Type Parameters
///
/// * `T` - The type of the parameter value
pub trait NamedParam<T> {
    /// Returns the parameter name as used in SQL
    fn name() -> &'static str;

    /// Returns the DuckDB logical type for this parameter
    fn kind() -> LogicalTypeHandle;

    /// Returns the complete parameter definition (name and type)
    fn definition() -> (String, LogicalTypeHandle) {
        (Self::name().to_string(), Self::kind())
    }

    /// Extracts the parameter value from bind information
    ///
    /// # Arguments
    ///
    /// * `bind` - The DuckDB bind information containing parameter values
    ///
    /// # Returns
    ///
    /// * `Option<T>` - The parameter value if present, None if not provided
    fn read(bind: &BindInfo) -> Option<T>;
}

/// Sheet name parameter handler
struct SheetNameParam;

/// Range parameter handler  
struct RangeParam;

/// Header parameter handler
struct HeaderParam;

/// Columns parameter handler
struct ColumnsParam;

/// Analyze rows parameter handler
struct AnalyzeRowsParam;

/// Error as null parameter handler
struct ErrorAsNullParam;

impl NamedParam<String> for SheetNameParam {
    fn name() -> &'static str {
        "sheet_name"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Varchar)
    }

    fn read(bind: &BindInfo) -> Option<String> {
        Some(bind.get_named_parameter(Self::name())?.to_varchar())
    }
}

/// Represents a cell range within a spreadsheet.
///
/// A range can specify partial boundaries - any bound can be None to indicate
/// no constraint in that direction. This allows for flexible range specifications
/// like "A1:", "B:D", ":10", etc.
pub struct Range {
    /// Starting row number (1-based, inclusive)
    pub row_lower_bound: Option<usize>,
    /// Ending row number (1-based, inclusive)  
    pub row_upper_bound: Option<usize>,
    /// Starting column number (1-based, inclusive)
    pub column_lower_bound: Option<usize>,
    /// Ending column number (1-based, inclusive)
    pub column_upper_bound: Option<usize>,
}

impl Range {
    /// Parses column letters to column numbers.
    ///
    /// Converts Excel-style column letters to 1-based column numbers:
    /// A = 1, B = 2, ..., Z = 26, AA = 27, AB = 28, ..., AZ = 52, BA = 53, ...
    ///
    /// # Arguments
    ///
    /// * `letters` - The column letters (case-insensitive)
    ///
    /// # Returns
    ///
    /// * `Option<usize>` - The 1-based column number, None if parsing fails
    fn parse_column(letters: &str) -> Option<usize> {
        letters
            .to_ascii_uppercase()
            .chars()
            .map(|index| index as usize - 'A' as usize + 1)
            .reduce(|index, digit| index * 26 + digit)
            .map(|column| column - 1)
    }

    /// Parses row number string to usize.
    ///
    /// # Arguments
    ///
    /// * `number` - The row number as string
    ///
    /// # Returns
    ///
    /// * `Option<usize>` - The row number, None if parsing fails
    fn parse_row(number: &str) -> Option<usize> {
        number
            .parse()
            .ok()
            .filter(|row| *row > 0)
            .map(|row: usize| row - 1)
    }
}

impl TryFrom<&str> for Range {
    type Error = ExtensionError;

    /// Parses a range string into a Range struct.
    ///
    /// Supports various range formats:
    /// - "A1:C3" - Full range from A1 to C3
    /// - "A1:" - From A1 to end of data
    /// - ":C3" - From beginning to C3  
    /// - "A:C" - All rows in columns A to C
    /// - "1:3" - Rows 1 to 3, all columns
    /// - "A1" - Single cell A1
    ///
    /// # Arguments
    ///
    /// * `value` - The range string to parse
    ///
    /// # Returns
    ///
    /// * `Result<Self, Self::Error>` - The parsed Range or an error
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let pattern =
            Regex::new(r"^([A-Z]*)(\d*)(:([A-Z]*)(\d*))?$").expect("Hardcode regex pattern");
        let captures = pattern.captures(value).ok_or(InvalidParameter {
            name: RangeParam::name().to_string(),
            message: format!("'{value}' is not a data range"),
        })?;
        Ok(Range {
            column_lower_bound: captures
                .get(1)
                .map(|matcher| matcher.as_str())
                .and_then(Self::parse_column),
            row_lower_bound: captures
                .get(2)
                .map(|matcher| matcher.as_str())
                .and_then(Self::parse_row),
            column_upper_bound: captures
                .get(4)
                .map(|matcher| matcher.as_str())
                .and_then(Self::parse_column),
            row_upper_bound: captures
                .get(5)
                .map(|matcher| matcher.as_str())
                .and_then(Self::parse_row),
        })
    }
}

impl NamedParam<Range> for RangeParam {
    fn name() -> &'static str {
        "range"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Varchar)
    }

    fn read(bind: &BindInfo) -> Option<Range> {
        let parameter = bind.get_named_parameter(Self::name())?.to_varchar();
        Range::try_from(parameter.as_str()).ok()
    }
}

impl NamedParam<bool> for HeaderParam {
    fn name() -> &'static str {
        "header"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Boolean)
    }

    fn read(bind: &BindInfo) -> Option<bool> {
        Some(bind.get_named_parameter(Self::name())?.to_bool())
    }
}

impl NamedParam<HashMap<String, CellKind>> for ColumnsParam {
    fn name() -> &'static str {
        "columns"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::map(
            &LogicalTypeHandle::from(LogicalTypeId::Varchar),
            &LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )
    }

    fn read(bind: &BindInfo) -> Option<HashMap<String, CellKind>> {
        Some(
            bind.get_named_parameter(Self::name())?
                .to_map_entries()
                .iter()
                .map(|(key, value)| {
                    let name = key.to_string();
                    let value = value.to_string();
                    let kind = CellKind::parse(value.as_str())
                        .expect(&format!("Unknown cell kind '{value}'"));
                    (name, kind)
                })
                .collect(),
        )
    }
}

impl NamedParam<usize> for AnalyzeRowsParam {
    fn name() -> &'static str {
        "analyze_rows"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::UInteger)
    }

    fn read(bind: &BindInfo) -> Option<usize> {
        Some(bind.get_named_parameter(Self::name())?.to_uint32() as usize)
    }
}

impl NamedParam<bool> for ErrorAsNullParam {
    fn name() -> &'static str {
        "error_as_null"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Boolean)
    }

    fn read(bind: &BindInfo) -> Option<bool> {
        Some(bind.get_named_parameter(Self::name())?.to_bool())
    }
}

// Sheet parsing utilities

/// Opens and configures a spreadsheet sheet with the given parameters.
///
/// This utility function handles the common workflow of:
/// 1. Opening a spreadsheet file
/// 2. Selecting the appropriate sheet
/// 3. Configuring range boundaries
/// 4. Setting header options
///
/// # Arguments
///
/// * `file_name` - Path to the spreadsheet file
/// * `sheet_name` - Optional sheet name (uses first sheet if None)
/// * `range` - Optional range to constrain data reading
/// * `header` - Optional header flag (defaults to true if None)
///
/// # Returns
///
/// * `Result<Sheet, ExtensionError>` - The configured sheet or an error
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be opened or read
/// - The specified sheet is not found
/// - The file format is not supported
pub fn open_sheet(
    file_name: &str,
    sheet_name: &Option<String>,
    range: &Option<Range>,
    header: &Option<bool>,
) -> Result<Sheet, ExtensionError> {
    let mut spreadsheet = Spreadsheet::open(Path::new(file_name))?;
    let sheet_name = sheet_name
        .to_owned()
        .or_else(|| spreadsheet.sheet_name_at(0))
        .ok_or(SheetNotFound)?;
    let mut sheet = spreadsheet.open_sheet(sheet_name.as_str(), header.unwrap_or(true))?;

    // Apply range constraints if specified
    if let Some(range) = range {
        if let Some(column_lower_bound) = range.column_lower_bound {
            sheet.column_lower_bound = column_lower_bound;
        }
        if let Some(row_lower_bound) = range.row_lower_bound {
            sheet.row_lower_bound = row_lower_bound;
        }
        if let Some(column_upper_bound) = range.column_upper_bound {
            sheet.column_upper_bound = column_upper_bound;
        }
        if let Some(row_upper_bound) = range.row_upper_bound {
            sheet.row_upper_bound = row_upper_bound;
        }
    }
    Ok(sheet)
}
