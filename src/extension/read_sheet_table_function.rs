//! # Read Sheet Table Function
//!
//! This module implements the main `read_sheet` table function that reads
//! spreadsheet data and makes it available for SQL queries in DuckDB.
extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use crate::extension::{
    open_sheet, AnalyzeRowsParam, ColumnsParam, ErrorAsNullParam, ExtensionError, HeaderParam,
    NamedParam, Range, RangeParam, SheetNameParam,
};
use crate::spreadsheet::{Cell, CellKind, Sheet};
use anyhow::Result;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use duckdb::{
    core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId},
    ffi::{duckdb_date, duckdb_interval, duckdb_time, duckdb_timestamp},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    collections::HashMap, error::Error, fmt::Debug, sync::atomic::AtomicUsize,
    sync::atomic::Ordering,
};

/// Parameters for the read_sheet table function.
///
/// These parameters control how spreadsheet data is read, parsed, and presented
/// to DuckDB for SQL querying.
struct ReadSheetParameters {
    /// Path to the spreadsheet file
    file_name: String,
    /// Optional sheet name (uses first sheet if None)
    sheet_name: Option<String>,
    /// Optional range constraint for data reading
    range: Option<Range>,
    /// Whether the first row contains headers (default: true)
    header: Option<bool>,
    /// Optional column type overrides
    columns: Option<HashMap<String, CellKind>>,
    /// Number of rows to analyze for type inference (default: 10)
    analyze_rows: Option<usize>,
    /// Whether to treat parsing errors as NULL values (default: false)
    error_as_null: Option<bool>,
}

impl TryFrom<&BindInfo> for ReadSheetParameters {
    type Error = ExtensionError;

    /// Extracts parameters from DuckDB bind information.
    ///
    /// # Arguments
    ///
    /// * `bind` - DuckDB bind information containing parameter values
    ///
    /// # Returns
    ///
    /// * `Result<Self, Self::Error>` - Parsed parameters or an error
    fn try_from(bind: &BindInfo) -> Result<Self, Self::Error> {
        Ok(ReadSheetParameters {
            file_name: bind.get_parameter(0).to_string(),
            sheet_name: SheetNameParam::read(bind),
            range: RangeParam::read(bind),
            header: HeaderParam::read(bind),
            columns: ColumnsParam::read(bind),
            analyze_rows: AnalyzeRowsParam::read(bind),
            error_as_null: ErrorAsNullParam::read(bind),
        })
    }
}

/// Bind data for the read_sheet table function.
///
/// Contains the spreadsheet data and metadata needed for query execution.
#[repr(C)]
pub(crate) struct ReadSheetBindData {
    /// The opened and configured spreadsheet sheet
    sheet: Sheet,
    /// Final column definitions (name, type) after analysis and user overrides
    columns: Vec<(String, CellKind)>,
    /// Whether to treat parsing errors as NULL values
    error_as_null: bool,
}

impl TryFrom<&ReadSheetParameters> for ReadSheetBindData {
    type Error = ExtensionError;

    /// Processes parameters and prepares the sheet for data reading.
    ///
    /// This includes:
    /// 1. Opening the spreadsheet file and sheet
    /// 2. Analyzing column types automatically
    /// 3. Applying user-defined column type overrides
    /// 4. Configuring error handling behavior
    ///
    /// # Arguments
    ///
    /// * `parameters` - The function parameters
    ///
    /// # Returns
    ///
    /// * `Result<Self, Self::Error>` - Bind data ready for execution
    fn try_from(parameters: &ReadSheetParameters) -> Result<Self, Self::Error> {
        let sheet = open_sheet(
            &parameters.file_name,
            &parameters.sheet_name,
            &parameters.range,
            &parameters.header,
        )?;

        // Analyze column types automatically
        let mut columns = sheet.analyze_columns(parameters.analyze_rows.unwrap_or(10))?;

        // Apply user-defined column type overrides
        if let Some(user_defined_columns) = parameters.columns.to_owned() {
            for (name, kind) in columns.iter_mut() {
                if let Some(user_defined_value) = user_defined_columns.get(name) {
                    *kind = user_defined_value.to_owned();
                }
            }
        }

        let error_as_null = parameters.error_as_null.unwrap_or(false);
        Ok(ReadSheetBindData {
            sheet,
            columns,
            error_as_null,
        })
    }
}

/// Initialization data for the read_sheet table function.
///
/// Tracks the current row position for batch processing of large datasets.
#[repr(C)]
pub(crate) struct ReadSheetInitData {
    /// Current row index for batch processing
    row: AtomicUsize,
}

/// Implementation of the read_sheet table function.
///
/// This function reads spreadsheet data in batches and provides it to DuckDB
/// for SQL querying. It supports automatic type inference, user type overrides,
/// and flexible error handling.
pub(crate) struct ReadSheetTableFunction;

impl VTab for ReadSheetTableFunction {
    type InitData = ReadSheetInitData;
    type BindData = ReadSheetBindData;

    /// Binds the function parameters and prepares the result schema.
    ///
    /// This phase validates parameters, analyzes the spreadsheet structure,
    /// and registers the output columns with their detected/configured types.
    ///
    /// # Arguments
    ///
    /// * `bind` - DuckDB bind information containing parameters
    ///
    /// # Returns
    ///
    /// * `Result<Self::BindData, Box<dyn Error>>` - Bind data or error
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let parameters = ReadSheetParameters::try_from(bind)?;
        let data = ReadSheetBindData::try_from(&parameters)?;

        // Register output columns with DuckDB
        for (name, kind) in &data.columns {
            bind.add_result_column(name, LogicalTypeHandle::from(kind.to_logical_type_id()));
        }
        Ok(data)
    }

    /// Initializes the function execution state.
    ///
    /// Sets up the starting row position for data reading, accounting for
    /// headers if present.
    ///
    /// # Arguments
    ///
    /// * `init` - DuckDB initialization information
    ///
    /// # Returns
    ///
    /// * `Result<Self::InitData, Box<dyn Error>>` - Initialization data
    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind: *const Self::BindData = init.get_bind_data();
        let row = unsafe {
            let sheet = &(*bind).sheet;
            // Skip header row if present
            sheet.row_lower_bound + if sheet.with_header { 1 } else { 0 }
        };
        Ok(ReadSheetInitData {
            row: AtomicUsize::new(row),
        })
    }

    /// Executes the function and returns spreadsheet data.
    ///
    /// Reads data in batches of 1000 rows for efficient memory usage and
    /// processing of large files. Each batch is converted to the appropriate
    /// DuckDB data types.
    ///
    /// # Arguments
    ///
    /// * `func` - Table function information
    /// * `output` - Output data chunk to populate
    ///
    /// # Returns
    ///
    /// * `Result<(), Box<dyn Error>>` - Success or error
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        const STEP: usize = 1000; // Process 1000 rows per batch

        let bind = func.get_bind_data();
        let init = func.get_init_data();
        let row_lower_bound = init.row.fetch_add(STEP, Ordering::Relaxed);
        let row_upper_bound = bind.sheet.row_upper_bound.min(row_lower_bound + STEP - 1);

        if row_lower_bound <= row_upper_bound {
            // Process the current batch
            output.set_len(row_upper_bound - row_lower_bound + 1);

            // Process each column
            for (index, (_, kind)) in bind.columns.iter().enumerate() {
                let column = index + bind.sheet.column_lower_bound;

                // Collect cells for this column in the current batch
                let cells: Vec<Option<&Cell>> = (row_lower_bound..=row_upper_bound)
                    .map(|row| bind.sheet.get(row, column))
                    .collect();

                let mut vector = output.flat_vector(index);
                populate(&mut vector, kind, &cells, bind.error_as_null)?;
            }
        } else {
            // No more data to process
            output.set_len(0);
        }
        Ok(())
    }

    /// Returns the positional parameter types.
    ///
    /// # Returns
    ///
    /// * `Option<Vec<LogicalTypeHandle>>` - Single VARCHAR parameter for file path
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    /// Returns the named parameter definitions.
    ///
    /// # Returns
    ///
    /// * `Option<Vec<(String, LogicalTypeHandle)>>` - All supported named parameters
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            SheetNameParam::definition(),
            RangeParam::definition(),
            HeaderParam::definition(),
            ColumnsParam::definition(),
            AnalyzeRowsParam::definition(),
            ErrorAsNullParam::definition(),
        ])
    }
}

/// Type alias for cell value extraction functions
type Getter<T> = fn(&Cell) -> Option<T>;

/// Type alias for DuckDB vector population functions
type Setter<T> = fn(&mut FlatVector, usize, T);

/// Populates a column vector with values from spreadsheet cells.
///
/// This function handles the conversion from spreadsheet cell values to
/// DuckDB-compatible data types, with proper error handling and NULL value
/// management.
///
/// # Arguments
///
/// * `vector` - The output vector to populate
/// * `kind` - The target data type for the column
/// * `values` - Cell values from the spreadsheet
/// * `error_as_null` - Whether to treat parsing errors as NULL
///
/// # Returns
///
/// * `Result<(), ExtensionError>` - Success or parsing error
fn populate(
    vector: &mut FlatVector,
    kind: &CellKind,
    values: &Vec<Option<&Cell>>,
    error_as_null: bool,
) -> Result<(), ExtensionError> {
    match kind {
        CellKind::Varchar => populate_values(
            vector,
            values,
            error_as_null,
            Cell::get_varchar,
            string_setter,
        ),

        CellKind::Bool => populate_values(
            vector,
            values,
            error_as_null,
            Cell::get_bool,
            primitive_setter,
        ),
        CellKind::BigInt => populate_values(
            vector,
            values,
            error_as_null,
            Cell::get_bigint,
            primitive_setter,
        ),
        CellKind::Double => populate_values(
            vector,
            values,
            error_as_null,
            Cell::get_double,
            primitive_setter,
        ),

        CellKind::DateTime => populate_values(
            vector,
            values,
            error_as_null,
            Cell::get_datetime,
            datetime_setter,
        ),
        CellKind::Date => {
            populate_values(vector, values, error_as_null, Cell::get_date, date_setter)
        }
        CellKind::Time => {
            populate_values(vector, values, error_as_null, Cell::get_time, time_setter)
        }
        CellKind::Interval => populate_values(
            vector,
            values,
            error_as_null,
            Cell::get_interval,
            interval_setter,
        ),
    }
}

/// Generic function to populate vector values with type-specific conversion.
///
/// This function applies a getter to extract typed values from cells and
/// a setter to store them in the DuckDB vector, with proper NULL handling.
///
/// # Type Parameters
///
/// * `T` - The target data type
///
/// # Arguments
///
/// * `vector` - The output vector to populate
/// * `values` - Cell values from the spreadsheet
/// * `error_as_null` - Whether to treat parsing errors as NULL
/// * `getter` - Function to extract typed value from cell
/// * `setter` - Function to store value in vector
///
/// # Returns
///
/// * `Result<(), ExtensionError>` - Success or parsing error
fn populate_values<T: Debug>(
    vector: &mut FlatVector,
    values: &Vec<Option<&Cell>>,
    error_as_null: bool,
    getter: Getter<T>,
    setter: Setter<T>,
) -> Result<(), ExtensionError> {
    for (index, option) in values.iter().enumerate() {
        if let Some(cell) = option {
            // Handle cell errors according to error_as_null setting
            if let Some(error) = cell.get_error().filter(|_| !error_as_null) {
                Err(error)?;
            } else if let Some(value) = getter(cell) {
                setter(vector, index, value);
                continue;
            }
        };
        // Set NULL for empty cells or when error_as_null is true
        vector.set_null(index);
    }
    Ok(())
}

/// Sets a string value in a DuckDB vector.
///
/// # Arguments
///
/// * `vector` - The vector to modify
/// * `index` - The position to set
/// * `value` - The string value to set
fn string_setter(vector: &mut FlatVector, index: usize, value: String) {
    vector.insert(index, &value);
}

/// Sets a primitive value in a DuckDB vector using direct memory access.
///
/// # Type Parameters
///
/// * `T` - The primitive type to set
///
/// # Arguments
///
/// * `vector` - The vector to modify
/// * `index` - The position to set
/// * `value` - The value to set
fn primitive_setter<T>(vector: &mut FlatVector, index: usize, value: T) {
    let pointer: *mut T = vector.as_mut_ptr();
    unsafe {
        std::ptr::write(pointer.add(index), value);
    }
}

/// Sets a datetime value in a DuckDB timestamp vector.
///
/// Converts from chrono::NaiveDateTime to DuckDB's timestamp format
/// (microseconds since Unix epoch).
///
/// # Arguments
///
/// * `vector` - The vector to modify
/// * `index` - The position to set
/// * `value` - The datetime value to set
fn datetime_setter(vector: &mut FlatVector, index: usize, value: NaiveDateTime) {
    let pointer: *mut duckdb_timestamp = vector.as_mut_ptr();
    unsafe {
        let pointer = pointer.add(index);
        (*pointer).micros = value.and_utc().timestamp_micros();
    }
}

/// Sets a date value in a DuckDB date vector.
///
/// Converts from chrono::NaiveDate to DuckDB's date format
/// (days since January 1, 1970).
///
/// # Arguments
///
/// * `vector` - The vector to modify
/// * `index` - The position to set
/// * `value` - The date value to set
fn date_setter(vector: &mut FlatVector, index: usize, value: NaiveDate) {
    let pointer: *mut duckdb_date = vector.as_mut_ptr();
    unsafe {
        let pointer = pointer.add(index);
        // Convert from Common Era days to Unix epoch days
        (*pointer).days = value.num_days_from_ce() - 719_163;
    }
}

/// Sets an interval value in a DuckDB interval vector.
///
/// Converts from chrono::Duration to DuckDB's interval format
/// (days and microseconds components).
///
/// # Arguments
///
/// * `vector` - The vector to modify
/// * `index` - The position to set
/// * `value` - The duration value to set
fn interval_setter(vector: &mut FlatVector, index: usize, value: Duration) {
    let pointer: *mut duckdb_interval = vector.as_mut_ptr();
    unsafe {
        let pointer = pointer.add(index);
        (*pointer).days = value.num_days() as i32;
        (*pointer).micros = value.subsec_micros() as i64;
    }
}

/// Sets a time value in a DuckDB time vector.
///
/// Converts from chrono::NaiveTime to DuckDB's time format
/// (microseconds since midnight).
///
/// # Arguments
///
/// * `vector` - The vector to modify
/// * `index` - The position to set
/// * `value` - The time value to set
fn time_setter(vector: &mut FlatVector, index: usize, value: NaiveTime) {
    let pointer: *mut duckdb_time = vector.as_mut_ptr();
    unsafe {
        let pointer = pointer.add(index);
        (*pointer).micros = value.num_seconds_from_midnight() as i64 * 1_000_000;
    }
}
