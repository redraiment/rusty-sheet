extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use crate::ReadSheetError::{
    EmptySheet, InvalidCellType, InvalidCellValue, InvalidParameter, MissingHeaderRow,
    SheetNotFound,
};
use anyhow::{Context, Result};
use calamine::{open_workbook, Data, DataType as CalamineDataType, Range, Reader, Xlsx};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use duckdb::{
    arrow::datatypes::ArrowNativeType,
    core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId},
    ffi::{duckdb_date, duckdb_time, duckdb_timestamp},
    vtab::Value,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};
use thiserror::Error;

/// Custom error types for the spreadsheet reader extension
#[derive(Error, Debug)]
enum ReadSheetError {
    #[error("Invalid parameter '{field}': {message}")]
    InvalidParameter { field: String, message: String },

    #[error("Missing header row")]
    MissingHeaderRow,

    #[error("Sheet '{sheet}' not found")]
    SheetNotFound { sheet: String },

    #[error("Empty sheet or missing data")]
    EmptySheet,

    #[error("Cell value '{value}' parsing error: {message}")]
    InvalidCellValue { value: String, message: String },

    #[error("Cell value '{value}' cast to {data_type} failed")]
    InvalidCellType { value: String, data_type: String },
}

/// Optional Named parameters supported by the read_sheet table function
///
/// These parameters allow fine-grained control over how spreadsheet data is read and processed.
#[derive(Copy, Clone, Debug)]
enum NamedParameter {
    // Sheet name to read from; defaults to the first sheet if not specified
    SheetName,
    /// Whether the first row contains column headers; defaults to true
    Header,
    /// Column name and type definitions; must match the table width
    Fields,
    /// Starting row number (inclusive), zero-based index
    StartRow,
    /// Starting column number (inclusive), zero-based index
    StartCol,
    /// Ending row number (inclusive)
    EndRow,
    /// Ending column number (inclusive)
    EndCol,
    /// Convert empty cells to NULL instead of empty strings; defaults to false
    EmptyAsNull,
    /// Convert unparseable cells to NULL instead of throwing errors; defaults to false
    ErrorAsNull,
}

impl NamedParameter {
    /// Returns the string representation of the named parameter for use in SQL
    const fn as_str(&self) -> &'static str {
        match self {
            Self::SheetName => "sheet_name",
            Self::Header => "header",
            Self::Fields => "fields",
            Self::StartRow => "start_row",
            Self::StartCol => "start_column",
            Self::EndRow => "end_row",
            Self::EndCol => "end_column",
            Self::EmptyAsNull => "empty_as_null",
            Self::ErrorAsNull => "error_as_null",
        }
    }
}

/// Trait for convenient extraction of named parameter values from BindInfo
///
/// This trait provides type-safe access to named parameters with automatic conversion
/// to common Rust types.
trait NamedParameterExtractor {
    /// Get a named parameter value as a generic Value
    fn get_value(&self, parameter: NamedParameter) -> Option<Value>;

    /// Get a varchar parameter value as a String
    fn get_varchar(&self, parameter: NamedParameter) -> Option<String>;

    /// Get a boolean parameter value
    fn get_bool(&self, parameter: NamedParameter) -> Option<bool>;

    /// Get an unsigned integer parameter value
    fn get_usize(&self, parameter: NamedParameter) -> Option<usize>;
}

impl NamedParameterExtractor for BindInfo {
    fn get_value(&self, parameter: NamedParameter) -> Option<Value> {
        self.get_named_parameter(parameter.as_str())
    }

    fn get_varchar(&self, parameter: NamedParameter) -> Option<String> {
        self.get_value(parameter).map(|value| value.to_varchar())
    }

    fn get_bool(&self, parameter: NamedParameter) -> Option<bool> {
        self.get_value(parameter).map(|value| value.to_bool())
    }

    fn get_usize(&self, parameter: NamedParameter) -> Option<usize> {
        self.get_value(parameter)
            .and_then(|value| value.to_uint32().to_usize())
    }
}

/// Supported DuckDB data types for spreadsheet columns
///
/// This enum maps common spreadsheet data types to their corresponding DuckDB types,
/// providing automatic conversion and validation.
#[derive(Copy, Clone, Debug)]
enum DataType {
    /// Boolean type for true/false values
    Boolean,
    /// 64-bit signed integer
    BigInt,
    /// Double-precision floating point number
    Double,
    /// Variable-length character string
    Varchar,
    /// Date and time with microsecond precision
    DateTime,
    /// Date without time component
    Date,
    /// Time without date component
    Time,
}

impl DataType {
    /// Returns the string representation of the date type
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::BigInt => "bigint",
            Self::Double => "double",
            Self::Varchar => "varchar",
            Self::DateTime => "datetime",
            Self::Date => "date",
            Self::Time => "time",
        }
    }

    /// Parse data type from string representation (case-insensitive)
    ///
    /// # Arguments
    /// * `name` - The string name of the data type
    ///
    /// # Returns
    /// * `Some(DataType)` if the name is recognized
    /// * `None` if the name is not supported
    fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => Some(Self::Boolean),
            "INT" | "BIGINT" | "INTEGER" => Some(Self::BigInt),
            "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => Some(Self::Double),
            "TEXT" | "STRING" | "VARCHAR" => Some(Self::Varchar),
            "DATETIME" => Some(Self::DateTime),
            "DATE" => Some(Self::Date),
            "TIME" => Some(Self::Time),
            _ => None,
        }
    }

    /// Convert to DuckDB LogicalTypeId for column definition
    const fn to_logical_type_id(&self) -> LogicalTypeId {
        match self {
            Self::Boolean => LogicalTypeId::Boolean,
            Self::BigInt => LogicalTypeId::Bigint,
            Self::Double => LogicalTypeId::Double,
            Self::Varchar => LogicalTypeId::Varchar,
            Self::DateTime => LogicalTypeId::Timestamp,
            Self::Date => LogicalTypeId::Date,
            Self::Time => LogicalTypeId::Time,
        }
    }
}

/// Represents a column field with name and data type
#[derive(Clone, Debug)]
struct Field {
    /// Column name as it appears in the result set
    name: String,
    // Expected data type for values in this column
    data_type: DataType,
}

/// Helper function to handle error and empty data scenarios
///
/// This function implements the common logic for handling empty cells and parsing errors
/// based on the `empty_as_null` and `error_as_null` flags.
fn handle_error_and_empty_data<T>(
    data: &Data,
    expected_type: DataType,
    empty_as_null: bool,
    error_as_null: bool,
) -> Result<Option<T>, ReadSheetError> {
    match data {
        Data::Error(_) if error_as_null => Ok(None),
        Data::Empty if empty_as_null => Ok(None),
        Data::Error(error) => Err(InvalidCellValue {
            value: data.to_string(),
            message: error.to_string(),
        }),
        Data::Empty => Err(InvalidCellType {
            value: data.to_string(),
            data_type: expected_type.as_str().to_owned(),
        }),
        _ => Err(InvalidCellType {
            value: data.to_string(),
            data_type: expected_type.as_str().to_owned(),
        }),
    }
}

/// Type alias for data conversion functions
type Converter<T> = fn(&Data, bool, bool) -> Result<Option<T>, ReadSheetError>;
/// Type alias for datetime setter functions
type Setter<T> = unsafe fn(*mut T, &NaiveDateTime);

impl Field {
    /// Fill a FlatVector with values from spreadsheet cells
    ///
    /// This method handles the conversion from spreadsheet cell data to DuckDB column data,
    /// applying appropriate type conversions and null handling.
    ///
    /// # Arguments
    /// * `vector` - Mutable reference to the DuckDB FlatVector to fill
    /// * `values` - Vector of optional cell data references
    /// * `empty_as_null` - Whether to convert empty cells to NULL
    /// * `error_as_null` - Whether to convert parsing errors to NULL
    fn fill(
        &self,
        vector: &mut FlatVector,
        values: &Vec<Option<&Data>>,
        empty_as_null: bool,
        error_as_null: bool,
    ) -> Result<()> {
        match self.data_type {
            DataType::Varchar => Self::fill_varchar(vector, values, empty_as_null, error_as_null),
            // Primitive types
            DataType::Boolean => Self::fill_with_primitive_converter(
                vector,
                values,
                empty_as_null,
                error_as_null,
                Self::bool_converter,
            ),
            DataType::BigInt => Self::fill_with_primitive_converter(
                vector,
                values,
                empty_as_null,
                error_as_null,
                Self::bigint_converter,
            ),
            DataType::Double => Self::fill_with_primitive_converter(
                vector,
                values,
                empty_as_null,
                error_as_null,
                Self::double_converter,
            ),
            // DateTime types
            DataType::DateTime => Self::fill_with_datetime_setter(
                vector,
                values,
                self.data_type,
                empty_as_null,
                error_as_null,
                Self::datetime_setter,
            ),
            DataType::Date => Self::fill_with_datetime_setter(
                vector,
                values,
                self.data_type,
                empty_as_null,
                error_as_null,
                Self::date_setter,
            ),
            DataType::Time => Self::fill_with_datetime_setter(
                vector,
                values,
                self.data_type,
                empty_as_null,
                error_as_null,
                Self::time_setter,
            ),
        }
    }

    /// Fill a vector with string values, handling various cell data types
    ///
    /// This method converts all supported cell types to string representation,
    /// with special handling for datetime values that might represent time-only data.
    fn fill_varchar(
        vector: &mut FlatVector,
        values: &Vec<Option<&Data>>,
        empty_as_null: bool,
        error_as_null: bool,
    ) -> Result<()> {
        let d1990_01_01 = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        );
        for (index, value) in values.iter().enumerate() {
            let result = if let Some(data) = value {
                match data {
                    Data::Bool(_)
                    | Data::Int(_)
                    | Data::Float(_)
                    | Data::String(_)
                    | Data::DateTimeIso(_)
                    | Data::DurationIso(_) => Ok(Some(data.to_string())),
                    Data::DateTime(datetime) => Ok(datetime.as_datetime().map(|value| {
                        if value < d1990_01_01 {
                            // Handle time-only data (dates before 1900 are likely time values)
                            value.time().to_string()
                        } else {
                            value.to_string()
                        }
                    })),
                    Data::Empty if empty_as_null => Ok(None),
                    Data::Empty => Ok(Some("".to_string())),
                    _ => handle_error_and_empty_data(
                        data,
                        DataType::Varchar,
                        empty_as_null,
                        error_as_null,
                    ),
                }
            } else {
                Ok(None)
            };

            match result {
                Ok(Some(value)) => vector.insert(index, &value),
                Ok(None) => vector.set_null(index),
                Err(_) if error_as_null => vector.set_null(index),
                Err(error) => return Err(anyhow::Error::new(error)),
            };
        }

        Ok(())
    }

    // Primitive Type Converters

    /// Convert cell data to boolean values
    fn bool_converter(
        data: &Data,
        empty_as_null: bool,
        error_as_null: bool,
    ) -> Result<Option<bool>, ReadSheetError> {
        match data {
            Data::Bool(value) => Ok(Some(*value)),
            _ => handle_error_and_empty_data(data, DataType::Boolean, empty_as_null, error_as_null),
        }
    }

    /// Convert cell data to 64-bit integer values
    fn bigint_converter(
        data: &Data,
        empty_as_null: bool,
        error_as_null: bool,
    ) -> Result<Option<i64>, ReadSheetError> {
        match data {
            Data::Int(value) => Ok(Some(*value)),
            Data::Float(value) => Ok(Some(*value as i64)),
            _ => handle_error_and_empty_data(data, DataType::BigInt, empty_as_null, error_as_null),
        }
    }

    /// Convert cell data to double-precision floating point values
    fn double_converter(
        data: &Data,
        empty_as_null: bool,
        error_as_null: bool,
    ) -> Result<Option<f64>, ReadSheetError> {
        match data {
            Data::Int(value) => Ok(Some(*value as f64)),
            Data::Float(value) => Ok(Some(*value)),
            _ => handle_error_and_empty_data(data, DataType::Double, empty_as_null, error_as_null),
        }
    }

    /// Generic helper to fill primitive values using a converter function
    fn fill_with_primitive_converter<T>(
        vector: &mut FlatVector,
        values: &Vec<Option<&Data>>,
        empty_as_null: bool,
        error_as_null: bool,
        converter: Converter<T>,
    ) -> Result<()> {
        for (index, value) in values.iter().enumerate() {
            let result = if let Some(data) = *value {
                converter(data, empty_as_null, error_as_null)
            } else {
                Ok(None)
            };
            Self::write_to_primitive_pointer(vector, index, result, error_as_null)?;
        }
        Ok(())
    }

    /// Generic helper to write primitive values to vector pointer
    fn write_to_primitive_pointer<T>(
        vector: &mut FlatVector,
        index: usize,
        result: Result<Option<T>, ReadSheetError>,
        error_as_null: bool,
    ) -> Result<()> {
        let pointer: *mut T = vector.as_mut_ptr();
        match result {
            Ok(Some(value)) => unsafe {
                std::ptr::write(pointer.add(index), value);
            },
            Ok(None) => vector.set_null(index),
            Err(_) if error_as_null => vector.set_null(index),
            Err(error) => return Err(anyhow::Error::new(error)),
        };
        Ok(())
    }

    // DateTime Setters

    /// Set datetime value in DuckDB timestamp format (microseconds since epoch)
    unsafe fn datetime_setter(pointer: *mut duckdb_timestamp, datetime: &NaiveDateTime) {
        (*pointer).micros = datetime.and_utc().timestamp_micros();
    }

    /// Set date value in DuckDB date format (days since epoch)
    unsafe fn date_setter(pointer: *mut duckdb_date, datetime: &NaiveDateTime) {
        (*pointer).days = datetime.num_days_from_ce() - 719_163;
    }

    /// Set time value in DuckDB time format (microseconds since midnight)
    unsafe fn time_setter(pointer: *mut duckdb_time, datetime: &NaiveDateTime) {
        let time = datetime.time();
        let micros = time.num_seconds_from_midnight() as i64 * 1_000_000;
        (*pointer).micros = micros;
    }

    /// Fill a vector with datetime values using the appropriate setter function
    fn fill_with_datetime_setter<T>(
        vector: &mut FlatVector,
        values: &Vec<Option<&Data>>,
        data_type: DataType,
        empty_as_null: bool,
        error_as_null: bool,
        setter: Setter<T>,
    ) -> Result<()> {
        let pointer: *mut T = vector.as_mut_ptr();
        for (index, value) in values.iter().enumerate() {
            let result = if let Some(data) = value {
                match data {
                    Data::DateTime(value) => Ok(value.as_datetime()),
                    Data::DateTimeIso(value) => DateTime::parse_from_rfc3339(value)
                        .map(|datetime| Some(datetime.naive_local()))
                        .map_err(|_| InvalidCellType {
                            value: data.to_string(),
                            data_type: data_type.as_str().to_owned(),
                        }),
                    _ => handle_error_and_empty_data(data, data_type, empty_as_null, error_as_null),
                }
            } else {
                Ok(None)
            };

            match result {
                Ok(Some(datetime)) => unsafe {
                    setter(pointer.add(index), &datetime);
                },
                Ok(None) => vector.set_null(index),
                Err(_) if error_as_null => vector.set_null(index),
                Err(error) => return Err(anyhow::Error::new(error)),
            };
        }
        Ok(())
    }
}

/// Parameters for the `read_sheet` table function
///
/// This struct encapsulates all the configuration options for reading spreadsheet data,
/// including file path, sheet selection, data ranges, and processing options.
struct ReadSheetParameters {
    /// Path to the spreadsheet file (from positional parameter)
    file_name: String,
    /// Name of the worksheet to read (from named parameter)
    sheet_name: Option<String>,
    /// Whether the first row contains headers (from named parameter)
    header: Option<bool>,
    /// Column definitions with names and types (from named parameter)
    fields: Option<Vec<Field>>,
    /// Starting row index for data reading (from named parameter)
    start_row: Option<usize>,
    /// Starting column index for data reading (from named parameter)
    start_col: Option<usize>,
    /// Ending row index for data reading (from named parameter)
    end_row: Option<usize>,
    /// Ending column index for data reading (from named parameter)
    end_col: Option<usize>,
    /// Convert empty cells to NULL values (from named parameter)
    empty_as_null: Option<bool>,
    /// Convert parsing errors to NULL values (from named parameter)
    error_as_null: Option<bool>,
}

impl TryFrom<&BindInfo> for ReadSheetParameters {
    type Error = anyhow::Error;

    /// Parse and validate parameters from DuckDB BindInfo
    ///
    /// This method extracts all named and positional parameters, validating
    /// the fields parameter structure if provided.
    fn try_from(bind: &BindInfo) -> Result<Self, Self::Error> {
        // Parse fields parameter if provided
        let parameter = bind
            .get_value(NamedParameter::Fields)
            .map(|value| value.to_list());
        let fields: Option<Vec<Field>> = if let Some(definitions) = parameter {
            let result: Result<Vec<Field>> = definitions
                .iter()
                .map(|definition| definition.to_list())
                .map(|definition| {
                    if definition.len() == 2 {
                        let name = definition[0].to_varchar();
                        let data_type = DataType::parse(&definition[1].to_varchar())
                            .expect("Unsupported data type");
                        Ok(Field { name, data_type })
                    } else {
                        Err(InvalidParameter {
                            field: "fields".to_string(),
                            message: "Each field definition must contain exactly field name and data type".to_string(),
                        })?
                    }
                })
                .collect();
            result.ok()
        } else {
            None
        };

        // Extract and parse all other parameters
        Ok(ReadSheetParameters {
            file_name: bind.get_parameter(0).to_string(),
            sheet_name: bind.get_varchar(NamedParameter::SheetName),
            header: bind.get_bool(NamedParameter::Header),
            fields,
            start_row: bind.get_usize(NamedParameter::StartRow),
            start_col: bind.get_usize(NamedParameter::StartCol),
            end_row: bind.get_usize(NamedParameter::EndRow),
            end_col: bind.get_usize(NamedParameter::EndCol),
            empty_as_null: bind.get_bool(NamedParameter::EmptyAsNull),
            error_as_null: bind.get_bool(NamedParameter::ErrorAsNull),
        })
    }
}

/// Represents a valid data range as (min, max) indices
type Interval = (usize, usize);

impl ReadSheetParameters {
    /// Open the specified worksheet from the spreadsheet file
    ///
    /// # Returns
    /// * `Ok(Range<Data>)` containing the worksheet data
    /// * `Err` if the file cannot be opened or the sheet is not found
    fn open(&self) -> Result<Range<Data>> {
        let mut spreadsheet: Xlsx<_> =
            open_workbook(Path::new(&self.file_name)).context("Unable to open spreadsheet")?;
        let sheet = if let Some(sheet_name) = &self.sheet_name {
            spreadsheet
                .worksheet_range(sheet_name)
                .map_err(|_| SheetNotFound {
                    sheet: sheet_name.clone(),
                })
        } else {
            spreadsheet
                .worksheet_range_at(0)
                .expect("No sheets found in workbook")
                .map_err(|_| EmptySheet)
        };
        sheet.context("Specified sheet not found or is empty")
    }

    /// Get and validate the starting row index
    fn start_row(&self, interval: Interval) -> Result<usize> {
        Self::interval_parameter("start_row", self.start_row.unwrap_or(interval.0), interval)
    }

    /// Get and validate the starting column index
    fn start_col(&self, interval: Interval) -> Result<usize> {
        Self::interval_parameter("start_col", self.start_col.unwrap_or(interval.0), interval)
    }

    /// Get and validate the ending row index
    fn end_row(&self, interval: Interval) -> Result<usize> {
        Self::interval_parameter("end_row", self.end_row.unwrap_or(interval.1), interval)
    }

    /// Get and validate the ending column index
    fn end_col(&self, interval: Interval) -> Result<usize> {
        Self::interval_parameter("end_col", self.end_col.unwrap_or(interval.1), interval)
    }

    /// Validate that a parameter value falls within the specified interval
    fn interval_parameter(name: &str, value: usize, interval: Interval) -> Result<usize> {
        if interval.0 <= value && value <= interval.1 {
            Ok(value)
        } else {
            Err(Self::raise_out_of_range(name, interval))?
        }
    }

    /// Create an out-of-range error for parameter validation
    fn raise_out_of_range(parameter_name: &str, interval: Interval) -> ReadSheetError {
        InvalidParameter {
            field: String::from(parameter_name),
            message: format!("Value out of valid range [{}, {}]", interval.0, interval.1),
        }
    }
}

/// Bind data containing all information needed for table function execution
///
/// This struct holds the parsed spreadsheet data and all configuration options
/// that will be used during the actual data reading phase.
#[repr(C)]
pub struct ReadSheetBindData {
    /// The opened worksheet range containing all cell data
    sheet: Range<Data>,
    /// Whether the first row should be treated as column headers
    header: bool,
    /// Column definitions including names and expected data types
    fields: Vec<Field>,
    /// Starting row index for data (after accounting for headers)
    row: usize,
    /// Starting column index for data
    column: usize,
    /// Number of columns to read
    width: usize,
    /// Number of rows to read (after accounting for headers)
    height: usize,
    /// Whether to convert empty cells to NULL
    empty_as_null: bool,
    /// Whether to convert parsing errors to NULL
    error_as_null: bool,
}

impl TryFrom<&ReadSheetParameters> for ReadSheetBindData {
    type Error = anyhow::Error;

    /// Convert validated parameters into bind data structure
    ///
    /// This method opens the spreadsheet, calculates data ranges, and sets up
    /// column definitions based on the provided parameters.
    fn try_from(parameters: &ReadSheetParameters) -> Result<Self, Self::Error> {
        let sheet = parameters.open()?;
        // Convert internal calculations to use usize consistently
        let start = sheet
            .start()
            .map(|(row, column)| (row.to_usize().unwrap(), column.to_usize().unwrap()))
            .unwrap_or((0, 0));
        let end = sheet
            .end()
            .map(|(row, column)| (row.to_usize().unwrap(), column.to_usize().unwrap()))
            .unwrap_or((0, 0));

        // Calculate row and column bounds
        let row_lower_bound = parameters.start_row((start.0, end.0))?;
        let col_lower_bound = parameters.start_col((start.1, end.1))?;
        let row_upper_bound = parameters.end_row((start.0, end.0))?;
        let col_upper_bound = parameters.end_col((start.1, end.1))?;

        let row = row_lower_bound - start.0; // 起始行号
        let column = col_lower_bound - start.1; // 起始列号
        let width = col_upper_bound - col_lower_bound + 1; // 宽度（列数）
        let height = row_upper_bound - row_lower_bound + 1; // 高度（行数）

        // Determine column names and types
        let header = parameters.header.unwrap_or(true);
        if !header && parameters.fields.is_none() && sheet.is_empty() {
            Err(EmptySheet)?;
        } else if header && sheet.is_empty() {
            Err(MissingHeaderRow)?;
        }
        let fields = if let Some(fields) = &parameters.fields {
            // Use provided field definitions, but validate count matches width
            if fields.len() == width {
                fields.to_owned()
            } else {
                Err(InvalidParameter {
                    field: "fields".to_string(),
                    message: "Number of field definitions must match the number of columns being read".to_string(),
                })?
            }
        } else if header {
            // Extract column names from first row, default all types to varchar
            (column..(column + width))
                .map(|index| sheet.get((row, index)).expect("Missing header cell"))
                .map(|cell| cell.as_string().expect("Header cell must be convertible to string"))
                .map(|name| Field {
                    name,
                    data_type: DataType::Varchar,
                }) // Default to varchar for auto-detected columns
                .collect::<Vec<_>>()
        } else {
            // Generate default column names when no header row
            (1..=width)
                .map(|index| Field {
                    name: format!("column{index}"),
                    data_type: DataType::Varchar,
                })
                .collect::<Vec<_>>()
        };

        let empty_as_null = parameters.empty_as_null.unwrap_or(false);
        let error_as_null = parameters.error_as_null.unwrap_or(false);

        Ok(ReadSheetBindData {
            sheet,
            header,
            fields,
            row: if header { row + 1 } else { row }, // Skip header row if present
            column,
            width,
            height: if header { height - 1 } else { height }, // Reduce height if header row present
            empty_as_null,
            error_as_null,
        })
    }
}

/// Initialization data for the table function execution
///
/// This struct maintains state during the table function execution to ensure
/// that data is only read once per query.
#[repr(C)]
pub struct ReadSheetInitData {
    done: AtomicBool,
}

/// Implementation of the `read_sheet` table function for DuckDB
///
/// This table function allows reading data from Excel (.xlsx) files directly
/// within SQL queries, with support for various data types, custom ranges,
/// and flexible error handling.
///
/// # Usage Examples
/// ```sql
/// -- Read entire first sheet with headers
/// SELECT * FROM read_sheet('data.xlsx');
///
/// -- Read specific sheet with custom data types
/// SELECT * FROM read_sheet('data.xlsx',
///   sheet_name='Sheet2',
///   fields=[['id', 'bigint'], ['name', 'varchar'], ['score', 'double']]
/// );
///
/// -- Read specific range without headers
/// SELECT * FROM read_sheet('data.xlsx',
///   header=false,
///   start_row=5,
///   end_row=100,
///   start_column=1,
///   end_column=5
/// );
/// ```
pub struct ReadSheetTableFunction;

impl VTab for ReadSheetTableFunction {
    type InitData = ReadSheetInitData;
    type BindData = ReadSheetBindData;

    /// Bind parameters and set up result columns
    ///
    /// This method is called during query planning to validate parameters,
    /// open the spreadsheet file, and define the output schema.
    ///
    /// # Arguments
    /// * `bind` - Contains all parameter values passed to the table function
    ///
    /// # Returns
    /// * `Ok(BindData)` with validated configuration and column definitions
    /// * `Err` if parameters are invalid or file cannot be accessed
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let parameters = ReadSheetParameters::try_from(bind)?;
        let data = ReadSheetBindData::try_from(&parameters)?;

        // Register each column with DuckDB's type system
        for field in &data.fields {
            bind.add_result_column(
                &field.name,
                LogicalTypeHandle::from(field.data_type.to_logical_type_id()),
            );
        }
        Ok(data)
    }

    /// Initialize function execution state
    ///
    /// Creates the initialization data structure that tracks execution state
    /// to ensure the table function only runs once per query execution.
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ReadSheetInitData {
            done: AtomicBool::new(false),
        })
    }

    /// Main function execution - reads data from spreadsheet and fills output
    ///
    /// This method performs the actual data reading and conversion, filling
    /// the output data chunk with values from the spreadsheet cells.
    ///
    /// # Arguments
    /// * `func` - Contains initialization and bind data
    /// * `output` - Mutable data chunk to fill with results
    ///
    /// # Returns
    /// * `Ok(())` on successful execution
    /// * `Err` if data conversion or reading fails
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init = func.get_init_data();
        let bind = func.get_bind_data();
        // Ensure the function only executes once
        if init.done.swap(true, Ordering::Relaxed) {
            output.set_len(0); // Ensure only execute once
        } else {
            output.set_len(bind.height);
            // Process each column
            for (index, field) in bind.fields.iter().enumerate() {
                let column = index + bind.column;
                let mut vector = output.flat_vector(column);
                // Extract values for this column from all rows
                let values: Vec<Option<&Data>> = (bind.row..(bind.row + bind.height))
                    .map(|row| bind.sheet.get((row, column)))
                    .collect();
                // Fill the vector with converted values
                field.fill(&mut vector, &values, bind.empty_as_null, bind.error_as_null)?;
            }
        }
        Ok(())
    }

    // Define required positional parameters
    ///
    /// The table function requires exactly one positional parameter:
    /// the file path to the spreadsheet.
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)]) // 文件路径字符串
    }

    /// Define optional named parameters
    ///
    /// Returns a list of all supported named parameters with their expected types.
    /// These parameters provide fine-grained control over data reading behavior.
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            (
                NamedParameter::SheetName.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                NamedParameter::Header.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                NamedParameter::Fields.as_str().to_string(),
                LogicalTypeHandle::list(&LogicalTypeHandle::list(&LogicalTypeHandle::from(
                    LogicalTypeId::Varchar,
                ))),
            ),
            (
                NamedParameter::StartRow.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ),
            (
                NamedParameter::StartCol.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ),
            (
                NamedParameter::EndRow.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ),
            (
                NamedParameter::EndCol.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ),
            (
                NamedParameter::EmptyAsNull.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                NamedParameter::ErrorAsNull.as_str().to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
        ])
    }
}

/// DuckDB extension entry point
///
/// This function is called when the extension is loaded into DuckDB.
/// It registers the `read_sheet` table function, making it available for use in SQL queries.
///
/// # Arguments
/// * `connection` - DuckDB connection handle for registering functions
///
/// # Returns
/// * `Ok(())` if registration succeeds
/// * `Err` if the table function cannot be registered
///
/// # Safety
/// This function is marked unsafe because it's called from C code via FFI.
/// The DuckDB loadable extension framework ensures proper calling conventions.
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(connection: Connection) -> Result<()> {
    connection
        .register_table_function::<ReadSheetTableFunction>("read_sheet")
        .context("Failed to register read_sheet table function")?;
    Ok(())
}
