use crate::error::RustySheetError;
use crate::spreadsheet::cell::CellType;
use duckdb::core::LogicalTypeId;
use thiserror::Error;

/// Errors related to column type parsing and validation.
#[derive(Error, Debug)]
pub(crate) enum ColumnError {
    #[error("Invalid column type '{0}'")]
    TypeError(String),
}

/// Supported column data types for spreadsheet data.
#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum ColumnType {
    /// Boolean values (true/false)
    Boolean,
    /// 64-bit signed integers
    BigInt,
    /// Double-precision floating point numbers
    Double,
    /// Variable-length strings
    Varchar,
    /// Date and time with microsecond precision
    Timestamp,
    /// Date without time component
    Date,
    /// Time without date component
    Time,
}

/// Represents a column in a spreadsheet table with name and data type.
#[derive(Clone, Debug)]
pub(crate) struct Column {
    /// Column name (from header row or generated)
    pub(crate) name: String,
    /// Column data type
    pub(crate) kind: ColumnType,
}

impl ColumnType {
    /// Returns the string representation of the column type for DuckDB.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            ColumnType::Boolean => "boolean",
            ColumnType::BigInt => "bigint",
            ColumnType::Double => "double",
            ColumnType::Varchar => "varchar",
            ColumnType::Timestamp => "timestamp",
            ColumnType::Date => "date",
            ColumnType::Time => "time",
        }
    }

    /// Parses a column type from a string representation.
    /// Supports various aliases for each type.
    pub(crate) fn parse(name: &str) -> Result<Self, RustySheetError> {
        match name.to_ascii_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => Ok(Self::Boolean),
            "INT" | "BIGINT" | "INTEGER" => Ok(Self::BigInt),
            "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => Ok(Self::Double),
            "TEXT" | "STRING" | "VARCHAR" => Ok(Self::Varchar),
            "DATETIME" | "TIMESTAMP" => Ok(Self::Timestamp),
            "DATE" => Ok(Self::Date),
            "TIME" => Ok(Self::Time),
            _ => Err(ColumnError::TypeError(name.to_string()))?,
        }
    }

    /// Infers column type from cell type and value.
    /// Handles various Excel date/time formats and numeric representations.
    pub(crate) fn from(cell_type: &CellType, value: &str) -> Option<Self> {
        match cell_type {
            CellType::Boolean => Some(ColumnType::Boolean),
            CellType::Number if Self::is_integer(value) => Some(ColumnType::BigInt),
            CellType::Number => Some(ColumnType::Double),
            CellType::NumberDateTime1900 | CellType::NumberDateTime1904 => Some(ColumnType::Timestamp),
            CellType::NumberDate1900 | CellType::NumberDate1904 => Some(ColumnType::Date),
            CellType::NumberTime1900 | CellType::NumberTime1904 => Some(ColumnType::Time),
            CellType::IsoDateTime if value.contains("1900-01-01") => Some(ColumnType::Time),
            CellType::IsoDateTime if value.contains("1904-01-01") => Some(ColumnType::Time),
            CellType::IsoDateTime if value.contains("00:00:00") => Some(ColumnType::Date),
            CellType::IsoDateTime if !value.contains("T") => Some(ColumnType::Date),
            CellType::IsoDateTime => Some(ColumnType::Timestamp),
            CellType::IsoDuration => Some(ColumnType::Time),
            CellType::InlineString | CellType::SharedString => Some(ColumnType::Varchar),
            _ => None,
        }
    }

    /// Converts column type to DuckDB's logical type ID.
    pub(crate) const fn to_logical_type_id(&self) -> LogicalTypeId {
        match self {
            Self::Boolean => LogicalTypeId::Boolean,
            Self::BigInt => LogicalTypeId::Bigint,
            Self::Double => LogicalTypeId::Double,
            Self::Varchar => LogicalTypeId::Varchar,
            Self::Timestamp => LogicalTypeId::Timestamp,
            Self::Date => LogicalTypeId::Date,
            Self::Time => LogicalTypeId::Time,
        }
    }

    /// Checks if a numeric string represents an integer value.
    /// Returns true if the decimal part contains only zeros or no decimal point.
    fn is_integer(value: &str) -> bool {
        if let Some(index) = value.find('.') {
            for char in value[(index+1)..].chars() {
                if char != '0' {
                    return false;
                }
            }
            true
        } else {
            true
        }
    }

    /// Detects the most specific common type from a collection of candidate types.
    /// Falls back to VARCHAR if types are inconsistent or empty.
    pub(crate) fn detect(types: Vec<Option<ColumnType>>) -> ColumnType {
        let types: Vec<ColumnType> = types.into_iter().filter_map(|it| it).collect();
        if types.is_empty() {
            ColumnType::Varchar
        } else if types.iter().all(|kind| kind.is_boolean()) {
            ColumnType::Boolean
        } else if types.iter().all(|kind| kind.is_int()) {
            ColumnType::BigInt
        } else if types.iter().all(|kind| kind.is_float()) {
            ColumnType::Double
        } else if types.iter().all(|kind| kind.is_date()) {
            ColumnType::Date
        } else if types.iter().all(|kind| kind.is_time()) {
            ColumnType::Time
        } else if types.iter().all(|kind| kind.is_datetime()) {
            ColumnType::Timestamp
        } else {
            ColumnType::Varchar
        }
    }

    /// Returns true if this column type represents boolean values.
    #[inline]
    pub(crate) fn is_boolean(&self) -> bool {
        match self {
            ColumnType::Boolean => true,
            _ => false,
        }
    }

    /// Returns true if this column type represents integer values.
    #[inline]
    pub(crate) fn is_int(&self) -> bool {
        match self {
            ColumnType::BigInt => true,
            _ => false,
        }
    }

    /// Returns true if this column type represents numeric values (integer or floating point).
    #[inline]
    pub(crate) fn is_float(&self) -> bool {
        match self {
            ColumnType::BigInt | ColumnType::Double => true,
            _ => false,
        }
    }

    /// Returns true if this column type represents date values.
    #[inline]
    pub(crate) fn is_date(&self) -> bool {
        match self {
            ColumnType::Date => true,
            _ => false,
        }
    }

    /// Returns true if this column type represents time values.
    #[inline]
    pub(crate) fn is_time(&self) -> bool {
        match self {
            ColumnType::Time => true,
            _ => false,
        }
    }

    /// Returns true if this column type represents date/time related values.
    #[inline]
    pub(crate) fn is_datetime(&self) -> bool {
        match self {
            ColumnType::Timestamp | ColumnType::Date | ColumnType::Time => true,
            _ => false,
        }
    }
}
