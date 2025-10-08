//! Data writing utilities for converting spreadsheet cells to DuckDB vectors.

use crate::database::column::Column;
use crate::database::column::ColumnType;
use crate::error::RustySheetError;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use duckdb::core::FlatVector;
use duckdb::core::Inserter;
use libduckdb_sys::duckdb_date;
use libduckdb_sys::duckdb_time;
use libduckdb_sys::duckdb_timestamp;
use crate::spreadsheet::sheet::Sheet;
use crate::spreadsheet::SpreadsheetError;

/// Writes a cell value to a DuckDB vector based on column type.
/// Handles type conversion and error mapping for different data types.
pub(super) fn write_to_vector(sheet: &Sheet, column: &Column, cell: &Cell, vector: &mut FlatVector, row: usize, shared_strings: &Vec<String>) -> Result<(), RustySheetError> {
    let mapper = |message: String| {
        SpreadsheetError::CellValueError(
            sheet.file_name.to_owned(),
            sheet.name.to_owned(),
            cell.reference(),
            message,
        )
    };
    match (column.kind, cell.kind) {
        (ColumnType::Varchar, CellType::SharedString) => {
            let index = cell.value.parse::<usize>()?;
            vector.insert(row, &shared_strings[index]);
        }
        (ColumnType::Varchar, _) => vector.insert(row, &cell.to_string()),
        (ColumnType::Boolean, _) => write_primitive(vector, row, cell.to_boolean()),
        (ColumnType::BigInt, _) => write_primitive(vector, row, cell.to_bigint().map_err(mapper)?),
        (ColumnType::Double, _) => write_primitive(vector, row, cell.to_double().map_err(mapper)?),
        (ColumnType::Timestamp, _) => write_timestamp(vector, row, cell.to_datetime().map_err(mapper)?),
        (ColumnType::Date, _) => write_date(vector, row, cell.to_date().map_err(mapper)?),
        (ColumnType::Time, _) => write_time(vector, row, cell.to_time().map_err(mapper)?),
    }
    Ok(())
}

/// Writes a primitive value directly to a vector using pointer arithmetic.
fn write_primitive<T>(vector: &mut FlatVector, index: usize, value: T) {
    let pointer: *mut T = vector.as_mut_ptr();
    unsafe {
        std::ptr::write(pointer.add(index), value);
    }
}

/// Writes a timestamp value (microseconds since epoch) to a DuckDB timestamp vector.
fn write_timestamp(vector: &mut FlatVector, index: usize, value: i64) {
    let pointer: *mut duckdb_timestamp = vector.as_mut_ptr();
    unsafe {
        let pointer = pointer.add(index);
        (*pointer).micros = value;
    }
}

/// Writes a date value (days since epoch) to a DuckDB date vector.
fn write_date(vector: &mut FlatVector, index: usize, value: i32) {
    let pointer: *mut duckdb_date = vector.as_mut_ptr();
    unsafe {
        let pointer = pointer.add(index);
        (*pointer).days = value;
    }
}

// fn write_interval(vector: &mut FlatVector, index: usize, value: Duration) {
//     let pointer: *mut duckdb_interval = vector.as_mut_ptr();
//     unsafe {
//         let pointer = pointer.add(index);
//         (*pointer).days = value.num_days() as i32;
//         (*pointer).micros = value.subsec_micros() as i64;
//     }
// }

/// Writes a time value (microseconds since midnight) to a DuckDB time vector.
fn write_time(vector: &mut FlatVector, index: usize, value: i64) {
    let pointer: *mut duckdb_time = vector.as_mut_ptr();
    unsafe {
        let pointer = pointer.add(index);
        (*pointer).micros = value;
    }
}
