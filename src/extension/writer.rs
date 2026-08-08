//! Data writing utilities for converting spreadsheet cells to DuckDB vectors.

use crate::database::column::Column;
use crate::database::column::ColumnType;
use crate::error::RustySheetError;
use crate::spreadsheet::SpreadsheetError;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::resolve_loaded_shared_string;
use crate::spreadsheet::sheet::Sheet;
use duckdb::core::FlatVector;
use duckdb::core::Inserter;
use libduckdb_sys::duckdb_date;
use libduckdb_sys::duckdb_time;
use libduckdb_sys::duckdb_timestamp;

/// Writes a cell value to a DuckDB vector based on column type.
/// Handles type conversion and error mapping for different data types.
pub(super) fn write_to_vector(sheet: &Sheet, column: &Column, cell: &Cell, vector: &mut FlatVector, row: usize, shared_strings: &Vec<Option<String>>) -> Result<(), RustySheetError> {
    let mapper = |message: String| {
        SpreadsheetError::CellValueError(
            sheet.file_name.to_owned(),
            sheet.name.to_owned(),
            cell.reference(),
            message,
        )
    };
    let cell = if cell.kind == CellType::SharedString {
        if let Some(shared_string) =
            resolve_loaded_shared_string(shared_strings, &sheet.file_name, &sheet.name, cell)?
        {
            &Cell {
                row: cell.row,
                col: cell.col,
                kind: cell.kind,
                value: shared_string.to_owned(),
            }
        } else {
            vector.set_null(row);
            return Ok(());
        }
    } else {
        cell
    };
    match column.kind {
        ColumnType::Varchar => vector.insert(row, &cell.to_display_string().map_err(mapper)?),
        ColumnType::Boolean => write_primitive(vector, row, cell.to_boolean()),
        ColumnType::BigInt => write_primitive(vector, row, cell.to_bigint().map_err(mapper)?),
        ColumnType::Double => write_primitive(vector, row, cell.to_double().map_err(mapper)?),
        ColumnType::Timestamp => write_timestamp(vector, row, cell.to_datetime().map_err(mapper)?),
        ColumnType::Date => write_date(vector, row, cell.to_date().map_err(mapper)?),
        ColumnType::Time => write_time(vector, row, cell.to_time().map_err(mapper)?),
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
