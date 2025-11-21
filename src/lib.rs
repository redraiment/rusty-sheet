//! Rusty Sheet - A DuckDB extension for reading Excel and OpenDocument spreadsheets.
//!
//! This extension provides high-performance spreadsheet parsing with automatic type detection
//! and flexible data range selection directly within SQL queries.

pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod extension;
pub(crate) mod helpers;
pub(crate) mod spreadsheet;

use crate::extension::analyze_sheet::AnalyzeSheetTableFunction;
use crate::extension::analyze_sheets::AnalyzeSheetsTableFunction;
use crate::extension::read_sheet::ReadSheetTableFunction;
use crate::extension::read_sheets::ReadSheetsTableFunction;
use anyhow::Context;
use anyhow::Result;
use duckdb::Connection;
use libduckdb_sys as ffi;

/// Internal Entrypoint for error handling
pub fn rusty_sheet_init_c_api_internal(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn std::error::Error>> {
    let have_api_struct = unsafe { ffi::duckdb_rs_extension_api_init(info, access, "v1.2.0")? };
    if !have_api_struct {
        return Ok(false);
    }
    let db: ffi::duckdb_database = unsafe { *(*access).get_database.unwrap()(info) };
    let connection = unsafe { Connection::open_from_raw(db.cast())? };
    extension_entrypoint(connection)?;
    Ok(true)
}

/// Entrypoint that will be called by DuckDB
#[unsafe(no_mangle)]
pub extern "C" fn rusty_sheet_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    let init_result = rusty_sheet_init_c_api_internal(info, access);
    if let Err(x) = init_result {
        let error_c_string = std::ffi::CString::new(x.to_string());
        match error_c_string {
            Ok(e) => unsafe {
                (*access).set_error.unwrap()(info, e.as_ptr());
            },
            Err(_e) => {
                let error_alloc_failure = c"An error occured but the extension failed to allocate memory for an error string";
                unsafe {
                    (*access).set_error.unwrap()(info, error_alloc_failure.as_ptr());
                }
            }
        }
        return false;
    }
    init_result.unwrap()
}

/// DuckDB extension entry point.
/// Registers all table functions with the database connection.
pub fn extension_entrypoint(connection: Connection) -> Result<()> {
    connection
        .register_table_function::<AnalyzeSheetTableFunction>("analyze_sheet")
        .context("Failed to register analyze_sheet table function")?;
    connection
        .register_table_function::<AnalyzeSheetsTableFunction>("analyze_sheets")
        .context("Failed to register analyze_sheets table function")?;
    connection
        .register_table_function::<ReadSheetTableFunction>("read_sheet")
        .context("Failed to register read_sheet table function")?;
    connection
        .register_table_function::<ReadSheetsTableFunction>("read_sheets")
        .context("Failed to register read_sheets table function")?;
    Ok(())
}
