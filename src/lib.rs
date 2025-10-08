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
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;

/// DuckDB extension entry point.
/// Registers all table functions with the database connection.
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(connection: Connection) -> Result<()> {
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
