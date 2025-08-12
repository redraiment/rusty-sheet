//! # DuckDB Spreadsheet Extension
//!
//! A DuckDB extension for reading Excel and OpenDocument spreadsheet files directly in SQL queries.
//! This extension provides seamless integration with DuckDB's powerful SQL engine for analyzing
//! spreadsheet data.
//!
//! ## Features
//!
//! - **Multi-format support**: Read Excel files (`.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.xla`, `.xlam`)
//!   and OpenDocument spreadsheet files (`.ods`)
//! - **Flexible data types**: Support for Bool, BigInt, Double, Varchar, DateTime, Date, Time,
//!   and Interval data types
//! - **Custom data ranges**: Specify precise row and column ranges for data extraction
//! - **Header handling**: Automatic detection and parsing of header rows
//! - **Error handling**: Configurable behavior for empty cells and parsing errors
//! - **Type safety**: Built-in data type validation and conversion
//! - **Pure Rust implementation**: No C++ dependencies, leveraging Rust's memory safety
//! - **Field type inference**: Automatic data analysis and accurate field type detection
//! - **High performance**: Fast processing of large files with millions of rows
//!
//! ## Table Functions
//!
//! This extension registers two table functions:
//!
//! - `read_sheet`: Read and query spreadsheet file data
//! - `analyze_sheet`: Analyze spreadsheet files and return field type information
extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod bridge;
mod extension;
mod spreadsheet;

use crate::extension::analyze_sheet_table_function::AnalyzeSheetTableFunction;
use crate::extension::read_sheet_table_function::ReadSheetTableFunction;
use anyhow::{Context, Result};
use duckdb::Connection;
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;

/// Extension entry point for DuckDB.
///
/// This function is called when the extension is loaded by DuckDB. It registers
/// the two main table functions provided by this extension:
///
/// - `analyze_sheet`: Analyzes spreadsheet structure and returns column type information
/// - `read_sheet`: Reads and queries data from spreadsheet files
///
/// # Arguments
///
/// * `connection` - The DuckDB connection to register functions with
///
/// # Returns
///
/// * `Result<()>` - Success if both functions are registered successfully
///
/// # Errors
///
/// Returns an error if either table function fails to register with DuckDB.
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(connection: Connection) -> Result<()> {
    connection
        .register_table_function::<AnalyzeSheetTableFunction>("analyze_sheet")
        .context("Failed to register analyze_sheet table function")?;
    connection
        .register_table_function::<ReadSheetTableFunction>("read_sheet")
        .context("Failed to register read_sheet table function")?;
    Ok(())
}
