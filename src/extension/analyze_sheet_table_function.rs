//! # Column Kinds Table Function
//!
//! This module implements the `analyze_sheet` table function that analyzes
//! spreadsheet files and returns column type information in a format that
//! can be directly used as the `columns` parameter for the `read_sheet` function.
extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use crate::extension::{
    open_sheet, AnalyzeRowsParam, ExtensionError, HeaderParam, NamedParam, Range, RangeParam,
    SheetNameParam,
};
use crate::spreadsheet::CellKind;
use anyhow::Result;
use duckdb::core::Inserter;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    error::Error,
    sync::atomic::{AtomicBool, Ordering},
};

/// Parameters for the analyze_sheet table function.
///
/// These parameters control how the spreadsheet is analyzed to determine
/// column types and structure.
struct AnalyzeSheetParameters {
    /// Path to the spreadsheet file
    file_name: String,
    /// Optional sheet name (uses first sheet if None)
    sheet_name: Option<String>,
    /// Optional range constraint for analysis
    range: Option<Range>,
    /// Whether the first row contains headers (default: true)
    header: Option<bool>,
    /// Number of rows to analyze for type inference (default: 10)
    analyze_rows: Option<usize>,
}

impl TryFrom<&BindInfo> for AnalyzeSheetParameters {
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
        Ok(AnalyzeSheetParameters {
            file_name: bind.get_parameter(0).to_string(),
            sheet_name: SheetNameParam::read(bind),
            range: RangeParam::read(bind),
            header: HeaderParam::read(bind),
            analyze_rows: AnalyzeRowsParam::read(bind),
        })
    }
}

/// Bind data for the analyze_sheet table function.
///
/// Contains the analyzed column information that will be returned as results.
#[repr(C)]
pub(crate) struct AnalyzeSheetBindData {
    /// List of (column_name, column_type) pairs
    columns: Vec<(String, CellKind)>,
}

impl TryFrom<&AnalyzeSheetParameters> for AnalyzeSheetBindData {
    type Error = ExtensionError;

    /// Analyzes the spreadsheet and extracts column type information.
    ///
    /// # Arguments
    ///
    /// * `parameters` - The function parameters
    ///
    /// # Returns
    ///
    /// * `Result<Self, Self::Error>` - Bind data with column analysis results
    fn try_from(parameters: &AnalyzeSheetParameters) -> Result<Self, Self::Error> {
        let sheet = open_sheet(
            &parameters.file_name,
            &parameters.sheet_name,
            &parameters.range,
            &parameters.header,
        )?;
        Ok(AnalyzeSheetBindData {
            columns: sheet.analyze_columns(parameters.analyze_rows.unwrap_or(10))?,
        })
    }
}

/// Initialization data for the analyze_sheet table function.
///
/// Tracks whether the single result row has been returned.
#[repr(C)]
pub(crate) struct AnalyzeSheetInitData {
    /// Whether the result has been returned (function returns single row)
    done: AtomicBool,
}

/// Implementation of the analyze_sheet table function.
///
/// This function analyzes a spreadsheet file and returns a single row containing
/// a formatted map of column names and their detected types. The output format
/// is designed to be directly usable as the `columns` parameter in `read_sheet`.
pub(crate) struct AnalyzeSheetTableFunction;

impl VTab for AnalyzeSheetTableFunction {
    type InitData = AnalyzeSheetInitData;
    type BindData = AnalyzeSheetBindData;

    /// Binds the function parameters and analyzes the spreadsheet.
    ///
    /// This phase validates parameters, opens the spreadsheet file, and performs
    /// column type analysis. The result schema is a single VARCHAR column named "columns".
    ///
    /// # Arguments
    ///
    /// * `bind` - DuckDB bind information containing parameters
    ///
    /// # Returns
    ///
    /// * `Result<Self::BindData, Box<dyn Error>>` - Bind data or error
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let parameters = AnalyzeSheetParameters::try_from(bind)?;
        let data = AnalyzeSheetBindData::try_from(&parameters)?;
        bind.add_result_column("column_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("column_type", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(data)
    }

    /// Initializes the function execution state.
    ///
    /// # Arguments
    ///
    /// * `_init` - DuckDB initialization information (unused)
    ///
    /// # Returns
    ///
    /// * `Result<Self::InitData, Box<dyn Error>>` - Initialization data
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(AnalyzeSheetInitData {
            done: AtomicBool::new(false),
        })
    }

    /// Executes the function and returns column analysis results.
    ///
    /// This function returns column names and detected types.
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
        let init = func.get_init_data();
        let bind = func.get_bind_data();

        // Return empty result set if already executed (single row function)
        if init.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let names = output.flat_vector(0);
            let kinds = output.flat_vector(1);
            for (index, (name, kind)) in bind.columns.iter().enumerate() {
                names.insert(index, name);
                kinds.insert(index, kind.as_str());
            }
            output.set_len(bind.columns.len());
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
    /// * `Option<Vec<(String, LogicalTypeHandle)>>` - Named parameter definitions
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            SheetNameParam::definition(),
            RangeParam::definition(),
            HeaderParam::definition(),
            AnalyzeRowsParam::definition(),
        ])
    }
}
