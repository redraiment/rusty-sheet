use crate::error::ResultMessage;
use crate::error::RustySheetError;
use crate::extension::AnalyzeRowsParam;
use crate::extension::ErrorAsNullParam;
use crate::extension::FilesParam;
use crate::extension::HeaderParam;
use crate::extension::NamedParam;
use crate::extension::NullsParam;
use crate::extension::Param;
use crate::extension::Range;
use crate::extension::RangeParam;
use crate::extension::SheetsParam;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::open_spreadsheet;
use duckdb::core::DataChunkHandle;
use duckdb::core::Inserter;
use duckdb::core::LogicalTypeHandle;
use duckdb::core::LogicalTypeId;
use duckdb::vtab::BindInfo;
use duckdb::vtab::InitInfo;
use duckdb::vtab::TableFunctionInfo;
use duckdb::vtab::VTab;
use glob::Pattern;
use std::collections::HashSet;
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Parameters for analyzing multiple spreadsheet sheets
struct AnalyzeSheetsParameters {
    /// List of file paths to analyze
    files: Vec<String>,
    /// Optional sheet name patterns with optional file name filters
    sheets: Option<Vec<(Option<Pattern>, Pattern)>>,
    /// Optional cell range to analyze
    range: Option<Range>,
    /// Whether the first row contains headers (default: true)
    header: Option<bool>,
    /// Number of rows to analyze for type detection (default: 10)
    analyze_rows: Option<usize>,
    /// null literals (default: empty string)
    nulls: Option<HashSet<String>>,
    /// Whether to convert errors to null values (default: false)
    error_as_null: Option<bool>,
}

impl TryFrom<&BindInfo> for AnalyzeSheetsParameters {
    type Error = RustySheetError;

    /// Constructs parameters from DuckDB bind information
    ///
    /// # Arguments
    /// * `bind` - DuckDB bind information containing function parameters
    ///
    /// # Returns
    /// * `Result<Self, RustySheetError>` - Parameters or error if parsing fails
    fn try_from(bind: &BindInfo) -> Result<Self, Self::Error> {
        Ok(AnalyzeSheetsParameters {
            files: FilesParam::read(bind, 0)?,
            sheets: SheetsParam::read(bind)?,
            range: RangeParam::read(bind)?,
            header: HeaderParam::read(bind)?,
            analyze_rows: AnalyzeRowsParam::read(bind)?,
            nulls: NullsParam::read(bind)?,
            error_as_null: ErrorAsNullParam::read(bind)?,
        })
    }
}

#[repr(C)]
/// Binding data containing analyzed column metadata for multiple sheets
pub(crate) struct AnalyzeSheetsBindData {
    /// Vector of tuples containing (file_name, sheet_name, column_name, column_type)
    columns: Vec<(String, String, String, String)>,
}

impl TryFrom<&AnalyzeSheetsParameters> for AnalyzeSheetsBindData {
    type Error = RustySheetError;

    /// Analyzes multiple spreadsheets and collects column metadata
    ///
    /// # Arguments
    /// * `parameters` - Analysis parameters including files, sheets, and analysis options
    ///
    /// # Returns
    /// * `Result<Self, RustySheetError>` - Binding data with column metadata or analysis error
    fn try_from(parameters: &AnalyzeSheetsParameters) -> Result<Self, Self::Error> {
        let mut columns = Vec::<(String, String, String, String)>::new();
        let mut spreadsheets = parameters.files
            .iter()
            .map(|path| open_spreadsheet(path).with_prefix(path))
            .collect::<Result<Vec<_>, _>>()?;
        let header = parameters.header.unwrap_or(true);
        let nulls = parameters.nulls.to_owned().unwrap_or(HashSet::from(["".to_string()]));
        for spreadsheet in &mut spreadsheets {
            let sheet_name_patterns = parameters.sheets.as_ref().map(|sheets| {
                sheets.iter()
                    .filter(|(it, _)| {
                        if let Some(file_name_pattern) = it {
                            file_name_pattern.matches(&spreadsheet.name())
                        } else {
                            true
                        }
                    })
                    .map(|(_, it)| it.to_owned())
                    .collect::<Vec<_>>()
            });
            for table in spreadsheet.analyze_sheets(header, &Criteria {
                sheet_name_patterns,
                sheet_limit: None,
                range: parameters.range,
                rows_limit: parameters.analyze_rows.or(Some(10)),
                nulls: nulls.to_owned(),
                error_as_null: parameters.error_as_null.unwrap_or(false),
                skip_empty_rows: false,
                end_at_empty_row: false,
            }, &Vec::new()).with_prefix(spreadsheet.name().as_str())? {
                for column in &table.columns {
                    columns.push((
                        spreadsheet.name(),
                        table.name.to_owned(),
                        column.name.to_owned(),
                        column.kind.as_str().to_owned(),
                    ));
                }
            }
        }
        Ok(AnalyzeSheetsBindData { columns })
    }
}

#[repr(C)]
/// Initialization data for tracking iteration state across function calls
pub(crate) struct AnalyzeSheetsInitData {
    /// Atomic counter tracking the current position in the column vector
    index: AtomicUsize,
}

/// DuckDB table function for analyzing multiple spreadsheet sheets
pub(crate) struct AnalyzeSheetsTableFunction;

impl VTab for AnalyzeSheetsTableFunction {
    type InitData = AnalyzeSheetsInitData;
    type BindData = AnalyzeSheetsBindData;

    /// Binds the table function by parsing parameters and analyzing spreadsheets
    ///
    /// # Arguments
    /// * `bind` - DuckDB bind information containing function parameters
    ///
    /// # Returns
    /// * `Result<Self::BindData, Box<dyn Error>>` - Binding data with analyzed column metadata
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let parameters = AnalyzeSheetsParameters::try_from(bind)?;
        let data = AnalyzeSheetsBindData::try_from(&parameters)?;
        bind.add_result_column(
            "file_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "sheet_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "column_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "column_type",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        Ok(data)
    }

    /// Initializes the table function with iteration state
    ///
    /// # Arguments
    /// * `_` - DuckDB initialization information (unused)
    ///
    /// # Returns
    /// * `Result<Self::InitData, Box<dyn Error>>` - Initialization data with atomic counter
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(AnalyzeSheetsInitData {
            index: AtomicUsize::new(0),
        })
    }

    /// Executes the table function to produce output data chunks
    ///
    /// # Arguments
    /// * `func` - Table function information containing bind and init data
    /// * `output` - Data chunk handle to populate with results
    ///
    /// # Returns
    /// * `Result<(), Box<dyn Error>>` - Success or execution error
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init = func.get_init_data();
        let bind = func.get_bind_data();
        let lower = init.index.fetch_add(2048, Ordering::Relaxed);
        let upper = bind.columns.len().min(lower + 2048);
        if lower < upper {
            let files = output.flat_vector(0);
            let sheets = output.flat_vector(1);
            let columns = output.flat_vector(2);
            let kinds = output.flat_vector(3);
            for index in lower..upper {
                let (file_name, sheet_name, column_name, kind_name) = &bind.columns[index];
                files.insert(index - lower, file_name);
                sheets.insert(index - lower, sheet_name);
                columns.insert(index - lower, column_name);
                kinds.insert(index - lower, kind_name);
            }
            output.set_len(upper - lower);
        } else {
            output.set_len(0);
        }
        Ok(())
    }

    /// Returns the required parameter types for the table function
    ///
    /// # Returns
    /// * `Option<Vec<LogicalTypeHandle>>` - Required parameter types (file path)
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            FilesParam::kind()
        ])
    }

    /// Returns the named parameter definitions for the table function
    ///
    /// # Returns
    /// * `Option<Vec<(String, LogicalTypeHandle)>>` - Named parameter definitions
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            SheetsParam::definition(),
            RangeParam::definition(),
            HeaderParam::definition(),
            AnalyzeRowsParam::definition(),
            NullsParam::definition(),
            ErrorAsNullParam::definition(),
        ])
    }
}
