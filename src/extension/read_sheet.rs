use std::collections::HashSet;
use crate::database::column::Column;
use crate::database::column::ColumnType;
use crate::error::ResultMessage;
use crate::error::RustySheetError;
use crate::extension::writer::write_to_vector;
use crate::extension::AnalyzeRowsParam;
use crate::extension::ColumnsParam;
use crate::extension::EndAtEmptyRowParam;
use crate::extension::ErrorAsNullParam;
use crate::extension::ExtensionError;
use crate::extension::FileNameColumnParam;
use crate::extension::FileParam;
use crate::extension::HeaderParam;
use crate::extension::NamedParam;
use crate::extension::NullsParam;
use crate::extension::Param;
use crate::extension::Range;
use crate::extension::RangeParam;
use crate::extension::SheetNameColumnParam;
use crate::extension::SheetParam;
use crate::extension::SkipEmptyRowsParam;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::open_spreadsheet;
use crate::spreadsheet::sheet::Sheet;
use anyhow::Result;
use duckdb::core::DataChunkHandle;
use duckdb::core::Inserter;
use duckdb::core::LogicalTypeHandle;
use duckdb::vtab::BindInfo;
use duckdb::vtab::InitInfo;
use duckdb::vtab::TableFunctionInfo;
use duckdb::vtab::VTab;
use glob::Pattern;
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Parameters for reading a single sheet from a spreadsheet file.
struct ReadSheetParameters {
    /// Path to the spreadsheet file
    file_name: String,
    /// Optional pattern to match sheet names (supports glob patterns)
    sheet_name: Option<Pattern>,
    /// Optional range specification for data extraction
    range: Option<Range>,
    /// Whether the first row contains column headers (default: true)
    header: Option<bool>,
    /// Column specifications with patterns and types for type detection
    columns: Option<Vec<(Pattern, ColumnType)>>,
    /// Number of rows to analyze for automatic type detection
    analyze_rows: Option<usize>,
    /// null literals (default: empty string)
    nulls: Option<HashSet<String>>,
    /// Convert parsing errors to NULL values instead of failing
    error_as_null: Option<bool>,
    /// Skip rows that contain no data
    skip_empty_rows: Option<bool>,
    /// Stop reading when encountering an empty row
    end_at_empty_row: Option<bool>,
    /// column name for file name of record
    file_name_column: Option<String>,
    /// column name for sheet name of record
    sheet_name_column: Option<String>,
}

impl TryFrom<&BindInfo> for ReadSheetParameters {
    type Error = RustySheetError;

    /// Converts DuckDB bind information into structured read parameters.
    /// Extracts all named parameters from the SQL function call and validates them.
    fn try_from(bind: &BindInfo) -> Result<Self, Self::Error> {
        Ok(ReadSheetParameters {
            file_name: FileParam::read(bind, 0)?,
            sheet_name: SheetParam::read(bind)?,
            range: RangeParam::read(bind)?,
            header: HeaderParam::read(bind)?,
            columns: ColumnsParam::read(bind)?,
            analyze_rows: AnalyzeRowsParam::read(bind)?,
            nulls: NullsParam::read(bind)?,
            error_as_null: ErrorAsNullParam::read(bind)?,
            skip_empty_rows: SkipEmptyRowsParam::read(bind)?,
            end_at_empty_row: EndAtEmptyRowParam::read(bind)?,
            file_name_column: FileNameColumnParam::read(bind)?,
            sheet_name_column: SheetNameColumnParam::read(bind)?,
        })
    }
}

#[repr(C)]
/// Data structure that holds the binding information for a single sheet read operation.
/// This data is shared between the bind, init, and function execution phases.
pub(crate) struct ReadSheetBindData {
    /// Column definitions including names, types, and metadata
    columns: Vec<Column>,
    /// file name column index
    file_name_column: Option<usize>,
    /// sheet name column index
    sheet_name_column: Option<usize>,
    /// Loaded sheet data organized in chunks for efficient processing
    sheets: Vec<Sheet>,
    /// Shared string table for efficient string storage (XLSX/XLSB format)
    shared_strings: Vec<Option<String>>,
}

impl TryFrom<&ReadSheetParameters> for ReadSheetBindData {
    type Error = RustySheetError;

    /// Converts read parameters into bind data by analyzing and loading the spreadsheet.
    /// This performs the actual file parsing and prepares data for DuckDB consumption.
    fn try_from(parameters: &ReadSheetParameters) -> Result<Self, Self::Error> {
        // Prepare sheet name pattern for matching
        let sheet_name_pattern = parameters.sheet_name.as_ref().map(|pattern| vec![pattern.to_owned()]);

        // Open the spreadsheet file and load shared strings (for XLSX/XLSB formats)
        let mut spreadsheet = open_spreadsheet(&parameters.file_name)?;
        let (shared_strings, _) = spreadsheet.load_shared_strings(None)?;

        // Set default values for optional parameters
        let header = parameters.header.unwrap_or(true);
        let nulls = parameters.nulls.to_owned().unwrap_or(HashSet::from(["".to_string()]));
        let error_as_null = parameters.error_as_null.unwrap_or(false);
        let skip_empty_rows = parameters.skip_empty_rows.unwrap_or(false);
        let end_at_empty_row = parameters.end_at_empty_row.unwrap_or(false);

        // Analyze the sheet structure to determine column types and bounds
        let tables = spreadsheet.analyze_sheets(header, &Criteria {
            sheet_name_patterns: sheet_name_pattern.to_owned(),
            sheet_limit: Some(1),
            range: parameters.range,
            rows_limit: parameters.analyze_rows.or(Some(10)),
            nulls: nulls.to_owned(),
            error_as_null,
            skip_empty_rows,
            end_at_empty_row,
        }, parameters.columns.as_ref().unwrap_or(&vec![]))?;

        // Extract the first matching sheet or return error if no match found
        let table = tables.get(0).ok_or_else(|| ExtensionError::SheetWildcardError(
            spreadsheet.name().to_owned(),
            parameters.sheet_name.as_ref().map(|it| it.to_string()).unwrap_or(String::new()),
        ))?;
        let mut columns = table.columns.to_owned();
        let sheet_name_column = parameters.sheet_name_column.as_ref().map(|_| columns.len());
        if let Some(name) = &parameters.sheet_name_column {
            columns.push(Column {
                name: name.to_owned(),
                kind: ColumnType::Varchar,
            });
        }
        let file_name_column = parameters.file_name_column.as_ref().map(|_| columns.len());
        if let Some(name) = &parameters.file_name_column {
            columns.push(Column {
                name: name.to_owned(),
                kind: ColumnType::Varchar,
            });
        }

        // Read the actual data from the spreadsheet using the analyzed structure
        let sheets = spreadsheet.read_sheets(&Criteria {
            sheet_name_patterns: sheet_name_pattern.to_owned(),
            sheet_limit: Some(1),
            range: Some(Range {
                row_lower_bound: table.row_lower_bound,
                row_upper_bound: parameters.range.and_then(|it| it.row_upper_bound),
                col_lower_bound: Some(table.col_lower_bound),
                col_upper_bound: Some(table.col_upper_bound),
            }),
            rows_limit: None,
            nulls: nulls.to_owned(),
            error_as_null,
            skip_empty_rows,
            end_at_empty_row,
        })?;

        let shared_strings = shared_strings
            .into_iter()
            .map(|shared_string| {
                if !nulls.contains(&shared_string) {
                    Some(shared_string)
                } else {
                    None
                }
            })
            .collect();
        Ok(ReadSheetBindData {
            columns,
            file_name_column,
            sheet_name_column,
            sheets,
            shared_strings,
        })
    }
}

#[repr(C)]
/// Initialization data for the table function execution phase.
/// This tracks the current processing state and column projections.
pub(crate) struct ReadSheetInitData {
    /// Atomic counter tracking the current chunk being processed
    index: AtomicUsize,
    /// Column indices that should be projected (output) from the source data
    projections: Vec<usize>,
}

/// Main table function implementation for reading single sheets from spreadsheets.
/// This implements the DuckDB VTab trait to provide SQL table function capabilities.
pub(crate) struct ReadSheetTableFunction;

impl VTab for ReadSheetTableFunction {
    type InitData = ReadSheetInitData;
    type BindData = ReadSheetBindData;

    /// Binds the table function by parsing parameters and preparing data structures.
    /// This is called once per query to set up the function's execution context.
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let parameters = ReadSheetParameters::try_from(bind)?;
        let data = ReadSheetBindData::try_from(&parameters).with_prefix(parameters.file_name.as_str())?;
        // Register output columns with DuckDB
        for column in &data.columns {
            bind.add_result_column(column.name.as_str(), LogicalTypeHandle::from(column.kind.to_logical_type_id()));
        }
        Ok(data)
    }

    /// Initializes the table function for execution.
    /// This sets up the processing state and column projections for the current query.
    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let projections = init.get_column_indices()
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        Ok(ReadSheetInitData {
            index: AtomicUsize::new(0),
            projections,
        })
    }

    /// Executes the table function to produce data chunks.
    /// This is called repeatedly until all data has been processed.
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind = func.get_bind_data();
        let init = func.get_init_data();
        let sheet = &bind.sheets[0];
        let shared_strings = &bind.shared_strings;
        let index = init.index.fetch_add(1, Ordering::Relaxed);
        if index < sheet.chunks.len() {
            let mut vectors: Vec<_> = (0..init.projections.len()).map(|index| output.flat_vector(index)).collect();
            if let Some(table) = sheet.chunk(index) {
                output.set_len(table.len());
                for (row, record) in table.iter().enumerate() {
                    for (index, col) in init.projections.iter().enumerate() {
                        let vector = &mut vectors[index];
                        if bind.file_name_column.map(|column| column == *col).unwrap_or(false) {
                            vector.insert(row, sheet.file_name.as_str());
                        } else if bind.sheet_name_column.map(|column| column == *col).unwrap_or(false) {
                            vector.insert(row, sheet.name.as_str());
                        } else if let Some(cell) = record[*col] {
                            let column = &bind.columns[*col];
                            write_to_vector(sheet, column, cell, vector, row, shared_strings)?;
                        } else {
                            vector.set_null(row);
                        }
                    }
                }
            } else {
                output.set_len(0);
            }
        } else {
            // No more data to process
            output.set_len(0);
        }
        Ok(())
    }

    /// Indicates whether this table function supports filter pushdown.
    /// Returns true to enable DuckDB's optimization capabilities.
    fn supports_pushdown() -> bool {
        true
    }

    /// Defines the required positional parameters for the table function.
    /// The first parameter is always the file name/path.
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            FileParam::kind(),
        ])
    }

    /// Defines the optional named parameters for the table function.
    /// These provide fine-grained control over the reading behavior.
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            SheetParam::definition(),
            RangeParam::definition(),
            HeaderParam::definition(),
            ColumnsParam::definition(),
            AnalyzeRowsParam::definition(),
            NullsParam::definition(),
            ErrorAsNullParam::definition(),
            SkipEmptyRowsParam::definition(),
            EndAtEmptyRowParam::definition(),
            FileNameColumnParam::definition(),
            SheetNameColumnParam::definition(),
        ])
    }
}
