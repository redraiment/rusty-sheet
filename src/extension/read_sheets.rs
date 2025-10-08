use crate::database::column::Column;
use crate::database::column::ColumnType;
use crate::error::RustySheetError;
use crate::extension::writer::write_to_vector;
use crate::extension::AnalyzeRowsParam;
use crate::extension::ColumnsParam;
use crate::extension::EndAtEmptyRowParam;
use crate::extension::ErrorAsNullParam;
use crate::extension::ExtensionError;
use crate::extension::FilesParam;
use crate::extension::HeaderParam;
use crate::extension::NamedParam;
use crate::extension::Param;
use crate::extension::Range;
use crate::extension::RangeParam;
use crate::extension::SheetsParam;
use crate::extension::SkipEmptyRowsParam;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::open_spreadsheets;
use crate::spreadsheet::sheet::Sheet;
use anyhow::Context;
use anyhow::Result;
use duckdb::core::DataChunkHandle;
use duckdb::core::LogicalTypeHandle;
use duckdb::core::LogicalTypeId;
use duckdb::vtab::BindInfo;
use duckdb::vtab::InitInfo;
use duckdb::vtab::TableFunctionInfo;
use duckdb::vtab::VTab;
use glob::Pattern;
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Parameters for the read_sheets table function
struct ReadSheetsParameters {
    /// List of spreadsheet file paths to read
    files: Vec<String>,
    /// Optional sheet name patterns to filter which sheets to read
    sheets: Option<Vec<(Option<Pattern>, Pattern)>>,
    /// Optional range specification for data extraction
    range: Option<Range>,
    /// Whether to treat first row as header (default: true)
    header: Option<bool>,
    /// Column type specifications with pattern matching
    columns: Option<Vec<(Pattern, ColumnType)>>,
    /// Number of rows to analyze for type detection
    analyze_rows: Option<usize>,
    /// Convert parsing errors to NULL values (default: false)
    error_as_null: Option<bool>,
    /// Skip rows with no data (default: false)
    skip_empty_rows: Option<bool>,
    /// Stop reading at first empty row (default: false)
    end_at_empty_row: Option<bool>,
}

impl TryFrom<&BindInfo> for ReadSheetsParameters {
    type Error = RustySheetError;

    /// Construct ReadSheetsParameters from DuckDB bind information
    ///
    /// # Arguments
    /// * `bind` - DuckDB bind information containing function parameters
    ///
    /// # Returns
    /// * `Result<Self, Self::Error>` - Parameters or error if parsing fails
    fn try_from(bind: &BindInfo) -> Result<Self, Self::Error> {
        Ok(ReadSheetsParameters {
            files: FilesParam::read(bind, 0)?,
            sheets: SheetsParam::read(bind)?,
            range: RangeParam::read(bind)?,
            header: HeaderParam::read(bind)?,
            columns: ColumnsParam::read(bind)?,
            analyze_rows: AnalyzeRowsParam::read(bind)?,
            error_as_null: ErrorAsNullParam::read(bind)?,
            skip_empty_rows: SkipEmptyRowsParam::read(bind)?,
            end_at_empty_row: EndAtEmptyRowParam::read(bind)?,
        })
    }
}

#[repr(C)]
/// Data structure for the bind phase of the read_sheets table function
pub(crate) struct ReadSheetsBindData {
    /// Column definitions with names and types
    columns: Vec<Column>,
    /// Loaded sheet data from all spreadsheets
    sheets: Vec<Sheet>,
    /// Mapping from sheet index to original spreadsheet index
    indexes: Vec<usize>,
    /// Shared string tables from each spreadsheet for string reference resolution
    shared_strings: Vec<Vec<String>>,
}

impl TryFrom<&ReadSheetsParameters> for ReadSheetsBindData {
    type Error = RustySheetError;

    /// Construct ReadSheetsBindData from function parameters
    ///
    /// This method performs the heavy lifting of:
    /// - Opening and analyzing spreadsheets
    /// - Loading shared strings
    /// - Setting up column definitions
    /// - Preparing sheet data for iteration
    ///
    /// # Arguments
    /// * `parameters` - Function parameters from user input
    ///
    /// # Returns
    /// * `Result<Self, Self::Error>` - Bind data or error if processing fails
    fn try_from(parameters: &ReadSheetsParameters) -> Result<Self, Self::Error> {
        let mut spreadsheets = open_spreadsheets(&parameters.files, &parameters.sheets)?;
        let shared_strings = spreadsheets.iter_mut().map(|(spreadsheet, _)| {
            spreadsheet.load_shared_strings(None)
                .map(|(shared_strings, _)| shared_strings)
                .with_context(|| spreadsheet.name())
        }).collect::<Result<Vec<_>, _>>()?;

        let header = parameters.header.unwrap_or(true);
        let error_as_null = parameters.error_as_null.unwrap_or(false);
        let skip_empty_rows = parameters.skip_empty_rows.unwrap_or(false);
        let end_at_empty_row = parameters.end_at_empty_row.unwrap_or(false);
        let (spreadsheet, sheet_name_patterns) = &mut spreadsheets[0];
        let tables = spreadsheet.analyze_sheets(header, &Criteria {
            sheet_name_patterns: sheet_name_patterns.to_owned(),
            sheet_limit: Some(1),
            range: parameters.range,
            rows_limit: parameters.analyze_rows.or(Some(10)),
            error_as_null,
            skip_empty_rows,
            end_at_empty_row,
        }, parameters.columns.as_ref().unwrap_or(&vec![])).with_context(|| spreadsheet.name())?;
        let table = tables.get(0).ok_or_else(|| ExtensionError::SheetWildcardError(
            spreadsheet.name().to_owned(),
            sheet_name_patterns.as_ref().map(|patterns| patterns.iter().map(|it| it.to_string()).collect::<Vec<String>>().join(", ")).unwrap_or(String::new()),
        ))?;

        let sheets = spreadsheets.into_iter().map(|(mut spreadsheet, sheet_name_patterns)| {
            spreadsheet.read_sheets(&Criteria {
                sheet_name_patterns,
                sheet_limit: None,
                range: Some(Range {
                    row_lower_bound: table.row_lower_bound,
                    row_upper_bound: parameters.range.and_then(|it| it.row_upper_bound),
                    col_lower_bound: Some(table.col_lower_bound),
                    col_upper_bound: Some(table.col_upper_bound),
                }),
                rows_limit: None,
                error_as_null,
                skip_empty_rows,
                end_at_empty_row,
            }).with_context(|| spreadsheet.name())
        }).collect::<Result<Vec<_>, _>>()?;

        let indexes = sheets.iter().enumerate().flat_map(|(index, sheets)| {
            vec![index; sheets.len()]
        }).collect();

        Ok(ReadSheetsBindData {
            columns: table.columns.to_owned(),
            sheets: sheets.into_iter().flatten().collect(),
            indexes,
            shared_strings,
        })
    }
}

#[repr(C)]
/// Data structure for the initialization phase of the read_sheets table function
pub(crate) struct ReadSheetsInitData {
    /// List of (sheet_index, chunk_index) pairs for iteration
    indexes: Vec<(usize, usize)>,
    /// Atomic counter tracking current iteration position
    index: AtomicUsize,
    /// Column projection indices for selective column reading
    projections: Vec<usize>,
}

/// DuckDB table function for reading multiple sheets from spreadsheet files
pub(crate) struct ReadSheetsTableFunction;

impl VTab for ReadSheetsTableFunction {
    type InitData = ReadSheetsInitData;
    type BindData = ReadSheetsBindData;

    /// Bind phase: parse parameters and prepare data structures
    ///
    /// # Arguments
    /// * `bind` - DuckDB bind information
    ///
    /// # Returns
    /// * `Result<Self::BindData, Box<dyn Error>>` - Prepared bind data
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let parameters = ReadSheetsParameters::try_from(bind)?;
        let data = ReadSheetsBindData::try_from(&parameters)?;
        // Register output columns with DuckDB
        for column in &data.columns {
            bind.add_result_column(column.name.as_str(), LogicalTypeHandle::from(column.kind.to_logical_type_id()));
        }
        Ok(data)
    }

    /// Initialize phase: prepare iteration state and projections
    ///
    /// # Arguments
    /// * `init` - DuckDB initialization information
    ///
    /// # Returns
    /// * `Result<Self::InitData, Box<dyn Error>>` - Initialized iteration state
    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind: *const Self::BindData = init.get_bind_data();
        let mut indexes = Vec::<(usize, usize)>::new();
        unsafe {
            for (sheet_index, sheet) in (*bind).sheets.iter().enumerate() {
                for chunk_index in 0..sheet.chunks.len() {
                    indexes.push((sheet_index, chunk_index));
                }
            }
        };
        let projections = init.get_column_indices()
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        Ok(ReadSheetsInitData {
            indexes,
            index: AtomicUsize::new(0),
            projections,
        })
    }

    /// Function phase: stream data chunks to DuckDB
    ///
    /// This method is called repeatedly to populate the output data chunk
    /// with spreadsheet data until all data has been processed.
    ///
    /// # Arguments
    /// * `func` - Table function information
    /// * `output` - Output data chunk to populate
    ///
    /// # Returns
    /// * `Result<(), Box<dyn Error>>` - Success or error during data streaming
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind = func.get_bind_data();
        let init = func.get_init_data();
        let index = init.index.fetch_add(1, Ordering::Relaxed);
        if index < init.indexes.len() {
            let mut vectors: Vec<_> = (0..init.projections.len()).map(|index| output.flat_vector(index)).collect();
            let (sheet_index, chunk_index) = init.indexes[index];
            let sheet = &bind.sheets[sheet_index];
            let spreadsheet_index = bind.indexes[sheet_index];
            let shared_strings = &bind.shared_strings[spreadsheet_index];

            if let Some(table) = sheet.chunk(chunk_index) {
                output.set_len(table.len());
                for (row, record) in table.iter().enumerate() {
                    for (index, col) in init.projections.iter().enumerate() {
                        let vector = &mut vectors[index];
                        if let Some(cell) = record[*col] {
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

    /// Indicates whether this table function supports predicate pushdown
    fn supports_pushdown() -> bool {
        true
    }

    /// Defines the required positional parameters for this table function
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    /// Defines the named parameters for this table function
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            SheetsParam::definition(),
            RangeParam::definition(),
            HeaderParam::definition(),
            ColumnsParam::definition(),
            AnalyzeRowsParam::definition(),
            ErrorAsNullParam::definition(),
            SkipEmptyRowsParam::definition(),
            EndAtEmptyRowParam::definition(),
        ])
    }
}
