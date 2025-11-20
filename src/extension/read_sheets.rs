use crate::database::column::Column;
use crate::database::column::ColumnType;
use crate::database::table::Table;
use crate::error::ResultMessage;
use crate::error::RustySheetError;
use crate::extension::writer::write_to_vector;
use crate::extension::AnalyzeRowsParam;
use crate::extension::ColumnsParam;
use crate::extension::EndAtEmptyRowParam;
use crate::extension::ErrorAsNullParam;
use crate::extension::ExtensionError;
use crate::extension::FileNameColumnParam;
use crate::extension::FilesParam;
use crate::extension::HeaderParam;
use crate::extension::NamedParam;
use crate::extension::NullsParam;
use crate::extension::Param;
use crate::extension::Range;
use crate::extension::RangeParam;
use crate::extension::SheetNameColumnParam;
use crate::extension::SheetsParam;
use crate::extension::SkipEmptyRowsParam;
use crate::extension::UnionByNameParam;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::open_spreadsheets;
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
use std::collections::{HashMap, HashSet};
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
    /// Union sheets data by name (true) or position (false) (default: false)
    union_by_name: Option<bool>,
    /// Column type specifications with pattern matching
    columns: Option<Vec<(Pattern, ColumnType)>>,
    /// Number of rows to analyze for type detection
    analyze_rows: Option<usize>,
    /// null literals (default: empty string)
    nulls: Option<HashSet<String>>,
    /// Convert parsing errors to NULL values (default: false)
    error_as_null: Option<bool>,
    /// Skip rows with no data (default: false)
    skip_empty_rows: Option<bool>,
    /// Stop reading at first empty row (default: false)
    end_at_empty_row: Option<bool>,
    /// column name for file name of record
    file_name_column: Option<String>,
    /// column name for sheet name of record
    sheet_name_column: Option<String>,
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
            union_by_name: UnionByNameParam::read(bind)?,
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
/// Data structure for the bind phase of the read_sheets table function
pub(crate) struct ReadSheetsBindData {
    /// Shared string tables for string reference resolution & loaded sheet data from each spreadsheet
    spreadsheets: Vec<(Vec<String>, Vec<Sheet>, Vec<HashMap<usize, usize>>)>,
    /// Column definitions with names and types
    columns: Vec<Column>,
    /// file name column index
    file_name_column: Option<usize>,
    /// sheet name column index
    sheet_name_column: Option<usize>,
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
        let header = parameters.header.unwrap_or(true);
        let union_by_name = parameters.union_by_name.unwrap_or(false);
        let nulls = parameters.nulls.to_owned().unwrap_or(HashSet::from(["".to_string()]));
        let error_as_null = parameters.error_as_null.unwrap_or(false);
        let skip_empty_rows = parameters.skip_empty_rows.unwrap_or(false);
        let end_at_empty_row = parameters.end_at_empty_row.unwrap_or(false);
        let rows_limit = parameters.analyze_rows.or(Some(10));
        let default_preset_columns = vec![];
        let preset = parameters.columns.as_ref().unwrap_or(&default_preset_columns);

        let mut spreadsheets = Vec::new();
        let mut shared_tables = None::<Vec<Table>>;
        let mut columns = Vec::<Column>::new();
        let mut columns_indexes = HashMap::<String, usize>::new();
        for (spreadsheet, sheet_name_patterns) in open_spreadsheets(&parameters.files, &parameters.sheets)?.iter_mut() {
            let tables = spreadsheet.analyze_sheets(header, &Criteria {
                sheet_name_patterns: sheet_name_patterns.to_owned(),
                sheet_limit: None,
                range: parameters.range,
                rows_limit,
                nulls: nulls.to_owned(),
                error_as_null,
                skip_empty_rows,
                end_at_empty_row,
            }, preset)?;
            if tables.is_empty() {
                continue
            } else if !union_by_name && shared_tables.is_none() {
                shared_tables = Some(tables.clone());
            }

            let mut sheets = Vec::new();
            let mut sheets_columns_mappings = Vec::new();
            for actual_table in &tables {
                let table = if union_by_name {
                    actual_table
                } else {
                    shared_tables.as_ref()
                        .and_then(|tables| tables.get(0))
                        .expect("Shared tables")
                };

                let mut columns_mappings = HashMap::<usize, usize>::new();
                for (index, column) in table.columns.iter().enumerate() {
                    let column_index = if let Some(column_index) = columns_indexes.get(&column.name) {
                        let column_index = *column_index;
                        let expected_column = &columns[column_index];
                        if expected_column.kind != column.kind {
                            Err(ExtensionError::ColumnTypeError(
                                spreadsheet.name().to_owned(),
                                table.name.to_owned(),
                                column.name.to_owned(),
                                expected_column.kind,
                                column.kind,
                            ))?
                        }
                        column_index
                    } else {
                        let column_index = columns.len();
                        columns_indexes.insert(column.name.to_owned(), column_index);
                        columns.push(column.clone());
                        column_index
                    };
                    columns_mappings.insert(column_index, index);
                }
                sheets_columns_mappings.push(columns_mappings);

                let actual_sheets = spreadsheet.read_sheets(&Criteria {
                    sheet_name_patterns: Some(vec![Pattern::new(&actual_table.name)?]), // 用实际的工作表名称精准匹配目标工作表
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
                }).with_prefix(table.name.as_str()).with_prefix(spreadsheet.name().as_str())?;
                assert_eq!(actual_sheets.len(), 1);
                sheets.extend(actual_sheets);
            }

            let shared_strings = spreadsheet.load_shared_strings(None)
                .map(|(shared_strings, _)| shared_strings)
                .with_prefix(spreadsheet.name().as_str())?;
            spreadsheets.push((shared_strings, sheets, sheets_columns_mappings));
        }
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

        if spreadsheets.is_empty() {
            Err(ExtensionError::SheetNotFoundError)?;
        }

        Ok(ReadSheetsBindData {
            spreadsheets,
            columns,
            file_name_column,
            sheet_name_column,
        })
    }
}

#[repr(C)]
/// Data structure for the initialization phase of the read_sheets table function
pub(crate) struct ReadSheetsInitData {
    /// List of (sheet_index, chunk_index) pairs for iteration
    indexes: Vec<(usize, usize, usize)>,
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
        let mut indexes = Vec::<(usize, usize, usize)>::new();
        unsafe {
            for (spreadsheet_index, (_, sheets, _)) in (*bind).spreadsheets.iter().enumerate() {
                for (sheet_index, sheet) in sheets.iter().enumerate() {
                    for chunk_index in 0..sheet.chunks.len() {
                        indexes.push((spreadsheet_index, sheet_index, chunk_index));
                    }
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
            let (spreadsheet_index, sheet_index, chunk_index) = init.indexes[index];
            let (shared_strings, sheets, sheets_columns_mappings) = &bind.spreadsheets[spreadsheet_index];
            let sheet = &sheets[sheet_index];
            let columns_mappings = &sheets_columns_mappings[sheet_index];
            if let Some(table) = sheet.chunk(chunk_index) {
                output.set_len(table.len());
                for (row, record) in table.iter().enumerate() {
                    for (index, col) in init.projections.iter().enumerate() {
                        let vector = &mut vectors[index];
                        if bind.file_name_column.map(|column| column == *col).unwrap_or(false) {
                            vector.insert(row, sheet.file_name.as_str());
                        } else if bind.sheet_name_column.map(|column| column == *col).unwrap_or(false) {
                            vector.insert(row, sheet.name.as_str());
                        } else if let Some(column_index) = columns_mappings.get(col) {
                            if let Some(cell) = record[*column_index] {
                                let column = &bind.columns[*col];
                                write_to_vector(sheet, column, cell, vector, row, shared_strings)?;
                            } else {
                                vector.set_null(row);
                            }
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
        Some(vec![
            FilesParam::kind()
        ])
    }

    /// Defines the named parameters for this table function
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            SheetsParam::definition(),
            RangeParam::definition(),
            HeaderParam::definition(),
            UnionByNameParam::definition(),
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
