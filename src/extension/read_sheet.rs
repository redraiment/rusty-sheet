use crate::database::column::Column;
use crate::database::column::ColumnType;
use crate::error::ResultMessage;
use crate::error::RustySheetError;
use crate::extension::SpreadMergedCellsParam;
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
use crate::spreadsheet::SheetBatch;
use anyhow::Result;
use duckdb::core::DataChunkHandle;
use duckdb::core::Inserter;
use duckdb::core::LogicalTypeHandle;
use duckdb::vtab::BindInfo;
use duckdb::vtab::InitInfo;
use duckdb::vtab::TableFunctionInfo;
use duckdb::vtab::VTab;
use glob::Pattern;
use std::any::Any;
use std::collections::HashSet;
use std::error::Error;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::sync::Mutex;

type StreamResult = Result<SheetBatch, String>;

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
    /// Whether to spread merged cells across merged ranges (default: false)
    spread_merged_cells: Option<bool>,
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
            spread_merged_cells: SpreadMergedCellsParam::read(bind)?,
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
    /// Spreadsheet path reopened by the scan worker after binding.
    file_name: String,
    /// Criteria for the full scan after the bounded schema sample.
    criteria: Criteria,
}

impl TryFrom<&ReadSheetParameters> for ReadSheetBindData {
    type Error = RustySheetError;

    /// Converts read parameters into bind data by analyzing and loading the spreadsheet.
    /// This performs the actual file parsing and prepares data for DuckDB consumption.
    fn try_from(parameters: &ReadSheetParameters) -> Result<Self, Self::Error> {
        // Prepare sheet name pattern for matching
        let sheet_name_pattern = parameters.sheet_name.as_ref().map(|pattern| vec![pattern.to_owned()]);

        // Open the spreadsheet file for the bounded schema sample.
        let mut spreadsheet = open_spreadsheet(&parameters.file_name)?;

        // Set default values for optional parameters
        let header = parameters.header.unwrap_or(true);
        let nulls = parameters.nulls.to_owned().unwrap_or(HashSet::from(["".to_string()]));
        let error_as_null = parameters.error_as_null.unwrap_or(false);
        let skip_empty_rows = parameters.skip_empty_rows.unwrap_or(false);
        let end_at_empty_row = parameters.end_at_empty_row.unwrap_or(false);
        let spread_merged_cells = parameters.spread_merged_cells.unwrap_or(false);
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
            spread_merged_cells,
        }, parameters.columns.as_ref().unwrap_or(&vec![]))?;

        // Extract the first matching sheet or return error if no match found
        let table = tables.first().ok_or_else(|| ExtensionError::SheetWildcardError(
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

        let criteria = Criteria {
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
            spread_merged_cells,
        };
        Ok(ReadSheetBindData {
            columns,
            file_name_column,
            sheet_name_column,
            file_name: parameters.file_name.clone(),
            criteria,
        })
    }
}

#[repr(C)]
/// Initialization data for the table function execution phase.
/// This tracks the current processing state and column projections.
pub(crate) struct ReadSheetInitData {
    /// Bounded queue fed by the spreadsheet parser.
    receiver: Mutex<Receiver<StreamResult>>,
    /// Column indices that should be projected (output) from the source data
    projections: Vec<usize>,
}

fn run_stream_worker<F>(sender: SyncSender<StreamResult>, stream: F)
where
    F: FnOnce(&SyncSender<StreamResult>) -> Result<(), RustySheetError>,
{
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stream(&sender)));
    match result {
        Ok(Ok(())) => (),
        Ok(Err(error)) => {
            let _ = sender.send(Err(error.to_string()));
        }
        Err(payload) => {
            let _ = sender.send(Err(format!(
                "spreadsheet stream panicked: {}",
                panic_payload_message(payload.as_ref())
            )));
        }
    }
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
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
        let bind: *const Self::BindData = init.get_bind_data();
        let (file_name, criteria) = unsafe {
            ((*bind).file_name.clone(), (*bind).criteria.clone())
        };
        let projections = init.get_column_indices()
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        let (sender, receiver) = sync_channel(2);
        std::thread::spawn(move || {
            run_stream_worker(sender, |sender| {
                open_spreadsheet(&file_name).and_then(|mut spreadsheet| {
                    spreadsheet.stream_sheets(&criteria, &mut |batch| sender.send(Ok(batch)).is_ok())
                })
            });
        });
        Ok(ReadSheetInitData {
            receiver: Mutex::new(receiver),
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
        let batch = init.receiver
            .lock()
            .map_err(|_| std::io::Error::other("spreadsheet stream lock poisoned"))?
            .recv();
        let batch = match batch {
            Ok(Ok(batch)) => batch,
            Ok(Err(error)) => return Err(std::io::Error::other(error).into()),
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };
        let sheet = &batch.sheet;
        let mut vectors: Vec<_> = (0..init.projections.len()).map(|index| output.flat_vector(index)).collect();
        if let Some(table) = sheet.chunk(0) {
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
                        write_to_vector(sheet, column, cell, vector, row, batch.shared_strings.as_ref())?;
                    } else {
                        vector.set_null(row);
                    }
                }
            }
        } else {
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
            SpreadMergedCellsParam::definition(),
            FileNameColumnParam::definition(),
            SheetNameColumnParam::definition(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_worker_sends_parser_errors() {
        let (sender, receiver) = sync_channel(1);

        run_stream_worker(sender, |_| {
            Err(RustySheetError::WithContextError("bad workbook".to_string()))
        });

        assert_eq!(receive_error(receiver), "bad workbook");
    }

    #[test]
    fn stream_worker_sends_panic_errors() {
        let (sender, receiver) = sync_channel(1);

        run_stream_worker(sender, |_| panic!("bad workbook"));

        assert_eq!(receive_error(receiver), "spreadsheet stream panicked: bad workbook");
    }

    fn receive_error(receiver: Receiver<StreamResult>) -> String {
        match receiver.recv().unwrap() {
            Ok(_) => panic!("expected stream error"),
            Err(error) => error,
        }
    }
}
