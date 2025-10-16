use crate::error::ResultMessage;
use crate::error::RustySheetError;
use crate::extension::AnalyzeRowsParam;
use crate::extension::ErrorAsNullParam;
use crate::extension::FileParam;
use crate::extension::HeaderParam;
use crate::extension::NamedParam;
use crate::extension::Param;
use crate::extension::Range;
use crate::extension::RangeParam;
use crate::extension::SheetParam;
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
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Parameters for the analyze_sheet table function
struct AnalyzeSheetParameters {
    /// Path to the spreadsheet file
    file_name: String,
    /// Optional sheet name pattern to filter sheets
    sheet_name: Option<Pattern>,
    /// Optional range specification (e.g., "A1:D10")
    range: Option<Range>,
    /// Whether to treat first row as header (default: true)
    header: Option<bool>,
    /// Number of rows to analyze for type detection (default: 10)
    analyze_rows: Option<usize>,
    /// Whether to convert errors to null values (default: false)
    error_as_null: Option<bool>,
}

impl TryFrom<&BindInfo> for AnalyzeSheetParameters {
    type Error = RustySheetError;

    /// Parse parameters from DuckDB bind info
    fn try_from(bind: &BindInfo) -> Result<Self, Self::Error> {
        Ok(AnalyzeSheetParameters {
            file_name: FileParam::read(bind, 0)?,
            sheet_name: SheetParam::read(bind)?,
            range: RangeParam::read(bind)?,
            header: HeaderParam::read(bind)?,
            analyze_rows: AnalyzeRowsParam::read(bind)?,
            error_as_null: ErrorAsNullParam::read(bind)?,
        })
    }
}

#[repr(C)]
/// Bind data for the analyze_sheet table function containing column analysis results
pub(crate) struct AnalyzeSheetBindData {
    /// Vector of (column_name, column_type) pairs from analyzed sheets
    columns: Vec<(String, String)>,
}

impl TryFrom<&AnalyzeSheetParameters> for AnalyzeSheetBindData {
    type Error = RustySheetError;

    /// Analyze spreadsheet and extract column metadata
    fn try_from(parameters: &AnalyzeSheetParameters) -> Result<Self, Self::Error> {
        let mut columns = Vec::<(String, String)>::new();
        let mut spreadsheet = open_spreadsheet(parameters.file_name.as_str())?;
        let sheet_name_patterns = parameters.sheet_name
            .as_ref()
            .map(|pattern| vec![pattern.to_owned()]);
        let header = parameters.header.unwrap_or(true);
        for table in spreadsheet.analyze_sheets(header, &Criteria {
            sheet_name_patterns,
            sheet_limit: Some(1),
            range: parameters.range,
            rows_limit: parameters.analyze_rows.or(Some(10)),
            error_as_null: parameters.error_as_null.unwrap_or(false),
            skip_empty_rows: false,
            end_at_empty_row: false,
        }, &Vec::new())? {
            for column in &table.columns {
                columns.push((
                    column.name.to_owned(),
                    column.kind.as_str().to_owned(),
                ));
            }
        }
        Ok(AnalyzeSheetBindData { columns })
    }
}

#[repr(C)]
/// Init data for the analyze_sheet table function tracking iteration state
pub(crate) struct AnalyzeSheetInitData {
    /// Atomic counter tracking the current processing index
    index: AtomicUsize,
}

/// Table function implementation for analyzing spreadsheet column structure
pub(crate) struct AnalyzeSheetTableFunction;

impl VTab for AnalyzeSheetTableFunction {
    type InitData = AnalyzeSheetInitData;
    type BindData = AnalyzeSheetBindData;

    /// Bind phase: parse parameters, analyze spreadsheet, and define result columns
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let parameters = AnalyzeSheetParameters::try_from(bind)?;
        let data = AnalyzeSheetBindData::try_from(&parameters).with_prefix(parameters.file_name.as_str())?;
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

    /// Init phase: initialize iteration state
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(AnalyzeSheetInitData {
            index: AtomicUsize::new(0),
        })
    }

    /// Function phase: stream column analysis results to DuckDB
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init = func.get_init_data();
        let bind = func.get_bind_data();
        let lower = init.index.fetch_add(2048, Ordering::Relaxed);
        let upper = bind.columns.len().min(lower + 2048);
        if lower < upper {
            let columns = output.flat_vector(0);
            let kinds = output.flat_vector(1);
            for index in lower..upper {
                let (column_name, kind_name) = &bind.columns[index];
                columns.insert(index - lower, column_name);
                kinds.insert(index - lower, kind_name);
            }
            output.set_len(upper - lower);
        } else {
            output.set_len(0);
        }
        Ok(())
    }

    /// Define required positional parameters (file path)
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            FileParam::kind(),
        ])
    }

    /// Define optional named parameters for advanced configuration
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            SheetParam::definition(),
            RangeParam::definition(),
            HeaderParam::definition(),
            AnalyzeRowsParam::definition(),
            ErrorAsNullParam::definition(),
        ])
    }
}
