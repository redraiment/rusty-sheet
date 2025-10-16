//! Extension module containing DuckDB table function implementations.
//! Provides functions for reading and analyzing spreadsheet files.

pub(crate) mod analyze_sheet;
pub(crate) mod analyze_sheets;
pub(crate) mod read_sheet;
pub(crate) mod read_sheets;
mod writer;

use crate::database::bridge::ValueBridge;
use crate::database::column::ColumnType;
use crate::database::range::Range;
use crate::error::RustySheetError;
use duckdb::core::LogicalTypeHandle;
use duckdb::core::LogicalTypeId;
use duckdb::vtab::BindInfo;
use duckdb::vtab::Value;
use glob::glob;
use glob::Pattern;
use thiserror::Error;

/// Errors specific to extension parameter processing and validation.
#[derive(Error, Debug)]
pub(crate) enum ExtensionError {
    #[error("No files matched wildcard '{0}'")]
    FileWildcardError(String),

    #[error("No worksheets matched the wildcard pattern in any of the files")]
    SheetNotFoundError,

    #[error("Spreadsheet '{0}': no sheets matched wildcard '{1}'")]
    SheetWildcardError(String, String),

    #[error("[{0}]{1}!{2}: expected {3:?}, actual {4:?}")]
    ColumnTypeError(String, String, String, ColumnType, ColumnType),
}

/// Trait for reading positional parameters from DuckDB bind info.
pub(crate) trait Param<T> {

    /// Returns the DuckDB logical type for this parameter.
    fn kind() -> LogicalTypeHandle;

    /// Reads a positional parameter at the specified index.
    fn read(bind: &BindInfo, index: u64) -> Result<T, RustySheetError>;
}

/// Trait for reading named parameters from DuckDB bind info.
pub(crate) trait NamedParam<T> {
    /// Returns the parameter name as used in SQL queries.
    fn name() -> &'static str;

    /// Returns the DuckDB logical type for this parameter.
    fn kind() -> LogicalTypeHandle;

    /// Returns the parameter definition tuple (name, type) for DuckDB registration.
    fn definition() -> (String, LogicalTypeHandle) {
        (Self::name().to_string(), Self::kind())
    }

    /// Reads the named parameter from bind info, returning None if not provided.
    fn read(bind: &BindInfo) -> Result<Option<T>, RustySheetError> {
        if let Some(value) = bind.get_named_parameter(Self::name()) {
            let value = Self::cast(value)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Converts a DuckDB Value to the parameter's target type.
    fn cast(value: Value) -> Result<T, RustySheetError>;
}

struct FileParam;
struct FilesParam;
struct SheetParam;
struct SheetsParam;
struct RangeParam;
struct HeaderParam;
struct UnionByNameParam;
struct ColumnsParam;
struct AnalyzeRowsParam;
struct ErrorAsNullParam;
struct SkipEmptyRowsParam;
struct EndAtEmptyRowParam;
struct FileNameColumnParam;
struct SheetNameColumnParam;

/// Parameter handler for file name (positional parameter).
impl Param<String> for FileParam {
    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Varchar)
    }

    fn read(bind: &BindInfo, index: u64) -> Result<String, RustySheetError> {
        let value = bind.get_parameter(index);
        Ok(value.to_string())
    }

}

/// Parameter handler for file patterns with glob expansion.
impl Param<Vec<String>> for FilesParam {
    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar))
    }

    fn read(bind: &BindInfo, index: u64) -> Result<Vec<String>, RustySheetError> {
        let wildcards = bind.get_parameter(index)
            .to_list()
            .iter()
            .map(|parameter| parameter.to_string())
            .collect::<Vec<_>>();

        let files = wildcards.iter()
            .map(|wildcard| glob(wildcard))
            .filter_map(Result::ok)
            .flat_map(|paths| paths.filter_map(Result::ok))
            .map(|path| path.to_str().unwrap().to_string())
            .collect::<Vec<_>>();
        if files.is_empty() {
            Err(ExtensionError::FileWildcardError(wildcards.join(", ")))?
        }
        Ok(files)
    }
}

/// Parameter handler for sheet name pattern matching.
impl NamedParam<Pattern> for SheetParam {
    fn name() -> &'static str {
        "sheet"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Varchar)
    }

    fn cast(value: Value) -> Result<Pattern, RustySheetError> {
        let sheet_name = value.to_string();
        let pattern = Pattern::new(&sheet_name)?;
        Ok(pattern)
    }
}

/// Parameter handler for multiple sheet specifications with file filtering.
impl NamedParam<Vec<(Option<Pattern>, Pattern)>> for SheetsParam {
    fn name() -> &'static str {
        "sheets"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar))
    }

    fn cast(value: Value) -> Result<Vec<(Option<Pattern>, Pattern)>, RustySheetError> {
        value
            .to_list()
            .iter()
            .map(Value::to_string)
            .map(parse_sheet)
            .collect()
    }
}

/// Parameter handler for Excel-style range specifications.
impl NamedParam<Range> for RangeParam {
    fn name() -> &'static str {
        "range"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Varchar)
    }

    fn cast(value: Value) -> Result<Range, RustySheetError> {
        Range::try_from(value.to_varchar().as_str())
    }
}

/// Parameter handler for header row presence flag.
impl NamedParam<bool> for HeaderParam {
    fn name() -> &'static str {
        "header"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Boolean)
    }

    fn cast(value: Value) -> Result<bool, RustySheetError> {
        Ok(value.to_bool())
    }
}

/// Parameter handler for union by name flag.
impl NamedParam<bool> for UnionByNameParam {
    fn name() -> &'static str {
        "union_by_name"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Boolean)
    }

    fn cast(value: Value) -> Result<bool, RustySheetError> {
        Ok(value.to_bool())
    }
}

/// Parameter handler for column type overrides.
impl NamedParam<Vec<(Pattern, ColumnType)>> for ColumnsParam {
    fn name() -> &'static str {
        "columns"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::map(
            &LogicalTypeHandle::from(LogicalTypeId::Varchar),
            &LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )
    }

    fn cast(value: Value) -> Result<Vec<(Pattern, ColumnType)>, RustySheetError> {
        let columns = value
            .to_map_entries()
            .iter()
            .map(|(key, value)| parse_column(key, value))
            .collect::<Result<Vec<(Pattern, ColumnType)>, RustySheetError>>()?;
        Ok(columns)
    }
}

/// Parameter handler for number of rows to analyze for type detection.
impl NamedParam<usize> for AnalyzeRowsParam {
    fn name() -> &'static str {
        "analyze_rows"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::UInteger)
    }

    fn cast(value: Value) -> Result<usize, RustySheetError> {
        Ok(value.to_usize())
    }
}

/// Parameter handler for error handling behavior (fail-fast vs null conversion).
impl NamedParam<bool> for ErrorAsNullParam {
    fn name() -> &'static str {
        "error_as_null"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Boolean)
    }

    fn cast(value: Value) -> Result<bool, RustySheetError> {
        Ok(value.to_bool())
    }
}

/// Parameter handler for skipping empty rows during processing.
impl NamedParam<bool> for SkipEmptyRowsParam {
    fn name() -> &'static str {
        "skip_empty_rows"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Boolean)
    }

    fn cast(value: Value) -> Result<bool, RustySheetError> {
        Ok(value.to_bool())
    }
}

/// Parameter handler for stopping data extraction at first empty row.
impl NamedParam<bool> for EndAtEmptyRowParam {
    fn name() -> &'static str {
        "end_at_empty_row"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Boolean)
    }

    fn cast(value: Value) -> Result<bool, RustySheetError> {
        Ok(value.to_bool())
    }
}

impl NamedParam<String> for FileNameColumnParam {
    fn name() -> &'static str {
        "file_name_column"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Varchar)
    }

    fn cast(value: Value) -> Result<String, RustySheetError> {
        Ok(value.to_string())
    }
}

impl NamedParam<String> for SheetNameColumnParam {
    fn name() -> &'static str {
        "sheet_name_column"
    }

    fn kind() -> LogicalTypeHandle {
        LogicalTypeHandle::from(LogicalTypeId::Varchar)
    }

    fn cast(value: Value) -> Result<String, RustySheetError> {
        Ok(value.to_string())
    }
}

/// Parses a sheet specification string in format "filename_pattern=sheet_pattern" or "sheet_pattern".
fn parse_sheet(value: String) -> Result<(Option<Pattern>, Pattern), RustySheetError> {
    let (file_name_wildcard, sheet_name_wildcard) = if let Some(index) = value.find('=') {
        (Some(value[..index].trim()), value[index + 1..].trim())
    } else {
        (None, value.as_str())
    };
    let file_name_pattern = file_name_wildcard
        .map(|wildcard| Pattern::new(wildcard))
        .transpose()?;
    let sheet_name_pattern = Pattern::new(sheet_name_wildcard)?;
    Ok((file_name_pattern, sheet_name_pattern))
}

/// Parses a column specification from map entries (name -> type).
fn parse_column(name: &Value, kind: &Value) -> Result<(Pattern, ColumnType), RustySheetError> {
    let name = name.to_string();
    let kind = kind.to_string();
    Ok((Pattern::new(&name)?, ColumnType::parse(&kind)?))
}
