use crate::database::column::Column;
use crate::database::column::ColumnType;
use crate::database::table::Table;
use crate::error::ResultMessage;
use crate::error::RustySheetError;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::ods::OdsSpreadsheet;
use crate::spreadsheet::reference::index_to_col;
use crate::spreadsheet::xls::XlsSpreadsheet;
use crate::spreadsheet::xlsb::XlsbSpreadsheet;
use crate::spreadsheet::xlsx::XlsxSpreadsheet;
use criteria::Criteria;
use glob::Pattern;
use sheet::Sheet;
use std::collections::HashMap;
use std::collections::HashSet;
use thiserror::Error;

pub(crate) mod cell;
pub(crate) mod ods;
pub(crate) mod excel;
pub(crate) mod reference;
pub(crate) mod xls;
pub(crate) mod xlsb;
pub(crate) mod xlsx;
pub(crate) mod criteria;
pub(crate) mod sheet;

#[derive(Error, Debug)]
pub(crate) enum SpreadsheetError {
    /// Error indicating the spreadsheet format is not supported
    #[error("Spreadsheet '{0}': unsupported format")]
    SpreadsheetFormatError(String),

    /// Error indicating the spreadsheet is password protected
    #[error("Spreadsheet '{0}': password protected")]
    SpreadsheetPasswordProtectedError(String),

    /// Error indicating the spreadsheet contains no data
    #[error("Spreadsheet '{0}': empty")]
    SpreadsheetEmptyError(String),

    /// Error indicating the file is missing or corrupted
    #[error("file '{0}' is missing or corrupted.")]
    FileError(String),

    /// Error indicating a specific cell value is invalid
    #[error("Cell '[{0}]{1}!{2}': {3}")]
    CellValueError(String, String, String, String),
}

pub(crate) trait Spreadsheet {
    /// Returns the name of the spreadsheet file
    fn name(&self) -> String;

    /// Loads shared strings from the spreadsheet
    ///
    /// If indexes are provided, only loads strings at those indices;
    /// otherwise loads all shared strings.
    /// Returns a tuple of (strings, index_mappings) where index_mappings
    /// maps original string indices to their positions in the returned vector.
    fn load_shared_strings(
        &mut self,
        indexes: Option<HashSet<usize>>,
    ) -> Result<(Vec<String>, HashMap<usize, usize>), RustySheetError>;

    /// Analyzes data within specified worksheet ranges
    ///
    /// Processes sheets according to criteria and detects column types
    /// automatically. Supports header detection and type presets.
    fn analyze_sheets(&mut self, has_header: bool, criteria: &Criteria, presets: &Vec<(Pattern, ColumnType)>) -> Result<Vec<Table>, RustySheetError> {
        let mut shared_indexes = HashSet::<usize>::new();
        let mut sheets = Vec::<(String, Vec<Option<Cell>>, Vec<ColumnType>, Option<usize>, usize, usize)>::new();
        for sheet in self.read_sheets(criteria)? {
            let row_lower_bound = criteria.range.and_then(|it| it.row_lower_bound).or(sheet.row_lower_bound);
            let col_lower_bound = criteria.range.and_then(|it| it.col_lower_bound).or(sheet.col_lower_bound);
            let col_upper_bound = criteria.range.and_then(|it| it.col_upper_bound).or(sheet.col_upper_bound);
            if (has_header && sheet.is_empty()) || (!has_header && (col_lower_bound.is_none() || col_upper_bound.is_none())) {
                continue; // 忽略空工作表
            }

            let col_lower_bound = col_lower_bound.unwrap();
            let col_upper_bound = col_upper_bound.unwrap();
            let mut header = vec![None::<Cell>; col_upper_bound - col_lower_bound + 1];
            let mut data = vec![Vec::<Cell>::new(); col_upper_bound - col_lower_bound + 1];

            for cell in &sheet.cells {
                if cell.kind == CellType::SharedString {
                    shared_indexes.insert(cell.value.parse::<usize>()?);
                }
                let index = cell.col - col_lower_bound;
                if row_lower_bound.map(|row_lower_bound| has_header && row_lower_bound == cell.row).unwrap_or(false) {
                    header[index] = Some(cell.to_owned());
                } else {
                    data[index].push(cell.to_owned());
                }
            }

            let kinds = (col_lower_bound..=col_upper_bound).map(|col| {
                let index = col - col_lower_bound;
                let types = data[index].iter()
                    .map(|cell| ColumnType::from(&cell.kind, &cell.value))
                    .collect::<Vec<_>>();
                ColumnType::detect(types)
            }).collect::<Vec<_>>();

            sheets.push((
                sheet.name.to_owned(),
                header,
                kinds,
                row_lower_bound.map(|row| if has_header { row + 1 } else { row }),
                col_lower_bound,
                col_upper_bound,
            ));
        }
        let (shared_strings, mappings) = self.load_shared_strings(Some(shared_indexes))?;

        let mut tables = Vec::<Table>::new();
        for (name, header, kinds, row_lower_bound, col_lower_bound, col_upper_bound) in sheets.into_iter() {
            let names = (col_lower_bound..=col_upper_bound).map(|col| {
                let index = col - col_lower_bound;
                if let Some(cell) = &header[index] {
                    let value = if cell.kind == CellType::SharedString {
                        let id = cell.value.parse::<usize>().expect("Shared string index");
                        let index = mappings[&id];
                        shared_strings[index].to_owned()
                    } else {
                        cell.to_string()
                    };
                    if !criteria.nulls.contains(&value) {
                        value
                    } else {
                        index_to_col(col).to_owned()
                    }
                } else {
                    index_to_col(col).to_owned()
                }
            }).collect::<Vec<_>>();

            let columns = names.iter().zip(kinds)
                .map(|(name, kind)| {
                    Column {
                        name: name.to_owned(),
                        kind: presets.iter()
                            .find(|(pattern, _)| pattern.matches(name))
                            .map(|(_, kind)| kind.to_owned())
                            .unwrap_or(kind.to_owned()),
                    }
                })
                .collect::<Vec<_>>();
            tables.push(Table {
                name,
                columns,
                row_lower_bound,
                col_lower_bound,
                col_upper_bound,
            });
        }

        Ok(tables)
    }

    /// Reads all non-empty cells within specified ranges
    ///
    /// Returns a collection of sheets with their cell data
    /// filtered according to the provided criteria.
    fn read_sheets(
        &mut self,
        criteria: &Criteria,
    ) -> Result<Vec<Sheet>, RustySheetError>;
}

/// Opens a spreadsheet file based on its format
///
/// Automatically detects the file format from the extension and returns
/// the appropriate spreadsheet implementation (XLSX, XLS, XLSB, or ODS).
pub(crate) fn open_spreadsheet(file_name: &str) -> Result<Box<dyn Spreadsheet + Send + Sync>, RustySheetError> {
    let uri = file_name.find('?').map(|index| &file_name[0..index]).unwrap_or(file_name);
    let extension = if let Some(index) = uri.rfind('.') {
        &uri.to_ascii_lowercase()[index + 1..]
    } else {
        ""
    };
    match extension {
        "xlsx" | "xlsm" | "xlam" => Ok(Box::new(XlsxSpreadsheet::open(file_name)?)),
        "xlsb" => Ok(Box::new(XlsbSpreadsheet::open(file_name)?)),
        "xls" | "xla" | "et" | "ett" => Ok(Box::new(XlsSpreadsheet::open(file_name)?)),
        "ods" => Ok(Box::new(OdsSpreadsheet::open(file_name)?)),
        _ => Err(SpreadsheetError::SpreadsheetFormatError(file_name.to_owned()))?,
    }
}

/// Opens multiple spreadsheet files and associates them with sheet name patterns
///
/// Returns a vector of tuples containing the spreadsheet and optional
/// sheet name patterns that match each file.
pub(crate) fn open_spreadsheets(files: &Vec<String>, patterns: &Option<Vec<(Option<Pattern>, Pattern)>>) -> Result<Vec<(Box<dyn Spreadsheet + Send + Sync>, Option<Vec<Pattern>>)>, RustySheetError> {
    let spreadsheets = files
        .iter()
        .map(|path| open_spreadsheet(path).with_prefix(path))
        .collect::<Result<Vec<_>, _>>()?;
    let spreadsheets = spreadsheets.into_iter().map(|spreadsheet| {
        let sheet_name_patterns = patterns.as_ref().map(|sheets| {
            sheets.iter()
                .filter(|(it, _)| {
                    if let Some(file_name_pattern) = it {
                        file_name_pattern.matches(spreadsheet.name().as_str())
                    } else {
                        true
                    }
                })
                .map(|(_, it)| it.to_owned())
                .collect::<Vec<_>>()
        }).filter(|sheets| !sheets.is_empty()); // 如果为空，则匹配所有Sheet
        (spreadsheet, sheet_name_patterns)
    }).collect::<Vec<_>>();
    Ok(spreadsheets)
}
