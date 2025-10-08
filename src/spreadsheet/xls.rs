use crate::error::ResultOptionChain;
use crate::error::RustySheetError;
use crate::helpers::biff8::Biff8Reader;
use crate::helpers::cfb::Cfb;
use crate::match_biff8_record;
use crate::spreadsheet::cell::to_error_value;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::excel::load_number_formats;
use crate::spreadsheet::reference::index_to_reference;
use crate::spreadsheet::sheet::Sheet;
use crate::spreadsheet::Spreadsheet;
use crate::spreadsheet::SpreadsheetError;
use either::Either;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use thiserror::Error;

// BIFF8 record type identifiers for Excel file parsing
const FORMULA: u16 = 6;        // Formula record containing calculation expressions
const EOF: u16 = 10;           // End of file record marking the end of a substream
const DATE1904: u16 = 34;      // Date system flag (1904 vs 1900 base)
const FILE_PASS: u16 = 47;     // File password protection record
const CODE_PAGE: u16 = 66;     // Character encoding specification
const BOUND_SHEET8: u16 = 133; // Worksheet definition and position
const MUL_RK: u16 = 189;       // Multiple RK number records for efficiency
const XF: u16 = 224;           // Extended format record for cell styling
const SST: u16 = 252;          // Shared string table containing repeated text
const LABEL_SST: u16 = 253;    // Label referencing shared string table
const NUMBER: u16 = 515;       // Numeric cell value
const LABEL: u16 = 516;        // Text label cell value
const BOOL_ERR: u16 = 517;     // Boolean or error cell value
const STRING: u16 = 519;       // String value for formula results
const RK: u16 = 638;           // RK number format for compressed numeric storage
const FORMAT: u16 = 1054;      // Custom number format definition
const BOF: u16 = 2057;         // Beginning of file record for substreams

/// Error types specific to XLS file parsing
#[derive(Error, Debug)]
pub(crate) enum XlsError {
    /// Invalid character encoding code page encountered
    #[error("Invalid Code page '{0}'")]
    CodePageError(u16),

    /// Invalid formula value or structure encountered
    #[error("Invalid Formula value '{0}'")]
    FormulaValueError(u64),
}

/// Main structure for reading and parsing XLS (Excel 97-2003) files
pub(crate) struct XlsSpreadsheet {
    /// Original file name for error reporting and identification
    pub(crate) name: String,
    /// BIFF8 reader for parsing Excel binary format records
    reader: Biff8Reader,
    /// Shared string table containing repeated text values
    shared_strings: Vec<String>,
    /// Number format mappings for cell type detection
    number_formats: Vec<CellType>,
    /// List of worksheets with their names and stream positions
    sheets: Vec<(String, usize)>,
}

impl XlsSpreadsheet {
    /// Opens and parses an XLS file, reading global workbook information
    ///
    /// # Arguments
    /// * `file_name` - Path to the XLS file to open
    ///
    /// # Returns
    /// * `Result<XlsSpreadsheet, RustySheetError>` - Initialized spreadsheet or error
    pub(crate) fn open(file_name: &str) -> Result<XlsSpreadsheet, RustySheetError> {
        let mut buf_reader = BufReader::new(File::open(file_name)?);
        let cfb = Cfb::new(&mut buf_reader)?;
        let mut reader = cfb.read("Workbook")
            .ok_none_else(|| cfb.read("Book"))?
            .map(Biff8Reader::new)
            .ok_or_else(|| SpreadsheetError::SpreadsheetEmptyError(file_name.to_owned()))?;
        let mut is_1904 = false;
        let mut shared_strings = Vec::new();
        let mut custom_formats: HashMap<String, CellType> = HashMap::new();
        let mut format_indexes: Vec<String> = Vec::new();
        let mut sheets: Vec<(String, usize)> = Vec::new();
        match_biff8_record!(reader => {
            EOF => break,
            FILE_PASS if reader.read_u16()? != 0 => Err(SpreadsheetError::SpreadsheetPasswordProtectedError(file_name.to_owned()))?,
            DATE1904 if reader.read_u16()? == 1 => is_1904 = true,
            CODE_PAGE => {
                let code_page = reader.read_u16()?;
                reader.encoding = codepage::to_encoding(code_page).ok_or(XlsError::CodePageError(code_page))?;
            }
            FORMAT => {
                let id = reader.read_u16()?;
                let format = reader.read_xl_unicode_string()?;
                custom_formats.insert(
                    id.to_string(),
                    CellType::parse_custom_number_format(format.as_ref(), is_1904),
                );
            }
            XF => {
                reader.skip(2)?;
                let id = reader.read_u16()?;
                format_indexes.push(id.to_string());
            }
            SST => shared_strings = load_shared_strings(&mut reader)?,
            BOUND_SHEET8 => {
                let pointer = reader.read_usize()?;
                reader.skip(2)?;
                let sheet_name = reader.read_short_xl_unicode_string()?;
                sheets.push((sheet_name, pointer));
            }
        });
        if sheets.is_empty() {
            Err(SpreadsheetError::SpreadsheetEmptyError(file_name.to_owned()))?
        }

        let number_formats = load_number_formats(format_indexes, custom_formats, is_1904);

        Ok(XlsSpreadsheet {
            name: file_name.to_owned(),
            reader,
            shared_strings,
            number_formats,
            sheets,
        })
    }
}

impl Spreadsheet for XlsSpreadsheet {
    /// Returns the original file name for identification
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Loads shared strings with optional index filtering
    ///
    /// XLS files are typically small enough to load all shared strings at once
    /// and maintain them in memory for efficient access during parsing.
    ///
    /// # Arguments
    /// * `indexes` - Optional set of string indexes to filter by
    ///
    /// # Returns
    /// * `Result<(Vec<String>, HashMap<usize, usize>)>` - Shared strings and index mappings
    fn load_shared_strings(
        &mut self,
        indexes: Option<HashSet<usize>>,
    ) -> Result<(Vec<String>, HashMap<usize, usize>), RustySheetError> {
        let shared_strings = self.shared_strings.to_owned();
        let mut mappings = HashMap::<usize, usize>::new();
        if let Some(keys) = indexes {
            for key in keys {
                mappings.insert(key, key);
            }
        }
        Ok((shared_strings, mappings))
    }

    /// Reads worksheets from the XLS file according to specified criteria
    ///
    /// Parses BIFF8 records to extract cell data, handling various record types
    /// including numbers, strings, formulas, and boolean/error values.
    ///
    /// # Arguments
    /// * `criteria` - Selection criteria for sheets, ranges, and processing options
    ///
    /// # Returns
    /// * `Result<Vec<Sheet>>` - Vector of parsed worksheet data
    fn read_sheets(&mut self, criteria: &Criteria) -> Result<Vec<Sheet>, RustySheetError> {
        let mut sheets = Vec::<Sheet>::new();
        let mut sheet_count = 0usize;
        for (sheet_name, pointer) in &self.sheets {
            if criteria.sheet_limit.map(|limit| sheet_count >= limit).unwrap_or(false) {
                break;
            } else if criteria.accept(sheet_name) {
                sheet_count += 1;
            } else {
                continue;
            }

            self.reader.goto(*pointer);
            self.reader.next()?;
            let mut sheet = Sheet::new(&self.name, sheet_name, criteria.range, criteria.rows_limit, criteria.skip_empty_rows);
            let mut last_row = sheet.chunk_row_lower;
            while let Some(tag) = self.reader.next()? {
                match tag {
                    BOF | EOF => break,
                    MUL_RK => {
                        let row = self.reader.read_u16()? as usize;
                        let col_lower_bound = self.reader.read_u16()? as usize;
                        let col_upper_bound = self.reader.get_u16_back(2)? as usize;
                        for col in col_lower_bound..=col_upper_bound {
                            if sheet.contains(row, col) {
                                if let Some(last_row) = last_row {
                                    if criteria.end_at_empty_row && ((sheet.is_empty() && last_row != row) || (!sheet.is_empty() && last_row + 1 < row)) {
                                        break;
                                    }
                                }
                                last_row = Some(row);
                                let index = self.reader.read_u16()? as usize;
                                let kind = self.number_formats[index];
                                let value = self.reader.read_rk_number()?;
                                sheet.push(Cell {
                                    row,
                                    col,
                                    kind,
                                    value,
                                });
                            } else {
                                self.reader.skip(6)?; // Skip RkRec
                            }
                        }
                    }
                    BOOL_ERR | NUMBER | RK | LABEL_SST | LABEL | FORMULA => {
                        let row = self.reader.read_u16()? as usize;
                        let col = self.reader.read_u16()? as usize;
                        if sheet.contains(row, col) {
                            if let Some(last_row) = last_row {
                                if criteria.end_at_empty_row && ((sheet.is_empty() && last_row != row) || (!sheet.is_empty() && last_row + 1 < row)) {
                                    break;
                                }
                            }
                            last_row = Some(row);
                            let (either, value) = match tag {
                                BOOL_ERR => read_bool_or_error_cell(&mut self.reader)?,
                                NUMBER => read_number_cell(&mut self.reader)?,
                                RK => read_rk_cell(&mut self.reader)?,
                                LABEL_SST => read_label_sst_cell(&mut self.reader)?,
                                LABEL => read_label_cell(&mut self.reader)?,
                                _ => read_formula_cell(&mut self.reader)?,
                            };
                            let kind = match either {
                                Either::Left(kind) => kind,
                                Either::Right(index) => self.number_formats[index],
                            };
                            if kind != CellType::Error {
                                if !value.is_empty() {
                                    sheet.push(Cell {
                                        row,
                                        col,
                                        kind,
                                        value,
                                    });
                                }
                            } else if !criteria.error_as_null {
                                let reference = index_to_reference(row, col);
                                Err(SpreadsheetError::CellValueError(
                                    sheet.file_name.to_owned(),
                                    sheet.name.to_owned(),
                                    reference,
                                    value.to_owned(),
                                ))?
                            }
                        }
                    }
                    _ => (),
                }
            }
            sheet.finish(criteria.end_at_empty_row);
            sheets.push(sheet);
        }

        Ok(sheets)
    }
}

/// Loads the shared string table from BIFF8 SST record
///
/// Shared strings are stored once in the file and referenced by index
/// to reduce file size for repeated text values.
///
/// # Arguments
/// * `reader` - BIFF8 reader positioned at SST record
///
/// # Returns
/// * `Result<Vec<String>>` - Vector of shared string values
fn load_shared_strings(reader: &mut Biff8Reader) -> Result<Vec<String>, RustySheetError> {
    let mut shared_strings: Vec<String> = Vec::new();
    reader.skip(4)?;
    let count = reader.read_usize()?;
    for _ in 0..count {
        let string = reader.read_xl_unicode_rich_extended_string()?;
        shared_strings.push(string);
    }
    Ok(shared_strings)
}

/// Reads a BOOL_ERR record containing boolean or error cell values
///
/// BOOL_ERR records store either boolean values (TRUE/FALSE) or error codes
/// with a flag indicating the value type.
///
/// # Arguments
/// * `reader` - BIFF8 reader positioned at BOOL_ERR record
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Cell type and value
fn read_bool_or_error_cell(reader: &mut Biff8Reader) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    reader.skip(2)?;
    let value = reader.read_u8()?;
    let flag = reader.read_u8()?;
    let kind = if flag == 0 {
        CellType::Boolean
    } else {
        CellType::Error
    };
    let value = if flag == 0 {
        value.to_string()
    } else {
        to_error_value(value).to_owned()
    };
    Ok((Either::Left(kind), value))
}

/// Reads a NUMBER record containing numeric cell values
///
/// NUMBER records store double-precision floating point numbers
/// with an associated format index for cell type detection.
///
/// # Arguments
/// * `reader` - BIFF8 reader positioned at NUMBER record
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Format index and numeric value
fn read_number_cell(reader: &mut Biff8Reader) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    let index = reader.read_u16()? as usize;
    let value = reader.read_f64()?;
    Ok((Either::Right(index), value.to_string()))
}

/// Reads an RK record containing compressed numeric values
///
/// RK records use a compressed format to store integers and floating point
/// numbers more efficiently than standard NUMBER records.
///
/// # Arguments
/// * `reader` - BIFF8 reader positioned at RK record
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Format index and numeric value
fn read_rk_cell(reader: &mut Biff8Reader) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    let index = reader.read_u16()? as usize;
    let value = reader.read_rk_number()?;
    Ok((Either::Right(index), value))
}

/// Reads a LABEL_SST record referencing shared string table
///
/// LABEL_SST records contain an index into the shared string table
/// rather than storing the string value directly.
///
/// # Arguments
/// * `reader` - BIFF8 reader positioned at LABEL_SST record
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Shared string type and index
fn read_label_sst_cell(reader: &mut Biff8Reader) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    reader.skip(2)?;
    let value = reader.read_usize()?;
    Ok((Either::Left(CellType::SharedString), value.to_string()))
}

/// Reads a LABEL record containing inline string values
///
/// LABEL records store string values directly in the cell record
/// rather than referencing the shared string table.
///
/// # Arguments
/// * `reader` - BIFF8 reader positioned at LABEL record
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Inline string type and value
fn read_label_cell(reader: &mut Biff8Reader) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    reader.skip(2)?;
    let value = reader.read_xl_unicode_string()?;
    Ok((Either::Left(CellType::InlineString), value))
}

/// Reads a FORMULA record containing calculation expressions
///
/// FORMULA records can contain numeric results, string results, boolean values,
/// error codes, or empty strings depending on the formula type and flags.
///
/// # Arguments
/// * `reader` - BIFF8 reader positioned at FORMULA record
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Cell type and formula result
fn read_formula_cell(
    reader: &mut Biff8Reader,
) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    let index = reader.read_u16()? as usize;
    let formula = reader.read_u64()?;
    let is_number = (formula & 0xFFFF000000000000) != 0xFFFF000000000000;
    let flag = formula & 0xFF;
    if is_number {
        Ok((Either::Right(index), f64::from_bits(formula).to_string(),))
    } else if flag == 0 {
        if let Some(kind) = reader.next()? {
            if kind == STRING {
                // Read Next String
                let value = reader.read_xl_unicode_string()?;
                Ok((Either::Left(CellType::InlineString), value))
            } else {
                Err(XlsError::FormulaValueError(formula))?
            }
        } else {
            Err(XlsError::FormulaValueError(formula))?
        }
    } else if flag == 1 {
        let value = if (formula & 0xFF0000) > 0 { "1" } else { "0" };
        Ok((Either::Left(CellType::Boolean), value.to_owned()))
    } else if flag == 2 {
        let code = ((formula >> 16) & 0xFF) as u8;
        let value = to_error_value(code).to_owned();
        Ok((Either::Left(CellType::Error), value))
    } else if flag == 3 {
        Ok((Either::Left(CellType::InlineString), "".to_owned()))
    } else {
        Err(XlsError::FormulaValueError(formula))?
    }
}
