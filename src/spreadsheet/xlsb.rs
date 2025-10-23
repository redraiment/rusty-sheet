use crate::error::RustySheetError;
use crate::helpers::biff12::Biff12Reader;
use crate::helpers::reader::UnifiedReader;
use crate::helpers::zip::ZipHelper;
use crate::match_biff12_record;
use crate::spreadsheet::cell::to_error_value;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::excel;
use crate::spreadsheet::excel::load_relationships;
use crate::spreadsheet::reference::index_to_reference;
use crate::spreadsheet::sheet::Sheet;
use crate::spreadsheet::Spreadsheet;
use crate::spreadsheet::SpreadsheetError;
use either::Either;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::BufReader;
use zip::read::ZipFile;
use zip::ZipArchive;

// BIFF12 record type constants for XLSB file format

/// Row header record
const BRT_ROW_HDR: u16 = 0;
/// Cell containing RK number (compressed floating point)
const BRT_CELL_RK: u16 = 2;
/// Cell containing error value
const BRT_CELL_ERROR: u16 = 3;
/// Cell containing boolean value
const BRT_CELL_BOOL: u16 = 4;
/// Cell containing real number (double precision)
const BRT_CELL_REAL: u16 = 5;
/// Cell containing inline string
const BRT_CELL_ST: u16 = 6;
/// Cell containing shared string reference
const BRT_CELL_ISST: u16 = 7;
/// Formula containing string result
const BRT_FMLA_STRING: u16 = 8;
/// Formula containing numeric result
const BRT_FMLA_NUM: u16 = 9;
/// Formula containing boolean result
const BRT_FMLA_BOOL: u16 = 10;
/// Formula containing error result
const BRT_FMLA_ERROR: u16 = 11;
/// Shared string table item
const BRT_SST_ITEM: u16 = 19;
/// Future record table begin marker
const BRT_FRT_BEGIN: u16 = 35;
/// Future record table end marker
const BRT_FRT_END: u16 = 36;
/// Number format definition
const BRT_FMT: u16 = 44;
/// Cell formatting (extended format)
const BRT_XF: u16 = 47;
/// Cell containing rich text string
const BRT_CELL_R_STRING: u16 = 62;
/// End of worksheet bundle
const BRT_END_BUNDLE_SHS: u16 = 144;
/// Begin sheet data section
const BRT_BEGIN_SHEET_DATA: u16 = 145;
/// End sheet data section
const BRT_END_SHEET_DATA: u16 = 146;
/// Workbook properties
const BRT_WB_PROP: u16 = 153;
/// Worksheet bundle
const BRT_BUNDLE_SH: u16 = 156;
/// Begin shared string table
const BRT_BEGIN_SST: u16 = 159;
/// Begin number formats section
const BRT_BEGIN_FMTS: u16 = 615;
/// Begin cell formatting section
const BRT_BEGIN_CELL_XFS: u16 = 617;

/// Represents an XLSB (Excel Binary Workbook) spreadsheet file
///
/// This struct handles the parsing and reading of XLSB format files,
/// which use the BIFF12 binary format for improved performance.
pub(crate) struct XlsbSpreadsheet {
    /// Original file name of the spreadsheet
    pub(crate) name: String,
    /// ZIP archive containing the XLSB file structure
    zip: ZipArchive<UnifiedReader>,
    /// Pre-parsed number formats for cell type detection
    number_formats: Vec<CellType>,
    /// List of worksheet names and their corresponding ZIP file paths
    sheets: Vec<(String, String)>,
}

impl XlsbSpreadsheet {
    /// Opens and initializes an XLSB spreadsheet file
    ///
    /// # Arguments
    /// * `file_name` - Path to the XLSB file to open
    ///
    /// # Returns
    /// * `Result<XlsbSpreadsheet, RustySheetError>` - Initialized spreadsheet or error
    pub(crate) fn open(file_name: &str) -> Result<XlsbSpreadsheet, RustySheetError> {
        let (zip, number_formats, sheets) = excel::open(file_name, load_workbook, load_number_formats)?;
        Ok(XlsbSpreadsheet {
            name: file_name.to_owned(),
            zip,
            number_formats,
            sheets,
        })
    }
}

impl Spreadsheet for XlsbSpreadsheet {
    /// Returns the original file name of the spreadsheet
    fn name(&self) -> String {
        self.name.to_owned()
    }

    /// Loads shared strings from the XLSB file
    ///
    /// Shared strings are stored in a separate table to optimize storage
    /// for repeated string values across multiple cells.
    ///
    /// # Arguments
    /// * `indexes` - Optional set of specific string indexes to load
    ///
    /// # Returns
    /// * `Result<(Vec<String>, HashMap<usize, usize>)>` - Tuple containing:
    ///   - Vector of loaded strings
    ///   - Mapping from original indexes to new positions
    fn load_shared_strings(&mut self, mut indexes: Option<HashSet<usize>>) -> Result<(Vec<String>, HashMap<usize, usize>), RustySheetError> {
        let mut shared_strings = Vec::<String>::new();
        let mut mappings = HashMap::<usize, usize>::new();
        let mut reader = match self.zip.biff_reader("xl/sharedStrings.bin")? {
            Some(reader) => reader,
            None => return Ok((shared_strings, mappings)),
        };

        reader.find(BRT_BEGIN_SST)?;
        for id in 0..reader.get_usize(4) {
            reader.find_with(BRT_SST_ITEM, &[(BRT_FRT_BEGIN, BRT_FRT_END)])?;
            if let Some(keys) = &mut indexes {
                if keys.contains(&id) {
                    keys.remove(&id);
                    let string = reader.get_str(1)?;
                    let index = shared_strings.len();
                    shared_strings.push(string.to_string());
                    mappings.insert(id, index);
                }
                if keys.is_empty() {
                    break;
                }
            } else {
                let string = reader.get_str(1)?;
                shared_strings.push(string.to_string());
            }
        }

        Ok((shared_strings, mappings))
    }

    /// Reads worksheet data from the XLSB file according to specified criteria
    ///
    /// Processes each worksheet, filtering by name and range constraints,
    /// and extracts cell data in BIFF12 binary format.
    ///
    /// # Arguments
    /// * `criteria` - Selection criteria for worksheets and data ranges
    ///
    /// # Returns
    /// * `Result<Vec<Sheet>>` - Vector of processed worksheet data
    fn read_sheets(&mut self, criteria: &Criteria) -> Result<Vec<Sheet>, RustySheetError> {
        let mut sheets = Vec::<Sheet>::new();
        let mut sheet_count = 0usize;
        for (sheet_name, zip_path) in &self.sheets {
            if criteria.sheet_limit.map(|limit| sheet_count >= limit).unwrap_or(false) {
                break;
            } else if criteria.accept(sheet_name) {
                sheet_count += 1;
            } else {
                continue;
            }

            let mut sheet = Sheet::new(&self.name, sheet_name, criteria.range, criteria.rows_limit, criteria.skip_empty_rows);
            let mut last_row = sheet.chunk_row_lower;
            let mut row = 0usize;
            let mut reader = self.zip.biff_reader(&zip_path)?
                .ok_or_else(|| SpreadsheetError::FileError(zip_path.to_owned()))?;
            reader.find(BRT_BEGIN_SHEET_DATA)?;
            loop {
                let tag = reader.next()?;
                match tag {
                    BRT_END_SHEET_DATA => break,
                    BRT_ROW_HDR => {
                        row = reader.get_usize(0);
                        if sheet.after_row_upper_bound(row) {
                            break;
                        }
                    }
                    BRT_CELL_RK
                    | BRT_CELL_BOOL | BRT_FMLA_BOOL
                    | BRT_CELL_REAL | BRT_FMLA_NUM
                    | BRT_CELL_ST | BRT_FMLA_STRING
                    | BRT_CELL_R_STRING
                    | BRT_CELL_ISST
                    | BRT_CELL_ERROR | BRT_FMLA_ERROR
                    if !sheet.before_row_lower_bound(row) => {
                        let col = reader.get_usize(0);
                        if sheet.contains(row, col) {
                            if let Some(last_row) = last_row {
                                if criteria.end_at_empty_row && ((sheet.is_empty() && last_row != row) || (!sheet.is_empty() && last_row + 1 < row)) {
                                    break;
                                }
                            }
                            last_row = Some(row);
                            let (either, value) = match tag {
                                BRT_CELL_BOOL | BRT_FMLA_BOOL => read_bool_cell(&mut reader),
                                BRT_CELL_REAL | BRT_FMLA_NUM => read_real_cell(&mut reader),
                                BRT_CELL_ST | BRT_FMLA_STRING => read_st_cell(&mut reader)?,
                                BRT_CELL_R_STRING => read_rich_string_cell(&mut reader)?,
                                BRT_CELL_ISST => read_shared_string_cell(&mut reader),
                                BRT_CELL_ERROR | BRT_FMLA_ERROR => read_error_cell(&mut reader),
                                _ => read_rk_cell(&mut reader),
                            };
                            let kind = match either {
                                Either::Left(kind) => kind,
                                Either::Right(index) => (self.number_formats)[index],
                            };
                            if kind != CellType::Error {
                                if !value.is_empty() {
                                    sheet.push(Cell {
                                        row: row,
                                        col: col,
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

/// Loads workbook metadata from the XLSB file
///
/// Parses the workbook.bin file to extract worksheet information
/// and determine the date system used (1900 or 1904).
///
/// # Arguments
/// * `zip` - ZIP archive containing the XLSB file structure
///
/// # Returns
/// * `Result<(Vec<(String, String)>, bool)>` - Tuple containing:
///   - Vector of worksheet names and their file paths
///   - Boolean indicating if 1904 date system is used
fn load_workbook(zip: &mut ZipArchive<UnifiedReader>) -> Result<(Vec<(String, String)>, bool), RustySheetError> {
    let relationships = load_relationships(zip, "xl/_rels/workbook.bin.rels")?;
    let mut reader = zip.biff_reader("xl/workbook.bin")?
        .ok_or_else(|| SpreadsheetError::FileError("xl/workbook.bin".to_string()))?;
    let mut sheets: Vec<(String, String)> = Vec::new();
    let mut is_1904 = false;
    match_biff12_record!(reader => {
        BRT_END_BUNDLE_SHS => break,
        BRT_BUNDLE_SH => {
            let (id, index) = reader.get_str_and_bound(8)?;
            if let Some(zip_path) = relationships.get(id.as_ref()) {
                let sheet_name = reader.get_str(index)?;
                sheets.push((sheet_name.to_string(), zip_path.to_owned()));
            }
        }
        BRT_WB_PROP => {
            is_1904 = (&reader.buffer[0] & 0x1) != 0;
        }
    });
    Ok((sheets, is_1904))
}

/// Loads number format definitions from the XLSB file
///
/// Parses the styles.bin file to extract custom number formats
/// and map them to internal cell type representations.
///
/// # Arguments
/// * `zip` - ZIP archive containing the XLSB file structure
/// * `is_1904` - Whether the workbook uses 1904 date system
///
/// # Returns
/// * `Result<Vec<CellType>>` - Vector of cell types for format indexes
fn load_number_formats(zip: &mut ZipArchive<UnifiedReader>, is_1904: bool) -> Result<Vec<CellType>, RustySheetError> {
    let mut reader = match zip.biff_reader("xl/styles.bin")? {
        Some(reader) => reader,
        None => return Ok(Vec::new()),
    };

    let mut custom_formats: HashMap<String, CellType> = HashMap::new();
    let mut format_indexes: Vec<String> = Vec::new();
    match_biff12_record!(reader => {
        BRT_BEGIN_FMTS => {
            for _ in 0..reader.get_usize(0) {
                reader.find(BRT_FMT)?;
                let id = reader.get_u16(0);
                let format = reader.get_str(2)?;
                custom_formats.insert(
                    id.to_string(),
                    CellType::parse_custom_number_format(format.as_ref(), is_1904),
                );
            }
        }
        BRT_BEGIN_CELL_XFS => {
            for _ in 0..reader.get_usize(0) {
                reader.find(BRT_XF)?;
                let id = reader.get_u16(2);
                format_indexes.push(id.to_string());
            }
            break;
        }
    });

    Ok(excel::load_number_formats(format_indexes, custom_formats, is_1904))
}

/// Reads a boolean cell value from BIFF12 data
///
/// # Arguments
/// * `reader` - BIFF12 reader positioned at boolean cell data
///
/// # Returns
/// * `(Either<CellType, usize>, String)` - Tuple containing:
///   - Cell type (boolean) and format index
///   - String representation of boolean value ("1" or "0")
fn read_bool_cell(reader: &mut Biff12Reader<BufReader<ZipFile<UnifiedReader>>>) -> (Either<CellType, usize>, String) {
    let value = if reader.buffer[8] != 0 { "1" } else { "0" };
    (Either::Left(CellType::Boolean), value.to_owned())
}

/// Reads a real number (double precision) cell value from BIFF12 data
///
/// # Arguments
/// * `reader` - BIFF12 reader positioned at real number cell data
///
/// # Returns
/// * `(Either<CellType, usize>, String)` - Tuple containing:
///   - Format index reference and cell type
///   - String representation of numeric value
fn read_real_cell(reader: &mut Biff12Reader<BufReader<ZipFile<UnifiedReader>>>) -> (Either<CellType, usize>, String) {
    let index = reader.get_style(4);
    let value = reader.get_f64(8).to_string();
    (Either::Right(index), value)
}

/// Reads an inline string cell value from BIFF12 data
///
/// # Arguments
/// * `reader` - BIFF12 reader positioned at inline string cell data
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Tuple containing:
///   - Cell type (inline string) and format index
///   - String value extracted from cell
fn read_st_cell(reader: &mut Biff12Reader<BufReader<ZipFile<UnifiedReader>>>) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    let value = reader.get_str(8)?.to_string();
    Ok((Either::Left(CellType::InlineString), value))
}

/// Reads a rich text string cell value from BIFF12 data
///
/// # Arguments
/// * `reader` - BIFF12 reader positioned at rich string cell data
///
/// # Returns
/// * `Result<(Either<CellType, usize>, String)>` - Tuple containing:
///   - Cell type (inline string) and format index
///   - String value extracted from rich text cell
fn read_rich_string_cell(reader: &mut Biff12Reader<BufReader<ZipFile<UnifiedReader>>>) -> Result<(Either<CellType, usize>, String), RustySheetError> {
    let value = reader.get_str(8 + 1)?.to_string();
    Ok((Either::Left(CellType::InlineString), value))
}

/// Reads a shared string reference cell value from BIFF12 data
///
/// # Arguments
/// * `reader` - BIFF12 reader positioned at shared string cell data
///
/// # Returns
/// * `(Either<CellType, usize>, String)` - Tuple containing:
///   - Cell type (shared string) and format index
///   - String representation of shared string index
fn read_shared_string_cell(reader: &mut Biff12Reader<BufReader<ZipFile<UnifiedReader>>>) -> (Either<CellType, usize>, String) {
    let value = reader.get_usize(8).to_string();
    (Either::Left(CellType::SharedString), value)
}

/// Reads an error cell value from BIFF12 data
///
/// # Arguments
/// * `reader` - BIFF12 reader positioned at error cell data
///
/// # Returns
/// * `(Either<CellType, usize>, String)` - Tuple containing:
///   - Cell type (error) and format index
///   - String representation of error value
fn read_error_cell(reader: &mut Biff12Reader<BufReader<ZipFile<UnifiedReader>>>) -> (Either<CellType, usize>, String) {
    let value = to_error_value(reader.buffer[8]).to_owned();
    (Either::Left(CellType::Error), value)
}

/// Reads an RK (compressed floating point) cell value from BIFF12 data
///
/// RK format stores numbers in a compressed format that can represent
/// both integers and floating-point values with reduced storage.
///
/// # Arguments
/// * `reader` - BIFF12 reader positioned at RK cell data
///
/// # Returns
/// * `(Either<CellType, usize>, String)` - Tuple containing:
///   - Format index reference and cell type
///   - String representation of decompressed numeric value
fn read_rk_cell(reader: &mut Biff12Reader<BufReader<ZipFile<UnifiedReader>>>) -> (Either<CellType, usize>, String) {
    let index = reader.get_style(4);
    let is_percentage = (reader.buffer[8] & 0x01) != 0;
    let is_integer = (reader.buffer[8] & 0x02) != 0;
    reader.buffer[8] &= 0xFC; // Clear A and B flag bits

    let mut value = if is_integer {
        (reader.get_i32(8) >> 2) as f64
    } else {
        let value = (reader.get_u32(8) >> 2) as u64;
        f64::from_bits(value << 34)
    };
    if is_percentage {
        value /= 100.0;
    }
    let value = if is_integer {
        (value.trunc() as i64).to_string()
    } else {
        value.to_string()
    };

    (Either::Right(index), value)
 }