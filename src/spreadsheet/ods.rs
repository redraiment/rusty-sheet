use crate::error::RustySheetError;
use crate::helpers::xml::XmlNodeHelper;
use crate::helpers::xml::XmlTextContextHelper;
use crate::helpers::zip::ZipHelper;
use crate::match_xml_events;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::reference::index_to_reference;
use crate::spreadsheet::sheet::Sheet;
use crate::spreadsheet::Spreadsheet;
use crate::spreadsheet::SpreadsheetError;
use quick_xml::events::Event;
use quick_xml::name::QName;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use thiserror::Error;
use zip::ZipArchive;

/// ODS file MIME type identifier
const MIME_TYPE: &[u8] = b"application/vnd.oasis.opendocument.spreadsheet";
/// XML element name for spreadsheet root
const SPREADSHEET: QName = QName(b"office:spreadsheet");
/// XML element name for table (sheet)
const TABLE: QName = QName(b"table:table");
/// XML element name for table row
const TABLE_ROW: QName = QName(b"table:table-row");
/// XML element name for table cell
const TABLE_CELL: QName = QName(b"table:table-cell");
/// XML element name for covered table cell (merged cells)
const TABLE_COVERED_CELL: QName = QName(b"table:covered-table-cell");
/// XML element name for annotations (comments)
const ANNOTATION: QName = QName(b"office:annotation");
/// XML element name for paragraph text
const PARAGRAPH: QName = QName(b"text:p");
/// XML element name for string (space) text
const STRING: QName = QName(b"text:s");

/// Error types specific to ODS spreadsheet processing
#[derive(Error, Debug)]
pub(crate) enum OdsError {
    /// Invalid ODS MIME type detected in file
    #[error("Invalid ODS MIME type")]
    MimeTypeError,
}

/// ODS spreadsheet handler for reading OpenDocument Spreadsheet files
pub(crate) struct OdsSpreadsheet {
    /// Name of the ODS file
    pub(crate) name: String,
    /// ZIP archive containing the ODS file contents
    zip: ZipArchive<BufReader<File>>,
}

impl OdsSpreadsheet {
    /// Opens an ODS file and validates its format
    ///
    /// # Arguments
    /// * `file_name` - Path to the ODS file to open
    ///
    /// # Returns
    /// * `Result<Self, RustySheetError>` - ODS spreadsheet instance or error
    pub(crate) fn open(file_name: &str) -> Result<Self, RustySheetError> {
        let file = File::open(Path::new(file_name))?;
        let mut zip = ZipArchive::new(BufReader::new(file))?;
        check_mime(&mut zip)?;
        if is_password_protected(&mut zip)? {
            Err(SpreadsheetError::SpreadsheetPasswordProtectedError(file_name.to_owned()))?;
        }
        Ok(OdsSpreadsheet {
            name: file_name.to_owned(),
            zip,
        })
    }
}

impl Spreadsheet for OdsSpreadsheet {
    /// Returns the name of the ODS file
    fn name(&self) -> String {
        self.name.to_owned()
    }

    /// Loads shared strings (not applicable for ODS format)
    ///
    /// ODS format stores strings inline rather than in a shared string table,
    /// so this function returns empty collections.
    ///
    /// # Arguments
    /// * `_indexes` - Optional set of string indexes to load (ignored for ODS)
    ///
    /// # Returns
    /// * `Result<(Vec<String>, HashMap<usize, usize>), RustySheetError>` - Empty collections
    fn load_shared_strings(
        &mut self,
        _indexes: Option<HashSet<usize>>,
    ) -> Result<(Vec<String>, HashMap<usize, usize>), RustySheetError> {
        Ok((Vec::new(), HashMap::new()))
    }

    /// Reads sheets from the ODS file according to specified criteria
    ///
    /// # Arguments
    /// * `criteria` - Selection criteria for sheets, ranges, and rows
    ///
    /// # Returns
    /// * `Result<Vec<Sheet>, RustySheetError>` - Vector of sheets or error
    fn read_sheets(&mut self, criteria: &Criteria) -> Result<Vec<Sheet>, RustySheetError> {
        let mut sheets = Vec::<Sheet>::new();
        let mut sheet_count = 0usize;
        let mut sheet_name = String::new();
        let mut reader = self.zip
            .xml_reader("content.xml")?
            .expect("content.xml");
        'sheets: loop {
            match_xml_events!(reader => {
                Event::End(event) if event.name() == SPREADSHEET => break 'sheets,
                Event::Start(event) if event.name() == TABLE => {
                    let table_name = event.get_attribute_value("table:name")?.expect("Sheet name");
                    sheet_name.clear();
                    sheet_name.push_str(&table_name);
                    if criteria.sheet_limit.map(|limit| sheet_count >= limit).unwrap_or(false) {
                        break 'sheets;
                    } else if criteria.accept(&sheet_name) {
                        sheet_count += 1;
                        break;
                    } else {
                        continue;
                    }
                }
            });
            let mut sheet = Sheet::new(&self.name, &sheet_name, criteria.range, criteria.rows_limit, criteria.skip_empty_rows);
            let mut last_row = sheet.chunk_row_lower;

            // Cell信息
            let mut row = 0usize;
            let mut col = 0usize;
            let mut row_count = 0usize;
            let mut col_count = 0usize;
            let mut kind = CellType::default();
            let mut value = String::new();
            // 上下文信息
            let mut element_context = false; // 是否读取子元素
            let mut comment_context = false; // 是否为注释内容
            match_xml_events!(reader => {
                Event::End(event) if event.name() == TABLE => break,
                Event::Start(event) if event.name() == TABLE_ROW => {
                    row_count = event.parse_attribute_value("table:number-rows-repeated")?.unwrap_or(1);
                    col = 0;
                }
                Event::End(event) if event.name() == TABLE_ROW => {
                    row += row_count;
                    if sheet.after_row_upper_bound(row) {
                        break;
                    }
                }
                Event::Start(event) if event.name() == TABLE_CELL || event.name() == TABLE_COVERED_CELL => {
                    value.clear();
                    col_count = event.parse_attribute_value::<usize>("table:number-columns-repeated")?.unwrap_or(1);
                    kind = if let Some(result_type) = event.get_attribute_value("office:value-type")? {
                        match result_type.as_ref() {
                            "boolean" => CellType::Boolean,
                            "date" => CellType::IsoDateTime,
                            "time" => CellType::IsoDuration,
                            "string" => if event.get_attribute_value("calcext:value-type")?.map(|cow| cow == "error").unwrap_or(false) {
                                if criteria.error_as_null {
                                    CellType::Empty
                                } else {
                                    CellType::Error
                                }
                            } else {
                                CellType::InlineString
                            },
                            _ => CellType::Number,
                        }
                    } else {
                        CellType::Empty
                    };

                    if let Some(result_type) = event.get_attribute_value("office:value-type")? {
                        match result_type.as_ref() {
                            "string" => element_context = kind != CellType::Empty, // error_as_null
                            "boolean" => if event.get_attribute_value("office:boolean-value")?.map(|cow| cow != "false" && cow != "0").unwrap_or(false) {
                                value.push_str("1");
                            } else {
                                value.push_str("0");
                            },
                            "date" => if let Some(data) = event.get_attribute_value("office:date-value")? {
                                value.push_str(&data);
                            }
                            "time" => if let Some(data) = event.get_attribute_value("office:time-value")? {
                                value.push_str(&data);
                            }
                            _ => if let Some(data) = event.get_attribute_value("office:value")? {
                                value.push_str(&data);
                            }
                        }
                    }
                }
                Event::End(event) if event.name() == TABLE_CELL || event.name() == TABLE_COVERED_CELL => {
                    if kind != CellType::Empty {
                        for row_offset in 0..row_count {
                            let row_number = row + row_offset;
                            if sheet.before_row_lower_bound(row_number) {
                                continue;
                            } else if sheet.after_row_upper_bound(row_number) {
                                break;
                            }
                            for col_offset in 0..col_count {
                                let col_number = col + col_offset;
                                if !sheet.before_col_lower_bound(col_number) && !sheet.after_col_upper_bound(col_number) {
                                    if let Some(last_row) = last_row {
                                        if criteria.end_at_empty_row && ((sheet.is_empty() && last_row != row) || (!sheet.is_empty() && last_row + 1 < row)) {
                                            break;
                                        }
                                    }
                                    last_row = Some(row);
                                    if kind != CellType::Error {
                                        if !value.is_empty() {
                                            sheet.push(Cell {
                                                row: row_number,
                                                col: col_number,
                                                kind,
                                                value: value.to_owned(),
                                            });
                                        }
                                    } else {
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
                        }
                    }
                    col += col_count;
                    element_context = false;
                    comment_context = false;
                }
                // 读取字符串内容
                Event::Start(event) if element_context && event.name() == ANNOTATION => comment_context = true,
                Event::End(event) if element_context && comment_context && event.name() == ANNOTATION => comment_context = false,
                Event::Start(event) if element_context && !comment_context && event.name() == PARAGRAPH => {
                    if !value.is_empty() {
                        value.push('\n');
                    }
                }
                Event::Start(event) if element_context && !comment_context && event.name() == STRING => {
                    let count = event.parse_attribute_value("text:c")?.unwrap_or(1);
                    for _ in 0..count {
                        value.push(' ');
                    }
                }
                Event::Text(event) if element_context && !comment_context => value.push_bytes_text(&event)?,
                Event::GeneralRef(event) if element_context && !comment_context => value.push_bytes_ref(&event)?,
            });
            sheet.finish(criteria.end_at_empty_row);
            sheets.push(sheet);

            if criteria.sheet_limit.map(|limit| sheet_count >= limit).unwrap_or(false) {
                break;
            }
        }

        Ok(sheets)
    }
}

/// Validates that the ZIP archive contains a valid ODS file by checking MIME type
///
/// # Arguments
/// * `zip` - ZIP archive to validate
///
/// # Returns
/// * `Result<(), RustySheetError>` - Success or MIME type error
fn check_mime(zip: &mut ZipArchive<BufReader<File>>) -> Result<(), RustySheetError> {
    if let Some(file) = &mut zip.file("mimetype")? {
        let mut buffer = [0u8; 46];
        file.read_exact(&mut buffer)?;
        if &buffer[..] != MIME_TYPE {
            Err(OdsError::MimeTypeError)?;
        }
    }
    Ok(())
}

/// Checks if the ODS file is password protected by examining the manifest
///
/// # Arguments
/// * `zip` - ZIP archive to check
///
/// # Returns
/// * `Result<bool, RustySheetError>` - True if password protected, false otherwise
fn is_password_protected(zip: &mut ZipArchive<BufReader<File>>) -> Result<bool, RustySheetError> {
    let mut reader = zip
        .xml_reader("META-INF/manifest.xml")?
        .expect("META-INF/manifest.xml");
    let mut in_file_entry = false;
    match_xml_events!(reader => {
        Event::Start(event) if event.name() == QName(b"manifest:file-entry") => in_file_entry = true,
        Event::Start(event) if in_file_entry && event.name() == QName(b"manifest:encryption-data") => {
            return Ok(true);
        }
    });
    Ok(false)
}
