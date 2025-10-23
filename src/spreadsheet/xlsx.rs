use crate::error::RustySheetError;
use crate::helpers::reader::UnifiedReader;
use crate::helpers::xml::XmlAttributeHelper;
use crate::helpers::xml::XmlNodeHelper;
use crate::helpers::xml::XmlReader;
use crate::helpers::xml::XmlTextContextHelper;
use crate::helpers::zip::ZipHelper;
use crate::match_xml_events;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::excel;
use crate::spreadsheet::excel::load_relationships;
use crate::spreadsheet::reference::index_to_reference;
use crate::spreadsheet::reference::reference_to_index;
use crate::spreadsheet::sheet::Sheet;
use crate::spreadsheet::Spreadsheet;
use crate::spreadsheet::SpreadsheetError;
use quick_xml::events::Event;
use quick_xml::name::QName;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::BufReader;
use zip::read::ZipFile;
use zip::ZipArchive;

// XML tag names for parsing Excel XLSX format
const TAG_CUSTOM_FORMATS: QName = QName(b"numFmts"); // Custom number formats container
const TAG_CUSTOM_FORMAT: QName = QName(b"numFmt");   // Individual custom number format
const TAG_FORMAT_INDEXES: QName = QName(b"cellXfs");  // Cell format indexes container
const TAG_FORMAT_INDEX: QName = QName(b"xf");         // Individual cell format index
const TAG_SHARED_STRING_ITEM: QName = QName(b"si");   // Shared string table item
const TAG_PHONETIC_TEXT: QName = QName(b"rPh");       // Phonetic text for Asian languages
const TAG_TEXT: QName = QName(b"t");                  // Text content within strings
const TAG_WORKBOOK_PROPERTIES: QName = QName(b"workbookPr"); // Workbook properties
const TAG_SHEET: QName = QName(b"sheet");             // Worksheet definition
const TAG_ROW: QName = QName(b"row");                 // Row in worksheet
const TAG_CELL: QName = QName(b"c");                  // Cell in worksheet
const TAG_INLINE_STRING: QName = QName(b"is");        // Inline string value
const TAG_VALUE: QName = QName(b"v");                 // Cell value content

/// Represents an Excel XLSX spreadsheet file
pub(crate) struct XlsxSpreadsheet {
    /// File name of the spreadsheet
    pub(crate) name: String,
    /// ZIP archive containing the XLSX file contents
    zip: ZipArchive<UnifiedReader>,
    /// Parsed number formats for cell type detection
    number_formats: Vec<CellType>,
    /// List of worksheets with (name, zip_path) pairs
    sheets: Vec<(String, String)>,
}

impl XlsxSpreadsheet {
    /// Opens an XLSX spreadsheet file and parses its structure
    ///
    /// # Arguments
    /// * `file_name` - Path to the XLSX file
    ///
    /// # Returns
    /// Result containing the initialized XlsxSpreadsheet or an error
    pub(crate) fn open(file_name: &str) -> Result<XlsxSpreadsheet, RustySheetError> {
        let (zip, number_formats, sheets) = excel::open(file_name, load_workbook, load_number_formats)?;
        Ok(XlsxSpreadsheet {
            name: file_name.to_owned(),
            zip,
            number_formats,
            sheets,
        })
    }
}

impl Spreadsheet for XlsxSpreadsheet {
    /// Returns the file name of this spreadsheet
    fn name(&self) -> String {
        self.name.to_owned()
    }

    /// Loads shared strings from the XLSX file
    ///
    /// Shared strings are stored in a separate XML file and referenced by index
    /// to reduce file size when the same string appears multiple times.
    ///
    /// # Arguments
    /// * `indexes` - Optional set of specific string indexes to load, or None to load all
    ///
    /// # Returns
    /// Tuple of (shared_strings, mappings) where mappings maps original indexes to loaded positions
    fn load_shared_strings(&mut self, mut indexes: Option<HashSet<usize>>) -> Result<(Vec<String>, HashMap<usize, usize>), RustySheetError> {
        let mut shared_strings = Vec::<String>::new();
        let mut mappings = HashMap::<usize, usize>::new();
        let mut reader = match self.zip.xml_reader("xl/sharedStrings.xml")? {
            Some(reader) => reader,
            None => return Ok((shared_strings, mappings)),
        };

        let mut id = 0usize;
        match_xml_events!(reader => {
            Event::Start(event) if event.name() == TAG_SHARED_STRING_ITEM => {
                if let Some(keys) = &mut indexes {
                    if keys.contains(&id) {
                        keys.remove(&id);
                        let string = read_string_value(&mut reader, TAG_SHARED_STRING_ITEM, false)?;
                        let index = shared_strings.len();
                        shared_strings.push(string);
                        mappings.insert(id, index);
                    }
                    if keys.is_empty() {
                        break;
                    }
                } else {
                    let string = read_string_value(&mut reader, TAG_SHARED_STRING_ITEM, false)?;
                    shared_strings.push(string);
                }
                id += 1;
            }
        });
        Ok((shared_strings, mappings))
    }

    /// Reads worksheets from the XLSX file according to the specified criteria
    ///
    /// Parses worksheet XML files and extracts cell data, applying range filtering,
    /// row limits, and other criteria specified by the user.
    ///
    /// # Arguments
    /// * `criteria` - Selection criteria for which data to extract
    ///
    /// # Returns
    /// Vector of Sheet objects containing the extracted data
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
            let mut row_count = 0usize;
            let mut col_count = 0usize;
            let mut row = 0usize;
            let mut col = 0usize;
            let mut kind = CellType::default();
            let mut value = String::new();
            let mut reader = self.zip.xml_reader(zip_path)?.expect(sheet_name);
            match_xml_events!(reader => {
                Event::End(event) if event.name() == TAG_ROW => {
                    row_count += 1;
                    col_count = 0;
                }
                Event::Start(event) if event.name() == TAG_CELL => {
                    (row, col) = event.get_attribute_value("r")?
                        .and_then(|reference| reference_to_index(&reference))
                        .unwrap_or((row_count, col_count));
                    col_count += 1;
                    if sheet.after_row_upper_bound(row) {
                        break;
                    } else if sheet.contains(row, col) {
                        kind = event.get_attribute_value("t")?.map(|t| {
                            match t.as_ref() {
                                "inlineStr" | "str" => CellType::InlineString,
                                "s" => CellType::SharedString,
                                "d" => CellType::IsoDateTime,
                                "b" => CellType::Boolean,
                                "e" => if criteria.error_as_null { CellType::Empty } else { CellType::Error },
                                _ => CellType::Number,
                            }
                        }).unwrap_or(CellType::Number);
                        if let Some(format_id) = event.get_attribute_value("s")? {
                            if kind == CellType::Number && !format_id.is_empty() {
                                let index = format_id.parse::<usize>()?;
                                kind = self.number_formats[index];
                            }
                        }
                    } else {
                        kind = CellType::default();
                    }
                }
                Event::Start(event) if kind != CellType::Empty && event.name() == TAG_INLINE_STRING => {
                    value = read_string_value(&mut reader, TAG_INLINE_STRING, false)?;
                }
                Event::Start(event) if kind != CellType::Empty && event.name() == TAG_VALUE => {
                    value = read_string_value(&mut reader, TAG_VALUE, true)?;
                }
                Event::End(event) if kind != CellType::Empty && !value.is_empty() && event.name() == TAG_CELL => {
                    if kind != CellType::Error {
                        if let Some(last_row) = last_row {
                            if criteria.end_at_empty_row && ((sheet.is_empty() && last_row != row) || (!sheet.is_empty() && last_row + 1 < row)) {
                                break;
                            }
                        }
                        last_row = Some(row);
                        sheet.push(Cell {
                            row,
                            col,
                            kind,
                            value: value.to_owned(),
                        });
                        value.clear();
                    } else {
                        let reference = index_to_reference(row, col);
                        Err(SpreadsheetError::CellValueError(
                            sheet.file_name.to_owned(),
                            sheet.name.to_owned(),
                            reference,
                            value.to_owned(),
                        ))?
                    }
                },
            });
            sheet.finish(criteria.end_at_empty_row);
            sheets.push(sheet);
        }

        Ok(sheets)
    }
}

/// Loads workbook structure and worksheet information from XLSX file
///
/// Parses the workbook.xml file to extract worksheet names and their corresponding
/// XML file paths, and determines the date system (1900 vs 1904) used in the file.
///
/// # Arguments
/// * `zip` - ZIP archive containing the XLSX file
///
/// # Returns
/// Tuple of (worksheets, is_1904_date_system) where worksheets are (name, zip_path) pairs
fn load_workbook(zip: &mut ZipArchive<UnifiedReader>) -> Result<(Vec<(String, String)>, bool), RustySheetError> {
    let relationships = load_relationships(zip, "xl/_rels/workbook.xml.rels")?;
    let mut reader = zip.xml_reader("xl/workbook.xml")?
        .ok_or_else(|| SpreadsheetError::FileError("xl/workbook.xml".to_string()))?;
    let mut sheets: Vec<(String, String)> = Vec::new();
    let mut is_1904 = false;
    match_xml_events!(reader => {
        Event::Start(event) if event.name() == TAG_SHEET => {
            let mut name = None::<Cow<str>>;
            let mut id = None::<Cow<str>>;
            for result in event.attributes() {
                let attribute = result?;
                let key = attribute.key.local_name();
                if key.as_ref() == b"name" {
                    name = Some(attribute.get_value()?);
                } else if key.as_ref() == b"id" {
                    id = Some(attribute.get_value()?);
                }
            }
            if let Some((name, id)) = name.zip(id) {
                if let Some(path) = relationships.get(&id.to_string()) {
                    sheets.push((name.to_string(), path.to_owned()));
                }
            }
        }
        Event::Start(event) if event.name() == TAG_WORKBOOK_PROPERTIES => {
            is_1904 = event.get_attribute_value("date1904")?
                .map(|value| value.eq("1") || value.eq("true"))
                .unwrap_or(false);
        }
    });
    Ok((sheets, is_1904))
}

/// Loads number formats and cell styles from XLSX styles.xml file
///
/// Parses custom number formats and cell style indexes to determine
/// how numeric values should be interpreted (dates, times, percentages, etc.)
///
/// # Arguments
/// * `zip` - ZIP archive containing the XLSX file
/// * `is_1904` - Whether the file uses the 1904 date system
///
/// # Returns
/// Vector of CellType values indexed by style ID
fn load_number_formats(zip: &mut ZipArchive<UnifiedReader>, is_1904: bool) -> Result<Vec<CellType>, RustySheetError> {
    let mut reader = match zip.xml_reader("xl/styles.xml")? {
        Some(reader) => reader,
        None => return Ok(Vec::new()),
    };

    let mut has_custom_formats = false;
    let mut custom_formats_context = false;
    let mut custom_formats = HashMap::<String, CellType>::new();

    let mut has_format_indexes = false;
    let mut format_indexes_context = false;
    let mut format_indexes = Vec::<String>::new();

    match_xml_events!(reader => {
        Event::Start(event) if !custom_formats_context && event.name() == TAG_CUSTOM_FORMATS => {
            has_custom_formats = true;
            custom_formats_context = true;
        }
        Event::End(event) if custom_formats_context && event.name() == TAG_CUSTOM_FORMATS => {
            custom_formats_context = false;
            if has_custom_formats && has_format_indexes {
                break;
            }
        }
        Event::Start(event) if custom_formats_context && event.name() == TAG_CUSTOM_FORMAT => {
            let id = event.get_attribute_value("numFmtId")?;
            let format = event.get_attribute_value("formatCode")?;
            if let Some((id, format)) = id.zip(format) {
                let style = CellType::parse_custom_number_format(&format, is_1904);
                custom_formats.insert(id.to_string(), style);
            }
        }

        Event::Start(event) if !format_indexes_context && event.name() == TAG_FORMAT_INDEXES => {
            has_format_indexes = true;
            format_indexes_context = true;
        }
        Event::End(event) if format_indexes_context && event.name() == TAG_FORMAT_INDEXES => {
            format_indexes_context = false;
            if has_custom_formats && has_format_indexes {
                break;
            }
        }
        Event::Start(event) if format_indexes_context && event.name() == TAG_FORMAT_INDEX => {
            if let Some(id) = event.get_attribute_value("numFmtId")? {
                format_indexes.push(id.to_string());
            }
        }
    });

    Ok(excel::load_number_formats(format_indexes, custom_formats, is_1904))
}

/// Reads string value from XML content, handling text and CDATA sections
///
/// Extracts string content from XML elements, skipping phonetic text annotations
/// and properly handling both text nodes and CDATA sections.
///
/// # Arguments
/// * `reader` - XML reader positioned at the start of the string content
/// * `end_tag` - XML tag that marks the end of the string content
/// * `is_text_content` - Whether to treat the content as text by default
///
/// # Returns
/// Extracted string value
fn read_string_value(
    reader: &mut XmlReader<BufReader<ZipFile<'_, UnifiedReader>>>,
    end_tag: QName,
    is_text_content: bool,
) -> Result<String, RustySheetError> {
    let mut is_phonetic_text = false;
    let mut is_text = is_text_content;
    let mut text = String::new();
    match_xml_events!(reader => {
        Event::End(event) if event.name() == end_tag => break,
        Event::Start(event) if event.name() == TAG_PHONETIC_TEXT => is_phonetic_text = true,
        Event::End(event) if event.name() == TAG_PHONETIC_TEXT => is_phonetic_text = false,
        Event::Start(event) if !is_phonetic_text && event.name() == TAG_TEXT => is_text = true,
        Event::End(event) if is_text && event.name() == TAG_TEXT => is_text = false,
        Event::Text(event) if is_text => text.push_str(&event.xml_content()?),
        Event::CData(event) if is_text => text.push_str(&event.xml_content()?),
        Event::GeneralRef(event) if is_text => text.push_bytes_ref(&event)?,
    });
    Ok(text)
}
