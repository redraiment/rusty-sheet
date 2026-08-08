use crate::error::RustySheetError;
use crate::helpers::reader::UnifiedReader;
use crate::helpers::xml::XmlAttributeHelper;
use crate::helpers::xml::XmlNodeHelper;
use crate::helpers::xml::XmlReader;
use crate::helpers::xml::XmlTextContextHelper;
use crate::helpers::zip::ZipHelper;
use crate::match_xml_events;
use crate::spreadsheet::Spreadsheet;
use crate::spreadsheet::SpreadsheetError;
use crate::spreadsheet::cell::Cell;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::criteria::Criteria;
use crate::spreadsheet::excel;
use crate::spreadsheet::excel::load_relationships;
use crate::spreadsheet::reference::index_to_reference;
use crate::spreadsheet::reference::reference_to_index;
use crate::spreadsheet::resolve_number_format;
use crate::spreadsheet::sheet::Sheet;
use quick_xml::events::Event;
use quick_xml::name::QName;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::BufReader;
use zip::ZipArchive;
use zip::read::ZipFile;

// XML tag names for parsing Excel XLSX format
const TAG_CUSTOM_FORMATS: QName = QName(b"numFmts"); // Custom number formats container
const TAG_CUSTOM_FORMAT: QName = QName(b"numFmt");   // Individual custom number format
const TAG_FORMAT_INDEXES: QName = QName(b"cellXfs");  // Cell format indexes container
const TAG_FORMAT_INDEX: QName = QName(b"xf");         // Individual cell format index
const TAG_SHARED_STRING_ITEM: QName = QName(b"si");   // Shared string table item
const TAG_PHONETIC_TEXT: QName = QName(b"rPh");       // Phonetic text for Asian languages
const TAG_TEXT: QName = QName(b"t");                  // Text content within strings
const TAG_WORKBOOK_PROPERTIES: &[u8] = b"workbookPr"; // Workbook properties
const TAG_SHEET: &[u8] = b"sheet";             // Worksheet definition
const TAG_ROW: QName = QName(b"row");                 // Row in worksheet
const TAG_CELL: QName = QName(b"c");                  // Cell in worksheet
const TAG_INLINE_STRING: QName = QName(b"is");        // Inline string value
const TAG_VALUE: QName = QName(b"v");                 // Cell value content
const TAG_MERGE_CELLS: QName = QName(b"mergeCells"); // Merged cells container
const TAG_MERGE_CELL: QName = QName(b"mergeCell"); // Individual merged cell range

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

            // Only allocate merged cell data structures when needed
            let (mut merged_cell_map, mut merged_ranges, mut seen_cells, mut merged_cell_values) = if criteria.spread_merged_cells {
                (
                    HashMap::<(usize, usize), (usize, usize)>::new(),
                    HashMap::<(usize, usize), (usize, usize, usize, usize)>::new(),
                    HashSet::<(usize, usize)>::new(),
                    HashMap::<(usize, usize), (CellType, String)>::new(),
                )
            } else {
                // Use empty placeholders - these won't be accessed when spread_merged_cells is false
                (
                    HashMap::new(),
                    HashMap::new(),
                    HashSet::new(),
                    HashMap::new(),
                )
            };

            // Single pass: Read cells and collect merged cell ranges
            let mut reader = self.zip.xml_reader(zip_path)?.expect(sheet_name);
            let mut in_merge_cells = false;
            
            match_xml_events!(reader => {
                // Collect merged cell ranges
                Event::Start(event) if criteria.spread_merged_cells && event.name() == TAG_MERGE_CELLS => {
                    in_merge_cells = true;
                }
                Event::End(event) if criteria.spread_merged_cells && event.name() == TAG_MERGE_CELLS => {
                    in_merge_cells = false;
                }
                Event::Start(event) if criteria.spread_merged_cells && in_merge_cells && event.name() == TAG_MERGE_CELL => {
                    if let Some(ref_attr) = event.get_attribute_value("ref")? {
                        if let Some((top_row, top_col, bottom_row, bottom_col)) = parse_merge_range(&ref_attr) {
                            merged_ranges.insert((top_row, top_col), (top_row, top_col, bottom_row, bottom_col));
                            // Pre-compute all cells in merged ranges for fast lookup
                            let row_count = (bottom_row - top_row + 1) as usize;
                            let col_count = (bottom_col - top_col + 1) as usize;
                            if merged_cell_map.is_empty() {
                                merged_cell_map.reserve(row_count * col_count);
                            }
                            for merged_row in top_row..=bottom_row {
                                for merged_col in top_col..=bottom_col {
                                    merged_cell_map.insert((merged_row, merged_col), (top_row, top_col));
                                }
                            }
                        }
                    }
                }
                
                // Read cells normally
                Event::End(event) if event.name() == TAG_ROW => {
                    row_count += 1;
                    col_count = 0;
                }
                Event::Start(event) if event.name() == TAG_CELL => {
                    if let Some(reference) = event.get_attribute_value("r")? {
                        (row, col) = reference_to_index(&reference).ok_or_else(|| {
                            SpreadsheetError::CellReferenceError(
                                sheet.file_name.to_owned(),
                                sheet.name.to_owned(),
                                reference.to_string(),
                            )
                        })?;
                    } else {
                        (row, col) = (row_count, col_count);
                    }
                    col_count += 1;
                    if sheet.after_row_upper_bound(row) {
                        break;
                    }
                    let cell_in_range = sheet.contains(row, col);
                    // When spread_merged_cells is enabled, read cell type for all cells
                    // (we need values for top-left cells even if outside range)
                    if cell_in_range || criteria.spread_merged_cells {
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
                                kind = resolve_number_format(
                                    &self.number_formats,
                                    &sheet.file_name,
                                    &sheet.name,
                                    row,
                                    col,
                                    index,
                                )?;
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
                Event::End(event) if kind != CellType::Empty && !criteria.nulls.contains(&value) && event.name() == TAG_CELL => {
                    if kind != CellType::Error {
                        let cell_in_range = sheet.contains(row, col);
                        
                        // When spread_merged_cells is enabled, store ALL cell values
                        // (we'll use them in post-processing to fill merged cells)
                        if criteria.spread_merged_cells {
                            merged_cell_values.insert((row, col), (kind, value.clone()));
                        }
                        
                        // Only add to sheet if in range
                        if cell_in_range {
                            if let Some(last_row) = last_row {
                                if criteria.end_at_empty_row && ((sheet.is_empty() && last_row != row) || (!sheet.is_empty() && last_row + 1 < row)) {
                                    break;
                                }
                            }
                            last_row = Some(row);

                            // Track that we've seen this cell (only needed for merged cells)
                            if criteria.spread_merged_cells {
                                seen_cells.insert((row, col));
                            }

                            // Add the cell to the sheet
                            sheet.push(Cell {
                                row,
                                col,
                                kind,
                                value: value.clone(),
                            });
                        }
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
                }
                Event::End(event) if event.name() == TAG_CELL => {
                    value.clear();
                },
            });

            // Post-processing: Fill in merged values for cells that are part of merged ranges
            // In Excel XML, only the top-left cell of a merged range appears.
            // We need to generate cells for all other positions in merged ranges that are in our range.
            if criteria.spread_merged_cells && !merged_ranges.is_empty() {
                for (&(top_row, top_col), &(_, _, bottom_row, bottom_col)) in &merged_ranges {
                    // Get the stored value from the top-left cell
                    if let Some(&(stored_kind, ref stored_value)) = merged_cell_values.get(&(top_row, top_col)) {
                        // Iterate through all cells in the merged range
                        for merged_row in top_row..=bottom_row {
                            for merged_col in top_col..=bottom_col {
                                // Skip the top-left cell (already processed)
                                if (merged_row, merged_col) == (top_row, top_col) {
                                    continue;
                                }

                                // Only generate cells that are in the requested range
                                if !sheet.contains(merged_row, merged_col) {
                                    continue;
                                }

                                // Only generate if this cell wasn't seen in the XML
                                if seen_cells.contains(&(merged_row, merged_col)) {
                                    continue;
                                }

                                // Check if we're still within bounds
                                if sheet.after_row_upper_bound(merged_row) {
                                    continue;
                                }

                                // Generate the cell with the value from the top-left
                                // Note: We don't check end_at_empty_row here because:
                                // 1. end_at_empty_row was already applied during the main pass
                                // 2. We're just filling in cells that should exist based on merged ranges
                                // 3. These cells are part of the same merged range as cells we already processed
                                sheet.push(Cell {
                                    row: merged_row,
                                    col: merged_col,
                                    kind: stored_kind,
                                    value: stored_value.clone(),
                                });
                                seen_cells.insert((merged_row, merged_col));
                            }
                        }
                    }
                }

                // Sort cells by (row, col) to ensure correct ordering for chunk method
                // The chunk method expects cells to be in sorted order
                sheet.cells.sort_by(|a, b| match a.row.cmp(&b.row) {
                    std::cmp::Ordering::Equal => a.col.cmp(&b.col),
                    other => other,
                });
            }

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
        Event::Start(event) if event.local_name().as_ref() == TAG_SHEET => {
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
        Event::Start(event) if event.local_name().as_ref() == TAG_WORKBOOK_PROPERTIES => {
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

/// Parses a merged cell range reference (e.g., "A1:B2") into row and column indices
///
/// # Arguments
/// * `range_ref` - Excel range reference like "A1:B2"
///
/// # Returns
/// Some((top_row, top_col, bottom_row, bottom_col)) if valid, None otherwise
fn parse_merge_range(range_ref: &str) -> Option<(usize, usize, usize, usize)> {
    // Split by colon to get start and end cells
    let parts: Vec<&str> = range_ref.split(':').collect();
    if parts.len() != 2 {
        return None;
    }

    let start_ref = parts[0].trim();
    let end_ref = parts[1].trim();

    let (top_row, top_col) = reference_to_index(start_ref)?;
    let (bottom_row, bottom_col) = reference_to_index(end_ref)?;

    Some((top_row, top_col, bottom_row, bottom_col))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spreadsheet::criteria::Criteria;
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    #[test]
    fn invalid_style_index_returns_error() {
        let path = invalid_workbook_path("style");
        write_invalid_style_workbook(&path);

        let mut spreadsheet = XlsxSpreadsheet::open(path.to_str().unwrap()).unwrap();
        let result = spreadsheet.read_sheets(&default_criteria());

        std::fs::remove_file(path).unwrap();
        let error = match result {
            Ok(_) => panic!("expected invalid style index error"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Sheet1!A11"));
        assert!(error.contains("invalid style index 999"));
        assert!(error.contains("workbook defines 1 styles"));
    }

    #[test]
    fn invalid_shared_string_index_returns_error() {
        let path = invalid_workbook_path("shared-string");
        write_invalid_shared_string_workbook(&path);

        let mut spreadsheet = XlsxSpreadsheet::open(path.to_str().unwrap()).unwrap();
        let result = spreadsheet.analyze_sheets(true, &default_criteria(), &Vec::new());

        std::fs::remove_file(path).unwrap();
        let error = match result {
            Ok(_) => panic!("expected invalid shared string index error"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Sheet1!A1"));
        assert!(error.contains("invalid shared string index 999"));
    }

    #[test]
    fn invalid_cell_reference_returns_error() {
        let path = invalid_workbook_path("cell-reference");
        write_invalid_cell_reference_workbook(&path);

        let mut spreadsheet = XlsxSpreadsheet::open(path.to_str().unwrap()).unwrap();
        let result = spreadsheet.read_sheets(&default_criteria());

        std::fs::remove_file(path).unwrap();
        let error = match result {
            Ok(_) => panic!("expected invalid cell reference error"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Sheet1"));
        assert!(error.contains("invalid cell reference 'XFE1'"));
    }

    #[test]
    fn invalid_date_header_value_returns_error() {
        let path = invalid_workbook_path("date-header");
        write_invalid_date_header_workbook(&path);

        let mut spreadsheet = XlsxSpreadsheet::open(path.to_str().unwrap()).unwrap();
        let result = spreadsheet.analyze_sheets(true, &default_criteria(), &Vec::new());

        std::fs::remove_file(path).unwrap();
        let error = match result {
            Ok(_) => panic!("expected invalid date header value error"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Sheet1!A1"));
        assert!(error.contains("parse 'not-a-number' to Date(1900) failed"));
    }

    fn invalid_workbook_path(kind: &str) -> PathBuf {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rusty-sheet-invalid-{kind}-{id}.xlsx"))
    }

    fn default_criteria() -> Criteria {
        Criteria {
            sheet_name_patterns: None,
            sheet_limit: None,
            range: None,
            rows_limit: None,
            nulls: HashSet::from(["".to_string()]),
            error_as_null: false,
            skip_empty_rows: false,
            end_at_empty_row: false,
            spread_merged_cells: false,
        }
    }

    fn write_invalid_style_workbook(path: &Path) {
        let file = File::create(path).unwrap();
        let mut zip = ZipWriter::new(file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        let rows = (2..11)
            .map(|row| format!("<row r=\"{row}\"><c r=\"A{row}\"><v>{row}</v></c></row>"))
            .collect::<String>();
        let sheet = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>value</t></is></c></row>{rows}<row r="11"><c r="A11" s="999"><v>11</v></c></row></sheetData>
</worksheet>"#
        );

        for (name, content) in [
            (
                "[Content_Types].xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"#.to_string(),
            ),
            (
                "_rels/.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/workbook.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#.to_string(),
            ),
            (
                "xl/_rels/workbook.xml.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/styles.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <cellXfs count="1"><xf numFmtId="0"/></cellXfs>
</styleSheet>"#.to_string(),
            ),
            ("xl/worksheets/sheet1.xml", sheet),
        ] {
            zip.start_file(name, options).unwrap();
            zip.write_all(content.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }

    fn write_invalid_shared_string_workbook(path: &Path) {
        let file = File::create(path).unwrap();
        let mut zip = ZipWriter::new(file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        for (name, content) in [
            (
                "[Content_Types].xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
</Types>"#.to_string(),
            ),
            (
                "_rels/.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/workbook.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#.to_string(),
            ),
            (
                "xl/_rels/workbook.xml.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/sharedStrings.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="1" uniqueCount="1">
  <si><t>name</t></si>
</sst>"#.to_string(),
            ),
            (
                "xl/worksheets/sheet1.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData><row r="1"><c r="A1" t="s"><v>999</v></c></row></sheetData>
</worksheet>"#.to_string(),
            ),
        ] {
            zip.start_file(name, options).unwrap();
            zip.write_all(content.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }

    fn write_invalid_cell_reference_workbook(path: &Path) {
        let file = File::create(path).unwrap();
        let mut zip = ZipWriter::new(file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        for (name, content) in [
            (
                "[Content_Types].xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"#.to_string(),
            ),
            (
                "_rels/.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/workbook.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#.to_string(),
            ),
            (
                "xl/_rels/workbook.xml.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/worksheets/sheet1.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData><row r="1"><c r="XFE1" t="inlineStr"><is><t>value</t></is></c></row></sheetData>
</worksheet>"#.to_string(),
            ),
        ] {
            zip.start_file(name, options).unwrap();
            zip.write_all(content.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }

    fn write_invalid_date_header_workbook(path: &Path) {
        let file = File::create(path).unwrap();
        let mut zip = ZipWriter::new(file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        for (name, content) in [
            (
                "[Content_Types].xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"#.to_string(),
            ),
            (
                "_rels/.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/workbook.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>
</workbook>"#.to_string(),
            ),
            (
                "xl/_rels/workbook.xml.rels",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"#.to_string(),
            ),
            (
                "xl/styles.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <cellXfs count="1"><xf numFmtId="14"/></cellXfs>
</styleSheet>"#.to_string(),
            ),
            (
                "xl/worksheets/sheet1.xml",
                r#"<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData><row r="1"><c r="A1" s="0"><v>not-a-number</v></c></row></sheetData>
</worksheet>"#.to_string(),
            ),
        ] {
            zip.start_file(name, options).unwrap();
            zip.write_all(content.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }
}
