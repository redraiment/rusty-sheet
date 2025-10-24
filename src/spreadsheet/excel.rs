//! Microsoft Office Excel Helpers
use crate::error::RustySheetError;
use crate::helpers::cfb::Cfb;
use crate::helpers::reader::UnifiedReader;
use crate::helpers::xml::XmlNodeHelper;
use crate::helpers::zip::ZipHelper;
use crate::match_xml_events;
use crate::spreadsheet::cell::CellType;
use crate::spreadsheet::SpreadsheetError;
use quick_xml::events::Event;
use std::borrow::Cow;
use std::collections::HashMap;
use zip::ZipArchive;

/// XML tag name for relationship elements in Excel files
const TAG_RELATIONSHIP: &[u8] = b"Relationship";

/// Opens an Excel file and loads its contents
///
/// # Arguments
/// * `file_name` - Path to the Excel file
/// * `load_workbook` - Function to load workbook metadata and sheets
/// * `load_number_formats` - Function to load number formatting information
///
/// # Returns
/// Tuple containing:
/// - Zip archive handle
/// - Number format mappings
/// - List of sheet names and their paths
pub(super) fn open<W, F>(file_name: &str, load_workbook: W, load_number_formats: F) -> Result<(
    ZipArchive<UnifiedReader>,
    Vec<CellType>,
    Vec<(String, String)>
), RustySheetError>
where
    W: Fn(&mut ZipArchive<UnifiedReader>) -> Result<(Vec<(String, String)>, bool), RustySheetError>,
    F: Fn(&mut ZipArchive<UnifiedReader>, bool) -> Result<Vec<CellType>, RustySheetError>,
{
    // Open file from local path or remote URL
    let mut reader = UnifiedReader::new(file_name)?;
    
    // Check if password protected
    if is_password_protected(&mut reader) {
        Err(SpreadsheetError::SpreadsheetPasswordProtectedError(file_name.to_owned()))?;
    }

    let mut zip = ZipArchive::new(reader)?;
    let (sheets, is_1904) = load_workbook(&mut zip)?;
    if sheets.is_empty() {
        Err(SpreadsheetError::SpreadsheetEmptyError(file_name.to_owned()))?
    }

    let number_formats = load_number_formats(&mut zip, is_1904)?;
    Ok((zip, number_formats, sheets))
}

/// Loads worksheet relationships from an Excel file
///
/// # Arguments
/// * `zip` - Zip archive handle
/// * `path` - Path to the relationships XML file within the archive
///
/// # Returns
/// Mapping of relationship IDs to worksheet paths
pub(super) fn load_relationships(zip: &mut ZipArchive<UnifiedReader>, path: &str) -> Result<HashMap<String, String>, RustySheetError> {
    let mut reader = zip.xml_reader(path)?
        .ok_or_else(|| SpreadsheetError::FileError(path.to_string()))?;
    let mut relationships: HashMap<String, String> = HashMap::new();
    match_xml_events!(reader => {
        Event::Start(event) if event.local_name().as_ref() == TAG_RELATIONSHIP => {
            let id = event.get_attribute_value("Id")?;
            let kind = event.get_attribute_value("Type")?;
            let target = event.get_attribute_value("Target")?;
            // Only process worksheet relationships
            if kind.map(|it| it.ends_with("/worksheet")).unwrap_or(true) {
                if let Some((id, target)) = id.zip(target) {
                    relationships.insert(id.to_string(), to_zip_path(target));
                }
            }
        }
    });
    Ok(relationships)
}

/// Maps format indexes to cell types using custom and built-in formats
///
/// # Arguments
/// * `format_indexes` - List of format identifiers
/// * `custom_formats` - Custom format mappings defined in the workbook
/// * `is_1904` - Whether the workbook uses the 1904 date system
///
/// # Returns
/// Vector of cell types corresponding to each format index
pub(super) fn load_number_formats(format_indexes: Vec<String>, custom_formats: HashMap<String, CellType>, is_1904: bool) -> Vec<CellType> {
    format_indexes
        .iter()
        .map(|id| {
            custom_formats
                .get(id)
                .map(Clone::clone)
                .or_else(|| CellType::parse_builtin_number_format_id(id, is_1904))
                .unwrap_or(CellType::Number)
        })
        .collect()
}

/// Normalizes a path to ensure it points to the correct location within the Excel zip archive
///
/// # Arguments
/// * `path` - Original path from relationship or reference
///
/// # Returns
/// Normalized path suitable for accessing files within the zip archive
pub(crate) fn to_zip_path(path: Cow<'_, str>) -> String {
    if path.starts_with("/xl/") {
        path[1..].to_string()
    } else if path.starts_with("xl/") {
        path.to_string()
    } else {
        format!("xl/{path}")
    }
}

/// Checks if an Excel file is password protected
///
/// # Arguments
/// * `reader` - File reader positioned at the beginning of the file
///
/// # Returns
/// `true` if the file contains an encrypted package, `false` otherwise
fn is_password_protected(reader: &mut UnifiedReader) -> bool {
    if let Ok(cfb) = Cfb::new(reader) {
        cfb.exists("EncryptedPackage")
    } else {
        false
    }
}
