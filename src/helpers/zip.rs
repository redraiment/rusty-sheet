//! ZIP archive helper utilities for Excel (.xlsx) and OpenDocument (.ods) formats
//! Provides convenient methods for accessing files within ZIP archives

use crate::error::RustySheetError;
use crate::helpers::biff12::Biff12Reader;
use crate::helpers::xml::XmlReader;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use zip::read::ZipFile;
use zip::result::ZipError;
use zip::ZipArchive;

/// Helper trait for ZIP archive operations with specialized reader creation
pub(crate) trait ZipHelper<RS: Read + Seek> {
    /// Gets a file from the ZIP archive by name (case-insensitive, path separator agnostic)
    fn file(&'_ mut self, name: &str) -> Result<Option<ZipFile<'_, RS>>, RustySheetError>;

    /// Creates an XML reader for a file within the ZIP archive
    fn xml_reader(
        &'_ mut self,
        name: &str,
    ) -> Result<Option<XmlReader<BufReader<ZipFile<'_, RS>>>>, RustySheetError>;

    /// Creates a BIFF12 reader for a file within the ZIP archive
    fn biff_reader(
        &'_ mut self,
        name: &str,
    ) -> Result<Option<Biff12Reader<BufReader<ZipFile<'_, RS>>>>, RustySheetError>;
}

impl<RS: Read + Seek> ZipHelper<RS> for ZipArchive<RS> {
    /// Gets a file from the ZIP archive by name with case-insensitive matching
    /// and path separator normalization (backslash to forward slash)
    fn file(&'_ mut self, name: &str) -> Result<Option<ZipFile<'_, RS>>, RustySheetError> {
        let pattern = name.replace('\\', "/");
        let path = self.file_names()
            .find(|file_name| pattern.eq_ignore_ascii_case(*file_name))
            .map(|file_name| file_name.to_owned());
        match path.map(|file_name| self.by_name(&file_name)).transpose() {
            Ok(Some(file)) => Ok(Some(file)),
            Ok(None) | Err(ZipError::FileNotFound) => Ok(None),
            Err(error) => Err(error)?,
        }
    }

    /// Creates an XML reader for a file within the ZIP archive
    fn xml_reader(
        &'_ mut self,
        name: &str,
    ) -> Result<Option<XmlReader<BufReader<ZipFile<'_, RS>>>>, RustySheetError> {
        let reader = self
            .file(name)?
            .map(|file| XmlReader::new(BufReader::new(file)));
        Ok(reader)
    }

    /// Creates a BIFF12 reader for a file within the ZIP archive
    fn biff_reader(
        &'_ mut self,
        name: &str,
    ) -> Result<Option<Biff12Reader<BufReader<ZipFile<'_, RS>>>>, RustySheetError> {
        let reader = self
            .file(name)?
            .map(|file| Biff12Reader::new(BufReader::new(file)));
        Ok(reader)
    }
}
