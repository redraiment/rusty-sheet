//! XML parsing utilities for OpenDocument Spreadsheet (ODS) format
//! Provides XML reader wrapper and helper traits for attribute and text processing

use crate::error::RustySheetError;
use quick_xml::escape::resolve_xml_entity;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::BytesRef;
use quick_xml::events::BytesStart;
use quick_xml::events::BytesText;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::borrow::Cow;
use std::io::BufRead;
use std::str::FromStr;
use thiserror::Error;

/// Errors specific to XML parsing operations
#[derive(Error, Debug)]
pub(crate) enum XmlError {
    #[error("Parse entity '{0}' failed")]
    ParseEntityError(String),

    #[error("Parse attribute value '{0}' failed")]
    ParseAttributeValueError(String),
}

/// XML reader wrapper with optimized configuration for spreadsheet parsing
pub(crate) struct XmlReader<R: BufRead> {
    reader: Reader<R>,
    buffer: Vec<u8>,
}

impl<R: BufRead> XmlReader<R> {
    /// Creates a new XML reader with optimized configuration for spreadsheet parsing
    pub(crate) fn new(buf_reader: R) -> XmlReader<R> {
        let mut reader = Reader::from_reader(buf_reader);
        let config = reader.config_mut();
        config.check_comments = false;
        config.check_end_names = false;
        config.expand_empty_elements = true;
        config.trim_text(false);

        let buffer = Vec::with_capacity(1024);
        XmlReader { reader, buffer }
    }

    /// Reads the next XML event from the reader
    pub(crate) fn next(&'_ mut self) -> Result<Option<Event<'_>>, RustySheetError> {
        self.buffer.clear();
        match self.reader.read_event_into(&mut self.buffer) {
            Ok(Event::Eof) => Ok(None),
            Ok(event) => Ok(Some(event)),
            Err(error) => Err(RustySheetError::XmlError(error)),
        }
    }
}

/// Helper trait for XML attributes providing convenient value extraction and parsing
pub(crate) trait XmlAttributeHelper<'a> {
    /// Gets the unescaped attribute value as a string
    fn get_value(&self) -> Result<Cow<'a, str>, RustySheetError>;

    /// Parses the attribute value to the specified type
    fn parse_value<T: FromStr>(&self) -> Result<T, RustySheetError>;
}

impl<'a> XmlAttributeHelper<'a> for Attribute<'a> {
    /// Gets the unescaped attribute value
    fn get_value(&self) -> Result<Cow<'a, str>, RustySheetError> {
        Ok(self.unescape_value()?)
    }

    /// Parses the attribute value to the specified type
    fn parse_value<T: FromStr>(&self) -> Result<T, RustySheetError> {
        self.get_value()?
            .parse()
            .map_err(|_| match str::from_utf8(&self.value) {
                Ok(value) => RustySheetError::XmlHelperError(XmlError::ParseAttributeValueError(value.to_string())),
                Err(error) => RustySheetError::StringEncodingError(error),
            })
    }
}

/// Helper trait for XML nodes providing attribute access methods
pub(crate) trait XmlNodeHelper<'a> {
    /// Gets an attribute value by name
    fn get_attribute_value(&'a self, name: &str) -> Result<Option<Cow<'a, str>>, RustySheetError>;

    /// Parses an attribute value to the specified type
    fn parse_attribute_value<T: FromStr>(&self, name: &str) -> Result<Option<T>, RustySheetError>;
}

impl<'a> XmlNodeHelper<'a> for BytesStart<'a> {
    /// Gets an attribute value by name
    fn get_attribute_value(&'a self, name: &str) -> Result<Option<Cow<'a, str>>, RustySheetError> {
        self.try_get_attribute(name)?
            .map(|attribute| attribute.get_value())
            .transpose()
    }

    /// Parses an attribute value to the specified type
    fn parse_attribute_value<T: FromStr>(&self, name: &str) -> Result<Option<T>, RustySheetError> {
        self.try_get_attribute(name)?
            .map(|attribute| attribute.parse_value())
            .transpose()
    }
}

/// Helper trait for building text content from XML events
pub(crate) trait XmlTextContextHelper {
    /// Appends text content from BytesText event
    fn push_bytes_text(&mut self, text: &BytesText) -> Result<(), RustySheetError>;

    /// Appends text content from BytesRef event (handles entities and character references)
    fn push_bytes_ref(&mut self, bytes: &BytesRef) -> Result<(), RustySheetError>;
}

impl XmlTextContextHelper for String {
    /// Appends text content from BytesText event
    fn push_bytes_text(&mut self, text: &BytesText) -> Result<(), RustySheetError> {
        self.push_str(&text.xml_content()?);
        Ok(())
    }

    /// Appends text content from BytesRef event, handling XML entities and character references
    fn push_bytes_ref(&mut self, bytes: &BytesRef) -> Result<(), RustySheetError> {
        let raw = bytes.xml_content()?;
        if let Some(number) = raw.strip_prefix('#') {
            let code = if let Some(hex) = number.strip_prefix('x') {
                u32::from_str_radix(hex, 16)?
            } else {
                u32::from_str_radix(number, 10)?
            };
            if let Some(character) = std::char::from_u32(code) {
                self.push_str(character.encode_utf8(&mut [0u8; 4]));
            }
        } else if let Some(entity) = resolve_xml_entity(&raw) {
            self.push_str(entity);
        } else {
            Err(XmlError::ParseEntityError(raw.to_string()))?;
        }

        Ok(())
    }
}

#[macro_export]
macro_rules! match_xml_events {
    ($reader:expr => { $($arms:tt)* }) => {
        while let Some(result) = $reader.next()? {
            match result {
                Event::Eof => break,
                $($arms)*
                _ => (),
            }
        }
    };
}
