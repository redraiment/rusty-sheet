//! Microsoft Office Binary Interchange File Format (BIFF12)
//! Reader for Excel 2007+ binary format (.xlsb files)
//! Handles the compressed binary format used in modern Excel files

use crate::error::RustySheetError;
use crate::helpers::string::to_f64;
use crate::helpers::string::to_i32;
use crate::helpers::string::to_u16;
use crate::helpers::string::to_u32;
use crate::helpers::string::to_usize;
use encoding_rs::UTF_16LE;
use std::borrow::Cow;
use std::io::BufRead;
use thiserror::Error;

/// Errors specific to BIFF12 format parsing
#[derive(Error, Debug)]
pub(crate) enum Biff12Error {
    #[error("No enough data: expect '{0}' bytes, actual '{1}' bytes")]
    NoEnoughData(usize, usize),
}

/// Reader for BIFF12 (Excel 2007+) binary format
/// Handles the compressed record-based structure of .xlsb files
pub(crate) struct Biff12Reader<R: BufRead> {
    reader: R,
    pub(crate) buffer: Vec<u8>,
}

impl<R: BufRead> Biff12Reader<R> {
    /// Creates a new BIFF12 reader with the given buffered reader
    pub(crate) fn new(reader: R) -> Biff12Reader<R> {
        Biff12Reader {
            reader,
            buffer: vec![0; 1024],
        }
    }

    /// Reads a UTF-16 string from the specified position and returns both the string and its upper bound
    /// Returns the decoded string and the position after the string data
    pub(crate) fn get_str_and_bound(
        &'_ self,
        at: usize,
    ) -> Result<(Cow<'_, str>, usize), RustySheetError> {
        let lower_bound = at + 4usize;
        let size = to_usize(&self.buffer[at..at + lower_bound]);
        let upper_bound = lower_bound + size * 2;
        if self.buffer.len() >= upper_bound {
            let (value, _, _) = UTF_16LE.decode(&self.buffer[lower_bound..upper_bound]);
            Ok((value, upper_bound))
        } else {
            Err(Biff12Error::NoEnoughData(upper_bound, self.buffer.len()))?
        }
    }

    /// Reads a UTF-16 string from the specified position
    pub(crate) fn get_str(&'_ self, at: usize) -> Result<Cow<'_, str>, RustySheetError> {
        let (data, _) = self.get_str_and_bound(at)?;
        Ok(data)
    }

    /// Reads a usize value from the specified position
    pub(crate) fn get_usize(&'_ self, at: usize) -> usize {
        to_usize(&self.buffer[at..at + 4])
    }

    /// Reads a u16 value from the specified position
    pub(crate) fn get_u16(&'_ self, at: usize) -> u16 {
        to_u16(&self.buffer[at..at + 2])
    }

    /// Reads a u32 value from the specified position
    pub(crate) fn get_u32(&'_ self, at: usize) -> u32 {
        to_u32(&self.buffer[at..at + 4])
    }

    /// Reads an i32 value from the specified position
    pub(crate) fn get_i32(&'_ self, at: usize) -> i32 {
        to_i32(&self.buffer[at..at + 4])
    }

    /// Reads an f64 value from the specified position
    pub(crate) fn get_f64(&'_ self, at: usize) -> f64 {
        to_f64(&self.buffer[at..at + 8])
    }

    /// Reads a style index from the specified position (3 bytes, padded to 4)
    pub(crate) fn get_style(&'_ self, at: usize) -> usize {
        to_usize(&[self.buffer[at], self.buffer[at + 1], self.buffer[at + 2], 0])
    }

    /// Reads a 7-bit continuation integer with the specified byte limit
    /// BIFF12 uses variable-length integers where each byte contributes 7 bits
    /// and the high bit indicates continuation
    fn read_7bit_continuation_integer(&mut self, limit: usize) -> Result<usize, RustySheetError> {
        let mut integer = 0usize;
        for index in 0..limit {
            self.reader.read_exact(&mut self.buffer[0..1])?;
            let byte = self.buffer[0];
            integer += ((byte & 0x7F) as usize) << (7 * index);
            if (byte & 0x80) == 0 {
                break;
            }
        }
        Ok(integer)
    }

    /// Returns the next record type (tag) without reading the record data
    pub(crate) fn next(&mut self) -> Result<u16, RustySheetError> {
        let (kind, _) = self.read()?;
        Ok(kind)
    }

    /// Reads the next record into the buffer and returns the record type and data size
    pub(crate) fn read(&mut self) -> Result<(u16, usize), RustySheetError> {
        let kind = self.read_7bit_continuation_integer(2)? as u16;
        let size = self.read_7bit_continuation_integer(4)?;
        if size > self.buffer.len() {
            // Insufficient space, reallocate buffer
            self.buffer = vec![0u8; size];
        }
        self.reader.read_exact(&mut self.buffer[..size])?;

        Ok((kind, size))
    }

    /// Finds a record of the specified type while skipping records in the skip ranges
    /// Returns the size of the found record
    pub(crate) fn find_with(
        &mut self,
        target: u16,
        skips: &[(u16, u16)],
    ) -> Result<usize, RustySheetError> {
        let mut expected = target;
        loop {
            let (actual, size) = self.read()?;
            if actual == expected && expected == target {
                return Ok(size);
            } else if actual == expected {
                expected = target;
            } else if let Some((_, ending)) =
                skips.iter().find(|(beginning, _)| actual == *beginning)
            {
                expected = *ending;
            }
        }
    }

    /// Finds a record of the specified type without any skip ranges
    pub(crate) fn find(&mut self, target: u16) -> Result<usize, RustySheetError> {
        self.find_with(target, &[])
    }
}

#[macro_export]
macro_rules! match_biff12_record {
    ($reader:expr => { $($arms:tt)* }) => {
        loop {
            match $reader.next()? {
                $($arms)*
                _ => (),
            }
        }
    };
}
