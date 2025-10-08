//! Microsoft Office Binary Interchange File Format (BIFF8)
//! Reader for Excel 97-2003 binary format (.xls files)
//! Handles the record-based binary format used in legacy Excel files

use crate::error::RustySheetError;
use crate::helpers::string::to_f64;
use crate::helpers::string::to_u16;
use crate::helpers::string::to_u32;
use crate::helpers::string::to_u64;
use crate::helpers::string::to_usize;
use encoding_rs::Encoding;
use thiserror::Error;

const CONTINUE: u16 = 60;

/// Errors specific to BIFF8 format parsing
#[derive(Error, Debug)]
pub(crate) enum Biff8Error {
    #[error("Fewer than {0} bytes remaining")]
    NoEnoughDataError(usize),
}

/// Reader for BIFF8 (Excel 97-2003) binary format
/// Handles the record-based structure with continuation records
pub(crate) struct Biff8Reader {
    pub(crate) encoding: &'static Encoding,
    buffer: Vec<u8>,
    pointer: usize, // Next read position in buffer
    chunks: Vec<(usize, usize)>, // Current record chunks (start, end)
    index: usize,  // Current chunk index
    offset: usize, // Offset within current chunk
}

impl Biff8Reader {
    /// Creates a new BIFF8 reader with the given data buffer
    pub(crate) fn new(data: Vec<u8>) -> Biff8Reader {
        Biff8Reader {
            encoding: &encoding_rs::UTF_16LE,
            buffer: data,
            pointer: 0,
            chunks: Vec::new(),
            index: 0,
            offset: 0,
        }
    }

    /// Reads the next record type and prepares for reading record data
    /// Returns None when no more records are available
    pub(crate) fn next(&mut self) -> Result<Option<u16>, RustySheetError> {
        if self.pointer + 4 < self.buffer.len() {
            self.index = 0;
            self.offset = 0;

            let kind = self.get_u16_at(self.pointer)?;
            let size = self.get_u16_at(self.pointer + 2)? as usize;
            let mut lower = self.pointer + 4;
            let mut upper = lower + size;
            self.pointer = upper;

            self.chunks.clear();
            self.chunks.push((lower, upper));
            while self.pointer + 4 < self.buffer.len() && self.get_u16_at(self.pointer)? == CONTINUE {
                let size = self.get_u16_at(self.pointer + 2)? as usize;
                lower = self.pointer + 4;
                upper = lower + size;
                self.pointer = upper;
                self.chunks.push((lower, upper));
            }

            Ok(Some(kind))
        } else {
            Ok(None)
        }
    }

    /// Sets the reader pointer to a specific position
    pub(crate) fn goto(&mut self, pointer: usize) {
        self.pointer = pointer;
    }

    /// Reads exactly `length` bytes, returning an error if insufficient data
    fn read_extract(&mut self, length: usize) -> Result<&[u8], RustySheetError> {
        let (data, size) = self.read(length);
        if size == length {
            Ok(data)
        } else {
            Err(Biff8Error::NoEnoughDataError(length))?
        }
    }

    /// Reads up to `length` bytes from the current record
    /// Returns the data slice and actual number of bytes read
    fn read(&mut self, length: usize) -> (&[u8], usize) {
        if let Some((lower, upper)) = self.chunks.get(self.index) {
            let source = (*upper).min(*lower + self.offset);
            let target = (*upper).min(source + length);
            let size = target - source;
            if source < *upper {
                if target == *upper {
                    self.index += 1;
                    self.offset = 0;
                } else {
                    self.offset += size;
                }
                return (&self.buffer[source..target], size);
            }
        }
        (&[], 0)
    }

    /// Skips `length` bytes and returns the skipped data
    pub(crate) fn skip(&mut self, length: usize) -> Result<&[u8], RustySheetError> {
        self.read_extract(length)
    }

    /// Reads a single byte
    pub(crate) fn read_u8(&mut self) -> Result<u8, RustySheetError> {
        self.read_extract(1).map(|data| data[0])
    }

    /// Reads a 16-bit unsigned integer
    pub(crate) fn read_u16(&mut self) -> Result<u16, RustySheetError> {
        self.read_extract(2).map(to_u16)
    }

    /// Gets a 16-bit unsigned integer from the specified offset from the end
    pub(crate) fn get_u16_back(&self, offset: usize) -> Result<u16, RustySheetError> {
        let mut offset = offset;
        for (lower, upper) in self.chunks.iter().rev() {
            if *lower + offset < *upper {
                let index = *upper - offset;
                return self.get_u16_at(index);
            } else {
                offset -= *upper - *lower;
            }
        }
        Err(Biff8Error::NoEnoughDataError(2))?
    }

    /// Gets a 16-bit unsigned integer from the specified absolute position
    pub(crate) fn get_u16_at(&self, index: usize) -> Result<u16, RustySheetError> {
        if index + 2 <= self.buffer.len() {
            Ok(to_u16(&self.buffer[index..index + 2]))
        } else {
            Err(Biff8Error::NoEnoughDataError(2))?
        }
    }

    /// Reads a 32-bit unsigned integer
    pub(crate) fn read_u32(&mut self) -> Result<u32, RustySheetError> {
        self.read_extract(4).map(to_u32)
    }

    /// Reads a usize value
    pub(crate) fn read_usize(&mut self) -> Result<usize, RustySheetError> {
        self.read_extract(4).map(to_usize)
    }

    /// Reads a 64-bit unsigned integer
    pub(crate) fn read_u64(&mut self) -> Result<u64, RustySheetError> {
        self.read_extract(8).map(to_u64)
    }

    /// Reads a 64-bit floating point number
    pub(crate) fn read_f64(&mut self) -> Result<f64, RustySheetError> {
        self.read_extract(8).map(to_f64)
    }

    /// Reads an RK number (compressed numeric format used in Excel)
    /// RK numbers can store integers or floats with optional percentage formatting
    pub(crate) fn read_rk_number(&mut self) -> Result<String, RustySheetError> {
        let value = self.read_u32()?;
        let is_percentage = (value & 0x01) != 0;
        let is_integer = (value & 0x02) != 0;

        let mut value = if is_integer {
            ((value as i32) >> 2) as f64
        } else {
            let value = (value >> 2) as u64;
            f64::from_bits(value << 34)
        };
        if is_percentage {
            value /= 100.0;
        }
        Ok(if is_integer {
            (value.trunc() as i64).to_string()
        } else {
            value.to_string()
        })
    }

    /// Reads a short Unicode string (1-byte length prefix)
    pub(crate) fn read_short_xl_unicode_string(&mut self) -> Result<String, RustySheetError> {
        let mut string = String::new();
        let chars = self.read_u8()? as usize;
        self.read_string_into(chars, false, &mut string)?;
        Ok(string)
    }

    /// Reads a Unicode string (2-byte length prefix)
    pub(crate) fn read_xl_unicode_string(&mut self) -> Result<String, RustySheetError> {
        let mut string = String::new();
        let chars = self.read_u16()? as usize;
        self.read_string_into(chars, false, &mut string)?;
        Ok(string)
    }

    /// Reads a rich extended Unicode string with formatting information
    pub(crate) fn read_xl_unicode_rich_extended_string(&mut self) -> Result<String, RustySheetError> {
        let mut string = String::new();
        let mut expected = self.read_u16()? as usize;
        let mut actual = self.read_string_into(expected, true, &mut string)?;
        while actual < expected {
            expected -= actual;
            actual = self.read_string_into(expected, false, &mut string)?;
        }
        Ok(string)
    }

    /// Reads string data into the provided content buffer
    /// Handles rich text formatting and phonetic information
    fn read_string_into(&mut self, chars: usize, is_extend: bool, content: &mut String) -> Result<usize, RustySheetError> {
        let encoding = self.encoding;
        let flag = self.read_u8()?;
        let is_high_byte = (flag & 0x1) > 0;
        let expected = Self::chars_to_bytes(is_high_byte, chars);
        let rich_string_count = if is_extend && (flag & 0x8) > 0 { // is_rich_string
            self.read_u16()? as usize
        } else {
            0
        };
        let phonetic_count = if is_extend && (flag & 0x4) > 0 { // contains_phonetic
            self.read_usize()?
        } else {
            0
        };
        let (bytes, actual) = self.read(expected);
        if is_high_byte {
            let (string, _, _) = encoding.decode(bytes);
            content.push_str(&string);
        } else {
            let u16s = bytes.iter().map(|byte| *byte as u16).collect::<Vec<u16>>();
            let string = String::from_utf16(&u16s).expect("ASCII string");
            content.push_str(&string);
        }
        // Skip rgRun
        self.skip(4 * rich_string_count)?;
        // Skip ExtRst
        self.skip(phonetic_count)?;
        Ok(Self::bytes_to_chars(is_high_byte, actual))
    }

    /// Converts character count to byte count based on encoding
    #[inline]
    fn chars_to_bytes(is_high_byte: bool, chars: usize) -> usize {
        if is_high_byte { chars << 1 } else { chars }
    }

    /// Converts byte count to character count based on encoding
    #[inline]
    fn bytes_to_chars(is_high_byte: bool, bytes: usize) -> usize {
        if is_high_byte { bytes >> 1 } else { bytes }
    }
}

#[macro_export]
macro_rules! match_biff8_record {
    ($reader:expr => { $($arms:tt)* }) => {
        while let Some(kind) = $reader.next()? {
            match kind {
                $($arms)*
                _ => (),
            }
        }
    };
}
