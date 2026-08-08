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
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use thiserror::Error;

const CONTINUE: u16 = 60;
const MAX_CACHED_RECORD_SIZE: usize = 64 * 1024;

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
    file: File,
    file_position: Option<usize>,
    buffered_file: BufReader<File>,
    buffered_position: Option<usize>,
    length: usize,
    pointer: usize, // Next record position in the file
    cache: Vec<u8>,
    cache_chunks: Vec<(usize, usize)>, // Current cached record chunks (start, end)
    cached: bool,
    record_start: usize,
    record_end: usize,
    record_length: usize,
    file_chunk: (usize, usize),
    index: usize,  // Current chunk index
    offset: usize, // Offset within current chunk
}

impl Biff8Reader {
    /// Creates a new BIFF8 reader backed by a seekable temporary file.
    pub(crate) fn new(file: File, length: usize) -> Result<Biff8Reader, RustySheetError> {
        let buffered_file = BufReader::with_capacity(64 * 1024, file.try_clone()?);
        Ok(Biff8Reader {
            encoding: &encoding_rs::UTF_16LE,
            file,
            file_position: Some(0),
            buffered_file,
            buffered_position: None,
            length,
            pointer: 0,
            cache: Vec::new(),
            cache_chunks: Vec::new(),
            cached: false,
            record_start: 0,
            record_end: 0,
            record_length: 0,
            file_chunk: (0, 0),
            index: 0,
            offset: 0,
        })
    }

    /// Reads the next record type and prepares for reading record data
    /// Returns None when no more records are available
    pub(crate) fn next(&mut self) -> Result<Option<u16>, RustySheetError> {
        if self.pointer + 4 < self.length {
            self.index = 0;
            self.offset = 0;
            self.cache.clear();
            self.cache_chunks.clear();

            let record_start = self.pointer;
            let kind = self.read_file_u16_at(self.pointer)?;
            let size = self.read_file_u16_at(self.pointer + 2)? as usize;
            let mut lower = self.pointer + 4;
            let mut upper = lower + size;
            self.pointer = upper;
            let first_chunk = (lower, upper);
            let mut record_length = size;
            let mut cacheable = record_length <= MAX_CACHED_RECORD_SIZE;
            if cacheable {
                self.cache_chunks.push((lower, upper));
            }

            while self.pointer + 4 < self.length && self.read_file_u16_at(self.pointer)? == CONTINUE
            {
                let size = self.read_file_u16_at(self.pointer + 2)? as usize;
                lower = self.pointer + 4;
                upper = lower + size;
                self.pointer = upper;
                record_length += size;
                if cacheable && record_length <= MAX_CACHED_RECORD_SIZE {
                    self.cache_chunks.push((lower, upper));
                } else {
                    cacheable = false;
                    self.cache_chunks.clear();
                }
            }

            self.cached = cacheable;
            self.record_start = record_start;
            self.record_end = upper;
            self.record_length = record_length;
            self.file_chunk = first_chunk;

            if self.cached {
                self.cache.reserve(record_length);
                for index in 0..self.cache_chunks.len() {
                    let (file_lower, file_upper) = self.cache_chunks[index];
                    let cache_lower = self.cache.len();
                    self.seek_file(file_lower)?;
                    self.cache.resize(cache_lower + file_upper - file_lower, 0);
                    self.file.read_exact(&mut self.cache[cache_lower..])?;
                    self.file_position = Some(file_upper);
                    let cache_upper = self.cache.len();
                    self.cache_chunks[index] = (cache_lower, cache_upper);
                }
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
    /// Reads up to `length` bytes from the current record
    /// Returns the data slice and actual number of bytes read
    fn read(&mut self, length: usize) -> Result<(Vec<u8>, usize), RustySheetError> {
        if let Some((source, size)) = self.take_range(length)? {
            return Ok((self.read_at(source, size)?, size));
        }
        Ok((Vec::new(), 0))
    }

    fn take_range(&mut self, length: usize) -> Result<Option<(usize, usize)>, RustySheetError> {
        let chunk = if self.cached {
            self.cache_chunks.get(self.index).copied()
        } else if self.file_chunk.0 < self.file_chunk.1 {
            Some(self.file_chunk)
        } else {
            None
        };

        let Some((lower, upper)) = chunk else {
            return Ok(None);
        };
        let source = upper.min(lower + self.offset);
        let target = upper.min(source + length);
        let size = target - source;
        if source >= upper {
            return Ok(None);
        }

        if target == upper {
            self.offset = 0;
            if self.cached {
                self.index += 1;
            } else if upper < self.record_end {
                let mut header = [0_u8; 4];
                self.read_buffered_at(upper, &mut header)?;
                let size = to_u16(&header[2..]) as usize;
                self.file_chunk = (upper + 4, upper + 4 + size);
            } else {
                self.file_chunk = (upper, upper);
            }
        } else {
            self.offset += size;
        }
        Ok(Some((source, size)))
    }

    /// Skips `length` bytes in the current record.
    pub(crate) fn skip(&mut self, length: usize) -> Result<(), RustySheetError> {
        let size = self.take_range(length)?.map_or(0, |(_, size)| size);
        if size == length {
            Ok(())
        } else {
            Err(Biff8Error::NoEnoughDataError(length))?
        }
    }

    /// Reads a single byte
    pub(crate) fn read_u8(&mut self) -> Result<u8, RustySheetError> {
        self.read_array::<1>().map(|data| data[0])
    }

    /// Reads a 16-bit unsigned integer
    pub(crate) fn read_u16(&mut self) -> Result<u16, RustySheetError> {
        self.read_array::<2>().map(|data| to_u16(&data))
    }

    /// Gets a 16-bit unsigned integer from the specified offset from the end
    pub(crate) fn get_u16_back(&mut self, offset: usize) -> Result<u16, RustySheetError> {
        let Some(mut logical_offset) = self.record_length.checked_sub(offset) else {
            return Err(Biff8Error::NoEnoughDataError(2).into());
        };

        if self.cached {
            for (lower, upper) in self.cache_chunks.iter().copied() {
                let chunk_length = upper - lower;
                if logical_offset < chunk_length && logical_offset + 2 <= chunk_length {
                    return Ok(to_u16(
                        &self.cache[lower + logical_offset..lower + logical_offset + 2],
                    ));
                }
                logical_offset = logical_offset.saturating_sub(chunk_length);
            }
        } else {
            let mut header = self.record_start;
            loop {
                let size = self.read_file_u16_at(header + 2)? as usize;
                if logical_offset < size && logical_offset + 2 <= size {
                    return self.read_file_u16_at(header + 4 + logical_offset);
                }
                logical_offset = logical_offset.saturating_sub(size);
                let next_header = header + 4 + size;
                if next_header >= self.record_end {
                    break;
                }
                header = next_header;
            }
        }
        Err(Biff8Error::NoEnoughDataError(2).into())
    }

    /// Reads a 32-bit unsigned integer
    pub(crate) fn read_u32(&mut self) -> Result<u32, RustySheetError> {
        self.read_array::<4>().map(|data| to_u32(&data))
    }

    /// Reads a usize value
    pub(crate) fn read_usize(&mut self) -> Result<usize, RustySheetError> {
        self.read_array::<4>().map(|data| to_usize(&data))
    }

    /// Reads a 64-bit unsigned integer
    pub(crate) fn read_u64(&mut self) -> Result<u64, RustySheetError> {
        self.read_array::<8>().map(|data| to_u64(&data))
    }

    /// Reads a 64-bit floating point number
    pub(crate) fn read_f64(&mut self) -> Result<f64, RustySheetError> {
        self.read_array::<8>().map(|data| to_f64(&data))
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
    pub(crate) fn read_xl_unicode_rich_extended_string(
        &mut self,
    ) -> Result<String, RustySheetError> {
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
    fn read_string_into(
        &mut self,
        chars: usize,
        is_extend: bool,
        content: &mut String,
    ) -> Result<usize, RustySheetError> {
        let encoding = self.encoding;
        let flag = self.read_u8()?;
        let is_high_byte = (flag & 0x1) > 0;
        let expected = Self::chars_to_bytes(is_high_byte, chars);
        let rich_string_count = if is_extend && (flag & 0x8) > 0 {
            // is_rich_string
            self.read_u16()? as usize
        } else {
            0
        };
        let phonetic_count = if is_extend && (flag & 0x4) > 0 {
            // contains_phonetic
            self.read_usize()?
        } else {
            0
        };
        let (bytes, actual) = self.read(expected)?;
        if is_high_byte {
            let (string, _, _) = encoding.decode(&bytes);
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

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], RustySheetError> {
        let Some((source, size)) = self.take_range(N)? else {
            return Err(Biff8Error::NoEnoughDataError(N).into());
        };
        if size != N {
            return Err(Biff8Error::NoEnoughDataError(N).into());
        }

        let mut bytes = [0_u8; N];
        if self.cached {
            bytes.copy_from_slice(&self.cache[source..source + N]);
        } else {
            self.read_buffered_at(source, &mut bytes)?;
        }
        Ok(bytes)
    }

    fn read_at(&mut self, offset: usize, length: usize) -> Result<Vec<u8>, RustySheetError> {
        if self.cached {
            return Ok(self.cache[offset..offset + length].to_vec());
        }
        let mut bytes = vec![0_u8; length];
        self.read_buffered_at(offset, &mut bytes)?;
        Ok(bytes)
    }

    fn read_file_u16_at(&mut self, offset: usize) -> Result<u16, RustySheetError> {
        if offset + 2 > self.length {
            return Err(Biff8Error::NoEnoughDataError(2).into());
        }
        self.seek_file(offset)?;
        let mut bytes = [0_u8; 2];
        self.file.read_exact(&mut bytes)?;
        self.file_position = Some(offset + bytes.len());
        Ok(to_u16(&bytes))
    }

    fn seek_file(&mut self, offset: usize) -> Result<(), RustySheetError> {
        self.buffered_position = None;
        if self.file_position != Some(offset) {
            self.file.seek(SeekFrom::Start(offset as u64))?;
            self.file_position = Some(offset);
        }
        Ok(())
    }

    fn read_buffered_at(&mut self, offset: usize, bytes: &mut [u8]) -> Result<(), RustySheetError> {
        self.file_position = None;
        if self.buffered_position != Some(offset) {
            self.buffered_file.seek(SeekFrom::Start(offset as u64))?;
        }
        self.buffered_file.read_exact(bytes)?;
        self.buffered_position = Some(offset + bytes.len());
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn small_records_are_cached() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&1_u16.to_le_bytes()).unwrap();
        file.write_all(&4_u16.to_le_bytes()).unwrap();
        file.write_all(&42_u32.to_le_bytes()).unwrap();
        let length = file.stream_position().unwrap() as usize;
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut reader = Biff8Reader::new(file, length).unwrap();
        assert_eq!(reader.next().unwrap(), Some(1));
        assert!(reader.cached);
        assert_eq!(reader.read_u32().unwrap(), 42);
    }

    #[test]
    fn large_continued_records_remain_file_backed() {
        let mut file = tempfile::tempfile().unwrap();
        for index in 0..9_u16 {
            let kind = if index == 0 { 1 } else { CONTINUE };
            file.write_all(&kind.to_le_bytes()).unwrap();
            file.write_all(&8192_u16.to_le_bytes()).unwrap();
            file.write_all(&vec![index as u8; 8192]).unwrap();
        }
        let length = file.stream_position().unwrap() as usize;
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut reader = Biff8Reader::new(file, length).unwrap();
        assert_eq!(reader.next().unwrap(), Some(1));
        assert!(!reader.cached);
        assert!(reader.cache.is_empty());
        assert!(reader.cache_chunks.is_empty());
        assert_eq!(reader.read_u8().unwrap(), 0);
        reader.skip(8191).unwrap();
        assert_eq!(reader.read_u8().unwrap(), 1);
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
