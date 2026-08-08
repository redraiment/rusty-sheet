use crate::error::RustySheetError;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

pub(crate) struct SharedStringStore {
    index: BufWriter<File>,
    values: BufWriter<File>,
    values_length: u64,
    count: usize,
    reader: Option<SharedStringReader>,
}

struct SharedStringReader {
    index: BufReader<File>,
    values: BufReader<File>,
    index_position: u64,
    values_position: u64,
}

impl SharedStringStore {
    const RECORD_SIZE: u64 = 16;

    pub(crate) fn new() -> Result<Self, RustySheetError> {
        Ok(Self {
            index: BufWriter::with_capacity(64 * 1024, tempfile::tempfile()?),
            values: BufWriter::with_capacity(64 * 1024, tempfile::tempfile()?),
            values_length: 0,
            count: 0,
            reader: None,
        })
    }

    pub(crate) fn push(&mut self, value: &str) -> Result<(), RustySheetError> {
        if self.reader.is_some() {
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "cannot append after reading shared strings",
            ))?;
        }
        let offset = self.values_length;
        self.values.write_all(value.as_bytes())?;
        self.index.write_all(&offset.to_le_bytes())?;
        self.index.write_all(&(value.len() as u64).to_le_bytes())?;
        self.values_length += value.len() as u64;
        self.count += 1;
        Ok(())
    }

    pub(crate) fn get(&mut self, id: usize) -> Result<String, RustySheetError> {
        if id >= self.count {
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "shared string index out of bounds",
            ))?;
        }
        let record_offset = (id as u64).checked_mul(Self::RECORD_SIZE).ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "shared string index overflow")
        })?;
        if self.reader.is_none() {
            self.index.flush()?;
            self.values.flush()?;
            self.reader = Some(SharedStringReader {
                index: BufReader::with_capacity(64 * 1024, self.index.get_ref().try_clone()?),
                values: BufReader::with_capacity(64 * 1024, self.values.get_ref().try_clone()?),
                index_position: self.count as u64 * Self::RECORD_SIZE,
                values_position: self.values_length,
            });
        }
        let reader = self
            .reader
            .as_mut()
            .expect("shared string reader initialized");
        if reader.index_position != record_offset {
            reader.index.seek(SeekFrom::Start(record_offset))?;
            reader.index_position = record_offset;
        }
        let mut record = [0_u8; Self::RECORD_SIZE as usize];
        reader.index.read_exact(&mut record)?;
        reader.index_position += Self::RECORD_SIZE;

        let offset = u64::from_le_bytes(record[..8].try_into().expect("shared string offset"));
        let length = u64::from_le_bytes(record[8..].try_into().expect("shared string length"));
        let length = usize::try_from(length).map_err(|_| {
            std::io::Error::new(ErrorKind::InvalidData, "shared string is too large")
        })?;
        let mut bytes = vec![0_u8; length];
        if reader.values_position != offset {
            reader.values.seek(SeekFrom::Start(offset))?;
            reader.values_position = offset;
        }
        reader.values.read_exact(&mut bytes)?;
        reader.values_position += length as u64;
        String::from_utf8(bytes)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error).into())
    }

    pub(crate) fn len(&self) -> usize {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use super::SharedStringStore;

    #[test]
    fn shared_string_store_reads_sequential_and_random_values() {
        let mut store = SharedStringStore::new().unwrap();
        store.push("alpha").unwrap();
        store.push("bravo").unwrap();
        store.push("charlie").unwrap();

        assert_eq!(store.get(0).unwrap(), "alpha");
        assert_eq!(store.get(1).unwrap(), "bravo");
        assert_eq!(store.get(0).unwrap(), "alpha");
        assert_eq!(store.get(2).unwrap(), "charlie");
    }
}
