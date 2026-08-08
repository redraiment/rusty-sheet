//! Bounded reader for Microsoft Compound File Binary (CFB) containers.

use crate::error::RustySheetError;
use std::io::Read;
use std::io::Seek;
use std::io::Write;

pub(crate) struct Cfb<F> {
    inner: cfb::CompoundFile<F>,
}

impl<F: Read + Seek> Cfb<F> {
    pub(crate) fn new(reader: F) -> Result<Self, RustySheetError> {
        let inner = cfb::OpenOptions::new()
            .max_buffer_size(64 * 1024)
            .open_with(reader)?;
        Ok(Self { inner })
    }

    pub(crate) fn exists(&self, name: &str) -> bool {
        self.inner.exists(format!("/{name}"))
    }

    pub(crate) fn copy_to<W: Write>(
        &mut self,
        name: &str,
        writer: &mut W,
    ) -> Result<bool, RustySheetError> {
        let path = format!("/{name}");
        if !self.inner.is_stream(&path) {
            return Ok(false);
        }
        let mut stream = self.inner.open_stream(path)?;
        std::io::copy(&mut stream, writer)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn non_cfb_input_stops_after_signature() {
        let mut reader = Cursor::new(vec![0_u8; 4096]);

        assert!(Cfb::new(&mut reader).is_err());
        assert_eq!(reader.stream_position().unwrap(), 8);
    }
}
