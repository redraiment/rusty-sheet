//! OLE Compound File Binary (CFB) reader for legacy Excel (.xls) format
//! Implements parsing of the Compound File Binary format used in older Office documents

use crate::error::RustySheetError;
use crate::helpers::string::to_u16;
use crate::helpers::string::to_u64;
use crate::helpers::string::to_usize;
use crate::helpers::string::to_usize_iter;
use encoding_rs::UTF_16LE;
use std::collections::HashMap;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::ops::Range;
use thiserror::Error;

// Sector type constants (commented out as they're not currently used)
// const FREE_SECT: usize = 0xFFFFFFFF;
// const END_OF_CHAIN: usize = 0xFFFFFFFE;
// const FAT_SECT: usize = 0xFFFFFFFD;
// const DIF_SECT: usize = 0xFFFFFFFC;
const MAX_REG_SECT: usize = 0xFFFFFFFB;

/// Errors specific to Compound File Binary format parsing
#[derive(Error, Debug)]
pub(crate) enum CfbError {
    #[error("The file is corrupted or has an invalid CFB structure")]
    FileFormatError,

    #[error("Invalid OLE signature (not an office document?)")]
    OleSignatureError,

    #[error("Invalid Sector size '2 ^ {1}' for major version '{0}'")]
    SectorSizeError(u16, u16),

    #[error("The number of double indirect file allocation table error: expect '{0}', actual '{1}'")]
    DoubleIndirectFileAllocationTableError(usize, usize),

    #[error("The number of file allocation table error: expect '{0}', actual '{1}'")]
    FileAllocationTableError(usize, usize),

    #[error("Empty Root directory")]
    RootDirectoryError,
}

/// Compound File Binary structure representing the entire OLE file
/// Contains directory entries, file allocation tables, and sector data
pub(crate) struct Cfb {
    /// Directory index mapping names to directory entries
    directories: HashMap<String, Directory>,
    /// File allocation table for regular sectors
    file_allocation_table: Vec<usize>,
    /// Regular sectors containing file data
    sectors: Sectors,
    /// Mini file allocation table for small files
    mini_file_allocation_table: Vec<usize>,
    /// Mini sectors for small files (64-byte sectors)
    mini_sectors: Sectors,
}

impl Cfb {
    /// Creates a new CFB structure by reading and parsing the entire file
    pub(crate) fn new<RS: Read + Seek>(reader: &mut RS) -> Result<Cfb, RustySheetError> {
        // Load the entire CFB content into memory
        let size = reader.seek(SeekFrom::End(0))?;
        if size < 512 {
            Err(CfbError::FileFormatError)?;
        }
        reader.seek(SeekFrom::Start(0))?;
        let mut data: Vec<u8> = vec![0u8; size as usize];
        reader.read_exact(&mut data)?;
        // Parse the data
        let header = Header::new(&data[..512])?;
        let sectors = Sectors { data, size: header.sector_size()? };
        let file_allocation_table = Self::load_file_allocation_table(&sectors, &header)?;
        let directories = Self::load_directories(&file_allocation_table, &sectors, header.directory_shift)?;
        let mini_file_allocation_table = Self::load_mini_file_allocation_table(&file_allocation_table, &sectors, &header)?;
        let mini_sectors= if directories.contains_key("Root Entry") {
            Self::load_mini_file_allocation_sectors(&file_allocation_table, &sectors, &directories["Root Entry"])?
        } else {
            Sectors { data: Vec::new(), size: 64 }
        };

        Ok(Cfb {
            directories,
            file_allocation_table,
            sectors,
            mini_file_allocation_table,
            mini_sectors,
        })
    }

    /// Checks if a file exists in the CFB structure
    pub(crate) fn exists(&self, name: &str) -> bool {
        self.directories.contains_key(name)
    }

    /// Reads the contents of a file from the CFB structure
    pub(crate) fn read(&self, name: &str) -> Result<Option<Vec<u8>>, RustySheetError> {
        if let Some(directory) = self.directories.get(name) {
            let mut bytes = if directory.count < 4096 {
                Self::read_bytes(&self.mini_file_allocation_table, &self.mini_sectors, directory.index)?
            } else {
                Self::read_bytes(&self.file_allocation_table, &self.sectors, directory.index)?
            };
            bytes.truncate(directory.count);
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    /// Loads the file allocation table using the double indirect file allocation table
    fn load_file_allocation_table(sectors: &Sectors, header: &Header) -> Result<Vec<usize>, RustySheetError> {
        let mut double_indirect_file_allocation_table = Vec::<usize>::new();
        double_indirect_file_allocation_table.extend(to_usize_iter(sectors.slice(76..512)));

        let mut count = 0usize;
        let mut index = header.double_indirect_file_allocation_table_shift;
        while index < MAX_REG_SECT {
            double_indirect_file_allocation_table.extend(to_usize_iter(sectors.get(index)));
            index = double_indirect_file_allocation_table.pop().expect("Next Sector ID");
            count += 1;
        }
        if count != header.double_indirect_file_allocation_table_count {
            Err(CfbError::DoubleIndirectFileAllocationTableError(header.double_indirect_file_allocation_table_count, count))?
        }

        let mut file_allocation_table: Vec<usize> = Vec::new();
        let mut count = 0usize;
        for index in double_indirect_file_allocation_table {
            if index < MAX_REG_SECT {
                file_allocation_table.extend(to_usize_iter(sectors.get(index)));
                count += 1;
            }
        }
        if count != header.file_allocation_table_count {
            Err(CfbError::FileAllocationTableError(header.file_allocation_table_count, count))?
        }

        Ok(file_allocation_table)
    }

    /// Loads directory entries from the specified sector index
    fn load_directories(file_allocation_table: &Vec<usize>, sectors: &Sectors, index: usize) -> Result<HashMap<String, Directory>, RustySheetError> {
        let bytes = Self::read_bytes(&file_allocation_table, &sectors, index)?;
        let directories: HashMap<String, Directory> = bytes.chunks(128).map(Directory::new).collect();
        if directories.is_empty() {
            Err(CfbError::RootDirectoryError)?
        }
        Ok(directories)
    }

    /// Loads the mini file allocation table for small files
    fn load_mini_file_allocation_table(file_allocation_table: &Vec<usize>, sectors: &Sectors, header: &Header) -> Result<Vec<usize>, RustySheetError> {
        Ok(if header.mini_file_allocation_table_sector_count > 0 {
            let mini_file_allocation_table = Self::read_bytes(file_allocation_table, sectors, header.mini_file_allocation_table_sector_shift)?;
            to_usize_iter(&mini_file_allocation_table).collect()
        } else {
            Vec::new()
        })
    }

    /// Loads mini file allocation sectors for small files
    fn load_mini_file_allocation_sectors(file_allocation_table: &Vec<usize>, sectors: &Sectors, mini: &Directory) -> Result<Sectors, RustySheetError> {
        let mut data = Self::read_bytes(file_allocation_table, sectors, mini.index)?;
        data.truncate(mini.count);
        Ok(Sectors { data, size: 64 }) // Mini sector size is fixed at 64 bytes
    }

    /// Reads the complete content of a file by following the file allocation table chain
    fn read_bytes(file_allocation_table: &Vec<usize>, sectors: &Sectors, index: usize) -> Result<Vec<u8>, RustySheetError> {
        let mut content: Vec<u8> = Vec::new();
        let mut index = index;
        while index < MAX_REG_SECT {
            content.extend(sectors.get(index));
            index = file_allocation_table[index];
        }
        Ok(content)
    }
}

/// Container for all sectors in the CFB file
#[derive(Debug)]
struct Sectors {
    data: Vec<u8>,
    // Size of individual sectors
    size: usize,
}

impl Sectors {
    /// Gets the data for the sector at the specified index
    fn get(&self, index: usize) -> &[u8] {
        let source = (index + 1) * self.size;
        let target = self.data.len().min((index + 2) * self.size);
        &self.data[source..target]
    }

    /// Gets a slice of data from the specified range
    fn slice(&self, range: Range<usize>) -> &[u8] {
        &self.data[range]
    }
}

/// CFB file header structure
#[derive(Debug)]
struct Header {
    signature: u64,
    major_version: u16,
    sector_shift: u16,
    file_allocation_table_count: usize,
    directory_shift: usize,
    mini_file_allocation_table_sector_shift: usize,
    mini_file_allocation_table_sector_count: usize,
    double_indirect_file_allocation_table_shift: usize,
    double_indirect_file_allocation_table_count: usize,
}

impl Header {
    /// Parses the CFB header from the first 512 bytes of data
    fn new(data: &[u8]) -> Result<Self, RustySheetError> {
        let header = Header {
            signature: to_u64(&data[0..8]),
            major_version: to_u16(&data[26..28]),
            sector_shift: to_u16(&data[30..32]),
            file_allocation_table_count: to_usize(&data[44..48]),
            directory_shift: to_usize(&data[48..52]),
            mini_file_allocation_table_sector_shift: to_usize(&data[60..64]),
            mini_file_allocation_table_sector_count: to_usize(&data[64..68]),
            double_indirect_file_allocation_table_shift: to_usize(&data[68..72]),
            double_indirect_file_allocation_table_count: to_usize(&data[72..76]),
        };

        if header.signature != 0xE11A_B1A1_E011_CFD0 {
            Err(CfbError::OleSignatureError)?;
        }

        Ok(header)
    }

    /// Calculates the sector size based on major version and sector shift
    fn sector_size(&self) -> Result<usize, RustySheetError> {
        if self.major_version == 3 && self.sector_shift == 0x0009 {
            Ok(512) // 2 ^ 9
        } else if self.major_version == 4 && self.sector_shift == 0x000C {
            Ok(4096) // 2 ^ 12
            // For version 4 compound files,
            // the header size (512 bytes) is less than the sector size (4,096 bytes),
            // so the remaining part of the header (3,584 bytes) MUST be filled with all zeroes.
        } else {
            Err(CfbError::SectorSizeError(self.major_version, self.sector_shift))?
        }
    }
}

/// Directory entry representing a file in the CFB structure
#[derive(Debug)]
struct Directory {
    index: usize,
    count: usize,
}

impl Directory {
    /// Creates a directory entry from raw bytes
    fn new(bytes: &[u8]) -> (String, Directory) {
        let size = to_u16(&bytes[64..66]) as usize;
        let (name, _, _) = UTF_16LE.decode(&bytes[..size]);
        let name = if let Some(position) = name.find('\0') {
            name[..position].to_owned()
        } else {
            name.to_string()
        };

        let index = to_usize(&bytes[116..120]);
        let count = to_u64(&bytes[120..128]) as usize;
        (name, Directory { index, count })
    }
}
