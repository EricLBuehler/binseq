//! # File and Block Header Definitions
//!
//! This module defines the header structures used in the VBINSEQ file format.
//!
//! The VBINSEQ format consists of two primary header types:
//!
//! 1. `VBinseqHeader` - The file header that appears at the beginning of a VBINSEQ file,
//!    containing information about the overall file format and configuration.
//!
//! 2. `BlockHeader` - Headers that appear before each block of records, containing
//!    information specific to that block like its size and number of records.
//!
//! Both headers are fixed-size and include magic numbers to validate file integrity.

use std::io::{Read, Write};

use byteorder::{ByteOrder, LittleEndian};

use crate::error::{HeaderError, ReadError, Result};

/// Magic number for file identification: "VSEQ" in ASCII (0x51455356)
///
/// This constant is used in the file header to identify VBINSEQ formatted files.
#[allow(clippy::unreadable_literal)]
const MAGIC: u32 = 0x51455356;

/// Magic number for block identification: "BLOCKSEQ" in ASCII (0x5145534B434F4C42)
///
/// This constant is used in block headers to validate block integrity.
#[allow(clippy::unreadable_literal)]
const BLOCK_MAGIC: u64 = 0x5145534B434F4C42;

/// Current format version number
///
/// This should be incremented when making backwards-incompatible changes to the format.
const FORMAT: u8 = 1;

/// Size of the file header in bytes (32 bytes)
///
/// The file header has a fixed size to simplify parsing.
pub const SIZE_HEADER: usize = 32;

/// Size of the block header in bytes (32 bytes)
///
/// Each block header has a fixed size to simplify block navigation.
pub const SIZE_BLOCK_HEADER: usize = 32;

/// Default block size in bytes: 128KB
///
/// This defines the default virtual size of each record block.
/// A larger block size can improve compression ratio but reduces random access granularity.
pub const BLOCK_SIZE: u64 = 128 * 1024;

/// Reserved bytes for future use in the file header (16 bytes)
///
/// These bytes are set to a placeholder value (42) and reserved for future extensions.
pub const RESERVED_BYTES: [u8; 15] = [42; 15];

/// Reserved bytes for future use in block headers (12 bytes)
///
/// These bytes are set to a placeholder value (42) and reserved for future extensions.
pub const RESERVED_BYTES_BLOCK: [u8; 12] = [42; 12];

/// File header for VBINSEQ files
///
/// This structure represents the 32-byte header that appears at the beginning of every
/// VBINSEQ file. It contains configuration information about the file format, including
/// whether quality scores are included, whether blocks are compressed, and whether
/// records contain paired sequences.
///
/// # Fields
///
/// * `magic` - Magic number to validate file format ("VSEQ", 4 bytes)
/// * `format` - Version number of the file format (1 byte)
/// * `block` - Size of each block in bytes (8 bytes)
/// * `qual` - Whether quality scores are included (1 byte boolean)
/// * `compressed` - Whether blocks are ZSTD compressed (1 byte boolean)
/// * `paired` - Whether records contain paired sequences (1 byte boolean)
/// * `reserved` - Reserved bytes for future extensions (16 bytes)
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VBinseqHeader {
    /// Magic number to identify the file format ("VSEQ")
    ///
    /// Always set to 0x51455356 (4 bytes)
    pub magic: u32,

    /// Version of the file format
    ///
    /// Currently set to 1 (1 byte)
    pub format: u8,

    /// Block size in bytes
    ///
    /// This is the virtual (uncompressed) size of each record block (8 bytes)
    pub block: u64,

    /// Whether quality scores are included with sequences
    ///
    /// If true, quality scores are stored for each nucleotide (1 byte)
    pub qual: bool,

    /// Whether internal blocks are compressed with ZSTD
    ///
    /// If true, blocks are compressed individually (1 byte)
    pub compressed: bool,

    /// Whether records contain paired sequences
    ///
    /// If true, each record has both primary and extended sequences (1 byte)
    pub paired: bool,

    /// The bitsize of the sequence data (1 byte)
    pub bits: u8,

    /// Reserved bytes for future format extensions
    ///
    /// Currently filled with placeholder values (16 bytes)
    pub reserved: [u8; 15],
}
impl Default for VBinseqHeader {
    /// Creates a default header with default block size and all features disabled
    ///
    /// The default header:
    /// - Uses the default block size (128KB)
    /// - Does not include quality scores
    /// - Does not use compression
    /// - Does not support paired sequences
    fn default() -> Self {
        Self::with_capacity(BLOCK_SIZE, false, false, false)
    }
}
impl VBinseqHeader {
    /// Creates a new VBINSEQ header with the default block size
    ///
    /// # Parameters
    ///
    /// * `qual` - Whether to include quality scores with sequences
    /// * `compressed` - Whether to use ZSTD compression for blocks
    /// * `paired` - Whether records contain paired sequences
    ///
    /// # Example
    ///
    /// ```rust
    /// use binseq::vbq::VBinseqHeader;
    ///
    /// // Create header with quality scores and compression, without paired sequences
    /// let header = VBinseqHeader::new(true, true, false);
    /// ```
    #[must_use]
    pub fn new(qual: bool, compressed: bool, paired: bool) -> Self {
        Self::with_capacity(BLOCK_SIZE, qual, compressed, paired)
    }

    /// Creates a new VBINSEQ header with a custom block size
    ///
    /// # Parameters
    ///
    /// * `block` - Custom block size in bytes (virtual/uncompressed size)
    /// * `qual` - Whether to include quality scores with sequences
    /// * `compressed` - Whether to use ZSTD compression for blocks
    /// * `paired` - Whether records contain paired sequences
    ///
    /// # Example
    ///
    /// ```rust
    /// use binseq::vbq::VBinseqHeader;
    ///
    /// // Create header with a 256KB block size, with quality scores and compression
    /// let header = VBinseqHeader::with_capacity(256 * 1024, true, true, false);
    /// ```
    #[must_use]
    pub fn with_capacity(block: u64, qual: bool, compressed: bool, paired: bool) -> Self {
        Self {
            magic: MAGIC,
            format: FORMAT,
            block,
            qual,
            compressed,
            paired,
            bits: 2,
            reserved: RESERVED_BYTES,
        }
    }

    /// Creates a header from a 32-byte buffer
    ///
    /// This function parses a raw byte buffer into a `VBinseqHeader` structure,
    /// validating the magic number and format version.
    ///
    /// # Parameters
    ///
    /// * `buffer` - A 32-byte array containing the header data
    ///
    /// # Returns
    ///
    /// * `Result<Self>` - A valid header if parsing was successful
    ///
    /// # Errors
    ///
    /// * `HeaderError::InvalidMagicNumber` - If the magic number doesn't match "VSEQ"
    /// * `HeaderError::InvalidFormatVersion` - If the format version is unsupported
    /// * `HeaderError::InvalidReservedBytes` - If the reserved bytes section is invalid
    pub fn from_bytes(buffer: &[u8; SIZE_HEADER]) -> Result<Self> {
        let magic = LittleEndian::read_u32(&buffer[0..4]);
        if magic != MAGIC {
            return Err(HeaderError::InvalidMagicNumber(magic).into());
        }
        let format = buffer[4];
        if format != FORMAT {
            return Err(HeaderError::InvalidFormatVersion(format).into());
        }
        let block = LittleEndian::read_u64(&buffer[5..13]);
        let qual = buffer[13] != 0;
        let compressed = buffer[14] != 0;
        let paired = buffer[15] != 0;
        let bits = match buffer[16] {
            0 | 2 | 42 => 2,
            4 => 4,
            x => return Err(HeaderError::InvalidBitSize(x).into()),
        };
        let Ok(reserved) = buffer[17..32].try_into() else {
            return Err(HeaderError::InvalidReservedBytes.into());
        };
        Ok(Self {
            magic,
            format,
            block,
            qual,
            compressed,
            paired,
            bits,
            reserved,
        })
    }

    /// Writes the header to a writer
    ///
    /// This function serializes the header structure into a 32-byte buffer and writes
    /// it to the provided writer.
    ///
    /// # Parameters
    ///
    /// * `writer` - Any type that implements the `Write` trait
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success if the header was written
    ///
    /// # Errors
    ///
    /// * IO errors if writing to the writer fails
    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_HEADER];
        LittleEndian::write_u32(&mut buffer[0..4], self.magic);
        buffer[4] = self.format;
        LittleEndian::write_u64(&mut buffer[5..13], self.block);
        buffer[13] = self.qual.into();
        buffer[14] = self.compressed.into();
        buffer[15] = self.paired.into();
        buffer[16] = self.bits;
        buffer[17..32].copy_from_slice(&self.reserved);
        writer.write_all(&buffer)?;
        Ok(())
    }

    /// Reads a header from a reader
    ///
    /// This function reads 32 bytes from the provided reader and parses them into
    /// a `VBinseqHeader` structure.
    ///
    /// # Parameters
    ///
    /// * `reader` - Any type that implements the `Read` trait
    ///
    /// # Returns
    ///
    /// * `Result<Self>` - A valid header if reading and parsing was successful
    ///
    /// # Errors
    ///
    /// * IO errors if reading from the reader fails
    /// * Header validation errors from `from_bytes()`
    pub fn from_reader<R: Read>(reader: &mut R) -> Result<Self> {
        let mut buffer = [0u8; SIZE_HEADER];
        reader.read_exact(&mut buffer)?;
        Self::from_bytes(&buffer)
    }

    #[must_use]
    pub fn is_paired(&self) -> bool {
        self.paired
    }
}

/// Block header for VBINSEQ block data
///
/// Each block in a VBINSEQ file is preceded by a 32-byte block header that contains
/// information about the block including its size and the number of records it contains.
///
/// # Fields
///
/// * `magic` - Magic number to validate block integrity ("BLOCKSEQ", 8 bytes)
/// * `size` - Actual size of the block in bytes (8 bytes)
/// * `records` - Number of records in the block (4 bytes)
/// * `reserved` - Reserved bytes for future extensions (12 bytes)
#[derive(Clone, Copy, Debug)]
pub struct BlockHeader {
    /// Magic number to identify the block ("BLOCKSEQ")
    ///
    /// Always set to 0x5145534B434F4C42 (8 bytes)
    pub magic: u64,

    /// Actual size of the block in bytes
    ///
    /// This can differ from the virtual block size in the file header
    /// when compression is enabled (8 bytes)
    pub size: u64,

    /// Number of records stored in this block
    ///
    /// Used to iterate through records efficiently (4 bytes)
    pub records: u32,

    /// Reserved bytes for future extensions
    ///
    /// Currently filled with placeholder values (12 bytes)
    pub reserved: [u8; 12],
}
impl BlockHeader {
    /// Creates a new block header
    ///
    /// # Parameters
    ///
    /// * `size` - The actual size of the block in bytes (can be compressed size)
    /// * `records` - The number of records contained in the block
    ///
    /// # Example
    ///
    /// ```rust
    /// use binseq::vbq::BlockHeader;
    ///
    /// // Create a block header for a block with 1024 bytes and 100 records
    /// let header = BlockHeader::new(1024, 100);
    /// ```
    #[must_use]
    pub fn new(size: u64, records: u32) -> Self {
        Self {
            magic: BLOCK_MAGIC,
            size,
            records,
            reserved: RESERVED_BYTES_BLOCK,
        }
    }

    /// Writes the block header to a writer
    ///
    /// This function serializes the block header structure into a 32-byte buffer and writes
    /// it to the provided writer.
    ///
    /// # Parameters
    ///
    /// * `writer` - Any type that implements the `Write` trait
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success if the header was written
    ///
    /// # Errors
    ///
    /// * IO errors if writing to the writer fails
    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_BLOCK_HEADER];
        LittleEndian::write_u64(&mut buffer[0..8], self.magic);
        LittleEndian::write_u64(&mut buffer[8..16], self.size);
        LittleEndian::write_u32(&mut buffer[16..20], self.records);
        buffer[20..].copy_from_slice(&self.reserved);
        writer.write_all(&buffer)?;
        Ok(())
    }

    /// Creates a block header from a 32-byte buffer
    ///
    /// This function parses a raw byte buffer into a `BlockHeader` structure,
    /// validating the magic number.
    ///
    /// # Parameters
    ///
    /// * `buffer` - A 32-byte array containing the block header data
    ///
    /// # Returns
    ///
    /// * `Result<Self>` - A valid block header if parsing was successful
    ///
    /// # Errors
    ///
    /// * `ReadError::InvalidBlockMagicNumber` - If the magic number doesn't match "BLOCKSEQ"
    pub fn from_bytes(buffer: &[u8; SIZE_BLOCK_HEADER]) -> Result<Self> {
        let magic = LittleEndian::read_u64(&buffer[0..8]);
        if magic != BLOCK_MAGIC {
            return Err(ReadError::InvalidBlockMagicNumber(magic, 0).into());
        }
        let size = LittleEndian::read_u64(&buffer[8..16]);
        let records = LittleEndian::read_u32(&buffer[16..20]);
        Ok(Self::new(size, records))
    }
}
