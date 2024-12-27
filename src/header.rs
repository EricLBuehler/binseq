use anyhow::{bail, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::io::{Read, Write};

use crate::error::HeaderError;

/// Current magic number: "BSEQ" in ASCII
const MAGIC: u32 = 0x42534551;

/// Current format version
const FORMAT: u8 = 2;

/// Size of the header in bytes
const SIZE_HEADER: usize = 32;

#[derive(Debug, Clone, Copy)]
pub struct BinseqHeader {
    /// Magic number to identify the file format
    ///
    /// 4 bytes
    pub magic: u32,

    /// Version of the file format
    ///
    /// 1 byte
    pub format: u8,

    /// Length of all sequences in the file
    ///
    /// 4 bytes
    pub slen: u32,

    /// Length of secondary sequences in the file
    ///
    /// 4 bytes
    pub xlen: u32,

    // Reserve remaining bytes for future use
    //
    // 19 bytes
    pub reserved: [u8; 19],
}
impl BinseqHeader {
    pub fn new(slen: u32) -> Self {
        Self {
            magic: MAGIC,
            format: FORMAT,
            slen,
            xlen: 0,
            reserved: [0; 19],
        }
    }

    pub fn new_extended(slen: u32, xlen: u32) -> Self {
        Self {
            magic: MAGIC,
            format: FORMAT,
            slen,
            xlen,
            reserved: [0; 19],
        }
    }

    pub fn from_bytes(buffer: &[u8; SIZE_HEADER]) -> Result<Self> {
        let magic = LittleEndian::read_u32(&buffer[0..4]);
        if magic != MAGIC {
            bail!(HeaderError::InvalidMagicNumber(magic));
        }
        let format = buffer[4];
        if format != FORMAT {
            bail!(HeaderError::InvalidFormatVersion(format));
        }
        let slen = LittleEndian::read_u32(&buffer[5..9]);
        let xlen = LittleEndian::read_u32(&buffer[9..13]);
        let reserved = match buffer[13..32].try_into() {
            Ok(reserved) => reserved,
            Err(_) => bail!(HeaderError::InvalidReservedBytes),
        };
        Ok(Self {
            magic,
            format,
            slen,
            xlen,
            reserved,
        })
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_HEADER];
        LittleEndian::write_u32(&mut buffer[0..4], self.magic);
        buffer[4] = self.format;
        LittleEndian::write_u32(&mut buffer[5..9], self.slen);
        LittleEndian::write_u32(&mut buffer[9..13], self.xlen);
        buffer[13..32].copy_from_slice(&self.reserved);
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn from_reader<R: Read>(reader: &mut R) -> Result<Self> {
        let mut buffer = [0u8; SIZE_HEADER];
        reader.read_exact(&mut buffer)?;
        Self::from_bytes(&buffer)
    }
}
