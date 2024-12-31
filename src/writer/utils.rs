use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{self, Write};

pub fn write_flag<W: Write>(writer: &mut W, flag: u64) -> Result<(), io::Error> {
    writer.write_u64::<LittleEndian>(flag as u64)
}

/// Write all the elements of the embedded buffer to the writer.
pub fn write_buffer<W: Write>(writer: &mut W, ebuf: &[u64]) -> Result<(), io::Error> {
    ebuf.iter()
        .try_for_each(|&x| writer.write_u64::<LittleEndian>(x))
}
