use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Read;

use crate::ReadError;

use super::RecordConfig;

/// Read the next flag from the reader
///
/// The flag is a 64-bit unsigned integer.
///
/// If the reader reaches the end of the stream, it returns `None`.
pub fn next_flag<R: Read>(reader: &mut R, n_processed: usize) -> Result<Option<u64>> {
    match reader.read_u64::<LittleEndian>() {
        Ok(flag) => Ok(Some(flag)),
        Err(e) => {
            let mut buf = [0u8; 1];
            match reader.read(&mut buf) {
                Ok(0) => Ok(None),
                _ => {
                    bail!(ReadError::UnexpectedEndOfStreamFlag(e, n_processed));
                }
            }
        }
    }
}

/// Read the next sequence from the reader
///
/// Can be used by the primary or extended sequence.
///
/// Pay attention to the `config` parameter, which contains the number of chunks to read.
pub fn next_binseq<R: Read>(
    reader: &mut R,
    buffer: &mut Vec<u64>,
    config: RecordConfig,
    n_processed: usize,
) -> Result<()> {
    (0..config.n_chunks).try_for_each(|_| match reader.read_u64::<LittleEndian>() {
        Ok(bits) => {
            buffer.push(bits);
            Ok(())
        }
        Err(e) => bail!(ReadError::UnexpectedEndOfStreamSequence(e, n_processed,)),
    })
}
