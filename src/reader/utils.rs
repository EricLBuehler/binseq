use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Read;

use crate::{ReadError, RecordConfig, RecordSet};

pub fn fill_record_set<R: Read>(
    reader: &mut R,
    record_set: &mut RecordSet,
    n_processed: &mut usize,
) -> Result<bool> {
    record_set.clear();

    let config = record_set.config();

    while !record_set.is_full() {
        match next_flag(reader, *n_processed) {
            Ok(Some(flag)) => {
                // Read sequence
                match next_binseq(reader, record_set.get_buffer_mut(), config, *n_processed) {
                    Ok(_) => {
                        record_set.get_flags_mut().push(flag);
                        record_set.increment_records();
                        *n_processed += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(None) => {
                return Ok(true); // EOF reached
            }
            Err(e) => return Err(e),
        }
    }

    Ok(false) // Not finished, just filled the buffer
}

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
