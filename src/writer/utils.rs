use anyhow::{bail, Result};
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

/// Embed a sequence into a buffer of 2-bit encoded nucleotides.
///
/// # Arguments
///
/// * `sequence` - The nucleotide sequence to encode.
/// * `n_chunks` - The number of 32-bit chunks to encode.
/// * `ebuf` - The buffer to write the encoded nucleotides to.
///
/// # Errors
///
/// If the sequence cannot be encoded, an error is returned.
pub fn embed(sequence: &[u8], n_chunks: usize, ebuf: &mut Vec<u64>) -> Result<()> {
    // Clear the buffer
    ebuf.clear();

    let mut l_bounds = 0;
    for _ in 0..n_chunks - 1 {
        let r_bounds = l_bounds + 32;
        let chunk = &sequence[l_bounds..r_bounds];

        match bitnuc::as_2bit(chunk) {
            Ok(bits) => ebuf.push(bits),
            Err(e) => bail!(e),
        }
        l_bounds = r_bounds;
    }

    match bitnuc::as_2bit(&sequence[l_bounds..]) {
        Ok(bits) => ebuf.push(bits),
        Err(e) => bail!(e),
    }

    Ok(())
}
