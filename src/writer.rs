use anyhow::{bail, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::io::Write;

use crate::{error::WriteError, BinseqHeader};

pub struct BinseqWriter<W: Write> {
    /// Inner writer
    inner: W,

    /// Header of the file
    header: BinseqHeader,

    /// Buffer for the flag
    fbuf: [u8; 8],

    /// Buffer for the sequence
    sbuf: [u8; 8],

    /// Reusable buffer for all nucleotides (written as 2-bit after conversion)
    buffer: Vec<u64>,

    /// Break on invalid nucleotide sequence if encountered (skipped otherwise)
    break_on_invalid: bool,

    /// Number of records written
    records_written: usize,
}
impl<W: Write> BinseqWriter<W> {
    pub fn new(mut inner: W, header: BinseqHeader, break_on_invalid: bool) -> Result<Self> {
        header.write_bytes(&mut inner)?;
        Ok(Self {
            inner,
            header,
            fbuf: [0; 8],
            sbuf: [0; 8],
            buffer: Vec::new(),
            break_on_invalid,
            records_written: 0,
        })
    }

    fn write_flag(&mut self, flag: u64) -> Result<()> {
        LittleEndian::write_u64(&mut self.fbuf, flag);
        self.inner.write_all(&self.fbuf)?;
        Ok(())
    }

    fn write_buffer(&mut self) -> Result<()> {
        self.buffer.iter().try_for_each(|chunk| {
            LittleEndian::write_u64(&mut self.sbuf, *chunk);
            self.inner.write_all(&self.sbuf)?;
            Ok(())
        })
    }

    /// Fills the buffer with the 2-bit representation of the nucleotides
    fn fill_buffer(&mut self, sequence: &[u8]) -> Result<()> {
        // Clear the last sequence if any
        self.buffer.clear();

        // Determine the number of chunks
        let n_chunks = self.header.slen.div_ceil(32);

        let mut l_bounds = 0;
        for _ in 0..n_chunks - 1 {
            let r_bounds = l_bounds + 32;
            let chunk = &sequence[l_bounds..r_bounds];

            match bitnuc::as_2bit(chunk) {
                Ok(bits) => self.buffer.push(bits),
                Err(e) => bail!(e),
            }
            l_bounds = r_bounds;
        }

        match bitnuc::as_2bit(&sequence[l_bounds..]) {
            Ok(bits) => self.buffer.push(bits),
            Err(e) => bail!(e),
        }

        Ok(())
    }

    /// Write a nucleotide sequence to the file
    ///
    /// Returns `Ok(true)` if the sequence was written successfully, `Ok(false)` if the sequence was
    /// skipped due to an invalid nucleotide sequence, and an error if the sequence length does not
    /// match the header.
    pub fn write_nucleotides(&mut self, flag: u64, sequence: &[u8]) -> Result<bool> {
        if sequence.len() != self.header.slen as usize {
            bail!(WriteError::UnexpectedSequenceLength {
                expected: self.header.slen,
                got: sequence.len()
            })
        }
        match self.fill_buffer(sequence) {
            Ok(_) => {
                self.write_flag(flag)?;
                self.write_buffer()?;
                self.records_written += 1;
                Ok(true)
            }
            Err(_) => {
                if self.break_on_invalid {
                    bail!(WriteError::InvalidNucleotideSequence)
                } else {
                    Ok(false)
                }
            }
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}
