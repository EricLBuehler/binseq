use anyhow::{bail, Result};
use std::io::Write;

use crate::{error::WriteError, BinseqHeader};

use super::utils::{embed, write_buffer, write_flag};

pub struct BinseqWriter<W: Write> {
    /// Inner writer
    inner: W,

    /// Header of the file
    header: BinseqHeader,

    /// Reusable buffer for all nucleotides (written as 2-bit after conversion)
    ///
    /// Used by the primary sequence (read 1)
    sbuffer: Vec<u64>,

    /// Reusable buffer for all nucleotides (written as 2-bit after conversion)
    ///
    /// Used by the extended sequence (read 2)
    xbuffer: Vec<u64>,

    /// Number of chunks in the primary sequence
    s_n_chunks: usize,

    /// Number of chunks in the extended sequence
    x_n_chunks: usize,

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
            sbuffer: Vec::new(),
            xbuffer: Vec::new(),
            s_n_chunks: header.slen.div_ceil(32) as usize,
            x_n_chunks: header.xlen.div_ceil(32) as usize,
            break_on_invalid,
            records_written: 0,
        })
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

        // Fill the buffer with the 2-bit representation of the nucleotides
        if embed(sequence, self.s_n_chunks, &mut self.sbuffer).is_err() {
            if self.break_on_invalid {
                bail!(WriteError::InvalidNucleotideSequence)
            } else {
                return Ok(false);
            }
        }

        write_flag(&mut self.inner, flag)?;
        write_buffer(&mut self.inner, &self.sbuffer)?;
        self.records_written += 1;
        Ok(true)
    }

    /// Write a pair of nucleotide sequences to the file
    ///
    /// Returns `Ok(true)` if the sequences were written successfully, `Ok(false)` if the sequences were
    /// skipped due to an invalid nucleotide sequence, and an error if the respective sequence lengths
    /// do not match the header.
    pub fn write_paired(&mut self, flag: u64, seq1: &[u8], seq2: &[u8]) -> Result<bool> {
        if seq1.len() != self.header.slen as usize {
            bail!(WriteError::UnexpectedSequenceLength {
                expected: self.header.slen,
                got: seq1.len()
            })
        }
        if seq2.len() != self.header.xlen as usize {
            bail!(WriteError::UnexpectedSequenceLength {
                expected: self.header.slen,
                got: seq2.len()
            })
        }

        if embed(seq1, self.s_n_chunks, &mut self.sbuffer).is_err()
            || embed(seq2, self.x_n_chunks, &mut self.xbuffer).is_err()
        {
            if self.break_on_invalid {
                bail!(WriteError::InvalidNucleotideSequence)
            } else {
                return Ok(false);
            }
        }

        write_flag(&mut self.inner, flag)?;
        write_buffer(&mut self.inner, &self.sbuffer)?;
        write_buffer(&mut self.inner, &self.xbuffer)?;
        self.records_written += 1;
        Ok(true)
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}
