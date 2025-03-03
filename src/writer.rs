use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use rand::rngs::ThreadRng;

use crate::{error::WriteError, BinseqHeader, Policy, Result};

/// Write a single flag to the writer.
pub fn write_flag<W: Write>(writer: &mut W, flag: u64) -> Result<()> {
    writer.write_u64::<LittleEndian>(flag)?;
    Ok(())
}

/// Write all the elements of the embedded buffer to the writer.
pub fn write_buffer<W: Write>(writer: &mut W, ebuf: &[u64]) -> Result<()> {
    ebuf.iter()
        .try_for_each(|&x| writer.write_u64::<LittleEndian>(x))?;
    Ok(())
}

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

    /// Reusable buffer for invalid nucleotide sequences
    s_ibuf: Vec<u8>,

    /// Reusable buffer for invalid nucleotide sequences
    x_ibuf: Vec<u8>,

    /// Invalid Nucleotide Policy
    policy: Policy,

    /// Random Number Generator
    rng: ThreadRng,

    /// Number of records written
    records_written: usize,
}
impl<W: Write> BinseqWriter<W> {
    pub fn new(mut inner: W, header: BinseqHeader) -> Result<Self> {
        header.write_bytes(&mut inner)?;
        Ok(Self {
            inner,
            header,
            sbuffer: Vec::new(),
            xbuffer: Vec::new(),
            s_ibuf: Vec::new(),
            x_ibuf: Vec::new(),
            policy: Policy::default(),
            rng: rand::thread_rng(),
            records_written: 0,
        })
    }

    pub fn new_with_policy(mut inner: W, header: BinseqHeader, policy: Policy) -> Result<Self> {
        header.write_bytes(&mut inner)?;
        Ok(Self {
            inner,
            header,
            sbuffer: Vec::new(),
            xbuffer: Vec::new(),
            s_ibuf: Vec::new(),
            x_ibuf: Vec::new(),
            policy,
            rng: rand::thread_rng(),
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
            return Err(WriteError::UnexpectedSequenceLength {
                expected: self.header.slen,
                got: sequence.len(),
            }
            .into());
        }

        // Fill the buffer with the 2-bit representation of the nucleotides
        if bitnuc::encode(sequence, &mut self.sbuffer).is_err() {
            if self
                .policy
                .handle(sequence, &mut self.s_ibuf, &mut self.rng)?
            {
                bitnuc::encode(&self.s_ibuf, &mut self.sbuffer)?;
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
            return Err(WriteError::UnexpectedSequenceLength {
                expected: self.header.slen,
                got: seq1.len(),
            }
            .into());
        }
        if seq2.len() != self.header.xlen as usize {
            return Err(WriteError::UnexpectedSequenceLength {
                expected: self.header.xlen,
                got: seq2.len(),
            }
            .into());
        }

        if bitnuc::encode(seq1, &mut self.sbuffer).is_err()
            || bitnuc::encode(seq2, &mut self.xbuffer).is_err()
        {
            self.sbuffer.clear(); // Clear the buffer to avoid writing invalid data
            self.xbuffer.clear(); // Clear the buffer to avoid writing invalid data

            if self.policy.handle(seq1, &mut self.s_ibuf, &mut self.rng)?
                && self.policy.handle(seq2, &mut self.x_ibuf, &mut self.rng)?
            {
                bitnuc::encode(&self.s_ibuf, &mut self.sbuffer)?;
                bitnuc::encode(&self.x_ibuf, &mut self.xbuffer)?;
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
