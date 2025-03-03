use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use rand::{rngs::SmallRng, SeedableRng};

use crate::{error::WriteError, BinseqHeader, Policy, Result, RNG_SEED};

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

/// Encapsulates the logic for encoding sequences into a binary format.
#[derive(Clone)]
pub struct Encoder {
    /// Header describing the sequence configuration
    header: BinseqHeader,

    /// Reusable buffers for all nucleotides (written as 2-bit after conversion)
    sbuffer: Vec<u64>,
    xbuffer: Vec<u64>,

    /// Reusable buffers for invalid nucleotide sequences
    s_ibuf: Vec<u8>,
    x_ibuf: Vec<u8>,

    /// Invalid Nucleotide Policy
    policy: Policy,

    /// Random Number Generator
    rng: SmallRng,
}
impl Encoder {
    pub fn new(header: BinseqHeader) -> Self {
        Self::with_policy(header, Policy::default())
    }

    /// Initialize a new encoder with the given policy.
    pub fn with_policy(header: BinseqHeader, policy: Policy) -> Self {
        Self {
            header,
            policy,
            sbuffer: Vec::default(),
            xbuffer: Vec::default(),
            s_ibuf: Vec::default(),
            x_ibuf: Vec::default(),
            rng: SmallRng::seed_from_u64(RNG_SEED),
        }
    }

    /// Encodes a single sequence as 2-bit.
    ///
    /// Will return `None` if the sequence is invalid and the policy does not allow correction.
    pub fn encode_single(&mut self, primary: &[u8]) -> Result<Option<&[u64]>> {
        if primary.len() != self.header.slen as usize {
            return Err(WriteError::UnexpectedSequenceLength {
                expected: self.header.slen,
                got: primary.len(),
            }
            .into());
        }

        // Fill the buffer with the 2-bit representation of the nucleotides
        self.clear();
        if bitnuc::encode(primary, &mut self.sbuffer).is_err() {
            self.clear();
            if self
                .policy
                .handle(primary, &mut self.s_ibuf, &mut self.rng)?
            {
                bitnuc::encode(&self.s_ibuf, &mut self.sbuffer)?;
            } else {
                return Ok(None);
            }
        }

        Ok(Some(&self.sbuffer))
    }

    /// Encodes a pair of sequences as 2-bit.
    ///
    /// Will return `None` if either sequence is invalid and the policy does not allow correction.
    pub fn encode_paired(
        &mut self,
        primary: &[u8],
        extended: &[u8],
    ) -> Result<Option<(&[u64], &[u64])>> {
        if primary.len() != self.header.slen as usize {
            return Err(WriteError::UnexpectedSequenceLength {
                expected: self.header.slen,
                got: primary.len(),
            }
            .into());
        }
        if extended.len() != self.header.xlen as usize {
            return Err(WriteError::UnexpectedSequenceLength {
                expected: self.header.xlen,
                got: extended.len(),
            }
            .into());
        }

        self.clear();
        if bitnuc::encode(primary, &mut self.sbuffer).is_err()
            || bitnuc::encode(extended, &mut self.xbuffer).is_err()
        {
            self.clear();
            if self
                .policy
                .handle(primary, &mut self.s_ibuf, &mut self.rng)?
                && self
                    .policy
                    .handle(extended, &mut self.x_ibuf, &mut self.rng)?
            {
                bitnuc::encode(&self.s_ibuf, &mut self.sbuffer)?;
                bitnuc::encode(&self.x_ibuf, &mut self.xbuffer)?;
            } else {
                return Ok(None);
            }
        }

        Ok(Some((&self.sbuffer, &self.xbuffer)))
    }

    /// Clear all buffers and reset the encoder.
    pub fn clear(&mut self) {
        self.sbuffer.clear();
        self.xbuffer.clear();
        self.s_ibuf.clear();
        self.x_ibuf.clear();
    }
}

pub struct BinseqWriter<W: Write> {
    /// Inner writer
    inner: W,

    /// Encoder used by the writer
    encoder: Encoder,
}
impl<W: Write> BinseqWriter<W> {
    pub fn new(mut inner: W, header: BinseqHeader) -> Result<Self> {
        header.write_bytes(&mut inner)?;
        Ok(Self {
            inner,
            encoder: Encoder::new(header),
        })
    }

    pub fn new_with_policy(mut inner: W, header: BinseqHeader, policy: Policy) -> Result<Self> {
        header.write_bytes(&mut inner)?;
        Ok(Self {
            inner,
            encoder: Encoder::with_policy(header, policy),
        })
    }

    /// Write a nucleotide sequence to the file
    ///
    /// Returns `Ok(true)` if the sequence was written successfully, `Ok(false)` if the sequence was
    /// skipped due to an invalid nucleotide sequence, and an error if the sequence length does not
    /// match the header.
    pub fn write_nucleotides(&mut self, flag: u64, sequence: &[u8]) -> Result<bool> {
        if let Some(sbuffer) = self.encoder.encode_single(sequence)? {
            write_flag(&mut self.inner, flag)?;
            write_buffer(&mut self.inner, sbuffer)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Write a pair of nucleotide sequences to the file
    ///
    /// Returns `Ok(true)` if the sequences were written successfully, `Ok(false)` if the sequences were
    /// skipped due to an invalid nucleotide sequence, and an error if the respective sequence lengths
    /// do not match the header.
    pub fn write_paired(&mut self, flag: u64, seq1: &[u8], seq2: &[u8]) -> Result<bool> {
        if let Some((sbuffer, xbuffer)) = self.encoder.encode_paired(seq1, seq2)? {
            write_flag(&mut self.inner, flag)?;
            write_buffer(&mut self.inner, sbuffer)?;
            write_buffer(&mut self.inner, xbuffer)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }

    /// Clone the encoder for the file
    ///
    /// Makes sure the new encoder is cleared before returning it.
    pub fn new_encoder(&self) -> Encoder {
        let mut encoder = self.encoder.clone();
        encoder.clear();
        encoder
    }
}
