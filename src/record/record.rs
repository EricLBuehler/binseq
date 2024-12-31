use std::ops::Range;

use anyhow::{bail, Result};

use super::RefBytes;
use crate::RecordConfig;

#[derive(Debug)]
pub struct RefRecord<'a> {
    /// The 8-byte flag
    pub flag: u64,

    /// The 2-bit encoded sequence
    pub sequence: RefBytes<'a>,

    /// Sizing information for the record
    pub config: RecordConfig,
}
impl<'a> RefRecord<'a> {
    pub fn new(flag: u64, sequence: RefBytes<'a>, config: RecordConfig) -> Self {
        Self {
            flag,
            sequence,
            config,
        }
    }

    pub fn flag(&self) -> u64 {
        self.flag
    }

    pub fn sequence(&self) -> RefBytes<'a> {
        self.sequence
    }

    pub fn decode(&self, buffer: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.sequence(), self.config.slen as usize, buffer)?;
        Ok(())
    }

    pub fn decode_alloc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.decode(&mut buffer)?;
        Ok(buffer)
    }

    pub fn decode_subsequence(
        &self,
        range: Range<usize>,
        sub_buffer: &mut Vec<u64>,
        seq_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        // Clear the buffers
        sub_buffer.clear();
        seq_buffer.clear();

        let n_bases = range.len();
        let (_n_chunks, _rem) = self.subsequence(range, sub_buffer)?;
        bitnuc::decode(&sub_buffer, n_bases, seq_buffer)?;
        Ok(())
    }

    pub fn decode_subsequence_alloc(&self, range: Range<usize>) -> Result<Vec<u8>> {
        let mut sub_buffer = Vec::new();
        let mut seq_buffer = Vec::new();
        self.decode_subsequence(range, &mut sub_buffer, &mut seq_buffer)?;
        Ok(seq_buffer)
    }

    /// Extract a subsequence from the 2-bit encoded sequence without decoding
    /// The sequence is stored in little-endian format where:
    /// A = 00, C = 01, G = 10, T = 11
    ///
    /// # Arguments
    /// * `start` - Start position in the sequence (0-based)
    /// * `end` - End position in the sequence (exclusive)
    /// * `buffer` - A mutable reference to a Vec<u64> to store the resulting subsequence
    ///
    /// # Returns
    /// The number of chunks used to store the subsequence and the number of bases in the last chunk
    pub fn subsequence(
        &self,
        range: Range<usize>,
        buffer: &mut Vec<u64>,
    ) -> Result<(usize, usize)> {
        let start = range.start;
        let end = range.end;

        if start >= end || end > self.config.slen as usize {
            bail!("Invalid subsequence range");
        }

        // Clear the buffer
        buffer.clear();

        // Calculate size of result
        let n_bases = end - start;
        let n_bits = n_bases * 2;
        let n_chunks = n_bits.div_ceil(64);

        // Calculate which input chunks we need
        let start_chunk = start / 32;
        let end_chunk = (end - 1) / 32;

        // Calculate bit positions within chunks
        let start_bit = (start % 32) * 2;
        let bits_remaining = n_bits;
        let mut current_out = 0u64;
        let mut bits_in_current = 0usize;

        // Handle first chunk
        let first_chunk = self.sequence[start_chunk];
        let available_bits = 64 - start_bit;
        let bits_to_take = std::cmp::min(bits_remaining, available_bits);
        let mask = if bits_to_take == 64 {
            u64::MAX
        } else {
            (1u64 << bits_to_take) - 1
        };
        current_out |= (first_chunk >> start_bit) & mask;
        bits_in_current += bits_to_take;

        // If we need bits from subsequent chunks
        if start_chunk != end_chunk && bits_remaining > available_bits {
            let mut remaining_bits = bits_remaining - bits_to_take;

            // Handle full intermediate chunks if any
            for chunk_idx in (start_chunk + 1)..end_chunk {
                let chunk = self.sequence[chunk_idx];
                if bits_in_current + 64 <= 64 {
                    // Can fit entire chunk in current output
                    if bits_in_current < 64 {
                        current_out |= chunk << bits_in_current;
                    }
                    bits_in_current += 64;
                } else {
                    // Need to split across output chunks
                    let bits_for_current = 64 - bits_in_current;
                    current_out |= (chunk & ((1 << bits_for_current) - 1)) << bits_in_current;
                    buffer.push(current_out);
                    current_out = chunk >> bits_for_current;
                    bits_in_current = 64 - bits_for_current;
                }
                remaining_bits -= 64;
            }

            // Handle final chunk if needed
            if remaining_bits > 0 {
                let last_chunk = self.sequence[end_chunk];
                let end_bit = ((end - 1) % 32) * 2;
                let mask = (1u64 << (end_bit + 2)) - 1;
                let final_bits = last_chunk & mask;

                if bits_in_current + remaining_bits <= 64 {
                    current_out |= final_bits << bits_in_current;
                    bits_in_current += remaining_bits;
                } else {
                    let bits_for_current = 64 - bits_in_current;
                    // Avoid overflow by checking shift amount
                    if bits_in_current < 64 {
                        current_out |=
                            (final_bits & ((1 << bits_for_current) - 1)) << bits_in_current;
                    }
                    buffer.push(current_out);
                    current_out = final_bits >> bits_for_current;
                    bits_in_current = remaining_bits - bits_for_current;
                }
            }
        }

        // Push final chunk if we have one
        if bits_in_current > 0 || buffer.is_empty() {
            buffer.push(current_out);
        }

        let rem = n_bases % 32;
        Ok((n_chunks, rem))
    }
}

#[cfg(test)]
mod testing {
    use super::*;
    use anyhow::Result;

    fn embed_sequence(nucl: &[u8]) -> Vec<u64> {
        let mut ebuf = Vec::new();
        bitnuc::encode(nucl, &mut ebuf).unwrap();
        ebuf
    }

    #[test]
    fn test_subsequence_small() -> Result<()> {
        let seq = b"ACTGACTG";
        let ebuf = embed_sequence(seq);
        let config = RecordConfig::new(seq.len() as u32);

        let record = RefRecord::new(0, ebuf.as_slice(), config);

        // First 4 bases
        let subseq = record.decode_subsequence_alloc(0..4)?;
        assert_eq!(subseq, b"ACTG");

        // Last 4 bases
        let subseq = record.decode_subsequence_alloc(4..8)?;
        assert_eq!(subseq, b"ACTG");

        // All bases
        let subseq = record.decode_subsequence_alloc(0..8)?;
        assert_eq!(subseq, b"ACTGACTG");

        // Middle 4 bases
        let subseq = record.decode_subsequence_alloc(2..6)?;
        assert_eq!(subseq, b"TGAC");

        Ok(())
    }

    #[test]
    fn test_subsequence_large() -> Result<()> {
        let seq = b"ACTGACTGACTGACTGACTGACTGACTGACTGACTGACTGACTGACTGACTG";
        let ebuf = embed_sequence(seq);
        let config = RecordConfig::new(seq.len() as u32);

        let record = RefRecord::new(0, ebuf.as_slice(), config);

        // First 4 bases
        let subseq = record.decode_subsequence_alloc(0..4)?;
        assert_eq!(subseq, b"ACTG");

        // Last 4 bases
        let subseq = record.decode_subsequence_alloc(48..52)?;
        assert_eq!(subseq, b"ACTG");

        // All bases
        let subseq = record.decode_subsequence_alloc(0..52)?;
        assert_eq!(subseq, seq);

        // Middle 4 bases
        let subseq = record.decode_subsequence_alloc(20..24)?;
        assert_eq!(subseq, b"ACTG");

        // Bases spanning 32-bp chunks
        let subseq = record.decode_subsequence_alloc(30..34)?;
        assert_eq!(subseq, b"TGAC");

        Ok(())
    }
}
