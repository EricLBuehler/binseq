mod config;
mod record;
mod record_pair;
mod ref_record;

pub type RefBytes<'a> = &'a [u64];

pub use config::RecordConfig;
pub use record::Record;
pub use record_pair::RefRecordPair;
pub use ref_record::RefRecord;

pub trait BinseqRecord {
    fn flag(&self) -> u64;
    fn sequence(&self) -> RefBytes;
    fn config(&self) -> RecordConfig;

    fn decode(&self, buffer: &mut Vec<u8>) -> Result<(), bitnuc::NucleotideError> {
        bitnuc::decode(self.sequence(), self.config().slen as usize, buffer)
    }

    fn decode_alloc(&self) -> Result<Vec<u8>, bitnuc::NucleotideError> {
        let mut buffer = Vec::new();
        self.decode(&mut buffer)?;
        Ok(buffer)
    }

    fn decode_subsequence(
        &self,
        range: std::ops::Range<usize>,
        sub_buffer: &mut Vec<u64>,
        seq_buffer: &mut Vec<u8>,
    ) -> anyhow::Result<()> {
        // Clear the buffers
        sub_buffer.clear();
        seq_buffer.clear();

        let n_bases = range.len();
        let (_n_chunks, _rem) = self.subsequence(range, sub_buffer)?;
        bitnuc::decode(&sub_buffer, n_bases, seq_buffer)?;
        Ok(())
    }

    fn decode_subsequence_alloc(&self, range: std::ops::Range<usize>) -> anyhow::Result<Vec<u8>> {
        let mut sub_buffer = Vec::new();
        let mut seq_buffer = Vec::new();
        self.decode_subsequence(range, &mut sub_buffer, &mut seq_buffer)?;
        Ok(seq_buffer)
    }

    fn subsequence(
        &self,
        range: std::ops::Range<usize>,
        buffer: &mut Vec<u64>,
    ) -> anyhow::Result<(usize, usize)> {
        let start = range.start;
        let end = range.end;

        if start >= end || end > self.config().slen as usize {
            anyhow::bail!("Invalid subsequence range");
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
        let first_chunk = self.sequence()[start_chunk];
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
                let chunk = self.sequence()[chunk_idx];
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
                let last_chunk = self.sequence()[end_chunk];
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
