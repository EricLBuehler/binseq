use crate::header::SIZE_HEADER;

/// Calculates the number of expected bytes in a binary sequence file
pub fn expected_file_size(num_records: usize, seq_len: usize) -> usize {
    // number of u64 chunks in the sequence
    let n_chunks = seq_len.div_ceil(32);

    // flag + sequence (8 bytes per chunk + 8 bytes for the flag)
    let record_size = 8 * (n_chunks + 1);

    // header + records
    // header is 32 bytes
    SIZE_HEADER + (num_records * record_size)
}
