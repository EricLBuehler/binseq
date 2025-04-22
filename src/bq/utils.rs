//! Utility functions for binary sequence file operations
//!
//! This module provides helper functions for common operations such as
//! calculating file sizes and other utility operations related to
//! binary sequence files.

use super::header::SIZE_HEADER;

/// Calculates the expected size in bytes of a binary sequence file
///
/// This function computes the total file size based on:
/// - Number of records
/// - Sequence length
/// - Header size (32 bytes)
/// - Record size (flag + encoded sequence)
///
/// Each record consists of:
/// - An 8-byte flag
/// - The encoded sequence (2 bits per nucleotide, packed into u64s)
///
/// # Arguments
///
/// * `num_records` - Number of sequence records in the file
/// * `seq_len` - Length of each sequence in nucleotides
///
/// # Returns
///
/// The total expected file size in bytes
///
/// # Examples
///
/// ```
/// use binseq::bq::expected_file_size;
///
/// // For 1000 sequences of length 100
/// let size = expected_file_size(1000, 100);
/// assert!(size > 0);
/// ```
pub fn expected_file_size(num_records: usize, seq_len: usize) -> usize {
    // number of u64 chunks in the sequence
    let n_chunks = seq_len.div_ceil(32);

    // flag + sequence (8 bytes per chunk + 8 bytes for the flag)
    let record_size = 8 * (n_chunks + 1);

    // header + records
    // header is 32 bytes
    SIZE_HEADER + (num_records * record_size)
}
