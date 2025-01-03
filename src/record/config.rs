/// Sizing information for records
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordConfig {
    /// The length of the sequence
    pub slen: u32,

    /// Number of u64 chunks required to represent the sequence (ceil(slen / 32))
    pub n_chunks: usize,

    /// Number of 2bits remaining after the last chunk (slen % 32)
    pub rem: usize,
}
impl RecordConfig {
    pub fn new(slen: u32) -> Self {
        Self {
            slen,
            n_chunks: slen.div_ceil(32) as usize,
            rem: match slen % 32 {
                0 => 32,
                rem => rem as usize,
            },
        }
    }
}
