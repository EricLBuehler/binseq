use anyhow::Result;

pub type RefBytes<'a> = &'a [u64];

#[derive(Debug)]
pub struct RefRecord<'a> {
    /// The 8-byte flag
    pub flag: u64,

    /// The 2-bit encoded sequence
    pub sequence: RefBytes<'a>,

    /// Length of the sequence in nucleotides
    pub slen: u32,

    /// Number of 64-bit chunks in the sequence
    pub n_chunks: usize,

    /// Number of nucleotides in the last chunk
    pub rem: usize,
}
impl<'a> RefRecord<'a> {
    pub fn new(flag: u64, sequence: RefBytes<'a>, slen: u32, n_chunks: usize, rem: usize) -> Self {
        Self {
            flag,
            sequence,
            slen,
            n_chunks,
            rem,
        }
    }
    pub fn flag(&self) -> u64 {
        self.flag
    }
    pub fn sequence(&self) -> RefBytes<'a> {
        self.sequence
    }
    pub fn decode(&self, buffer: &mut Vec<u8>) -> Result<()> {
        // Process all chunks except the last one
        self.sequence()
            .iter()
            .take(self.n_chunks - 1)
            .try_for_each(|component| bitnuc::from_2bit(*component, 32, buffer))?;

        // Process the last one with the remainder
        let component = self.sequence[self.n_chunks - 1];
        bitnuc::from_2bit(component, self.rem, buffer)?;

        Ok(())
    }
    pub fn decode_alloc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.decode(&mut buffer)?;
        Ok(buffer)
    }
}
