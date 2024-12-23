use anyhow::Result;

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
        // Process all chunks except the last one
        self.sequence()
            .iter()
            .take(self.config.n_chunks - 1)
            .try_for_each(|component| bitnuc::from_2bit(*component, 32, buffer))?;

        // Process the last one with the remainder
        let component = self.sequence[self.config.n_chunks - 1];
        bitnuc::from_2bit(component, self.config.rem, buffer)?;

        Ok(())
    }
    pub fn decode_alloc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.decode(&mut buffer)?;
        Ok(buffer)
    }
}
