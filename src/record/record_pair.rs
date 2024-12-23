use anyhow::Result;

use super::RefBytes;
use crate::RecordConfig;

#[derive(Debug)]
pub struct RefRecordPair<'a> {
    /// The 8-byte flag
    pub flag: u64,

    /// The 2-bit encoded primary sequence
    pub s_seq: RefBytes<'a>,

    /// The 2-bit encoded extended sequence
    pub x_seq: RefBytes<'a>,

    // Configuration for the primary sequence
    pub s_config: RecordConfig,

    // Configuration for the extended sequence
    pub x_config: RecordConfig,
}
impl<'a> RefRecordPair<'a> {
    pub fn new(
        flag: u64,
        s_seq: RefBytes<'a>,
        x_seq: RefBytes<'a>,
        s_config: RecordConfig,
        x_config: RecordConfig,
    ) -> Self {
        Self {
            flag,
            s_seq,
            x_seq,
            s_config,
            x_config,
        }
    }
    pub fn flag(&self) -> u64 {
        self.flag
    }
    pub fn s_seq(&self) -> RefBytes<'a> {
        self.s_seq
    }
    pub fn x_seq(&self) -> RefBytes<'a> {
        self.x_seq
    }
    pub fn s_config(&self) -> RecordConfig {
        self.s_config
    }
    pub fn x_config(&self) -> RecordConfig {
        self.x_config
    }

    fn decode(
        &self,
        sequence: RefBytes<'a>,
        config: RecordConfig,
        buffer: &mut Vec<u8>,
    ) -> Result<()> {
        // Process all chunks except the last one
        sequence
            .iter()
            .take(config.n_chunks - 1)
            .try_for_each(|component| bitnuc::from_2bit(*component, 32, buffer))?;

        // Process the last one with the remainder
        let component = sequence[config.n_chunks - 1];
        bitnuc::from_2bit(component, config.rem, buffer)?;

        Ok(())
    }

    pub fn decode_s(&self, buffer: &mut Vec<u8>) -> Result<()> {
        self.decode(self.s_seq, self.s_config, buffer)
    }

    pub fn decode_x(&self, buffer: &mut Vec<u8>) -> Result<()> {
        self.decode(self.x_seq, self.x_config, buffer)
    }

    pub fn decode_s_alloc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.decode_s(&mut buffer)?;
        Ok(buffer)
    }

    pub fn decode_x_alloc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.decode_x(&mut buffer)?;
        Ok(buffer)
    }
}
