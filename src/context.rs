use crate::{BinseqRecord, Result};

pub const DEFAULT_QUALITY: u8 = b'?';

/// A context for storing reusable buffers for internal sequence data.
#[derive(Clone, Debug, Default)]
pub struct Context {
    sbuf: Vec<u8>,
    xbuf: Vec<u8>,

    sheader: Vec<u8>,
    xheader: Vec<u8>,

    squal: Vec<u8>,
    xqual: Vec<u8>,
}
impl Context {
    /// Buffer for primary sequence data
    pub fn sbuf(&self) -> &[u8] {
        &self.sbuf
    }

    /// Mutable reference to primary sequence data
    pub fn sbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sbuf
    }

    /// Buffer for extended sequence data
    pub fn xbuf(&self) -> &[u8] {
        &self.xbuf
    }

    /// Mutable reference to extended sequence data
    pub fn xbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xbuf
    }

    /// Buffer for primary sequence header
    pub fn sheader(&self) -> &[u8] {
        &self.sheader
    }

    /// Mutable reference to primary sequence header
    pub fn sheader_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sheader
    }

    /// Buffer for extended sequence header
    pub fn xheader(&self) -> &[u8] {
        &self.xheader
    }

    /// Mutable reference to extended sequence header
    pub fn xheader_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xheader
    }

    /// Buffer for primary sequence quality scores
    pub fn squal(&self) -> &[u8] {
        &self.squal
    }

    /// Mutable reference to primary sequence quality scores
    pub fn squal_mut(&mut self) -> &mut Vec<u8> {
        &mut self.squal
    }

    /// Buffer for extended sequence quality scores
    pub fn xqual(&self) -> &[u8] {
        &self.xqual
    }

    /// Mutable reference to extended sequence quality scores
    pub fn xqual_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xqual
    }

    /// Clear all buffers
    pub fn clear(&mut self) {
        self.sbuf.clear();
        self.xbuf.clear();
        self.sheader.clear();
        self.xheader.clear();
        self.squal.clear();
        self.xqual.clear();
    }

    /// Fill missing quality scores with default value on primary sequence
    pub fn fill_missing_squal(&mut self) {
        if self.squal.len() != self.sbuf.len() {
            self.squal.clear();
            self.squal.resize(self.sbuf.len(), DEFAULT_QUALITY);
        }
    }

    /// Fill missing quality scores with default value on extended sequence
    pub fn fill_missing_xqual(&mut self) {
        if self.xqual.len() != self.xbuf.len() {
            self.xqual.clear();
            self.xqual.resize(self.xbuf.len(), DEFAULT_QUALITY);
        }
    }

    /// Fill the context with *only* sequence data from the record.
    pub fn fill_sequences<R: BinseqRecord>(&mut self, record: R) -> Result<()> {
        self.clear();

        // Primary sequence
        {
            record.decode_s(&mut self.sbuf)?;
        }

        // Extended sequence
        if record.is_paired() {
            record.decode_x(&mut self.xbuf)?;
        }

        Ok(())
    }

    /// Fill the context with all data from a BinseqRecord.
    ///
    /// Note: Clears the context before filling it with data.
    pub fn fill<R: BinseqRecord>(&mut self, record: R) -> Result<()> {
        self.clear();

        // Primary sequence
        {
            record.decode_s(&mut self.sbuf)?;
            record.sheader(&mut self.sheader);
            self.squal.extend_from_slice(record.squal());
            if self.squal.is_empty() {
                self.fill_missing_squal();
            }
        }

        // Extended sequence
        if record.is_paired() {
            record.decode_x(&mut self.xbuf)?;
            record.xheader(&mut self.xheader);
            self.xqual.extend_from_slice(record.xqual());
            if self.xqual.is_empty() {
                self.fill_missing_xqual();
            }
        }

        Ok(())
    }
}
