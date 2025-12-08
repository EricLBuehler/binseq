use crate::{BinseqRecord, Result};

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
    pub fn sbuf(&self) -> &[u8] {
        &self.sbuf
    }
    pub fn xbuf(&self) -> &[u8] {
        &self.xbuf
    }
    pub fn sheader(&self) -> &[u8] {
        &self.sheader
    }
    pub fn xheader(&self) -> &[u8] {
        &self.xheader
    }
    pub fn squal(&self) -> &[u8] {
        &self.squal
    }
    pub fn xqual(&self) -> &[u8] {
        &self.xqual
    }
    pub fn clear(&mut self) {
        self.sbuf.clear();
        self.xbuf.clear();
        self.sheader.clear();
        self.xheader.clear();
        self.squal.clear();
        self.xqual.clear();
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
        }

        // Extended sequence
        if record.is_paired() {
            record.decode_x(&mut self.xbuf)?;
            record.xheader(&mut self.xheader);
            self.xqual.extend_from_slice(record.xqual());
        }

        Ok(())
    }
}
