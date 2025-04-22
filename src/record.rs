use super::Result;

pub trait BinseqRecord {
    /// Returns the global index of the record.
    fn index(&self) -> u64;

    /// Returns the flag value of this record
    fn flag(&self) -> u64;

    /// Returns the length of the primary sequence of this record
    fn slen(&self) -> u64;

    /// Returns the length of the extended sequence of this record
    fn xlen(&self) -> u64;

    /// Returns a reference to the **encoded** primary sequence of this record
    fn sbuf(&self) -> &[u64];

    /// Returns a reference to the **encoded** extended sequence of this record.
    ///
    /// Empty if no extended sequence is present.
    fn xbuf(&self) -> &[u64];

    /// Returns a reference to the quality scores of the primary sequence of this record.
    ///
    /// Empty if no quality scores are present.
    fn squal(&self) -> &[u8] {
        &[]
    }

    /// Returns a reference to the quality scores of the extended sequence of this record.
    ///
    /// Empty if no quality scores are present.
    fn xqual(&self) -> &[u8] {
        &[]
    }

    /// Decodes the primary sequence of this record into the provided buffer.
    fn decode_s(&self, buf: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.sbuf(), self.slen() as usize, buf)?;
        Ok(())
    }

    /// Decodes the extended sequence of this record into the provided buffer.
    fn decode_x(&self, buf: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.xbuf(), self.xlen() as usize, buf)?;
        Ok(())
    }

    /// A convenience function to check if the record is paired.
    fn is_paired(&self) -> bool {
        self.xlen() > 0
    }

    /// A convenience function to check if record has associated quality scores
    fn has_quality(&self) -> bool {
        !self.squal().is_empty()
    }
}
