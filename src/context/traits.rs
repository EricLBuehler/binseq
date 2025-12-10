use crate::{BinseqRecord, Result};

pub const DEFAULT_QUALITY: u8 = b'?';

/// Trait for handling reusable buffers in decoding BINSEQ records.
pub trait Context: Clone + Default {
    /// Replaces the contents of the context with the contents of the given record.
    ///
    /// This will clear all existing data and fill the context with the contents of the record.
    fn fill<R: BinseqRecord>(&mut self, record: &R) -> Result<()>;
}

/// Trait for handling reusable buffers in decoding BINSEQ records focused on nucleotide sequences.
pub trait SequenceContext {
    fn sbuf(&self) -> &[u8];
    fn xbuf(&self) -> &[u8];
    fn sbuf_mut(&mut self) -> &mut Vec<u8>;
    fn xbuf_mut(&mut self) -> &mut Vec<u8>;
    #[inline]
    fn clear_sequences(&mut self) {
        self.sbuf_mut().clear();
        self.xbuf_mut().clear();
    }
    #[inline]
    fn fill_sequences<R: BinseqRecord>(&mut self, record: &R) -> Result<()> {
        self.clear_sequences();
        record.decode_s(self.sbuf_mut())?;
        if record.is_paired() {
            record.decode_x(self.xbuf_mut())?;
        }
        Ok(())
    }
}

/// Trait for handling reusable buffers in decoding BINSEQ records focused on quality data.
pub trait QualityContext {
    fn squal(&self) -> &[u8];
    fn xqual(&self) -> &[u8];
    fn squal_mut(&mut self) -> &mut Vec<u8>;
    fn xqual_mut(&mut self) -> &mut Vec<u8>;
    #[inline]
    fn clear_qualities(&mut self) {
        self.squal_mut().clear();
        self.xqual_mut().clear();
    }
    #[inline]
    fn fill_qualities<R: BinseqRecord>(&mut self, record: &R) -> Result<()> {
        if record.has_quality() {
            let slen = record.slen() as usize;
            let squal = self.squal_mut();
            if squal.len() != slen {
                squal.resize(slen, DEFAULT_QUALITY);
            }
            squal.copy_from_slice(record.squal());

            if record.is_paired() {
                let xlen = record.xlen() as usize;
                let xqual = self.xqual_mut();
                if xqual.len() != xlen {
                    xqual.resize(xlen, DEFAULT_QUALITY);
                }
                xqual.copy_from_slice(record.xqual());
            }
        } else {
            self.ensure_quality_capacity(record);
        }
        Ok(())
    }
    #[inline]
    fn ensure_quality_capacity<R: BinseqRecord>(&mut self, record: &R) {
        let slen = record.slen() as usize;
        let xlen = record.xlen() as usize;

        // only resize if its not the right size
        let squal = self.squal_mut();
        if squal.len() != slen {
            squal.resize(slen, DEFAULT_QUALITY);
        }

        // Only resize if there's an extended sequence and it's not already the right size
        let xqual = self.xqual_mut();
        if xqual.len() != xlen {
            xqual.resize(xlen, DEFAULT_QUALITY);
        }
    }
}

/// Trait for handling reusable buffers in decoding BINSEQ records focused on header data.
pub trait HeaderContext {
    fn sheader(&self) -> &[u8];
    fn sheader_mut(&mut self) -> &mut Vec<u8>;
    fn xheader(&self) -> &[u8];
    fn xheader_mut(&mut self) -> &mut Vec<u8>;
    #[inline]
    fn clear_headers(&mut self) {
        self.sheader_mut().clear();
        self.xheader_mut().clear();
    }

    #[inline]
    fn fill_headers<R: BinseqRecord>(&mut self, record: &R) {
        self.clear_headers();
        self.sheader_mut().extend_from_slice(&record.sheader());
        if record.is_paired() {
            self.xheader_mut().extend_from_slice(&record.xheader());
        }
    }
}
