use crate::{BinseqRecord, Result};

pub const DEFAULT_QUALITY: u8 = b'?';

pub trait Context: Clone + Default {
    /// Replaces the contents of the context with the contents of the given record.
    ///
    /// This will clear all existing data and fill the context with the contents of the record.
    fn fill<R: BinseqRecord>(&mut self, record: &R) -> Result<()>;
}

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
                squal.reserve(slen);
                unsafe {
                    squal.set_len(slen);
                }
            }
            squal[..slen].copy_from_slice(record.squal());

            if record.is_paired() {
                let xlen = record.xlen() as usize;
                let xqual = self.xqual_mut();
                if xqual.len() != xlen {
                    xqual.reserve(xlen);
                    unsafe {
                        xqual.set_len(xlen);
                    }
                }
                xqual[..xlen].copy_from_slice(record.xqual());
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
        if self.squal().len() < slen {
            self.squal_mut().clear();
            self.squal_mut().resize(slen, DEFAULT_QUALITY);
        }

        // Only resize if there's an extended sequence and it's not already the right size
        if xlen > 0 && self.xqual().len() < xlen {
            self.xqual_mut().clear();
            self.xqual_mut().resize(xlen, DEFAULT_QUALITY);
        }
    }
}

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
        record.sheader(self.sheader_mut());
        if record.is_paired() {
            record.xheader(self.xheader_mut());
        }
    }
}
