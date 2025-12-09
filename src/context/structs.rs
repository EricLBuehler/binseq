use super::traits::{Context, HeaderContext, QualityContext, SequenceContext};
use crate::{BinseqRecord, Result};

#[derive(Clone, Default)]
pub struct Ctx {
    sbuf: Vec<u8>,
    xbuf: Vec<u8>,
    sheader: Vec<u8>,
    xheader: Vec<u8>,
    squal: Vec<u8>,
    xqual: Vec<u8>,
}
impl SequenceContext for Ctx {
    #[inline]
    fn sbuf(&self) -> &[u8] {
        &self.sbuf
    }
    #[inline]
    fn xbuf(&self) -> &[u8] {
        &self.xbuf
    }
    #[inline]
    fn sbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sbuf
    }
    #[inline]
    fn xbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xbuf
    }
}
impl QualityContext for Ctx {
    #[inline]
    fn squal(&self) -> &[u8] {
        &self.squal
    }
    #[inline]
    fn xqual(&self) -> &[u8] {
        &self.xqual
    }
    #[inline]
    fn squal_mut(&mut self) -> &mut Vec<u8> {
        &mut self.squal
    }
    #[inline]
    fn xqual_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xqual
    }
}
impl HeaderContext for Ctx {
    #[inline]
    fn sheader(&self) -> &[u8] {
        &self.sheader
    }
    #[inline]
    fn xheader(&self) -> &[u8] {
        &self.xheader
    }
    #[inline]
    fn sheader_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sheader
    }
    #[inline]
    fn xheader_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xheader
    }
}
impl Context for Ctx {
    #[inline]
    fn fill<R: BinseqRecord>(&mut self, record: &R) -> Result<()> {
        self.fill_sequences(record)?;
        self.fill_qualities(record)?;
        self.fill_headers(record);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SeqCtx {
    sbuf: Vec<u8>,
    xbuf: Vec<u8>,
}
impl SequenceContext for SeqCtx {
    #[inline]
    fn sbuf(&self) -> &[u8] {
        &self.sbuf
    }
    #[inline]
    fn xbuf(&self) -> &[u8] {
        &self.xbuf
    }
    #[inline]
    fn sbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sbuf
    }
    #[inline]
    fn xbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xbuf
    }
}
impl Context for SeqCtx {
    #[inline]
    fn fill<R: BinseqRecord>(&mut self, record: &R) -> Result<()> {
        self.fill_sequences(record)
    }
}

#[derive(Clone, Default)]
pub struct SeqHeaderCtx {
    sbuf: Vec<u8>,
    xbuf: Vec<u8>,
    sheader: Vec<u8>,
    xheader: Vec<u8>,
}
impl SequenceContext for SeqHeaderCtx {
    #[inline]
    fn sbuf(&self) -> &[u8] {
        &self.sbuf
    }
    #[inline]
    fn xbuf(&self) -> &[u8] {
        &self.xbuf
    }
    #[inline]
    fn sbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sbuf
    }
    #[inline]
    fn xbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xbuf
    }
}
impl HeaderContext for SeqHeaderCtx {
    #[inline]
    fn sheader(&self) -> &[u8] {
        &self.sheader
    }
    #[inline]
    fn xheader(&self) -> &[u8] {
        &self.xheader
    }
    #[inline]
    fn sheader_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sheader
    }
    #[inline]
    fn xheader_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xheader
    }
}
impl Context for SeqHeaderCtx {
    #[inline]
    fn fill<R: BinseqRecord>(&mut self, record: &R) -> Result<()> {
        self.fill_sequences(record)?;
        self.fill_headers(record);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SeqQualCtx {
    sbuf: Vec<u8>,
    xbuf: Vec<u8>,
    squal: Vec<u8>,
    xqual: Vec<u8>,
}
impl SequenceContext for SeqQualCtx {
    #[inline]
    fn sbuf(&self) -> &[u8] {
        &self.sbuf
    }
    #[inline]
    fn xbuf(&self) -> &[u8] {
        &self.xbuf
    }
    #[inline]
    fn sbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.sbuf
    }
    #[inline]
    fn xbuf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xbuf
    }
}
impl QualityContext for SeqQualCtx {
    #[inline]
    fn squal(&self) -> &[u8] {
        &self.squal
    }
    #[inline]
    fn xqual(&self) -> &[u8] {
        &self.xqual
    }
    #[inline]
    fn squal_mut(&mut self) -> &mut Vec<u8> {
        &mut self.squal
    }
    #[inline]
    fn xqual_mut(&mut self) -> &mut Vec<u8> {
        &mut self.xqual
    }
}
impl Context for SeqQualCtx {
    #[inline]
    fn fill<R: BinseqRecord>(&mut self, record: &R) -> Result<()> {
        self.fill_sequences(record)?;
        self.fill_qualities(record)?;
        Ok(())
    }
}
