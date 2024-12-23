use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Read;

use crate::{BinseqHeader, ReadError, RecordConfig, RefRecord};

#[derive(Debug)]
pub struct BinseqReader<R: Read> {
    inner: R,
    header: BinseqHeader,
    flag: u64,
    buffer: Vec<u64>,
    config: RecordConfig,
    n_processed: usize,
}
impl<R: Read> BinseqReader<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let header = BinseqHeader::from_reader(&mut inner)?;
        if header.xlen != 0 {
            bail!(ReadError::UnexpectedPairedBinseq(header.xlen))
        }
        let buffer = Vec::new();
        let flag = 0;
        let config = RecordConfig::new(header.slen);
        Ok(Self {
            inner,
            header,
            flag,
            buffer,
            config,
            n_processed: 0,
        })
    }

    fn next_flag(&mut self) -> Result<bool> {
        match self.inner.read_u64::<LittleEndian>() {
            Ok(flag) => {
                self.flag = flag;
                Ok(true)
            }
            Err(e) => {
                // check if there are any bytes left in the reader
                let mut buf = [0u8; 1];
                match self.inner.read(&mut buf) {
                    Ok(0) => Ok(false),
                    _ => {
                        bail!(ReadError::UnexpectedEndOfStreamFlag(e, self.n_processed));
                    }
                }
            }
        }
    }

    fn next_long<'a>(&'a mut self) -> Result<()> {
        (0..self.config.n_chunks).try_for_each(|_| match self.inner.read_u64::<LittleEndian>() {
            Ok(bits) => {
                self.buffer.push(bits);
                Ok(())
            }
            Err(e) => bail!(ReadError::UnexpectedEndOfStreamSequence(
                e,
                self.n_processed
            )),
        })
    }

    pub fn next<'a>(&'a mut self) -> Option<Result<RefRecord<'a>>> {
        // Clear the last sequence buffer
        self.buffer.clear();

        // Read the flag
        match self.next_flag() {
            Ok(true) => {}                 // continue with the next step
            Ok(false) => return None,      // end of file
            Err(e) => return Some(Err(e)), // unexpected error
        }

        // Read the sequence
        match self.next_long() {
            Ok(_) => {}
            Err(e) => return Some(Err(e)),
        }

        // Create the record
        let ref_record = RefRecord::new(self.flag, &self.buffer, self.config);

        // Increment the number of processed records
        self.n_processed += 1;

        // Return the record as a reference
        Some(Ok(ref_record))
    }

    pub fn header(&self) -> BinseqHeader {
        self.header
    }
}
