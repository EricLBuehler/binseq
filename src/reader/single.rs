use anyhow::{bail, Result};
use std::io::Read;

use crate::{BinseqHeader, ReadError, RecordConfig, RefRecord};

use super::{
    utils::{next_binseq, next_flag},
    BinseqRead, SingleEndRead,
};

#[derive(Debug)]
pub struct SingleReader<R: Read> {
    inner: R,
    header: BinseqHeader,
    flag: u64,
    buffer: Vec<u64>,
    config: RecordConfig,
    n_processed: usize,
    finished: bool,
}
impl<R: Read> SingleReader<R> {
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
            finished: false,
        })
    }

    fn next_record<'a>(&'a mut self) -> Option<Result<RefRecord<'a>>> {
        // Clear the last sequence buffer
        self.buffer.clear();

        // Read the flag
        match next_flag(&mut self.inner, self.n_processed) {
            Ok(Some(flag)) => {
                self.flag = flag;
            }
            Ok(None) => {
                self.finished = true;
                return None;
            }
            Err(e) => return Some(Err(e)),
        }

        // Read the sequence
        match next_binseq(
            &mut self.inner,
            &mut self.buffer,
            self.config,
            self.n_processed,
        ) {
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
}

impl<R: Read> BinseqRead for SingleReader<R> {
    fn next(&mut self) -> Option<Result<RefRecord>> {
        self.next_record()
    }

    fn header(&self) -> BinseqHeader {
        self.header
    }

    fn is_paired(&self) -> bool {
        false
    }

    fn record_size(&self) -> usize {
        // flag + sequence
        8 + self.config.n_chunks * 8
    }

    fn n_processed(&self) -> usize {
        self.n_processed
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl<R: Read> SingleEndRead for SingleReader<R> {}
