use anyhow::{bail, Result};
use std::io::Read;

use crate::{BinseqHeader, ReadError, RecordConfig, RefRecord};

use super::{utils::fill_record_set, BinseqRead, RecordSet, SingleEndRead};

const DEFAULT_CAPACITY: usize = 2048;

#[derive(Debug)]
pub struct SingleReader<R: Read> {
    inner: R,
    header: BinseqHeader,
    record_set: RecordSet,
    config: RecordConfig,
    pos: usize,
    n_processed: usize,
    finished: bool,
}
impl<R: Read> SingleReader<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let header = BinseqHeader::from_reader(&mut inner)?;
        if header.xlen != 0 {
            bail!(ReadError::UnexpectedPairedBinseq(header.xlen))
        }
        let config = RecordConfig::new(header.slen);
        let record_set = RecordSet::new(DEFAULT_CAPACITY, config);
        Ok(Self {
            inner,
            header,
            record_set,
            config,
            pos: 0,
            n_processed: 0,
            finished: false,
        })
    }

    pub fn config(&self) -> RecordConfig {
        self.config
    }

    fn fill_record_set(&mut self) -> Result<bool> {
        self.finished =
            fill_record_set(&mut self.inner, &mut self.record_set, &mut self.n_processed)?;
        Ok(self.finished)
    }

    fn next_record<'a>(&'a mut self) -> Option<Result<RefRecord<'a>>> {
        if self.record_set.is_empty() || self.pos == self.record_set.n_records() {
            match self.fill_record_set() {
                Ok(true) => {
                    // EOF reached and no more records in set
                    if self.record_set.is_empty() {
                        return None;
                    }
                    self.pos = 0;
                }
                Ok(false) => {
                    // More records in set and not EOF
                    self.pos = 0;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        let record = self.record_set.get_record(self.pos)?;
        self.pos += 1;

        Some(Ok(record))
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Fill an external record set with records
    /// Returns true if EOF was reached, false if the record set was filled
    pub fn fill_external_set(&mut self, record_set: &mut RecordSet) -> Result<bool> {
        // Verify the external record set has compatible configuration
        if record_set.config() != self.config {
            bail!(ReadError::IncompatibleRecordSet(
                self.config,
                record_set.config(),
            ));
        }

        // Use the existing fill_record_set utility function
        fill_record_set(&mut self.inner, record_set, &mut self.n_processed)
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
