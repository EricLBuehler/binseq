use anyhow::{bail, Result};
use std::io::Read;

use crate::{
    BinseqHeader, BinseqRead, PairedEndRead, PairedRead, ReadError, RecordConfig, RecordSet,
    RefRecord, RefRecordPair,
};

use super::utils::fill_paired_record_set;

#[derive(Debug)]
pub struct PairedReader<R: Read> {
    /// Inner reader
    inner: R,

    /// Header of the file
    header: BinseqHeader,

    /// Record set for paired reads
    record_set: RecordSet,

    /// Configuration for the primary sequence
    sconfig: RecordConfig,

    /// Configuration for the extended sequence
    xconfig: RecordConfig,

    /// Current position in the record set
    pos: usize,

    /// Number of record pairs processed
    n_processed: usize,

    /// Finished reading the file
    finished: bool,
}
impl<R: Read> PairedReader<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let header = BinseqHeader::from_reader(&mut inner)?;
        if header.xlen == 0 {
            bail!(ReadError::MissingPairedSequence(header.slen))
        }
        let sconfig = RecordConfig::new(header.slen);
        let xconfig = RecordConfig::new(header.xlen);
        let record_set = RecordSet::new_paired(sconfig, xconfig);
        Ok(Self {
            inner,
            header,
            record_set,
            sconfig,
            xconfig,
            pos: 0,
            n_processed: 0,
            finished: false,
        })
    }

    fn fill_record_set(&mut self) -> Result<bool> {
        self.finished =
            fill_paired_record_set(&mut self.inner, &mut self.record_set, &mut self.n_processed)?;
        Ok(self.finished)
    }

    fn next_pair<'a>(&'a mut self) -> Option<Result<RefRecordPair<'a>>> {
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

        let record = self.record_set.get_record_pair(self.pos)?;
        self.pos += 1;

        Some(Ok(record))
    }
}

impl<R: Read> BinseqRead for PairedReader<R> {
    fn next(&mut self) -> Option<Result<RefRecord>> {
        self.next_pair().map(|pair| pair.map(|pair| pair.primary()))
    }

    fn header(&self) -> BinseqHeader {
        self.header
    }

    fn is_paired(&self) -> bool {
        true
    }

    fn record_size(&self) -> usize {
        // flag + primary + extended
        8 + (self.sconfig.n_chunks * 8) + (self.xconfig.n_chunks * 8)
    }

    fn n_processed(&self) -> usize {
        self.n_processed
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl<R: Read> PairedRead for PairedReader<R> {
    fn next_paired(&mut self) -> Option<Result<RefRecordPair>> {
        self.next_pair()
    }

    fn next_primary(&mut self) -> Option<Result<RefRecord>> {
        self.next_paired()
            .map(|record| record.map(|record| record.primary()))
    }

    fn next_extended(&mut self) -> Option<Result<RefRecord>> {
        self.next_paired()
            .map(|record| record.map(|record| record.extended()))
    }
}

impl<R: Read> PairedEndRead for PairedReader<R> {}
