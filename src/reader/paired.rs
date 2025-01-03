use anyhow::{bail, Result};
use std::io::Read;

use crate::{
    BinseqHeader, BinseqRead, PairedEndRead, PairedRead, ReadError, RecordConfig, RefRecord,
    RefRecordPair,
};

use super::utils::{next_binseq, next_flag};

#[derive(Debug)]
pub struct PairedReader<R: Read> {
    /// Inner reader
    inner: R,

    /// Header of the file
    header: BinseqHeader,

    /// Buffer for the flag
    flag: u64,

    /// Buffer for the primary sequence
    sbuf: Vec<u64>,

    /// Buffer for the extended sequence
    xbuf: Vec<u64>,

    /// Configuration for the primary sequence
    sconfig: RecordConfig,

    /// Configuration for the extended sequence
    xconfig: RecordConfig,

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
        Ok(Self {
            inner,
            header,
            flag: 0,
            sbuf: Vec::new(),
            xbuf: Vec::new(),
            sconfig: RecordConfig::new(header.slen),
            xconfig: RecordConfig::new(header.xlen),
            n_processed: 0,
            finished: false,
        })
    }

    fn next_pair<'a>(&'a mut self) -> Option<Result<RefRecordPair<'a>>> {
        // Clear the last sequence buffer
        self.sbuf.clear();
        self.xbuf.clear();

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

        // Read the primary sequence
        match next_binseq(
            &mut self.inner,
            &mut self.sbuf,
            self.sconfig,
            self.n_processed,
        ) {
            Ok(_) => {}
            Err(e) => return Some(Err(e)),
        }

        // Read the extended sequence
        match next_binseq(
            &mut self.inner,
            &mut self.xbuf,
            self.xconfig,
            self.n_processed,
        ) {
            Ok(_) => {}
            Err(e) => return Some(Err(e)),
        }

        // Create the record
        let ref_record = RefRecordPair::new(
            self.flag,
            &self.sbuf,
            &self.xbuf,
            self.sconfig,
            self.xconfig,
        );

        // Increment the number of processed records
        self.n_processed += 1;

        // Return the record as a reference
        Some(Ok(ref_record))
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
