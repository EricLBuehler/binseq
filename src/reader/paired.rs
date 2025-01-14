use anyhow::{bail, Result};
use std::{
    io::Read,
    sync::{Arc, Mutex},
    thread,
};

use crate::{
    BinseqHeader, BinseqRead, PairedEndRead, PairedRead, ParallelPairedProcessor, ReadError,
    RecordConfig, RecordSet, RefRecord, RefRecordPair,
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

    fn next_pair(&mut self) -> Option<Result<RefRecordPair<'_>>> {
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

    /// Fill an external record set with records
    /// Returns true if EOF was reached, false if the record set was filled
    pub fn fill_external_set(&mut self, record_set: &mut RecordSet) -> Result<bool> {
        // Verify the external record set has compatible configuration
        if record_set.sconfig() != self.sconfig {
            bail!(ReadError::IncompatibleRecordSet(
                self.sconfig,
                record_set.sconfig(),
            ));
        }

        if record_set.xconfig() != self.xconfig {
            bail!(ReadError::IncompatibleRecordSet(
                self.xconfig,
                record_set.xconfig(),
            ));
        }

        // Use the existing fill_record_set utility function
        fill_paired_record_set(&mut self.inner, record_set, &mut self.n_processed)
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

/// Implementation of parallel processing for single-end readers
impl<R: Read + Send + Sync + 'static> PairedReader<R> {
    /// Process records in parallel using the provided processor
    pub fn process_parallel<P: ParallelPairedProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()> {
        let sconfig = self.sconfig;
        let xconfig = self.xconfig;

        // Create shared reader
        let reader = Arc::new(Mutex::new(self));

        let mut handles = Vec::new();

        // Spawn worker threads
        for thread_id in 0..num_threads {
            let reader = Arc::clone(&reader);
            let mut processor = processor.clone();
            processor.set_tid(thread_id);

            let handle = thread::spawn(move || -> Result<()> {
                let mut record_set = RecordSet::new_paired(sconfig, xconfig);

                loop {
                    // Fill this thread's record set
                    let finished = {
                        let mut reader = reader.lock().unwrap();
                        reader.fill_external_set(&mut record_set)?
                    };

                    // Process records in this batch

                    for i in 0..record_set.n_records() {
                        let pair = record_set.get_record_pair(i).unwrap();
                        processor.process_record_pair(pair)?;
                    }
                    processor.on_batch_complete()?;

                    // Exit if we hit EOF and processed all records
                    if finished && record_set.is_empty() {
                        break;
                    }
                }

                Ok(())
            });

            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap()?;
        }

        Ok(())
    }
}
