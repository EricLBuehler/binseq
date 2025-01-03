use anyhow::{bail, Result};
use std::{
    io::Read,
    sync::{Arc, Mutex},
    thread,
};

use super::utils::fill_single_record_set;
use crate::{
    BinseqHeader, BinseqRead, ParallelProcessor, ReadError, RecordConfig, RecordSet, RefRecord,
    SingleEndRead,
};

const DEFAULT_CAPACITY: usize = 10 * 1024;

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
            fill_single_record_set(&mut self.inner, &mut self.record_set, &mut self.n_processed)?;
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
        if record_set.sconfig() != self.config {
            bail!(ReadError::IncompatibleRecordSet(
                self.config,
                record_set.sconfig(),
            ));
        }

        // Use the existing fill_record_set utility function
        fill_single_record_set(&mut self.inner, record_set, &mut self.n_processed)
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

/// Implementation of parallel processing for single-end readers
impl<R: Read + Send + Sync + 'static> SingleReader<R> {
    /// Process records in parallel using the provided processor
    pub fn process_parallel<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()> {
        let config = self.config();

        // Create shared reader
        let reader = Arc::new(Mutex::new(self));

        // Create processors for each thread
        let processors = Arc::new(Mutex::new(
            (0..num_threads)
                .map(|_| processor.clone())
                .collect::<Vec<_>>(),
        ));

        let mut handles = Vec::new();

        // Spawn worker threads
        for thread_id in 0..num_threads {
            let reader = Arc::clone(&reader);
            let processors = Arc::clone(&processors);

            let handle = thread::spawn(move || -> Result<()> {
                let mut record_set = RecordSet::new(DEFAULT_CAPACITY, config);

                loop {
                    // Fill this thread's record set
                    let finished = {
                        let mut reader = reader.lock().unwrap();
                        reader.fill_external_set(&mut record_set)?
                    };

                    // Process records in this batch
                    {
                        let mut processors = processors.lock().unwrap();
                        let processor = &mut processors[thread_id];

                        for i in 0..record_set.n_records() {
                            let record = record_set.get_record(i).unwrap();
                            processor.process_record(record)?;
                        }

                        processor.on_batch_complete()?;
                    }

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
