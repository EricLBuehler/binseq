use anyhow::{bail, Result};
use memmap2::Mmap;
use std::{fs::File, path::Path, sync::Arc};

use crate::{
    header::SIZE_HEADER, BinseqHeader, BinseqRead, ParallelProcessor, ReadError, RecordConfig,
    RecordSet, RefRecord, SingleEndRead,
};

pub struct MmapReader {
    /// Memory mapped file contents
    mmap: Arc<Mmap>,

    /// Header information
    header: BinseqHeader,

    /// Record set for efficient batch processing
    record_set: RecordSet,

    /// Current position in the record set
    pos: usize,

    /// Current file offset
    offset: usize,

    /// Number of records processed
    n_processed: usize,

    /// Whether we've reached the end
    finished: bool,
}

impl MmapReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;

        // Verify it's a regular file before attempting to map
        if !file.metadata()?.is_file() {
            bail!("Not a regular file");
        }

        // Safety: The file is open and won't be modified while mapped
        let mmap = unsafe { Mmap::map(&file)? };

        // Read header from mapped memory
        let header = {
            let mut header_bytes = [0u8; SIZE_HEADER];
            header_bytes.copy_from_slice(&mmap[..SIZE_HEADER]);
            BinseqHeader::from_bytes(&header_bytes)?
        };

        if header.xlen != 0 {
            bail!(ReadError::UnexpectedPairedBinseq(header.xlen));
        }

        let config = RecordConfig::new(header.slen);
        let record_set = RecordSet::new(config);

        Ok(Self {
            mmap: Arc::new(mmap),
            header,
            record_set,
            pos: 0,
            offset: SIZE_HEADER,
            n_processed: 0,
            finished: false,
        })
    }

    fn fill_record_set(&mut self) -> Result<bool> {
        let finished =
            self.record_set
                .fill_from_mmap_single(&self.mmap, &mut self.offset, self.mmap.len())?;
        self.n_processed += self.record_set.n_records();
        Ok(finished)
    }

    fn next_record(&mut self) -> Option<Result<RefRecord<'_>>> {
        if self.record_set.is_empty() || self.pos == self.record_set.n_records() {
            match self.fill_record_set() {
                Ok(true) => {
                    if self.record_set.is_empty() {
                        self.finished = true;
                        return None;
                    }
                    self.pos = 0;
                }
                Ok(false) => {
                    self.pos = 0;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        let record = self.record_set.get_record(self.pos)?;
        self.pos += 1;
        Some(Ok(record))
    }

    /// Fill an external record set with records
    /// Returns true if EOF was reached, false if the record set was filled
    pub fn fill_external_set(&mut self, record_set: &mut RecordSet) -> Result<bool> {
        // Verify the external record set has compatible configuration
        if record_set.sconfig() != self.record_set.sconfig() {
            bail!(ReadError::IncompatibleRecordSet(
                self.record_set.sconfig(),
                record_set.sconfig(),
            ));
        }

        let finished =
            record_set.fill_from_mmap_single(&self.mmap, &mut self.offset, self.mmap.len())?;
        self.n_processed += record_set.n_records();
        Ok(finished)
    }

    /// Returns the number of records in the file
    pub fn num_records(&self) -> usize {
        (self.mmap.len() - SIZE_HEADER) / self.record_size()
    }
}

impl BinseqRead for MmapReader {
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
        8 + self.record_set.sconfig().n_chunks * 8 // flag + sequence chunks
    }

    fn n_processed(&self) -> usize {
        self.n_processed
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl SingleEndRead for MmapReader {}

/// Parallel processing for memory-mapped readers
impl MmapReader {
    pub fn process_parallel<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()> {
        let file_size = self.mmap.len();
        let record_size = self.record_size();
        let config = self.record_set.sconfig();
        let mmap = self.mmap;

        // Calculate chunk size for each thread
        let records_per_thread = ((file_size - SIZE_HEADER) / record_size).div_ceil(num_threads);
        let mut handles = Vec::new();

        for thread_id in 0..num_threads {
            let mmap = Arc::clone(&mmap);
            let mut processor = processor.clone();
            processor.set_tid(thread_id);

            let handle = std::thread::spawn(move || -> Result<()> {
                // Calculate this thread's range
                let start_record = thread_id * records_per_thread;
                let start_offset = SIZE_HEADER + (start_record * record_size);
                let end_offset = std::cmp::min(
                    file_size,
                    SIZE_HEADER + ((thread_id + 1) * records_per_thread * record_size),
                );

                let mut offset = start_offset;
                let mut record_set = RecordSet::new(config);

                loop {
                    // Fill record set from our assigned range
                    let finished =
                        record_set.fill_from_mmap_single(&mmap, &mut offset, end_offset)?;

                    // Process records in this batch
                    for i in 0..record_set.n_records() {
                        let record = record_set
                            .get_record(i)
                            .expect("Record should exist within range of set");
                        processor.process_record(record)?;
                    }
                    processor.on_batch_complete()?;

                    // Exit if we've processed our chunk
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
