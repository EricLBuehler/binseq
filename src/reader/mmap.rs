use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use bytemuck::cast_slice;
use memmap2::Mmap;

use crate::error::{ReadError, Result};
use crate::header::{BinseqHeader, SIZE_HEADER};
use crate::ParallelProcessor;

#[derive(Clone, Copy)]
pub struct RefRecord<'a> {
    /// The position of this record in the file
    id: usize,
    /// The underlying u64 buffer representing the record
    buffer: &'a [u64],
    /// The configuration that defines the boundaries of the record components
    config: RecordConfig,
}
impl<'a> RefRecord<'a> {
    pub fn new(id: usize, buffer: &'a [u64], config: RecordConfig) -> Self {
        assert_eq!(buffer.len(), config.record_size_u64());
        Self { id, buffer, config }
    }
    pub fn id(&self) -> usize {
        self.id
    }
    pub fn flag(&self) -> u64 {
        self.buffer[0]
    }
    pub fn sbuf(&self) -> &[u64] {
        &self.buffer[1..1 + self.config.schunk]
    }
    pub fn xbuf(&self) -> &[u64] {
        &self.buffer[1 + self.config.schunk..]
    }
    pub fn decode_s(&self, dbuf: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.sbuf(), self.config.slen, dbuf)?;
        Ok(())
    }
    pub fn decode_x(&self, dbuf: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.xbuf(), self.config.xlen, dbuf)?;
        Ok(())
    }
    pub fn paired(&self) -> bool {
        self.config.paired()
    }
}

#[derive(Clone, Copy)]
pub struct RecordConfig {
    /// The primary sequence length (bp)
    slen: usize,
    /// The extended sequence length (bp)
    xlen: usize,
    /// The number of u64 chunks required to represent the primary sequence
    schunk: usize,
    /// The number of u64 chunks required to represent the extended sequence
    xchunk: usize,
}
impl RecordConfig {
    pub fn new(slen: usize, xlen: usize) -> Self {
        Self {
            slen,
            xlen,
            schunk: slen.div_ceil(32),
            xchunk: xlen.div_ceil(32),
        }
    }

    pub fn from_header(header: &BinseqHeader) -> Self {
        Self::new(header.slen as usize, header.xlen as usize)
    }

    pub fn paired(&self) -> bool {
        self.xlen > 0
    }

    /// Returns the full record size in bytes (u8):
    /// 8 * (schunk + xchunk + 1 (flag))
    pub fn record_size_bytes(&self) -> usize {
        8 * self.record_size_u64()
    }

    /// Returns the full record size in u64
    /// schunk + xchunk + 1 (flag)
    pub fn record_size_u64(&self) -> usize {
        self.schunk + self.xchunk + 1
    }
}

pub struct MmapReader {
    /// Memory mapped file contents
    mmap: Arc<Mmap>,

    /// Record configuration
    config: RecordConfig,
}

impl MmapReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Verify input file is a file before attempting to map
        let file = File::open(path)?;
        if !file.metadata()?.is_file() {
            return Err(ReadError::IncompatibleFile.into());
        }

        // Safety: the file is open and won't be modified while mapped
        let mmap = unsafe { Mmap::map(&file)? };

        // Read header from mapped memory
        let header = BinseqHeader::from_buffer(&mmap)?;

        // Record configuraration
        let config = RecordConfig::from_header(&header);

        // Immediately validate the size of the file against the expected byte size of records
        if (mmap.len() - SIZE_HEADER) % config.record_size_bytes() != 0 {
            return Err(ReadError::FileTruncation(mmap.len()).into());
        }

        Ok(Self {
            mmap: Arc::new(mmap),
            config,
        })
    }

    /// Returns the number of records
    pub fn num_records(&self) -> usize {
        (self.mmap.len() - SIZE_HEADER) / self.config.record_size_bytes()
    }

    /// Returns a specific record
    pub fn get(&self, idx: usize) -> Result<RefRecord> {
        if idx > self.num_records() {
            return Err(ReadError::OutOfRange(idx, self.num_records()).into());
        }
        let lbound = SIZE_HEADER + (idx * self.config.record_size_bytes());
        let rbound = lbound + self.config.record_size_bytes();
        let bytes = &self.mmap[lbound..rbound];
        let buffer = cast_slice(bytes);
        Ok(RefRecord::new(idx, buffer, self.config))
    }
}

pub const BATCH_SIZE: usize = 1024;

/// Parallel processing for memory-mapped readers
impl MmapReader {
    pub fn process_parallel<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()> {
        // Calculate number of records for each thread
        let num_records = self.num_records();
        let records_per_thread = num_records.div_ceil(num_threads);

        // Arc self
        let reader = Arc::new(self);

        // Build thread handles
        let mut handles = Vec::new();
        for tid in 0..num_threads {
            let mut processor = processor.clone();
            let reader = reader.clone();
            processor.set_tid(tid);

            let handle = std::thread::spawn(move || -> Result<()> {
                let start_idx = tid * records_per_thread;
                let end_idx = (start_idx + records_per_thread).min(num_records);

                for (batch_idx, idx) in (start_idx..end_idx).enumerate() {
                    let record = reader.get(idx)?;
                    processor.process_record(record)?;

                    if batch_idx % BATCH_SIZE == 0 {
                        processor.on_batch_complete()?;
                    }
                }
                processor.on_batch_complete()?;

                Ok(())
            });

            handles.push(handle);
        }

        for handle in handles {
            handle
                .join()
                .expect("Error joining handle (1)")
                .expect("Error joining handle (2)");
        }

        Ok(())
    }
}
