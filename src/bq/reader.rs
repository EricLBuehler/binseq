//! Binary sequence reader module
//!
//! This module provides functionality for reading binary sequence files using either:
//! 1. Memory mapping for efficient access to entire files
//! 2. Streaming for processing data as it arrives
//!
//! It supports both sequential and parallel processing of records,
//! with configurable record layouts for different sequence types.

use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use bitnuc::BitSize;
use bytemuck::cast_slice;
use memmap2::Mmap;

use super::header::{BinseqHeader, SIZE_HEADER};
use crate::{
    error::{ReadError, Result},
    BinseqRecord, Error, ParallelProcessor, ParallelReader,
};

/// A reference to a binary sequence record in a memory-mapped file
///
/// This struct provides a view into a single record within a binary sequence file,
/// allowing access to the record's components (sequence data, flags, etc.) without
/// copying the data from the memory-mapped file.
///
/// The record's data is stored in a compact binary format where:
/// - The first u64 contains flags
/// - Subsequent u64s contain the primary sequence data
/// - If present, final u64s contain the extended sequence data
#[derive(Clone, Copy)]
pub struct RefRecord<'a> {
    /// The position (index) of this record in the file (0-based record index, not byte offset)
    id: u64,
    /// The underlying u64 buffer representing the record's binary data
    buffer: &'a [u64],
    /// The configuration that defines the layout and size of record components
    config: RecordConfig,
}
impl<'a> RefRecord<'a> {
    /// Creates a new record reference
    ///
    /// # Arguments
    ///
    /// * `id` - The record's position in the file (0-based record index, not byte offset)
    /// * `buffer` - The u64 slice containing the record's binary data
    /// * `config` - Configuration defining the record's layout
    ///
    /// # Panics
    ///
    /// Panics if the buffer length doesn't match the expected size from the config
    #[must_use]
    pub fn new(id: u64, buffer: &'a [u64], config: RecordConfig) -> Self {
        assert_eq!(buffer.len(), config.record_size_u64());
        Self { id, buffer, config }
    }
    /// Returns the record's configuration
    ///
    /// The configuration defines the layout and size of the record's components.
    #[must_use]
    pub fn config(&self) -> RecordConfig {
        self.config
    }
}

impl BinseqRecord for RefRecord<'_> {
    fn bitsize(&self) -> BitSize {
        self.config.bitsize
    }
    fn index(&self) -> u64 {
        self.id
    }
    fn sheader(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        buffer.extend_from_slice(itoa::Buffer::new().format(self.id).as_bytes());
    }
    fn xheader(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        buffer.extend_from_slice(itoa::Buffer::new().format(self.id).as_bytes());
    }
    fn flag(&self) -> u64 {
        self.buffer[0]
    }
    fn slen(&self) -> u64 {
        self.config.slen
    }
    fn xlen(&self) -> u64 {
        self.config.xlen
    }
    fn sbuf(&self) -> &[u64] {
        &self.buffer[1..=(self.config.schunk as usize)]
    }
    fn xbuf(&self) -> &[u64] {
        &self.buffer[1 + self.config.schunk as usize..]
    }
}

/// Configuration for binary sequence record layout
///
/// This struct defines the size and layout of binary sequence records,
/// including both primary sequence data and optional extended data.
/// It handles the translation between sequence lengths in base pairs
/// and the number of u64 chunks needed to store the compressed data.
#[derive(Clone, Copy)]
pub struct RecordConfig {
    /// The primary sequence length in base pairs
    slen: u64,
    /// The extended sequence length in base pairs
    xlen: u64,
    /// The number of u64 chunks needed to store the primary sequence
    /// (each u64 stores 32 nucleotides)
    schunk: u64,
    /// The number of u64 chunks needed to store the extended sequence
    /// (each u64 stores 32 values)
    xchunk: u64,
    /// The bitsize of the record
    bitsize: BitSize,
}
impl RecordConfig {
    /// Creates a new record configuration
    ///
    /// This constructor initializes a configuration for a binary sequence record
    /// with specified primary and extended sequence lengths.
    ///
    /// # Arguments
    ///
    /// * `slen` - The length of primary sequences in the file
    /// * `xlen` - The length of secondary/extended sequences in the file
    /// * `bitsize` - The bitsize of the record
    ///
    /// # Returns
    ///
    /// A new `RecordConfig` instance with the specified sequence lengths
    pub fn new(slen: usize, xlen: usize, bitsize: BitSize) -> Self {
        let (schunk, xchunk) = match bitsize {
            BitSize::Two => (slen.div_ceil(32), xlen.div_ceil(32)),
            BitSize::Four => (slen.div_ceil(16), xlen.div_ceil(16)),
        };
        Self {
            slen: slen as u64,
            xlen: xlen as u64,
            schunk: schunk as u64,
            xchunk: xchunk as u64,
            bitsize,
        }
    }

    /// Creates a new record configuration from a header
    ///
    /// This constructor initializes a configuration based on a header that contains
    /// the sequence lengths for primary and extended sequences.
    ///
    /// # Arguments
    ///
    /// * `header` - A reference to a `BinseqHeader` containing sequence lengths
    ///
    /// # Returns
    ///
    /// A new `RecordConfig` instance with the sequence lengths from the header
    pub fn from_header(header: &BinseqHeader) -> Self {
        Self::new(header.slen as usize, header.xlen as usize, header.bits)
    }

    /// Returns whether this record contains extended sequence data
    ///
    /// A record is considered paired if it has a non-zero extended sequence length.
    pub fn paired(&self) -> bool {
        self.xlen > 0
    }

    /// Returns the primary sequence length in base pairs
    ///
    /// This method returns the length of the primary sequence in base pairs.
    pub fn slen(&self) -> usize {
        self.slen as usize
    }

    /// Returns the extended sequence length in base pairs
    ///
    /// This method returns the length of the extended sequence in base pairs.
    pub fn xlen(&self) -> usize {
        self.xlen as usize
    }

    /// Returns the number of u64 chunks needed to store the primary sequence
    ///
    /// This method returns the number of u64 chunks required to store the primary
    /// sequence, where each u64 stores 32 nucleotides.
    pub fn schunk(&self) -> usize {
        self.schunk as usize
    }

    /// Returns the number of u64 chunks needed to store the extended sequence
    ///
    /// This method returns the number of u64 chunks required to store the extended
    /// sequence, where each u64 stores 32 values.
    pub fn xchunk(&self) -> usize {
        self.xchunk as usize
    }

    /// Returns the full record size in bytes (u8):
    /// 8 * (schunk + xchunk + 1 (flag))
    pub fn record_size_bytes(&self) -> usize {
        8 * self.record_size_u64()
    }

    /// Returns the full record size in u64
    /// schunk + xchunk + 1 (flag)
    pub fn record_size_u64(&self) -> usize {
        (self.schunk + self.xchunk + 1) as usize
    }
}

/// A memory-mapped reader for binary sequence files
///
/// This reader provides efficient access to binary sequence files by memory-mapping
/// them instead of performing traditional I/O operations. It supports both
/// sequential access to individual records and parallel processing of records
/// across multiple threads.
///
/// The reader ensures thread-safety through the use of `Arc` for sharing the
/// memory-mapped data between threads.
///
/// Records are returned as [`RefRecord`] which implement the [`BinseqRecord`] trait.
///
/// # Examples
///
/// ```
/// use binseq::bq::MmapReader;
/// use binseq::Result;
///
/// fn main() -> Result<()> {
///     let path = "./data/subset.bq";
///     let reader = MmapReader::new(path)?;
///
///     // Calculate the number of records in the file
///     let num_records = reader.num_records();
///     println!("Number of records: {}", num_records);
///
///     // Get the record at index 20 (0-indexed)
///     let record = reader.get(20)?;
///
///     Ok(())
/// }
/// ```
pub struct MmapReader {
    /// Memory mapped file contents, wrapped in Arc for thread-safe sharing
    mmap: Arc<Mmap>,

    /// Binary sequence file header containing format information
    header: BinseqHeader,

    /// Configuration defining the layout of records in the file
    config: RecordConfig,
}

impl MmapReader {
    /// Creates a new memory-mapped reader for a binary sequence file
    ///
    /// This method opens the file, memory-maps its contents, and validates
    /// the file structure to ensure it contains valid binary sequence data.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the binary sequence file
    ///
    /// # Returns
    ///
    /// * `Ok(MmapReader)` - A new reader if the file is valid
    /// * `Err(Error)` - If the file is invalid or cannot be opened
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The file cannot be opened
    /// * The file is not a regular file
    /// * The file header is invalid
    /// * The file size doesn't match the expected size based on the header
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
            header,
            config,
        })
    }

    /// Returns the total number of records in the file
    ///
    /// This is calculated by subtracting the header size from the total file size
    /// and dividing by the size of each record.
    #[must_use]
    pub fn num_records(&self) -> usize {
        (self.mmap.len() - SIZE_HEADER) / self.config.record_size_bytes()
    }

    /// Returns a copy of the binary sequence file header
    ///
    /// The header contains format information and sequence length specifications.
    #[must_use]
    pub fn header(&self) -> BinseqHeader {
        self.header
    }

    /// Checks if the file has paired-records
    #[must_use]
    pub fn is_paired(&self) -> bool {
        self.header.is_paired()
    }

    /// Returns a reference to a specific record
    ///
    /// # Arguments
    ///
    /// * `idx` - The index of the record to retrieve (0-based)
    ///
    /// # Returns
    ///
    /// * `Ok(RefRecord)` - A reference to the requested record
    /// * `Err(Error)` - If the index is out of bounds
    ///
    /// # Errors
    ///
    /// Returns an error if the requested index is beyond the number of records in the file
    pub fn get(&self, idx: usize) -> Result<RefRecord<'_>> {
        if idx > self.num_records() {
            return Err(ReadError::OutOfRange(idx, self.num_records()).into());
        }
        let lbound = SIZE_HEADER + (idx * self.config.record_size_bytes());
        let rbound = lbound + self.config.record_size_bytes();
        let bytes = &self.mmap[lbound..rbound];
        let buffer = cast_slice(bytes);
        Ok(RefRecord::new(idx as u64, buffer, self.config))
    }
}

/// A reader for streaming binary sequence data from any source that implements Read
///
/// Unlike `MmapReader` which requires the entire file to be accessible at once,
/// `StreamReader` processes data as it becomes available, making it suitable for:
/// - Processing data as it arrives over a network
/// - Handling very large files that exceed available memory
/// - Pipeline processing where data is flowing continuously
///
/// The reader maintains an internal buffer and can handle partial record reconstruction
/// across chunk boundaries.
pub struct StreamReader<R: Read> {
    /// The source reader for binary sequence data
    reader: R,

    /// Binary sequence file header containing format information
    header: Option<BinseqHeader>,

    /// Configuration defining the layout of records in the file
    config: Option<RecordConfig>,

    /// Buffer for storing incoming data
    buffer: Vec<u8>,

    /// Current position in the buffer
    buffer_pos: usize,

    /// Length of valid data in the buffer
    buffer_len: usize,
}

impl<R: Read> StreamReader<R> {
    /// Creates a new `StreamReader` with the default buffer size
    ///
    /// This constructor initializes a `StreamReader` that will read from the provided
    /// source, using an 8K default buffer size.
    ///
    /// # Arguments
    ///
    /// * `reader` - The source to read binary sequence data from
    ///
    /// # Returns
    ///
    /// A new `StreamReader` instance
    pub fn new(reader: R) -> Self {
        Self::with_capacity(reader, 8192)
    }

    /// Creates a new `StreamReader` with a specified buffer capacity
    ///
    /// This constructor initializes a `StreamReader` with a custom buffer size,
    /// which can be tuned based on the expected usage pattern.
    ///
    /// # Arguments
    ///
    /// * `reader` - The source to read binary sequence data from
    /// * `capacity` - The size of the internal buffer in bytes
    ///
    /// # Returns
    ///
    /// A new `StreamReader` instance with the specified buffer capacity
    pub fn with_capacity(reader: R, capacity: usize) -> Self {
        Self {
            reader,
            header: None,
            config: None,
            buffer: vec![0; capacity],
            buffer_pos: 0,
            buffer_len: 0,
            // buffer_capacity: capacity,
        }
    }

    /// Reads and validates the header from the underlying reader
    ///
    /// This method reads the binary sequence file header and validates it.
    /// It caches the header internally for future use.
    ///
    /// # Returns
    ///
    /// * `Ok(&BinseqHeader)` - A reference to the validated header
    /// * `Err(Error)` - If reading or validating the header fails
    ///
    /// # Panics
    ///
    /// Panics if the header is missing when expected in the stream.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * There is an I/O error when reading from the source
    /// * The header data is invalid
    /// * End of stream is reached before the full header can be read
    pub fn read_header(&mut self) -> Result<&BinseqHeader> {
        if self.header.is_some() {
            return Ok(self
                .header
                .as_ref()
                .expect("Missing header when expected in stream"));
        }

        // Ensure we have enough data for the header
        while self.buffer_len - self.buffer_pos < SIZE_HEADER {
            self.fill_buffer()?;
        }

        // Parse header
        let header_slice = &self.buffer[self.buffer_pos..self.buffer_pos + SIZE_HEADER];
        let header = BinseqHeader::from_buffer(header_slice)?;

        self.header = Some(header);
        self.config = Some(RecordConfig::from_header(&header));
        self.buffer_pos += SIZE_HEADER;

        Ok(self.header.as_ref().unwrap())
    }

    /// Fills the internal buffer with more data from the reader
    ///
    /// This method reads more data from the underlying reader, handling
    /// the case where some unprocessed data remains in the buffer.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the buffer was successfully filled with new data
    /// * `Err(Error)` - If reading from the source fails
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * There is an I/O error when reading from the source
    /// * End of stream is reached (no more data available)
    fn fill_buffer(&mut self) -> Result<()> {
        // Move remaining data to beginning of buffer if needed
        if self.buffer_pos > 0 && self.buffer_pos < self.buffer_len {
            self.buffer.copy_within(self.buffer_pos..self.buffer_len, 0);
            self.buffer_len -= self.buffer_pos;
            self.buffer_pos = 0;
        } else if self.buffer_pos == self.buffer_len {
            self.buffer_len = 0;
            self.buffer_pos = 0;
        }

        // Read more data
        let bytes_read = self.reader.read(&mut self.buffer[self.buffer_len..])?;
        if bytes_read == 0 {
            return Err(ReadError::EndOfStream.into());
        }

        self.buffer_len += bytes_read;
        Ok(())
    }

    /// Retrieves the next record from the stream
    ///
    /// This method reads and processes the next complete record from the stream.
    /// It handles the case where a record spans multiple buffer fills.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(RefRecord))` - The next record was successfully read
    /// * `Ok(None)` - End of stream was reached (no more records)
    /// * `Err(Error)` - If an error occurred during reading
    ///
    /// # Panics
    ///
    /// Panics if the configuration is missing when expected in the stream.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * There is an I/O error when reading from the source
    /// * The header has not been read yet
    /// * The data format is invalid
    pub fn next_record(&mut self) -> Option<Result<RefRecord<'_>>> {
        // Ensure header is read
        if self.header.is_none() {
            if let Some(e) = self.read_header().err() {
                return Some(Err(e));
            }
        }

        let config = self
            .config
            .expect("Missing configuration when expected in stream");
        let record_size = config.record_size_bytes();

        // Ensure we have enough data for a complete record
        while self.buffer_len - self.buffer_pos < record_size {
            match self.fill_buffer() {
                Ok(()) => {}
                Err(Error::ReadError(ReadError::EndOfStream)) => {
                    // End of stream reached - if we have any partial data, it's an error
                    if self.buffer_len - self.buffer_pos > 0 {
                        return Some(Err(ReadError::PartialRecord(
                            self.buffer_len - self.buffer_pos,
                        )
                        .into()));
                    }
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // Process record
        let record_start = self.buffer_pos;
        self.buffer_pos += record_size;

        let record_bytes = &self.buffer[record_start..record_start + record_size];
        let record_u64s = cast_slice(record_bytes);

        // Create record with incremental ID (based on read position)
        let id = (record_start - SIZE_HEADER) / record_size;
        Some(Ok(RefRecord::new(id as u64, record_u64s, config)))
    }

    /// Consumes the stream reader and returns the inner reader
    ///
    /// This method is useful when you need access to the underlying reader
    /// after processing is complete.
    ///
    /// # Returns
    ///
    /// The inner reader that was used by this `StreamReader`
    pub fn into_inner(self) -> R {
        self.reader
    }
}

/// Default batch size for parallel processing
///
/// This constant defines how many records each thread processes at a time
/// during parallel processing operations.
pub const BATCH_SIZE: usize = 1024;

/// Parallel processing implementation for memory-mapped readers
impl ParallelReader for MmapReader {
    /// Processes all records in parallel using multiple threads
    ///
    /// This method distributes the records across the specified number of threads
    /// and processes them using the provided processor. Each thread receives its
    /// own clone of the processor and processes a contiguous chunk of records.
    ///
    /// # Arguments
    ///
    /// * `processor` - The processor to use for handling records
    /// * `num_threads` - The number of threads to use for processing
    ///
    /// # Type Parameters
    ///
    /// * `P` - A type that implements `ParallelProcessor` and can be cloned
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If all records were processed successfully
    /// * `Err(Error)` - If an error occurred during processing
    fn process_parallel<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()> {
        let num_records = self.num_records();
        self.process_parallel_range(processor, num_threads, 0..num_records)
    }

    /// Process records in parallel within a specified range
    ///
    /// This method allows parallel processing of a subset of records within the file,
    /// defined by a start and end index. The range is distributed across the specified
    /// number of threads.
    ///
    /// # Arguments
    ///
    /// * `processor` - The processor to use for each record
    /// * `num_threads` - The number of threads to spawn
    /// * `range` - The range of record indices to process
    ///
    /// # Type Parameters
    ///
    /// * `P` - A type that implements `ParallelProcessor` and can be cloned
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If all records were processed successfully
    /// * `Err(Error)` - If an error occurred during processing
    fn process_parallel_range<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
        range: Range<usize>,
    ) -> Result<()> {
        // Calculate the number of threads to use
        let num_threads = if num_threads == 0 {
            num_cpus::get()
        } else {
            num_threads.min(num_cpus::get())
        };

        // Validate range
        let num_records = self.num_records();
        if range.start >= num_records || range.end > num_records || range.start >= range.end {
            return Ok(()); // Nothing to process or invalid range
        }

        // Calculate number of records for each thread within the range
        let range_size = range.end - range.start;
        let records_per_thread = range_size.div_ceil(num_threads);

        // Arc self
        let reader = Arc::new(self);

        // Build thread handles
        let mut handles = Vec::new();
        for tid in 0..num_threads {
            let mut processor = processor.clone();
            let reader = reader.clone();
            processor.set_tid(tid);

            let handle = std::thread::spawn(move || -> Result<()> {
                let start_idx = range.start + tid * records_per_thread;
                let end_idx = (start_idx + records_per_thread).min(range.end);

                if start_idx >= end_idx {
                    return Ok(()); // No records for this thread
                }

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
