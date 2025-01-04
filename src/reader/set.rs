use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use memmap2::Mmap;

use crate::{RecordConfig, RefRecord, RefRecordPair};

pub const DEFAULT_CAPACITY: usize = 128 * 1024;

#[derive(Debug, Clone)]
pub struct RecordSet {
    /// The raw buffer containing all flags
    flags: Vec<u64>,

    /// The raw buffer containing all packed sequences
    buffer: Vec<u64>,

    /// Number of records currently in the buffer
    n_records: usize,

    /// Current position in the buffer
    current_pos: usize,

    /// Configuration for record sizing
    sconfig: RecordConfig,

    /// Configuration for record sizing
    xconfig: RecordConfig,

    /// Maximum capacity of the record set
    capacity: usize,
}

impl RecordSet {
    pub fn with_capacity(capacity: usize, config: RecordConfig) -> Self {
        Self {
            flags: Vec::with_capacity(capacity),
            buffer: Vec::with_capacity(capacity * config.n_chunks),
            n_records: 0,
            current_pos: 0,
            sconfig: config,
            xconfig: config, // duplicated but unused
            capacity,
        }
    }

    pub fn with_capacity_paired(
        capacity: usize,
        sconfig: RecordConfig,
        xconfig: RecordConfig,
    ) -> Self {
        let buffer_size = capacity * (sconfig.n_chunks + xconfig.n_chunks);
        Self {
            flags: Vec::with_capacity(capacity),
            buffer: Vec::with_capacity(buffer_size),
            n_records: 0,
            current_pos: 0,
            sconfig,
            xconfig,
            capacity,
        }
    }

    pub fn new(config: RecordConfig) -> Self {
        Self::with_capacity(DEFAULT_CAPACITY, config)
    }

    pub fn new_paired(sconfig: RecordConfig, xconfig: RecordConfig) -> Self {
        Self::with_capacity_paired(DEFAULT_CAPACITY, sconfig, xconfig)
    }

    pub fn n_records(&self) -> usize {
        self.n_records
    }

    pub fn is_full(&self) -> bool {
        self.n_records >= self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.n_records == 0
    }

    // Get a reference to record at specific index
    pub fn get_record(&self, idx: usize) -> Option<RefRecord> {
        if idx >= self.n_records {
            return None;
        }

        let flag = self.flags[idx];
        let start = idx * self.sconfig.n_chunks;
        let end = start + self.sconfig.n_chunks;
        let sequence = &self.buffer[start..end];

        Some(RefRecord::new(flag, sequence, self.sconfig))
    }

    // Get a reference to a record pair at specific index
    pub fn get_record_pair(&self, idx: usize) -> Option<RefRecordPair> {
        if idx >= self.n_records {
            return None;
        }

        let flag = self.flags[idx];
        let pair_size = self.sconfig.n_chunks + self.xconfig.n_chunks;

        let s_start = idx * pair_size;
        let s_end = s_start + self.sconfig.n_chunks;
        let x_start = s_end;
        let x_end = x_start + self.xconfig.n_chunks;

        let s_sequence = &self.buffer[s_start..s_end];
        let x_sequence = &self.buffer[x_start..x_end];

        Some(RefRecordPair::new(
            flag,
            s_sequence,
            x_sequence,
            self.sconfig,
            self.xconfig,
        ))
    }

    pub fn get_flags_mut(&mut self) -> &mut Vec<u64> {
        &mut self.flags
    }

    pub fn get_buffer_mut(&mut self) -> &mut Vec<u64> {
        &mut self.buffer
    }

    // Clear and prepare for refill
    pub fn clear(&mut self) {
        self.flags.clear();
        self.buffer.clear();
        self.n_records = 0;
        self.current_pos = 0;
    }

    pub fn sconfig(&self) -> RecordConfig {
        self.sconfig
    }

    pub fn xconfig(&self) -> RecordConfig {
        self.xconfig
    }

    pub fn increment_records(&mut self) {
        self.n_records += 1;
    }
}

/// Memory-mapped record set
impl RecordSet {
    /// Fills the record set from a memory mapped file, handling single-end data
    /// Returns true if EOF was reached, false if the record set was filled
    ///
    /// This method is specific to single-end data, reading data directly from the memory mapped file
    /// into the internal buffers.
    pub fn fill_from_mmap_single(
        &mut self,
        mmap: &Mmap,
        offset: &mut usize,
        end_offset: usize,
    ) -> Result<bool> {
        self.clear();
        let record_size = 8 + (self.sconfig.n_chunks * 8); // flag + sequence
        let config = self.sconfig;

        while !self.is_full() {
            // Check if we've reached our assigned chunk end
            if *offset + record_size > end_offset {
                return Ok(true);
            }

            // Read flag
            let flag_bytes = &mmap[*offset..*offset + 8];
            let flag = LittleEndian::read_u64(flag_bytes);
            *offset += 8;

            // Read sequence chunks
            let buffer = self.get_buffer_mut();

            // Safety: We've verified the range is within bounds
            unsafe {
                let src_ptr = mmap[*offset..*offset + (config.n_chunks * 8)].as_ptr() as *const u64;
                let chunk_slice = std::slice::from_raw_parts(src_ptr, config.n_chunks);
                buffer.extend_from_slice(chunk_slice);
            }
            *offset += self.sconfig.n_chunks * 8;

            self.get_flags_mut().push(flag);
            self.increment_records();
        }

        Ok(false)
    }

    /// Fills the record set from a memory mapped file, handling paired-end data
    /// Returns true if EOF was reached, false if the record set was filled
    ///
    /// This method is specific to paired-end data, handling both primary and extended sequences.
    /// It reads data directly from the memory mapped file into the internal buffers.
    pub fn fill_from_mmap_paired(
        &mut self,
        mmap: &Mmap,
        offset: &mut usize,
        end_offset: usize,
    ) -> Result<bool> {
        // Clear existing data
        self.clear();

        // Calculate total record size including flag and both sequences
        let pair_size = self.sconfig.n_chunks + self.xconfig.n_chunks;
        let record_size = 8 + (pair_size * 8); // flag (8 bytes) + sequence chunks

        let sconfig = self.sconfig;
        let xconfig = self.xconfig;

        while !self.is_full() {
            // Check if we've reached the end of our assigned chunk
            if *offset + record_size > end_offset {
                return Ok(true);
            }

            // Read the flag
            let flag_bytes = &mmap[*offset..*offset + 8];
            let flag = LittleEndian::read_u64(flag_bytes);
            *offset += 8;

            let buffer = self.get_buffer_mut();

            // Read primary sequence (R1)
            // Safety: We've verified the range is within bounds above
            unsafe {
                let src_ptr =
                    mmap[*offset..*offset + (sconfig.n_chunks * 8)].as_ptr() as *const u64;
                let chunk_slice = std::slice::from_raw_parts(src_ptr, sconfig.n_chunks);
                buffer.extend_from_slice(chunk_slice);
            }
            *offset += sconfig.n_chunks * 8;

            // Read extended sequence (R2)
            // Safety: We've verified the range is within bounds above
            unsafe {
                let src_ptr =
                    mmap[*offset..*offset + (xconfig.n_chunks * 8)].as_ptr() as *const u64;
                let chunk_slice = std::slice::from_raw_parts(src_ptr, xconfig.n_chunks);
                buffer.extend_from_slice(chunk_slice);
            }
            *offset += self.xconfig.n_chunks * 8;

            // Add the flag and increment our record count
            self.get_flags_mut().push(flag);
            self.increment_records();
        }

        Ok(false)
    }
}
