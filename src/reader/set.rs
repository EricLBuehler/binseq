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
