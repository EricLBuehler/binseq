use crate::{RecordConfig, RefRecord};

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
    config: RecordConfig,

    /// Maximum capacity of the record set
    capacity: usize,
}

impl RecordSet {
    pub fn new(capacity: usize, config: RecordConfig) -> Self {
        Self {
            flags: Vec::with_capacity(capacity),
            buffer: Vec::with_capacity(capacity * config.n_chunks),
            n_records: 0,
            current_pos: 0,
            config,
            capacity,
        }
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
        let start = idx * self.config.n_chunks;
        let end = start + self.config.n_chunks;
        let sequence = &self.buffer[start..end];

        Some(RefRecord::new(flag, sequence, self.config))
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

    pub fn config(&self) -> RecordConfig {
        self.config
    }

    pub fn increment_records(&mut self) {
        self.n_records += 1;
    }
}
