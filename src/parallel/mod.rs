use anyhow::Result;

use crate::{RefRecord, RefRecordPair};

/// Trait for types that can process records in parallel
pub trait ParallelProcessor: Send + Clone {
    /// Process a single record
    fn process_record(&mut self, record: RefRecord, tid: usize) -> Result<()>;

    /// Called when a thread finishes processing its batch
    /// Default implementation does nothing
    #[allow(unused_variables)]
    fn on_batch_complete(&mut self, tid: usize) -> Result<()> {
        Ok(())
    }
}

pub trait ParallelPairedProcessor: Send + Clone {
    /// Process a single record pair
    fn process_record_pair(&mut self, record: RefRecordPair, tid: usize) -> Result<()>;

    /// Called when a thread finishes processing its batch
    /// Default implementation does nothing
    #[allow(unused_variables)]
    fn on_batch_complete(&mut self, tid: usize) -> Result<()> {
        Ok(())
    }
}
