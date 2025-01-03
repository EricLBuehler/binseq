use anyhow::Result;

use crate::{RefRecord, RefRecordPair};

/// Trait for types that can process records in parallel
pub trait ParallelProcessor: Send + Clone {
    /// Process a single record
    fn process_record(&mut self, record: RefRecord) -> Result<()>;

    /// Called when a thread finishes processing its batch
    /// Default implementation does nothing
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }
}

pub trait ParallelPairedProcessor: Send + Clone {
    /// Process a single record pair
    fn process_record_pair(&mut self, record: RefRecordPair) -> Result<()>;

    /// Called when a thread finishes processing its batch
    /// Default implementation does nothing
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }
}
