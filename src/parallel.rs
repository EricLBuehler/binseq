//! Parallel processing module for binary sequence data
//!
//! This module provides traits and utilities for processing binary sequence records
//! in parallel across multiple threads, allowing for efficient data processing
//! on multi-core systems.

use crate::{RefRecord, Result};

/// Trait for types that can process binary sequence records in parallel
///
/// This trait defines the interface for record processors that can be used
/// in multi-threaded contexts. Implementors must be both `Send` and `Clone`
/// to allow for distribution across threads. Each thread will receive its own
/// clone of the processor.
///
/// # Examples
///
/// ```
/// # use binseq::{ParallelProcessor, RefRecord, Result};
/// #[derive(Clone)]
/// struct MyProcessor {
///     thread_id: Option<usize>,
///     count: usize,
/// }
///
/// impl ParallelProcessor for MyProcessor {
///     fn process_record(&mut self, record: RefRecord) -> Result<()> {
///         self.count += 1;
///         Ok(())
///     }
///
///     fn set_tid(&mut self, tid: usize) {
///         self.thread_id = Some(tid);
///     }
///
///     fn get_tid(&self) -> Option<usize> {
///         self.thread_id
///     }
/// }
/// ```
pub trait ParallelProcessor: Send + Clone {
    /// Process a single binary sequence record
    ///
    /// This method is called for each record that needs to be processed.
    /// It may be called from multiple threads simultaneously on different
    /// clones of the processor.
    ///
    /// # Arguments
    ///
    /// * `record` - A reference to the record to be processed
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the record was processed successfully
    /// * `Err(Error)` - If an error occurred during processing
    fn process_record(&mut self, record: RefRecord) -> Result<()>;

    /// Called when a thread finishes processing its batch of records
    ///
    /// This method provides an opportunity to perform any cleanup or aggregation
    /// operations after a thread has finished processing its assigned batch of records.
    /// It is called exactly once per thread after all records assigned to that thread
    /// have been processed.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the batch completion handling was successful
    /// * `Err(Error)` - If an error occurred during batch completion handling
    ///
    /// # Default Implementation
    ///
    /// The default implementation does nothing and returns `Ok(())`.
    #[allow(unused_variables)]
    fn on_batch_complete(&mut self) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }

    /// Set the thread ID for this processor instance
    ///
    /// This method is called by the parallel processing framework to assign
    /// a unique thread ID to each processor clone. This allows the processor
    /// to track which thread it's running on, which can be useful for thread-specific
    /// operations or debugging.
    ///
    /// # Arguments
    ///
    /// * `tid` - A unique thread identifier
    ///
    /// # Default Implementation
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn set_tid(&mut self, tid: usize) {
        // Default implementation does nothing
    }

    /// Get the thread ID assigned to this processor instance
    ///
    /// This method returns the thread ID previously set via `set_tid()`, if any.
    /// It can be used to identify which thread a processor instance is running on.
    ///
    /// # Returns
    ///
    /// * `Some(usize)` - The thread ID if one has been set
    /// * `None` - If no thread ID has been set or the default implementation is used
    ///
    /// # Default Implementation
    ///
    /// The default implementation returns `None`.
    fn get_tid(&self) -> Option<usize> {
        None
    }
}
