use crate::{bq, error::ExtensionError, vbq, BinseqRecord, Result};

/// An enum abstraction for BINSEQ readers that can process records in parallel
///
/// This is a convenience enum that can be used for general workflows where the
/// distinction between BQ and VBQ readers is not important.
///
/// For more specialized workflows see [`bq::MmapReader`] and [`vbq::MmapReader`].
pub enum BinseqReader {
    Bq(bq::MmapReader),
    Vbq(vbq::MmapReader),
}
impl BinseqReader {
    pub fn new(path: &str) -> Result<Self> {
        if path.ends_with(".bq") {
            Ok(Self::Bq(bq::MmapReader::new(path)?))
        } else if path.ends_with(".vbq") {
            Ok(Self::Vbq(vbq::MmapReader::new(path)?))
        } else {
            return Err(ExtensionError::UnsupportedExtension(path.to_string()).into());
        }
    }

    #[must_use]
    pub fn is_paired(&self) -> bool {
        match self {
            Self::Bq(reader) => reader.is_paired(),
            Self::Vbq(reader) => reader.is_paired(),
        }
    }
}
impl ParallelReader for BinseqReader {
    fn process_parallel<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()> {
        match self {
            Self::Bq(reader) => reader.process_parallel(processor, num_threads),
            Self::Vbq(reader) => reader.process_parallel(processor, num_threads),
        }
    }
}

/// Trait for BINSEQ readers that can process records in parallel
///
/// This is implemented by the **reader** not by the **processor**.
/// For the **processor**, see the [`ParallelProcessor`] trait.
pub trait ParallelReader {
    fn process_parallel<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()>;
}

/// Trait for types that can process records in parallel.
///
/// This is implemented by the **processor** not by the **reader**.
/// For the **reader**, see the [`ParallelReader`] trait.
pub trait ParallelProcessor: Send + Clone {
    /// Process a single record
    fn process_record<R: BinseqRecord>(&mut self, record: R) -> Result<()>;

    /// Called when a thread finishes processing its batch
    /// Default implementation does nothing
    #[allow(unused_variables)]
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Set the thread ID for this processor
    ///
    /// Each thread should call this method with its own unique ID.
    #[allow(unused_variables)]
    fn set_tid(&mut self, _tid: usize) {
        // Default implementation does nothing
    }

    /// Get the thread ID for this processor
    fn get_tid(&self) -> Option<usize> {
        None
    }
}
