use anyhow::Result;

use crate::{BinseqHeader, RefRecord, RefRecordPair};

/// Core trait for all BINSEQ readers
pub trait BinseqRead {
    /// Returns the next record in the sequence
    ///
    /// For paired readers, this returns only the primary (R1) sequence
    fn next(&mut self) -> Option<Result<RefRecord>>;

    /// Returns the header information for this BINSEQ file
    fn header(&self) -> BinseqHeader;

    /// Returns true if this reader processes paired-end data
    fn is_paired(&self) -> bool;

    /// Returns the record size in bytes (including both reads if paired)
    fn record_size(&self) -> usize;

    /// Returns the number of records processed so far
    fn n_processed(&self) -> usize;

    /// Returns true if the reader has processed all records
    fn is_finished(&self) -> bool;
}

/// Additional capabilities for paired-end readers
pub trait PairedRead: BinseqRead {
    /// Returns the next complete pair of records
    ///
    /// This advances the reader to the next record
    fn next_paired(&mut self) -> Option<Result<RefRecordPair>>;

    /// Returns the primary record (R1) from the next pair
    ///
    /// Note: This advances the reader to the next complete record
    fn next_primary(&mut self) -> Option<Result<RefRecord>>;

    /// Returns the extended record (R2) from the next pair
    ///
    /// Note: This advances the reader to the next complete record
    fn next_extended(&mut self) -> Option<Result<RefRecord>>;
}

/// Marker trait for single-end readers
pub trait SingleEndRead: BinseqRead {}

/// Marker trait for paired-end readers
pub trait PairedEndRead: BinseqRead + PairedRead {}
