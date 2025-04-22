#![doc = include_str!("../README.md")]
//!
//! # Overview
//!
//! The `binseq` library provides efficient tools for working with binary-encoded
//! nucleotide sequences. It offers:
//!
//! - Compact 2-bit encoding of nucleotide sequences
//! - Memory-mapped file access for efficient reading
//! - Parallel processing capabilities
//! - Configurable policies for handling invalid nucleotides
//! - Support for both single and paired-end sequences
//!
//! # Core Components
//!
//! - [`BinseqWriter`]: Writes sequences to binary format
//! - [`MmapReader`]: Reads sequences using memory mapping
//! - [`BinseqHeader`]: Defines file format and sequence lengths
//! - [`Policy`]: Configures invalid nucleotide handling
//! - [`ParallelProcessor`]: Enables parallel sequence processing
//!
//! # Example
//!
//! ```
//! use binseq::bq::{BinseqHeader, BinseqWriterBuilder, MmapReader};
//! use binseq::{Policy, Result};
//! use std::io::Cursor;
//!
//! fn main() -> Result<()> {
//!     // Create a writer for sequences of length 100
//!     let header = BinseqHeader::new(100);
//!
//!     let mut writer = BinseqWriterBuilder::default()
//!         .header(header)
//!         .build(Cursor::new(Vec::new()))?;
//!
//!     // Write a sequence
//!     let sequence = b"ACGT".repeat(25); // 100 nucleotides
//!     writer.write_nucleotides(0, &sequence)?;
//!
//!     Ok(())
//! }
//! ```

#![allow(clippy::module_inception)]

/// BQ - fixed length, no quality scores
pub mod bq;

/// Error definitions
pub mod error;

/// Parallel processing
mod parallel;

/// Invalid nucleotide policy
mod policy;

/// Record trait shared between BINSEQ variants
mod record;

/// VBQ - Variable length, optional quality scores, compressed blocks
pub mod vbq;

pub use error::{Error, Result};
pub use parallel::ParallelProcessor;
pub use policy::{Policy, RNG_SEED};
pub use record::BinseqRecord;
