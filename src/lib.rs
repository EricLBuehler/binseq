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

pub mod bq;
pub mod error;
mod parallel;
mod policy;
mod record;
pub mod vbq;

pub use error::{Error, Result};
pub use parallel::ParallelProcessor;
pub use policy::{Policy, RNG_SEED};
pub use record::BinseqRecord;
