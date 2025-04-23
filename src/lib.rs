#![doc = include_str!("../README.md")]
//!
//! # BINSEQ
//!
//! The `binseq` library provides efficient tools for working with the [BINSEQ](https://www.biorxiv.org/content/10.1101/2025.04.08.647863v1) file format family.
//!
//! It offers tools to read and write BINSEQ files, providing:
//!
//! - Compact 2-bit encoding and decoding of nucleotide sequences through [`bitnuc`](https://docs.rs/bitnuc/latest/bitnuc/)
//! - Memory-mapped file access for efficient reading
//! - Parallel processing capabilities for arbitrary tasks through the [`ParallelProcessor`] trait.
//! - Configurable [`Policy`] for handling invalid nucleotides
//! - Support for both single and paired-end sequences
//! - Abstract [`BinseqRecord`] trait for representing records from both `.bq` and `.vbq` files.
//!
//! ## Crate Organization
//!
//! This library is split into 3 major parts.
//!
//! There are the [`bq`] and [`vbq`] modules, which provide tools for reading and writing `BQ` and `VBQ` files respectively.
//! Then there are traits and utilities that are ubiquitous across the library which are available at the top-level of the crate.
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

/// BQ - fixed length records, no quality scores
pub mod bq;

/// Error definitions
pub mod error;

/// Parallel processing
mod parallel;

/// Invalid nucleotide policy
mod policy;

/// Record trait shared between BINSEQ variants
mod record;

/// VBQ - Variable length records, optional quality scores, compressed blocks
pub mod vbq;

pub use error::{Error, Result};
pub use parallel::{BinseqReader, ParallelProcessor, ParallelReader};
pub use policy::{Policy, RNG_SEED};
pub use record::BinseqRecord;
