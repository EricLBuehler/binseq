//! # VBINSEQ Format
//!
//! VBINSEQ is a high-performance binary format for variable-length nucleotide sequences
//! that optimizes both storage efficiency and parallel processing capabilities.
//!
//! For more information on the format, please refer to our [preprint](https://www.biorxiv.org/content/10.1101/2025.04.08.647863v1).
//!
//! ## Overview
//!
//! VBINSEQ extends the core principles of BINSEQ to accommodate:
//!
//! * **Variable-length sequences**: Unlike BINSEQ which requires fixed-length reads, VBINSEQ can store
//!   sequences of any length, making it suitable for technologies like PacBio and Oxford Nanopore.
//!
//! * **Quality scores**: Optional storage of quality scores alongside nucleotide data when needed.
//!
//! * **Block-based organization**: Data is organized into fixed-size independent record blocks
//!   for efficient parallel processing.
//!
//! * **Compression**: Optional ZSTD compression of individual blocks balances storage
//!   efficiency with processing speed.
//!
//! * **Paired-end support**: Native support for paired sequences without needing multiple files.
//!
//! ## File Structure
//!
//! A VBINSEQ file consists of a 32-byte header followed by a series of record blocks.
//! Each block has a 32-byte header and contains one or more variable-length records.
//!
//! ```text
//! ┌───────────────────┐
//! │    File Header    │ 32 bytes
//! ├───────────────────┤
//! │   Block Header    │ 32 bytes
//! ├───────────────────┤
//! │                   │
//! │   Block Records   │ Variable size
//! │                   │
//! ├───────────────────┤
//! │   Block Header    │ 32 bytes
//! ├───────────────────┤
//! │                   │
//! │   Block Records   │ Variable size
//! │                   │
//! └───────────────────┘
//! ```
//!
//! ## Record Format
//!
//! Each record contains:
//!
//! * Flag field (8 bytes)
//! * Primary sequence length (8 bytes)
//! * Extended sequence length (8 bytes)
//! * Primary sequence data (2-bit encoded)
//! * Primary quality scores (optional)
//! * Extended sequence data (optional, for paired-end)
//! * Extended quality scores (optional)
//!
//! ## Performance Characteristics
//!
//! VBINSEQ is designed for high-throughput parallel processing:
//!
//! * Independent blocks enable true parallel processing without synchronization
//! * Memory-mapped access provides efficient I/O
//! * 2-bit encoding reduces storage requirements
//! * Optional ZSTD compression reduces file size with minimal performance impact
//!
//! ## Usage Example
//!
//! ```
//! use std::fs::File;
//! use std::io::BufWriter;
//! use binseq::vbq::{VBinseqHeader, VBinseqWriterBuilder, MmapReader};
//! use binseq::BinseqRecord;
//!
//! /*
//!    WRITING
//! */
//!
//! // Create a header for sequences with quality scores
//! let with_qual = true;
//! let compressed = true;
//! let paired = false;
//! let header = VBinseqHeader::new(with_qual, compressed, paired);
//!
//! // Create a writer for sequences with quality scores
//! let file = File::create("example.vbq").unwrap();
//! let mut writer = VBinseqWriterBuilder::default()
//!     .header(header)
//!     .build(BufWriter::new(file))
//!     .unwrap();
//!
//! // Write a sequence with quality scores
//! let sequence = b"ACGTACGT";
//! let quality = b"IIIIFFFF";
//! writer.write_nucleotides_quality(0, sequence, quality).unwrap();
//! writer.finish().unwrap();
//!
//! /*
//!    READING
//! */
//!
//! // Read the sequences back
//! let mut reader = MmapReader::new("example.vbq").unwrap();
//! let mut block = reader.new_block();
//!
//! // Process blocks one at a time
//! let mut seq_buffer = Vec::new();
//! while reader.read_block_into(&mut block).unwrap() {
//!     for record in block.iter() {
//!         record.decode_s(&mut seq_buffer).unwrap();
//!         println!("Sequence: {}", std::str::from_utf8(&seq_buffer).unwrap());
//!         println!("Quality: {}", std::str::from_utf8(record.squal()).unwrap());
//!         seq_buffer.clear();
//!     }
//! }
//! # std::fs::remove_file("example.vbq").unwrap_or(());
//! ```

mod header;
mod index;
mod reader;
mod writer;

pub use header::{BlockHeader, VBinseqHeader};
pub use index::{BlockIndex, BlockRange};
pub use reader::{MmapReader, RecordBlock, RecordBlockIter, RefRecord};
pub use writer::{VBinseqWriter, VBinseqWriterBuilder};
