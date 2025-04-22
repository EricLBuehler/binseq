mod header;
mod reader;
mod utils;
pub mod writer;

pub use header::{BinseqHeader, SIZE_HEADER};
pub use reader::{MmapReader, RefRecord};
pub use utils::expected_file_size;
pub use writer::{BinseqWriter, BinseqWriterBuilder, Encoder};
