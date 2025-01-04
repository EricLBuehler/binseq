mod paired;
mod read;
mod set;
mod single;
mod single_mmap;
mod utils;

pub use paired::PairedReader;
pub use read::{BinseqRead, PairedEndRead, PairedRead, SingleEndRead};
pub use set::RecordSet;
pub use single::SingleReader;
pub use single_mmap::MmapReader;
