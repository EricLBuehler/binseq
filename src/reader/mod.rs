mod paired;
mod paired_mmap;
mod read;
mod set;
mod single;
mod single_mmap;
mod utils;

pub use paired::PairedReader;
pub use paired_mmap::PairedMmapReader;
pub use read::{BinseqRead, PairedEndRead, PairedRead, SingleEndRead};
pub use set::RecordSet;
pub use single::SingleReader;
pub use single_mmap::MmapReader;
