mod paired;
mod parallel;
mod read;
mod set;
mod single;
mod utils;

pub use paired::PairedReader;
pub use parallel::ParallelProcessor;
pub use read::{BinseqRead, PairedEndRead, PairedRead, SingleEndRead};
pub use set::RecordSet;
pub use single::SingleReader;
