mod paired;
mod read;
mod single;
mod utils;

pub use paired::PairedReader;
pub use read::{BinseqRead, PairedEndRead, PairedRead, SingleEndRead};
pub use single::SingleReader;
