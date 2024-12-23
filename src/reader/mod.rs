mod paired;
mod read;
mod record_config;
mod single;

pub use paired::PairedReader;
pub use read::{BinseqRead, PairedEndRead, PairedRead, SingleEndRead};
pub use record_config::RecordConfig;
pub use single::SingleReader;
