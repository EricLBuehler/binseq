mod config;
mod record;
mod record_pair;

pub type RefBytes<'a> = &'a [u64];

pub use config::RecordConfig;
pub use record::RefRecord;
pub use record_pair::RefRecordPair;
