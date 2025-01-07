use super::{BinseqRecord, Record, RefBytes};
use crate::RecordConfig;

#[derive(Debug)]
pub struct RefRecord<'a> {
    /// The 8-byte flag
    pub flag: u64,

    /// The 2-bit encoded sequence
    pub sequence: RefBytes<'a>,

    /// Sizing information for the record
    pub config: RecordConfig,
}
impl<'a> RefRecord<'a> {
    pub fn new(flag: u64, sequence: RefBytes<'a>, config: RecordConfig) -> Self {
        Self {
            flag,
            sequence,
            config,
        }
    }
    pub fn to_owned(&self) -> Record {
        Record::new(self.flag, self.sequence.to_vec(), self.config)
    }
    pub fn update_record(&self, record: &mut Record) {
        record.update(self.flag, self.sequence, self.config);
    }
}

impl BinseqRecord for RefRecord<'_> {
    fn flag(&self) -> u64 {
        self.flag
    }
    fn sequence(&self) -> RefBytes {
        self.sequence
    }
    fn config(&self) -> RecordConfig {
        self.config
    }
}

#[cfg(test)]
mod testing {
    use super::*;
    use anyhow::Result;

    fn embed_sequence(nucl: &[u8]) -> Vec<u64> {
        let mut ebuf = Vec::new();
        bitnuc::encode(nucl, &mut ebuf).unwrap();
        ebuf
    }

    #[test]
    fn test_subsequence_small() -> Result<()> {
        let seq = b"ACTGACTG";
        let ebuf = embed_sequence(seq);
        let config = RecordConfig::new(seq.len() as u32);

        let record = RefRecord::new(0, ebuf.as_slice(), config);

        // First 4 bases
        let subseq = record.decode_subsequence_alloc(0..4)?;
        assert_eq!(subseq, b"ACTG");

        // Last 4 bases
        let subseq = record.decode_subsequence_alloc(4..8)?;
        assert_eq!(subseq, b"ACTG");

        // All bases
        let subseq = record.decode_subsequence_alloc(0..8)?;
        assert_eq!(subseq, b"ACTGACTG");

        // Middle 4 bases
        let subseq = record.decode_subsequence_alloc(2..6)?;
        assert_eq!(subseq, b"TGAC");

        Ok(())
    }

    #[test]
    fn test_subsequence_large() -> Result<()> {
        let seq = b"ACTGACTGACTGACTGACTGACTGACTGACTGACTGACTGACTGACTGACTG";
        let ebuf = embed_sequence(seq);
        let config = RecordConfig::new(seq.len() as u32);

        let record = RefRecord::new(0, ebuf.as_slice(), config);

        // First 4 bases
        let subseq = record.decode_subsequence_alloc(0..4)?;
        assert_eq!(subseq, b"ACTG");

        // Last 4 bases
        let subseq = record.decode_subsequence_alloc(48..52)?;
        assert_eq!(subseq, b"ACTG");

        // All bases
        let subseq = record.decode_subsequence_alloc(0..52)?;
        assert_eq!(subseq, seq);

        // Middle 4 bases
        let subseq = record.decode_subsequence_alloc(20..24)?;
        assert_eq!(subseq, b"ACTG");

        // Bases spanning 32-bp chunks
        let subseq = record.decode_subsequence_alloc(30..34)?;
        assert_eq!(subseq, b"TGAC");

        Ok(())
    }
}
