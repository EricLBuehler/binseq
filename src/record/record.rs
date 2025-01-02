use super::{BinseqRecord, RefBytes};
use crate::RecordConfig;

#[derive(Debug, Clone)]
pub struct Record {
    /// The 8-byte flag
    pub flag: u64,

    /// The 2-bit encoded sequence
    pub sequence: Vec<u64>,

    /// Sizing information for the record
    pub config: RecordConfig,
}

impl Record {
    pub fn new<'a>(flag: u64, sequence: Vec<u64>, config: RecordConfig) -> Self {
        Self {
            flag,
            sequence,
            config,
        }
    }

    pub fn empty() -> Self {
        Self {
            flag: 0,
            sequence: Vec::new(),
            config: RecordConfig::new(0),
        }
    }

    pub fn update(&mut self, flag: u64, sequence: RefBytes, config: RecordConfig) {
        self.flag = flag;
        self.sequence.clear();
        self.sequence.extend_from_slice(sequence);
        self.config = config;
    }
}

impl BinseqRecord for Record {
    fn flag(&self) -> u64 {
        self.flag
    }

    fn sequence(&self) -> RefBytes {
        &self.sequence
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

        let record = Record::new(0, ebuf, config);

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

        let record = Record::new(0, ebuf, config);

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
