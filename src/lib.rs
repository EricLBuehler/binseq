mod error;
mod header;
mod reader;
mod record;
mod utils;
mod writer;

pub use error::{HeaderError, ReadError, WriteError};
pub use header::BinseqHeader;
pub use reader::{
    BinseqRead, PairedEndRead, PairedRead, PairedReader, SingleEndRead, SingleReader,
};
pub use record::{RecordConfig, RefBytes, RefRecord, RefRecordPair};
pub use utils::expected_file_size;
pub use writer::BinseqWriter;

#[cfg(test)]
mod testing {

    use super::*;
    use anyhow::Result;
    use nucgen::Sequence;
    use std::io::Cursor;

    #[test]
    fn test_binseq_short() -> Result<()> {
        let header = BinseqHeader::new(16);
        let mut writer = BinseqWriter::new(Cursor::new(Vec::new()), header, false)?;

        let sequence = b"ACGTACGTACGTACGT";
        writer.write_nucleotides(0, sequence)?;

        let cursor = writer.into_inner().into_inner();
        let mut reader = SingleReader::new(cursor.as_slice())?;
        let record = reader.next().unwrap()?;
        assert_eq!(record.flag(), 0);
        let bitseq = record.sequence()[0];
        let readout = bitnuc::from_2bit_alloc(bitseq, 16)?;
        assert_eq!(&readout, sequence);

        Ok(())
    }

    #[test]
    fn test_binseq_short_multiple() -> Result<()> {
        let header = BinseqHeader::new(16);
        let mut writer = BinseqWriter::new(Cursor::new(Vec::new()), header, false)?;

        let sequence = b"ACGTACGTACGTACGT";
        writer.write_nucleotides(0, sequence)?;
        writer.write_nucleotides(0, sequence)?;
        writer.write_nucleotides(0, sequence)?; // write 3 times

        let cursor = writer.into_inner().into_inner();
        let mut reader = SingleReader::new(cursor.as_slice())?;

        for _ in 0..3 {
            let record = reader.next().unwrap()?;
            assert_eq!(record.flag(), 0);
            let bitseq = record.sequence()[0];
            let dbuf = bitnuc::from_2bit_alloc(bitseq, 16)?;
            assert_eq!(&dbuf, sequence);
        }

        Ok(())
    }

    #[test]
    fn test_binseq_long() -> Result<()> {
        let header = BinseqHeader::new(40);
        let mut writer = BinseqWriter::new(Cursor::new(Vec::new()), header, false)?;

        let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        writer.write_nucleotides(0, sequence)?;

        let cursor = writer.into_inner().into_inner();
        let mut reader = SingleReader::new(cursor.as_slice())?;
        let record = reader.next().unwrap()?;
        assert_eq!(record.flag(), 0);

        let dbuf = record.decode_alloc()?;
        assert_eq!(&dbuf, sequence);

        Ok(())
    }

    #[test]
    fn test_binseq_long_multiple() -> Result<()> {
        let header = BinseqHeader::new(40);
        let mut writer = BinseqWriter::new(Cursor::new(Vec::new()), header, false)?;

        let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        writer.write_nucleotides(0, sequence)?;
        writer.write_nucleotides(0, sequence)?;
        writer.write_nucleotides(0, sequence)?; // write 3 times

        let cursor = writer.into_inner().into_inner();
        let mut reader = SingleReader::new(cursor.as_slice())?;

        for _ in 0..3 {
            let record = reader.next().unwrap()?;
            assert_eq!(record.flag(), 0);

            let dbuf = record.decode_alloc()?;
            assert_eq!(&dbuf, sequence);
        }

        Ok(())
    }

    #[test]
    fn test_n_in_sequence() -> Result<()> {
        let header = BinseqHeader::new(40);
        let mut writer = BinseqWriter::new(Cursor::new(Vec::new()), header, false)?;

        let sequence = b"ACGTACGTACGTACNTACGTACGTACGTACGTACGTACGT";
        writer.write_nucleotides(0, sequence)?;

        let cursor = writer.into_inner().into_inner();
        let mut reader = SingleReader::new(cursor.as_slice())?;
        let record = reader.next();
        dbg!(&record);
        assert!(record.is_none());

        Ok(())
    }

    fn valid_reconstruction(seq_len: usize, num_records: usize) -> Result<()> {
        let mut rng = rand::thread_rng();
        let mut sequence = Sequence::new();

        // stores the original sequences
        let mut seq_vec = Vec::new();

        // write the sequences to a binseq file
        // and store the original sequences
        let header = BinseqHeader::new(seq_len as u32);
        let mut writer = BinseqWriter::new(Cursor::new(Vec::new()), header, false)?;
        for _ in 0..num_records {
            sequence.fill_buffer(&mut rng, seq_len);
            seq_vec.push(sequence.bytes().to_vec());
            writer.write_nucleotides(0, sequence.bytes())?;
        }

        // Verify that the file size is as expected
        let cursor = writer.into_inner().into_inner();
        let file_size = cursor.len();
        let expected_size = expected_file_size(num_records, seq_len);
        assert_eq!(file_size, expected_size);

        // read the sequences back from the binseq file
        // and compare them to the original sequences
        // stored in seq_vec
        let mut reader = SingleReader::new(cursor.as_slice())?;
        for seq in seq_vec.iter() {
            let record = reader.next().unwrap()?;
            assert_eq!(record.flag(), 0);
            let dbuf = record.decode_alloc()?;
            assert_eq!(&dbuf, seq);
        }
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn test_reconstruction() -> Result<()> {
        // test various sequence lengths
        for n_bases in [10, 32, 50, 64, 100, 1000] {
            for n_records in [1, 10, 32, 100, 256, 1000] {
                valid_reconstruction(n_bases, n_records)?;
            }
        }
        Ok(())
    }
}
