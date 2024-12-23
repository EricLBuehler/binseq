mod error;
mod header;
mod reader;
mod record;
mod writer;

pub use error::{HeaderError, ReadError, WriteError};
pub use header::BinseqHeader;
pub use reader::{
    BinseqRead, PairedEndRead, PairedRead, PairedReader, RecordConfig, SingleEndRead, SingleReader,
};
pub use record::{RefBytes, RefRecord, RefRecordPair};
pub use writer::BinseqWriter;

#[cfg(test)]
mod testing {

    use super::*;
    use anyhow::Result;
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
            let readout = bitnuc::from_2bit_alloc(bitseq, 16)?;
            assert_eq!(&readout, sequence);
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

        let mut readout = Vec::new();
        let n_chunks = 40usize.div_ceil(32);
        let remainder = 40 % 32;
        for i in 0..n_chunks - 1 {
            let component = record.sequence()[i];
            bitnuc::from_2bit(component, 32, &mut readout)?;
        }
        let component = record.sequence()[n_chunks - 1];
        bitnuc::from_2bit(component, remainder, &mut readout)?;

        assert_eq!(&readout, sequence);

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

            let mut readout = Vec::new();
            let n_chunks = 40usize.div_ceil(32);
            let remainder = 40 % 32;
            for i in 0..n_chunks - 1 {
                let component = record.sequence()[i];
                bitnuc::from_2bit(component, 32, &mut readout)?;
            }
            let component = record.sequence()[n_chunks - 1];
            bitnuc::from_2bit(component, remainder, &mut readout)?;

            assert_eq!(&readout, sequence);
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
}
