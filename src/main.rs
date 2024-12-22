use anyhow::Result;
use seq_io::fastq::{Reader, Record};
use std::{
    fs::File,
    io::{BufReader, BufWriter},
};

use binseq::{BinseqHeader, BinseqReader, BinseqWriter};

fn read_write_single(fastq_path: &str, binseq_path: &str, seq_size: usize) -> Result<()> {
    // Open the input FASTQ file
    let (in_handle, _comp) = niffler::from_path(fastq_path)?;

    // Open the output file
    let header = BinseqHeader::new(seq_size as u32);
    let out_handle = File::create(binseq_path).map(BufWriter::new)?;
    let mut writer = BinseqWriter::new(out_handle, header, false)?;

    let mut all_sequences = Vec::new();

    // Write the binary sequence
    let mut reader = Reader::new(in_handle);
    let mut num_records_write = 0;
    let mut skipped_records = 0;
    while let Some(record) = reader.next() {
        let record = record?;
        let seq = record.seq();
        if writer.write_nucleotides(0, seq)? {
            num_records_write += 1;
            all_sequences.push(seq.to_vec());
        } else {
            skipped_records += 1;
        }
    }
    writer.flush()?;
    eprintln!("Finished writing {} records", num_records_write);
    eprintln!("Skipped {} records", skipped_records);

    // Read the binary sequence
    let bufreader = File::open(binseq_path).map(BufReader::new)?;
    let mut reader = BinseqReader::new(bufreader)?;
    let mut num_records_read = 0;
    let mut record_buffer = Vec::new();
    while let Some(record) = reader.next() {
        let record = record?;

        record.decode(&mut record_buffer)?;

        // Check if the decoded sequence matches the original
        assert_eq!(record_buffer, all_sequences[num_records_read]);

        num_records_read += 1;
        record_buffer.clear();
    }
    eprintln!("Finished reading {} records", num_records_read);
    eprintln!(
        "Difference in total records: {}",
        num_records_write - num_records_read
    );
    eprintln!("Number of records in vec: {}", all_sequences.len());

    Ok(())
}

fn main() -> Result<()> {
    // INPUT ARGUMENTS
    let fastq_path_r1 = "./data/subset_R1.fastq.gz";
    let fastq_path_r2 = "./data/subset_R2.fastq.gz";
    let binseq_path_r1 = "./data/subset_R1.bq";
    let binseq_path_r2 = "./data/subset_R2.bq";
    let seq_size_r1 = 28;
    let seq_size_r2 = 90;

    read_write_single(fastq_path_r1, binseq_path_r1, seq_size_r1)?;
    read_write_single(fastq_path_r2, binseq_path_r2, seq_size_r2)?;

    Ok(())
}
