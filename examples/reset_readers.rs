use std::fs::File;
use std::io::BufWriter;

use anyhow::{bail, Result};
use binseq::{BinseqHeader, BinseqRead, BinseqWriter, MmapReader, PairedMmapReader};
use nucgen::Sequence;

pub fn single_mmap_process(binseq_path: &str) -> Result<()> {
    let mut reader = MmapReader::new(binseq_path)?;
    let mut num_records = 0;
    while let Some(record) = reader.next() {
        let _record = record?;
        num_records += 1;
    }

    // reset reader back to head
    reader.reset();
    let mut rerun_records = 0;
    while let Some(record) = reader.next() {
        let _record = record?;
        rerun_records += 1;
    }

    assert_eq!(num_records, rerun_records);
    eprintln!("MmapReader: Found {num_records} records in set, {rerun_records} in reset set");
    Ok(())
}

pub fn paired_mmap_process(binseq_path: &str) -> Result<()> {
    let mut reader = PairedMmapReader::new(binseq_path)?;
    let mut num_records = 0;
    while let Some(record) = reader.next() {
        let _record = record?;
        num_records += 1;
    }

    // reset reader back to head
    reader.reset();
    let mut rerun_records = 0;
    while let Some(record) = reader.next() {
        let _record = record?;
        rerun_records += 1;
    }

    assert_eq!(num_records, rerun_records);
    eprintln!("PairedMmapReader: Found {num_records} records in set, {rerun_records} in reset set");
    Ok(())
}

pub fn main() -> Result<()> {
    let binseq_path_single = "./data/test.bq";
    let binseq_path_paired = "./data/test_paired.bq";
    let r1_size = 150;
    let r2_size = 300;
    let num_seq = 100_000;

    write_single(binseq_path_single, num_seq, r1_size)?;
    write_paired(binseq_path_paired, num_seq, r1_size, r2_size)?;

    single_mmap_process(binseq_path_single)?;
    paired_mmap_process(binseq_path_paired)?;

    Ok(())
}

fn write_single(binseq_path: &str, num_seq: usize, seq_size: usize) -> Result<()> {
    // Open the output file
    let header = BinseqHeader::new(seq_size as u32);
    let out_handle = File::create(binseq_path).map(BufWriter::new)?;
    let mut writer = BinseqWriter::new(out_handle, header)?;

    // Write the binary sequence
    let mut sequence = Sequence::new();
    let mut rng = rand::thread_rng();
    for _ in 0..num_seq {
        sequence.fill_buffer(&mut rng, seq_size);
        if !writer.write_nucleotides(0, sequence.bytes())? {
            bail!("Error writing nucleotides")
        }
    }
    writer.flush()?;
    eprintln!(
        "Finished writing {} records to path: {}",
        num_seq, binseq_path
    );
    Ok(())
}

fn write_paired(binseq_path: &str, num_seq: usize, r1_size: usize, r2_size: usize) -> Result<()> {
    // Open the output file
    let header = BinseqHeader::new_extended(r1_size as u32, r2_size as u32);
    let out_handle = File::create(binseq_path).map(BufWriter::new)?;
    let mut writer = BinseqWriter::new(out_handle, header)?;

    // Write the binary sequence
    let mut r1 = Sequence::new();
    let mut r2 = Sequence::new();
    let mut rng = rand::thread_rng();
    for _ in 0..num_seq {
        r1.fill_buffer(&mut rng, r1_size);
        r2.fill_buffer(&mut rng, r2_size);

        if !writer.write_paired(0, r1.bytes(), r2.bytes())? {
            bail!("Error writing nucleotides")
        }
    }
    writer.flush()?;
    eprintln!(
        "Finished writing {} records to path: {}",
        num_seq, binseq_path
    );
    Ok(())
}
