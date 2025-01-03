use anyhow::{bail, Result};
use binseq::*;
use nucgen::Sequence;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Clone, Default)]
pub struct MyProcessor {
    counter: Arc<AtomicUsize>,
    dbuf: Vec<u8>,
}
impl ParallelProcessor for MyProcessor {
    fn process_record(&mut self, record: binseq::RefRecord) -> Result<()> {
        self.dbuf.clear();
        record.decode(&mut self.dbuf)?;
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn native_parallel_processing(binseq_path: &str) -> Result<()> {
    let bufreader = File::open(binseq_path).map(BufReader::new)?;
    let reader = SingleReader::new(bufreader)?;
    let proc = MyProcessor::default();
    let n_threads = 2;

    reader.process_parallel(proc.clone(), n_threads)?;
    Ok(())
}

fn sequential_processing(binseq_path: &str) -> Result<()> {
    let bufreader = File::open(binseq_path).map(BufReader::new)?;
    let mut reader = SingleReader::new(bufreader)?;
    let mut proc = MyProcessor::default();
    while let Some(record) = reader.next() {
        let record = record?;
        proc.process_record(record)?;
    }
    Ok(())
}

pub fn main() -> Result<()> {
    let binseq_path = "./data/test.bq";
    let seq_size = 150;
    let num_seq = 5_000_000;

    time_it(
        || {
            write_single(binseq_path, num_seq, seq_size)?;
            Ok(())
        },
        "write_single",
    );

    time_it(
        || {
            sequential_processing(binseq_path)?;
            Ok(())
        },
        "sequential_processing",
    );

    time_it(
        || {
            native_parallel_processing(binseq_path)?;
            Ok(())
        },
        "parallel_processing",
    );

    Ok(())
}

fn time_it<F>(f: F, name: &str)
where
    F: Fn() -> Result<()>,
{
    let now = std::time::Instant::now();
    f().unwrap();
    let elapsed = now.elapsed();
    eprintln!("Elapsed time ({}): {:?}", name, elapsed);
}

fn write_single(binseq_path: &str, num_seq: usize, seq_size: usize) -> Result<()> {
    // Open the output file
    let header = BinseqHeader::new(seq_size as u32);
    let out_handle = File::create(binseq_path).map(BufWriter::new)?;
    let mut writer = BinseqWriter::new(out_handle, header, false)?;

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
