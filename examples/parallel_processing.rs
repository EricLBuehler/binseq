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
    local_counter: usize,
    counter: Arc<AtomicUsize>,
    sbuf: Vec<u8>,
    xbuf: Vec<u8>,
}
impl MyProcessor {
    pub fn counter(&self) -> usize {
        self.counter.load(Ordering::Relaxed)
    }
}
impl ParallelProcessor for MyProcessor {
    fn process_record(&mut self, record: RefRecord) -> Result<()> {
        self.sbuf.clear();
        record.decode(&mut self.sbuf)?;
        self.local_counter += 1;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.counter
            .fetch_add(self.local_counter, Ordering::Relaxed);
        self.local_counter = 0;
        Ok(())
    }
}
impl ParallelPairedProcessor for MyProcessor {
    fn process_record_pair(&mut self, pair: RefRecordPair) -> Result<()> {
        self.sbuf.clear();
        self.xbuf.clear();
        pair.decode_s(&mut self.sbuf)?;
        pair.decode_x(&mut self.xbuf)?;
        self.local_counter += 1;
        Ok(())
    }
    fn on_batch_complete(&mut self) -> Result<()> {
        self.counter
            .fetch_add(self.local_counter, Ordering::Relaxed);
        self.local_counter = 0;
        Ok(())
    }
}

fn native_parallel_processing(binseq_path: &str, n_threads: usize) -> Result<()> {
    let bufreader = File::open(binseq_path).map(BufReader::new)?;
    let reader = SingleReader::new(bufreader)?;
    let proc = MyProcessor::default();
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

fn mmap_processing(binseq_path: &str) -> Result<()> {
    let mut reader = MmapReader::new(binseq_path)?;
    let mut proc = MyProcessor::default();
    while let Some(record) = reader.next() {
        let record = record?;
        proc.process_record(record)?;
    }
    Ok(())
}

fn mmap_processing_parallel(binseq_path: &str, n_threads: usize) -> Result<()> {
    let reader = MmapReader::new(binseq_path)?;
    let proc = MyProcessor::default();
    reader.process_parallel(proc.clone(), n_threads)?;
    Ok(())
}

fn paired_sequential_processing(binseq_path: &str) -> Result<()> {
    let bufreader = File::open(binseq_path).map(BufReader::new)?;
    let mut reader = PairedReader::new(bufreader)?;
    let mut proc = MyProcessor::default();
    while let Some(pair) = reader.next_paired() {
        let pair = pair?;
        proc.process_record_pair(pair)?;
    }
    Ok(())
}

fn paired_native_parallel_processing(binseq_path: &str, n_threads: usize) -> Result<()> {
    let bufreader = File::open(binseq_path).map(BufReader::new)?;
    let reader = PairedReader::new(bufreader)?;
    let proc = MyProcessor::default();
    reader.process_parallel(proc.clone(), n_threads)?;
    Ok(())
}

fn paired_mmap_processing(binseq_path: &str) -> Result<()> {
    let mut reader = PairedMmapReader::new(binseq_path)?;
    let mut proc = MyProcessor::default();
    while let Some(pair) = reader.next_paired() {
        let pair = pair?;
        proc.process_record_pair(pair)?;
    }
    Ok(())
}

fn paired_mmap_processing_parallel(binseq_path: &str, n_threads: usize) -> Result<()> {
    let reader = PairedMmapReader::new(binseq_path)?;
    let proc = MyProcessor::default();
    reader.process_parallel(proc.clone(), n_threads)?;
    Ok(())
}

pub fn main() -> Result<()> {
    let binseq_path_single = "./data/test.bq";
    let binseq_path_paired = "./data/test_paired.bq";
    let r1_size = 150;
    let r2_size = 300;
    let num_seq = 1_000_000;

    time_it(
        || {
            write_single(binseq_path_single, num_seq, r1_size)?;
            Ok(())
        },
        "write_single",
    );

    time_it(
        || {
            sequential_processing(binseq_path_single)?;
            Ok(())
        },
        "single - sequential_processing",
    );

    for n_threads in 2..=16 {
        if n_threads % 2 != 0 {
            continue;
        }
        time_it(
            || {
                native_parallel_processing(binseq_path_single, n_threads)?;
                Ok(())
            },
            &format!("single - parallel_processing ({})", n_threads),
        );
    }

    time_it(
        || {
            mmap_processing(binseq_path_single)?;
            Ok(())
        },
        "single - mmap_processing",
    );

    for n_threads in 2..=16 {
        if n_threads % 2 != 0 {
            continue;
        }
        time_it(
            || {
                mmap_processing_parallel(binseq_path_single, n_threads)?;
                Ok(())
            },
            &format!("single - mmap_parallel_processing ({})", n_threads),
        );
    }

    time_it(
        || {
            write_paired(binseq_path_paired, num_seq, r1_size, r2_size)?;
            Ok(())
        },
        "write_paired",
    );

    time_it(
        || {
            paired_sequential_processing(binseq_path_paired)?;
            Ok(())
        },
        "paired - sequential_processing",
    );

    for n_threads in 2..=16 {
        if n_threads % 2 != 0 {
            continue;
        }
        time_it(
            || {
                paired_native_parallel_processing(binseq_path_paired, n_threads)?;
                Ok(())
            },
            &format!("paired - parallel_processing ({})", n_threads),
        );
    }

    time_it(
        || {
            paired_mmap_processing(binseq_path_paired)?;
            Ok(())
        },
        "paired - mmap_processing",
    );

    for n_threads in 2..=16 {
        if n_threads % 2 != 0 {
            continue;
        }
        time_it(
            || {
                paired_mmap_processing_parallel(binseq_path_paired, n_threads)?;
                Ok(())
            },
            &format!("paired - mmap_parallel_processing ({})", n_threads),
        );
    }

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
