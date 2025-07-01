use binseq::{BinseqReader, BinseqRecord, ParallelProcessor, ParallelReader, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct RangeProcessor {
    counter: Arc<AtomicUsize>,
    tid: Option<usize>,
    range_start: usize,
    range_end: usize,
    sbuf: Vec<u8>,
}

impl RangeProcessor {
    fn new(range_start: usize, range_end: usize) -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(0)),
            tid: None,
            range_start,
            range_end,
            sbuf: Vec::new(),
        }
    }

    fn count(&self) -> usize {
        self.counter.load(Ordering::Relaxed)
    }
}

impl ParallelProcessor for RangeProcessor {
    fn process_record<R: BinseqRecord>(&mut self, record: R) -> Result<()> {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);

        // Print progress every 10,000 records
        if count % 10_000 == 0 {
            if let Some(tid) = self.tid {
                // Decode the sequence to get its length
                self.sbuf.clear();
                record.decode_s(&mut self.sbuf)?;
                println!(
                    "Thread {}: Processed {} records (Range: {}-{}, Index: {}, Len: {})",
                    tid,
                    count + 1,
                    self.range_start,
                    self.range_end,
                    record.index(),
                    self.sbuf.len()
                );
            }
        }

        Ok(())
    }

    fn set_tid(&mut self, tid: usize) {
        self.tid = Some(tid);
    }

    fn get_tid(&self) -> Option<usize> {
        self.tid
    }

    fn on_batch_complete(&mut self) -> Result<()> {
        if let Some(tid) = self.tid {
            println!("Thread {} completed batch processing", tid);
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: {} <binseq_file> [num_threads] [start] [end]",
            args[0]
        );
        eprintln!("Example: {} data/subset.bq 4 1000 5000", args[0]);
        std::process::exit(1);
    }

    let file_path = &args[1];
    let num_threads = args
        .get(2)
        .unwrap_or(&"4".to_string())
        .parse::<usize>()
        .map_err(|e| binseq::Error::from(anyhow::Error::from(e)))?;

    // Create reader to get total record count
    let reader = BinseqReader::new(file_path)?;
    let total_records = reader.num_records()?;

    println!("File: {}", file_path);
    println!("Total records in file: {}", total_records);

    // Parse range arguments or use defaults
    let start = args
        .get(3)
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|e| binseq::Error::from(anyhow::Error::from(e)))?
        .unwrap_or(0);
    let end = args
        .get(4)
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|e| binseq::Error::from(anyhow::Error::from(e)))?
        .unwrap_or(total_records.min(10_000)); // Default to first 10k records

    // Validate range
    if start >= total_records {
        eprintln!(
            "Error: Start index {} is >= total records {}",
            start, total_records
        );
        std::process::exit(1);
    }
    if end > total_records {
        eprintln!(
            "Warning: End index {} is > total records {}, clamping to {}",
            end, total_records, total_records
        );
    }
    let end = end.min(total_records);

    if start >= end {
        eprintln!("Error: Start index {} must be < end index {}", start, end);
        std::process::exit(1);
    }

    println!(
        "Processing range: {} to {} ({} records)",
        start,
        end,
        end - start
    );
    println!("Using {} threads", num_threads);
    println!();

    // Demonstrate processing the full file
    println!("=== Processing full file ===");
    let reader_full = BinseqReader::new(file_path)?;
    let processor_full = RangeProcessor::new(0, total_records);
    let start_time = std::time::Instant::now();

    reader_full.process_parallel(processor_full.clone(), num_threads)?;

    let elapsed_full = start_time.elapsed();
    println!("Full file processing completed!");
    println!("Records processed: {}", processor_full.count());
    println!("Time taken: {:.2?}", elapsed_full);
    println!();

    // Demonstrate processing a specific range
    println!("=== Processing specific range ===");
    let reader_range = BinseqReader::new(file_path)?;
    let processor_range = RangeProcessor::new(start, end);
    let start_time = std::time::Instant::now();

    reader_range.process_parallel_range(processor_range.clone(), num_threads, start..end)?;

    let elapsed_range = start_time.elapsed();
    println!("Range processing completed!");
    println!("Records processed: {}", processor_range.count());
    println!("Expected records: {}", end - start);
    println!("Time taken: {:.2?}", elapsed_range);

    // Compare performance
    if processor_range.count() > 0 && processor_full.count() > 0 {
        let full_rate = processor_full.count() as f64 / elapsed_full.as_secs_f64();
        let range_rate = processor_range.count() as f64 / elapsed_range.as_secs_f64();
        println!();
        println!("=== Performance Comparison ===");
        println!("Full file rate: {:.0} records/sec", full_rate);
        println!("Range rate: {:.0} records/sec", range_rate);

        if range_rate > full_rate {
            println!(
                "Range processing was {:.1}x faster per record",
                range_rate / full_rate
            );
        } else {
            println!(
                "Full file processing was {:.1}x faster per record",
                full_rate / range_rate
            );
        }
    }

    Ok(())
}
