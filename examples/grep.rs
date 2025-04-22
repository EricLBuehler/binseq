use std::sync::Arc;

use anyhow::Result;
use binseq::{BinseqReader, ParallelProcessor, ParallelReader};
use memchr::memmem::Finder;
use parking_lot::Mutex;

#[derive(Clone)]
pub struct GrepCounter {
    // (thread) local variables
    sbuf: Vec<u8>,
    xbuf: Vec<u8>,
    local_count: usize,

    // search pattern (using memchr::memmem::Finder for fast searching)
    pattern: Finder<'static>,

    // global variables
    count: Arc<Mutex<usize>>,
}
impl GrepCounter {
    pub fn new(pattern: &[u8]) -> Self {
        Self {
            sbuf: Vec::new(),
            xbuf: Vec::new(),
            pattern: Finder::new(pattern).into_owned(),
            local_count: 0,
            count: Arc::new(Mutex::new(0)),
        }
    }

    fn match_sequence(&self, seq: &[u8]) -> bool {
        self.pattern.find(seq).is_some()
    }

    fn clear_buffers(&mut self) {
        self.sbuf.clear();
        self.xbuf.clear();
    }

    fn pprint(&self) {
        println!("Matching records: {}", self.count.lock());
    }
}
impl ParallelProcessor for GrepCounter {
    fn process_record<R: binseq::BinseqRecord>(&mut self, record: R) -> binseq::Result<()> {
        self.clear_buffers();

        record.decode_s(&mut self.sbuf)?;
        if record.is_paired() {
            record.decode_x(&mut self.xbuf)?;
        }

        if self.match_sequence(&self.sbuf) || self.match_sequence(&self.xbuf) {
            self.local_count += 1;
        }

        Ok(())
    }

    fn on_batch_complete(&mut self) -> binseq::Result<()> {
        *self.count.lock() += self.local_count;
        self.local_count = 0;
        Ok(())
    }
}

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or("./data/subset.bq".to_string());
    let pattern = std::env::args()
        .nth(2)
        .unwrap_or("ACGT".to_string())
        .as_bytes()
        .to_vec();
    let n_threads = std::env::args().nth(3).unwrap_or("1".to_string()).parse()?;

    let reader = BinseqReader::new(&path)?;
    let counter = GrepCounter::new(&pattern);
    reader.process_parallel(counter.clone(), n_threads)?;
    counter.pprint();

    Ok(())
}
