use std::fs::File;
use std::io::{stdout, BufWriter, Write};
use std::sync::Arc;

use anyhow::Result;
use binseq::{MmapReader, ParallelProcessor, RefRecord};
use parking_lot::Mutex;

/// A struct for decoding BINSEQ data back to FASTQ format.
#[derive(Clone)]
pub struct Decoder {
    /// Local values
    buffer: Vec<u8>,
    /// Local buffer for decoding index
    ibuf: itoa::Buffer,
    /// Local buffer for decoding primary
    sbuf: Vec<u8>,
    /// Local buffer for decoding secondary
    xbuf: Vec<u8>,
    /// Local count of records
    local_count: usize,
    /// Quality buffer
    quality: Vec<u8>,

    ///  values
    global_buffer: Arc<Mutex<Box<dyn Write + Send>>>,
    num_records: Arc<Mutex<usize>>,
}

impl Decoder {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        let global_buffer = Arc::new(Mutex::new(writer));
        Decoder {
            buffer: Vec::new(),
            ibuf: itoa::Buffer::new(),
            sbuf: Vec::new(),
            xbuf: Vec::new(),
            local_count: 0,
            quality: Vec::new(),
            global_buffer,
            num_records: Arc::new(Mutex::new(0)),
        }
    }

    pub fn num_records(&self) -> usize {
        *self.num_records.lock()
    }
}
impl ParallelProcessor for Decoder {
    fn process_record(&mut self, record: RefRecord) -> Result<(), binseq::Error> {
        // clear decoding buffers
        self.sbuf.clear();
        self.xbuf.clear();

        // decode index
        let index = self.ibuf.format(record.id()).as_bytes();

        // write primary fastq to local buffer
        record.decode_s(&mut self.sbuf)?;
        if self.quality.len() < self.sbuf.len() {
            self.quality.resize(self.sbuf.len(), b'?');
        }
        write_fastq_parts(
            &mut self.buffer,
            index,
            &self.sbuf,
            &self.quality[..self.sbuf.len()],
        )?;

        // write extended fastq to local buffer
        if record.paired() {
            record.decode_x(&mut self.xbuf)?;
            if self.quality.len() < self.xbuf.len() {
                self.quality.resize(self.xbuf.len(), b'?');
            }
            write_fastq_parts(
                &mut self.buffer,
                index,
                &self.xbuf,
                &self.quality[..self.xbuf.len()],
            )?;
        }

        self.local_count += 1;
        Ok(())
    }

    fn on_batch_complete(&mut self) -> Result<(), binseq::Error> {
        // Lock the mutex to write to the global buffer
        {
            let mut lock = self.global_buffer.lock();
            lock.write_all(&self.buffer)?;
            lock.flush()?;
        }
        // Lock the mutex to update the number of records
        {
            let mut num_records = self.num_records.lock();
            *num_records += self.local_count;
        }

        // Clear the local buffer and reset the local record count
        self.buffer.clear();
        self.local_count = 0;
        Ok(())
    }
}

pub fn write_fastq_parts<W: Write>(
    writer: &mut W,
    index: &[u8],
    sequence: &[u8],
    quality: &[u8],
) -> Result<(), std::io::Error> {
    writer.write_all(b"@seq.")?;
    writer.write_all(index)?;
    writer.write_all(b"\n")?;
    writer.write_all(sequence)?;
    writer.write_all(b"\n+\n")?;
    writer.write_all(quality)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn match_output(path: Option<&str>) -> Result<Box<dyn Write + Send>> {
    match path {
        Some(path) => {
            let writer = File::create(path).map(BufWriter::new)?;
            Ok(Box::new(writer))
        }
        None => {
            let stdout = stdout();
            Ok(Box::new(BufWriter::new(stdout)))
        }
    }
}

fn main() -> Result<()> {
    let file = std::env::args()
        .nth(1)
        .unwrap_or("./data/subset.bq".to_string());
    let n_threads = std::env::args().nth(2).unwrap_or("1".to_string()).parse()?;

    let reader = MmapReader::new(&file)?;
    let writer = match_output(None)?;
    let proc = Decoder::new(writer);

    reader.process_parallel(proc.clone(), n_threads)?;
    eprintln!("Read {} records", proc.num_records());

    Ok(())
}
