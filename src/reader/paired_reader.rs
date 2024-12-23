use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Read;

use crate::{BinseqHeader, ReadError, RecordConfig, RefRecordPair};

#[derive(Debug)]
pub struct PairedBinseqReader<R: Read> {
    /// Inner reader
    inner: R,

    /// Header of the file
    header: BinseqHeader,

    /// Buffer for the flag
    flag: u64,

    /// Buffer for the primary sequence
    sbuf: Vec<u64>,

    /// Buffer for the extended sequence
    xbuf: Vec<u64>,

    /// Configuration for the primary sequence
    sconfig: RecordConfig,

    /// Configuration for the extended sequence
    xconfig: RecordConfig,

    /// Number of record pairs processed
    n_processed: usize,
}
impl<R: Read> PairedBinseqReader<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let header = BinseqHeader::from_reader(&mut inner)?;
        if header.xlen == 0 {
            bail!(ReadError::MissingPairedSequence(header.slen))
        }
        Ok(Self {
            inner,
            header,
            flag: 0,
            sbuf: Vec::new(),
            xbuf: Vec::new(),
            sconfig: RecordConfig::new(header.slen),
            xconfig: RecordConfig::new(header.xlen),
            n_processed: 0,
        })
    }

    fn next_flag(&mut self) -> Result<bool> {
        match self.inner.read_u64::<LittleEndian>() {
            Ok(flag) => {
                self.flag = flag;
                Ok(true)
            }
            Err(e) => {
                // check if there are any bytes left in the reader
                let mut buf = [0u8; 1];
                match self.inner.read(&mut buf) {
                    Ok(0) => Ok(false),
                    _ => {
                        bail!(ReadError::UnexpectedEndOfStreamFlag(e, self.n_processed));
                    }
                }
            }
        }
    }

    fn next_primary<'a>(&'a mut self) -> Result<()> {
        (0..self.sconfig.n_chunks).try_for_each(|_| match self.inner.read_u64::<LittleEndian>() {
            Ok(bits) => {
                self.sbuf.push(bits);
                Ok(())
            }
            Err(e) => bail!(ReadError::UnexpectedEndOfStreamSequence(
                e,
                self.n_processed
            )),
        })
    }

    fn next_extended<'a>(&'a mut self) -> Result<()> {
        (0..self.xconfig.n_chunks).try_for_each(|_| match self.inner.read_u64::<LittleEndian>() {
            Ok(bits) => {
                self.xbuf.push(bits);
                Ok(())
            }
            Err(e) => bail!(ReadError::UnexpectedEndOfStreamSequence(
                e,
                self.n_processed
            )),
        })
    }

    pub fn next<'a>(&'a mut self) -> Option<Result<RefRecordPair<'a>>> {
        // Clear the last sequence buffer
        self.sbuf.clear();
        self.xbuf.clear();

        // Read the flag
        match self.next_flag() {
            Ok(true) => {}                 // continue with the next step
            Ok(false) => return None,      // end of file
            Err(e) => return Some(Err(e)), // unexpected error
        }

        // Read the sequence
        match self.next_primary() {
            Ok(_) => {}
            Err(e) => return Some(Err(e)),
        }

        // Read the extended sequence
        match self.next_extended() {
            Ok(_) => {}
            Err(e) => return Some(Err(e)),
        }

        // Create the record
        let ref_record = RefRecordPair::new(
            self.flag,
            &self.sbuf,
            &self.xbuf,
            self.sconfig,
            self.xconfig,
        );

        // Increment the number of processed records
        self.n_processed += 1;

        // Return the record as a reference
        Some(Ok(ref_record))
    }

    pub fn header(&self) -> BinseqHeader {
        self.header
    }

    pub fn n_processed(&self) -> usize {
        self.n_processed
    }
}
