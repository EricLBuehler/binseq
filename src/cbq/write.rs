use std::io;

use zstd::zstd_safe;

use crate::{
    Result, SequencingRecord,
    cbq::core::{BlockHeader, ColumnarBlock, FileHeader, Index, IndexFooter, IndexHeader},
};

/// Writer for CBQ files operating on generic writers (streaming).
pub struct ColumnarBlockWriter<W: io::Write> {
    /// Internal writer for the block
    inner: W,

    /// A reusable block for this writer
    block: ColumnarBlock,

    /// All block headers written by this writer
    headers: Vec<BlockHeader>,

    /// Compression context for the thread
    cctx: zstd_safe::CCtx<'static>,
}
impl<W: io::Write + Clone> Clone for ColumnarBlockWriter<W> {
    fn clone(&self) -> Self {
        let mut writer = Self {
            inner: self.inner.clone(),
            block: self.block.clone(),
            headers: self.headers.clone(),
            cctx: zstd_safe::CCtx::create(),
        };
        writer
            .init_compressor()
            .expect("Failed to set compression level in writer clone");
        writer
    }
}
impl<W: io::Write> ColumnarBlockWriter<W> {
    /// Creates a new writer with the header written to the inner writer
    pub fn new(inner: W, header: FileHeader) -> Result<Self> {
        // Build the writer
        let mut writer = Self::new_headless(inner, header)?;

        // Ensure the header is written to the file
        writer.inner.write_all(header.as_bytes())?;

        Ok(writer)
    }

    /// Creates a new writer without writing the header to the inner writer
    pub fn new_headless(inner: W, header: FileHeader) -> Result<Self> {
        let mut writer = Self {
            inner,
            block: ColumnarBlock::new(header),
            headers: Vec::default(),
            cctx: zstd_safe::CCtx::create(),
        };

        // Set the compression level for this writer
        writer.init_compressor()?;

        Ok(writer)
    }

    /// Sets the compression level for Writer
    ///
    /// Note: only used on init, shouldn't be set by the user
    fn init_compressor(&mut self) -> Result<()> {
        // Initialize the compressor with the compression level
        self.cctx
            .set_parameter(zstd_safe::CParameter::CompressionLevel(
                self.block.header.compression_level as i32,
            ))
            .map_err(|e| io::Error::other(zstd_safe::get_error_name(e)))?;

        // Set long distance matching
        self.cctx
            .set_parameter(zstd_safe::CParameter::EnableLongDistanceMatching(true))
            .map_err(|e| io::Error::other(zstd_safe::get_error_name(e)))?;
        Ok(())
    }

    pub fn header(&self) -> FileHeader {
        self.block.header
    }

    /// Calculate the usage of the block as a percentage
    pub fn usage(&self) -> f64 {
        self.block.usage()
    }

    /// Push a record to the writer
    ///
    /// Returns `Ok(true)` if the record was written successfully.
    /// CBQ handles N's explicitly in its encoding, so records are never skipped.
    pub fn push(&mut self, record: SequencingRecord) -> Result<bool> {
        if !self.block.can_fit(&record) {
            self.flush()?;
        }
        self.block.push(record)?;
        Ok(true)
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(header) = self.block.flush_to(&mut self.inner, &mut self.cctx)? {
            self.headers.push(header);
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.flush()?;
        self.write_index()?;
        Ok(())
    }

    fn write_index(&mut self) -> Result<()> {
        let index = Index::from_block_headers(&self.headers);
        let z_index = index.encoded()?;
        let header = IndexHeader::new(index.size(), z_index.len() as u64);
        let footer = IndexFooter::new(z_index.len() as u64);

        // Write the index to the inner writer
        {
            self.inner.write_all(header.as_bytes())?;
            self.inner.write_all(&z_index)?;
            self.inner.write_all(footer.as_bytes())?;
        }
        Ok(())
    }

    /// Ingest only the *completed* (already-compressed) blocks from `other`.
    ///
    /// Unlike [`ingest`](Self::ingest), this never touches either writer's
    /// incomplete block, so it performs no zstd compression. The work done
    /// under a global lock is reduced to a `write_all` of pre-compressed bytes
    /// plus a header copy — compression has already been paid for on the worker
    /// thread when `other`'s blocks were flushed in `push`.
    ///
    /// `other` keeps building its incomplete block across calls; only its
    /// completed-block buffer and headers are drained.
    pub fn ingest_completed(&mut self, other: &mut ColumnarBlockWriter<Vec<u8>>) -> Result<()> {
        // Write all completed blocks from the other
        self.inner.write_all(other.inner_data())?;

        // Take all headers from the other
        self.headers.extend_from_slice(&other.headers);

        // Clear only the drained completed-block state, leaving the incomplete
        // block intact so the worker thread can keep accumulating into it.
        other.clear_completed_data();

        Ok(())
    }

    /// Ingests only the *incomplete* (non-compressed) blocks from the `other`.
    ///
    /// This should not be used in isolation and should be handled from the [`ingest`](Self::ingest) API only
    /// to avoid any mistakes.
    ///
    /// [`ingest_completed`](Self::ingest_completed) should always be called first.
    fn ingest_incompleted(&mut self, other: &mut ColumnarBlockWriter<Vec<u8>>) -> Result<()> {
        // Attempt to ingest the incomplete block from the other
        if !self.block.can_ingest(&other.block) {
            // Make space by flushing the current block
            // Then ingest the incomplete block from the other
            self.flush()?;
        }
        self.block.take_incomplete(&other.block)?;

        // clear the drained incomplete-block state
        other.clear_incomplete_data();

        Ok(())
    }

    pub fn ingest(&mut self, other: &mut ColumnarBlockWriter<Vec<u8>>) -> Result<()> {
        self.ingest_completed(other)?;
        self.ingest_incompleted(other)?;
        Ok(())
    }
}

/// Specialized implementation when using a local `Vec<u8>` as the inner data structure
impl ColumnarBlockWriter<Vec<u8>> {
    #[must_use]
    pub fn inner_data(&self) -> &[u8] {
        &self.inner
    }

    /// Clears only the completed-block state (compressed bytes + headers),
    /// leaving the incomplete block intact.
    ///
    /// Used by [`ingest_completed`](ColumnarBlockWriter::ingest_completed) so a
    /// worker thread can keep accumulating records into its in-progress block
    /// across batches.
    pub fn clear_completed_data(&mut self) {
        self.inner.clear();
        self.headers.clear();
    }

    /// Clears the incomplete-block state
    pub fn clear_incomplete_data(&mut self) {
        self.block.clear();
    }

    /// Returns the number of bytes written to the inner data structure
    #[must_use]
    pub fn bytes_written(&self) -> usize {
        self.inner.len()
    }
}
