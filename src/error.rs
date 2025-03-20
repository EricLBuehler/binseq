/// Custom Result type for binseq operations, wrapping the custom [`Error`] type
pub type Result<T> = std::result::Result<T, Error>;

/// The main error type for the binseq library, encompassing all possible error cases
/// that can occur during binary sequence operations.
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum Error {
    /// Errors related to binary sequence header processing
    HeaderError(#[from] HeaderError),
    /// Errors that occur during read operations
    ReadError(#[from] ReadError),
    /// Errors that occur during write operations
    WriteError(#[from] WriteError),
    /// Standard I/O errors from the Rust standard library
    IoError(#[from] std::io::Error),
    /// UTF-8 encoding/decoding errors
    Utf8Error(#[from] std::str::Utf8Error),
    /// Errors from the bitnuc nucleotide processing librar
    BitnucError(#[from] bitnuc::NucleotideError),
    /// Generic errors that can occur in any part of the system
    AnyhowError(#[from] anyhow::Error),
}

/// Errors specific to processing and validating binary sequence headers
#[derive(thiserror::Error, Debug)]
pub enum HeaderError {
    /// The magic number in the header does not match the expected value
    /// 
    /// # Arguments
    /// * `u32` - The invalid magic number that was found
    #[error("Invalid magic number: {0}")]
    InvalidMagicNumber(u32),

    /// The format version in the header is not supported
    /// 
    /// # Arguments
    /// * `u8` - The unsupported version number that was found
    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u8),

    /// The reserved bytes in the header contain unexpected values
    #[error("Invalid reserved bytes")]
    InvalidReservedBytes,

    /// The size of the data does not match what was specified in the header
    /// 
    /// # Arguments
    /// * First `usize` - The actual number of bytes provided
    /// * Second `usize` - The expected number of bytes according to the header
    #[error("Invalid number of bytes provided: {0}. Expected: {1}")]
    InvalidSize(usize, usize),
}

/// Errors that can occur while reading binary sequence data
#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    /// The file being read is not a regular file (e.g., it might be a directory or special file)
    #[error("File is not regular")]
    IncompatibleFile,

    /// The file appears to be truncated or corrupted
    /// 
    /// # Arguments
    /// * `usize` - The byte position where the truncation was detected
    #[error(
        "Number of bytes in file does not match expectation - possibly truncated at byte pos {0}"
    )]
    FileTruncation(usize),

    /// Attempted to access a record index that is beyond the available range
    /// 
    /// # Arguments
    /// * First `usize` - The requested record index
    /// * Second `usize` - The maximum available record index
    #[error("Requested record index ({0}) is out of record range ({1})")]
    OutOfRange(usize, usize),
}

/// Errors that can occur while writing binary sequence data
#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    /// The length of the sequence being written does not match what was specified in the header
    /// 
    /// # Fields
    /// * `expected` - The sequence length specified in the header
    /// * `got` - The actual length of the sequence being written
    #[error("Sequence length ({got}) does not match the header ({expected})")]
    UnexpectedSequenceLength { expected: u32, got: usize },

    /// The sequence contains invalid nucleotide characters
    /// 
    /// # Arguments
    /// * `String` - Description of the invalid nucleotides found
    #[error("Invalid nucleotides found in sequence: {0}")]
    InvalidNucleotideSequence(String),

    /// Attempted to write data without first setting up the header
    #[error("Missing header in writer builder")]
    MissingHeader,
}
