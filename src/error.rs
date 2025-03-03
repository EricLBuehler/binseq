pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum Error {
    HeaderError(#[from] HeaderError),
    ReadError(#[from] ReadError),
    WriteError(#[from] WriteError),
    IoError(#[from] std::io::Error),
    Utf8Error(#[from] std::str::Utf8Error),
    BitnucError(#[from] bitnuc::NucleotideError),
    AnyhowError(#[from] anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum HeaderError {
    #[error("Invalid magic number: {0}")]
    InvalidMagicNumber(u32),
    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u8),
    #[error("Invalid reserved bytes")]
    InvalidReservedBytes,
    #[error("Invalid number of bytes provided: {0}. Expected: {1}")]
    InvalidSize(usize, usize),
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error("File is not regular")]
    IncompatibleFile,
    #[error(
        "Number of bytes in file does not match expectation - possibly truncated at byte pos {0}"
    )]
    FileTruncation(usize),
    #[error("Requested record index ({0}) is out of record range ({1})")]
    OutOfRange(usize, usize),
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("Sequence length ({got}) does not match the header ({expected})")]
    UnexpectedSequenceLength { expected: u32, got: usize },
    #[error("Invalid nucleotides found in sequence: {0}")]
    InvalidNucleotideSequence(String),
}
