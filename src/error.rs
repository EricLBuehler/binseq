#[derive(thiserror::Error, Debug)]
pub enum HeaderError {
    #[error("Invalid magic number: {0}")]
    InvalidMagicNumber(u32),

    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u8),

    #[error("Invalid reserved bytes")]
    InvalidReservedBytes,
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error("Unexpected end of stream in flag (record number: {1}): {0}")]
    UnexpectedEndOfStreamFlag(std::io::Error, usize),

    #[error("Unexpected end of stream in sequence (record number: {1}): {0}")]
    UnexpectedEndOfStreamSequence(std::io::Error, usize),
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("Sequence length ({got}) does not match the header ({expected})")]
    UnexpectedSequenceLength { expected: u32, got: usize },

    #[error("Invalid nucleotide sequence")]
    InvalidNucleotideSequence,
}
