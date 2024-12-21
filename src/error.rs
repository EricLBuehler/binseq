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
    #[error("Unexpected end of stream in flag: {0}")]
    UnexpectedEndOfStreamFlag(std::io::Error),

    #[error("Unexpected end of stream in sequence: {0}")]
    UnexpectedEndOfStreamSequence(std::io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("Sequence length ({got}) does not match the header ({expected})")]
    UnexpectedSequenceLength { expected: u32, got: usize },

    #[error("Invalid nucleotide sequence")]
    InvalidNucleotideSequence,
}
