mod structs;
mod traits;

/// Instances of common contexts
pub use structs::{FullCtx, SeqCtx, SeqHeaderCtx, SeqQualCtx};

/// Traits for different context behaviors
pub use traits::{Context, HeaderContext, QualityContext, SequenceContext};
