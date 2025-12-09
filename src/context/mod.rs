/// Instances of common contexts
mod structs;

/// Traits for different context behaviors
mod traits;

pub use structs::{Ctx, SeqCtx, SeqHeaderCtx, SeqQualCtx};
pub use traits::{Context, HeaderContext, QualityContext, SequenceContext};
