//! Backfiller path for the indexer integration.
//!
//! The backfiller path is split into:
//! - [`state`], which owns the shared upload state and the application-side
//!   [`Recorder`] that persists finalized block digests; and
//! - [`drainer`], which owns the background retry loop that drains those queue
//!   rows and uploads blocks.
//!
//! The parent module's live [`crate::indexer::Pusher`] cooperates with both via
//! [`SharedUploadState`].

mod drainer;
mod state;

pub use drainer::Drainer;
pub use state::{FinalizedEntry, Recorder, SharedUploadState, UploadDecision, UploadState};
