//! Backfiller path for the indexer integration.
//!
//! The backfiller path is split into:
//! - [`state`], which owns the shared upload state and the application-side
//!   data structures used by the uploader actors;
//! - [`recorder`], which persists finalized block digests; and
//! - [`drainer`], which owns the background retry loop that drains those queue
//!   rows and uploads blocks.
//!
//! The parent module's live [`crate::indexer::Pusher`] cooperates with both via
//! [`SharedUploadState`].

mod drainer;
mod recorder;
mod state;

pub use drainer::Drainer;
pub use recorder::Recorder;
pub use state::{FinalizedEntry, SharedUploadState, UploadDecision, UploadState};
