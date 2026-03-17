//! Durable raw block upload path for the indexer integration.
//!
//! The durable path is split into:
//! - [`state`], which owns the shared upload state and the application-side
//!   [`Recorder`] that persists finalized block digests; and
//! - [`drainer`], which owns the background retry loop that drains those queue
//!   rows and uploads raw blocks.
//!
//! The parent module's live [`crate::indexer::Pusher`] cooperates with both via
//! [`SharedUploadState`].

mod drainer;
mod state;

pub(crate) use drainer::Drainer;
pub(crate) use state::{
    DrainerMetrics, FinalizedEntry, RawUploadDecision, Recorder, SharedUploadState, UploadState,
};
