//! Backfiller path for the indexer integration.
//!
//! `Producer` persists finalized block digests into the backfill queue, and
//! `Consumer` drains that queue and retries block uploads.
//!
//! Both cooperate through `SharedState`, which wraps the shared `State`
//! used to deduplicate uploads, cache blocks, and coordinate with the parent
//! module's live `Pusher`.

mod consumer;
mod producer;
mod state;

pub use consumer::Consumer;
pub use producer::Producer;
pub use state::{Decision, Entry, SharedState, State};
