//! Backfiller path for the indexer integration.
//!
//! `Producer` persists finalized block digests into the backfill queue, and
//! `Consumer` drains that queue and retries block uploads.
//!
//! Both cooperate through `SharedState`, which wraps the shared `State`
//! used to deduplicate uploads, cache blocks, and coordinate with the parent
//! module's live `Pusher`.

pub(crate) mod consumer;
pub(crate) mod producer;
pub(crate) mod state;

pub(crate) use consumer::Consumer;
pub(crate) use producer::Producer;
pub(crate) use state::{Decision, Entry, SharedState, State};
