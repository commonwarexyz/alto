//! Indexer upload integration for the chain engine.
//!
//! The indexer integration has two cooperating upload paths:
//! - the live path, where [`Pusher`] uploads seeds and certificate-bearing
//!   objects as consensus activity happens;
//! - the durable path, where [`Recorder`] persists finalized block digests and
//!   [`Drainer`] retries raw block uploads from the durable queue across
//!   restarts.
//!
//! [`IndexerRuntime`] is the top-level abstraction over those pieces. It owns
//! the shared [`UploadState`] used to deduplicate uploads, cache blocks, and
//! coordinate the live and durable paths. The actors are still exposed
//! separately because they plug into three different integration points:
//! the application's finalized block stream, the consensus reporter, and a
//! background drainer task.

use alto_types::{Block, Finalized, Notarized, Scheme, Seed};
use commonware_consensus::marshal::{core::Mailbox as MarshalMailbox, standard::Standard};
use commonware_parallel::Strategy;
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use commonware_storage::queue;
use commonware_utils::sync::Mutex;
use std::{future::Future, sync::Arc};

mod durable;
#[cfg(test)]
mod mock;
mod pusher;

pub(crate) use durable::{Drainer, FinalizedEntry, Recorder};
use durable::{DrainerMetrics, SharedUploadState, UploadState};
#[cfg(test)]
pub use mock::Mock;
pub(crate) use pusher::Pusher;

/// Trait for interacting with an indexer.
pub trait Indexer: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Upload a seed to the indexer.
    fn seed_upload(&self, seed: Seed) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Upload a notarization to the indexer.
    fn notarized_upload(
        &self,
        notarized: Notarized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Upload a finalization to the indexer.
    fn finalized_upload(
        &self,
        finalized: Finalized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Upload a block (without certificate) to the indexer.
    fn block_upload(&self, block: Block) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<S: Strategy> Indexer for alto_client::Client<S> {
    type Error = alto_client::Error;

    fn seed_upload(&self, seed: Seed) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.seed_upload(seed)
    }

    fn notarized_upload(
        &self,
        notarized: Notarized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.notarized_upload(notarized)
    }

    fn finalized_upload(
        &self,
        finalized: Finalized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.finalized_upload(finalized)
    }

    fn block_upload(&self, block: Block) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.block_upload(block)
    }
}

/// Builds and owns the indexer's live and durable upload actors.
///
/// This is the high-level abstraction over the indexer upload subsystem. It
/// constructs the shared upload state once, then hands out the specific actor
/// handles needed by the engine:
/// - a recorder for the application's finalized block stream;
/// - a live pusher for consensus activity;
/// - a durable drainer plus queue reader for the background retry task.
pub(crate) struct IndexerRuntime<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    recorder: Recorder<E>,
    pusher: Pusher<E, I>,
    drainer: (Drainer<E, I>, queue::Reader<E, FinalizedEntry>),
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> IndexerRuntime<E, I> {
    pub(crate) async fn new(
        context: E,
        indexer: I,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        durable_queue: (
            queue::Writer<E, FinalizedEntry>,
            queue::Reader<E, FinalizedEntry>,
        ),
    ) -> Self {
        let uploads: SharedUploadState = Arc::new(Mutex::new(UploadState::new()));
        let pusher = Pusher::new(
            context.clone(),
            indexer.clone(),
            marshal.clone(),
            uploads.clone(),
        );
        let (writer, reader) = durable_queue;
        let queue_size = writer.size().await;
        let ack_floor = reader.ack_floor().await;
        let pending = queue_size.saturating_sub(ack_floor);
        let metrics = DrainerMetrics::new(&context.with_label("queue"));
        metrics.depth.set(pending as i64);

        let recorder = Recorder::new(uploads.clone(), writer.clone(), metrics.clone());
        let drainer = (
            Drainer::new(context, indexer, marshal, metrics, uploads, writer),
            reader,
        );

        Self {
            recorder,
            pusher,
            drainer,
        }
    }

    /// Returns the application-side recorder.
    pub(crate) fn recorder(&self) -> Recorder<E> {
        self.recorder.clone()
    }

    /// Returns the live consensus-activity pusher.
    pub(crate) fn live_pusher(&self) -> Pusher<E, I> {
        self.pusher.clone()
    }

    /// Consumes the runtime and returns the durable queue drainer task inputs.
    pub(crate) fn into_durable_drainer(self) -> (Drainer<E, I>, queue::Reader<E, FinalizedEntry>) {
        self.drainer
    }
}
