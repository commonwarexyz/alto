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

pub(crate) use durable::{Drainer, Enqueuer, FinalizedEntry};
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

/// Bundles the shared upload state and the actors built from it.
pub(crate) struct IndexerRuntime<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    enqueuer: Enqueuer<E>,
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

        let enqueuer = Enqueuer::new(uploads.clone(), writer.clone(), metrics.clone());
        let drainer = (
            Drainer::new(context, indexer, marshal, metrics, uploads, writer),
            reader,
        );

        Self {
            enqueuer,
            pusher,
            drainer,
        }
    }

    pub(crate) fn enqueuer(&self) -> Enqueuer<E> {
        self.enqueuer.clone()
    }

    pub(crate) fn pusher(&self) -> Pusher<E, I> {
        self.pusher.clone()
    }

    pub(crate) fn into_drainer(self) -> (Drainer<E, I>, queue::Reader<E, FinalizedEntry>) {
        self.drainer
    }
}
