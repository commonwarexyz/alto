//! Indexer upload integration for the chain engine.
//!
//! The indexer integration has two cooperating upload paths:
//! - the live path, where `Pusher` uploads seeds and certificate-bearing
//!   objects as consensus activity happens;
//! - the backfiller path, where `Producer` persists finalized block digests and
//!   `Consumer` retries block uploads from the backfill queue across
//!   restarts.
//!
//! `Indexer` is the top-level abstraction over those pieces. It owns
//! the shared `State` used to deduplicate uploads, cache blocks, and
//! coordinate the live and backfiller paths. The actors are still exposed
//! separately because they plug into three different integration points:
//! the application's finalized block stream, the consensus reporter, and a
//! background consumer task.

use alto_types::{Block, Finalized, Notarized, Scheme, Seed};
use commonware_consensus::marshal::{core::Mailbox as MarshalMailbox, standard::Standard};
use commonware_parallel::Strategy;
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use commonware_storage::queue;
use commonware_utils::sync::Mutex;
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

mod backfiller;
#[cfg(test)]
pub mod mocks;
mod pusher;

pub(crate) use backfiller::{Consumer, Entry, Producer};
use backfiller::{SharedState, State};
pub(crate) use pusher::Pusher;

/// Trait for interacting with an indexer backend.
pub trait Client: Clone + Send + Sync + 'static {
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

impl<S: Strategy> Client for alto_client::Client<S> {
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

/// Builds and owns the indexer's live and backfiller upload actors.
///
/// This is the high-level abstraction over the indexer upload subsystem. It
/// constructs the shared upload state once, then hands out the specific actor
/// handles needed by the engine:
/// - a producer for the application's finalized block stream;
/// - a pusher for consensus activity;
/// - a consumer for the background retry task.
pub(crate) struct Indexer<E: Spawner + Clock + Storage + Metrics, C: Client> {
    producer: Producer<E>,
    pusher: Pusher<E, C>,
    consumer: Consumer<E, C>,
}

impl<E: Spawner + Clock + Storage + Metrics, C: Client> Indexer<E, C> {
    pub(crate) async fn new(
        context: E,
        client: C,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        backfiller: (queue::Writer<E, Entry>, queue::Reader<E, Entry>),
        backfiller_max_active: NonZeroUsize,
        backfiller_retry: Duration,
    ) -> Self {
        let uploads: SharedState = Arc::new(Mutex::new(State::new()));
        let pusher = Pusher::new(
            context.with_label("pusher"),
            client.clone(),
            marshal.clone(),
            uploads.clone(),
        );
        let (writer, reader) = backfiller;
        let producer = Producer::new(uploads.clone(), writer.clone());
        let consumer = Consumer::new(
            context.with_label("consumer"),
            client,
            marshal,
            uploads,
            (writer, reader),
            backfiller_max_active,
            backfiller_retry,
        );

        Self {
            producer,
            pusher,
            consumer,
        }
    }

    /// Consumes the runtime and returns the actor handles it constructed.
    pub(crate) fn split(self) -> (Producer<E>, Pusher<E, C>, Consumer<E, C>) {
        let Self {
            producer,
            pusher,
            consumer,
        } = self;
        (producer, pusher, consumer)
    }
}
