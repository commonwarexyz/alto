//! Indexer upload integration for the chain engine.
//!
//! The indexer integration has two cooperating upload paths:
//! - the live path, where [`Pusher`] uploads seeds and certificate-bearing
//!   objects as consensus activity happens;
//! - the backfiller path, where [`Recorder`] persists finalized block digests and
//!   [`Drainer`] retries block uploads from the backfill queue across
//!   restarts.
//!
//! [`Indexer`] is the top-level abstraction over those pieces. It owns
//! the shared [`UploadState`] used to deduplicate uploads, cache blocks, and
//! coordinate the live and backfiller paths. The actors are still exposed
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

mod backfiller;
#[cfg(test)]
mod mock;
mod pusher;

pub(crate) use backfiller::{Drainer, FinalizedEntry, Recorder};
use backfiller::{SharedUploadState, UploadState};
#[cfg(test)]
pub use mock::Mock;
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
/// - a recorder for the application's finalized block stream;
/// - a pusher for consensus activity;
/// - a backfiller for the background retry task.
pub(crate) struct Indexer<E: Spawner + Clock + Storage + Metrics, C: Client> {
    recorder: Recorder<E>,
    pusher: Pusher<E, C>,
    backfiller: Drainer<E, C>,
}

impl<E: Spawner + Clock + Storage + Metrics, C: Client> Indexer<E, C> {
    pub(crate) async fn new(
        context: E,
        client: C,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        backfill_queue: (
            queue::Writer<E, FinalizedEntry>,
            queue::Reader<E, FinalizedEntry>,
        ),
    ) -> Self {
        let uploads: SharedUploadState = Arc::new(Mutex::new(UploadState::new()));
        let pusher = Pusher::new(
            context.clone(),
            client.clone(),
            marshal.clone(),
            uploads.clone(),
        );
        let (writer, reader) = backfill_queue;
        let recorder = Recorder::new(uploads.clone(), writer.clone());
        let backfiller = Drainer::new(context, client, marshal, uploads, writer, reader);

        Self {
            recorder,
            pusher,
            backfiller,
        }
    }

    /// Consumes the runtime and returns the actor handles it constructed.
    pub(crate) fn split(self) -> (Recorder<E>, Pusher<E, C>, Drainer<E, C>) {
        let Self {
            recorder,
            pusher,
            backfiller,
        } = self;
        (recorder, pusher, backfiller)
    }
}
