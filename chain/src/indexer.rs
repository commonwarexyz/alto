#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seed, Seedable};
use commonware_consensus::{
    marshal,
    types::{Round, View},
    Reporter, Viewable,
};
use commonware_cryptography::sha256::Digest;
use commonware_parallel::Strategy;
use commonware_runtime::{Clock, Metrics, Spawner};
use std::future::Future;
#[cfg(test)]
use std::sync::{atomic::AtomicBool, Arc};
use tracing::{debug, warn};

use crate::upload_queue::Mailbox;

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
}

/// A mock indexer implementation for testing.
#[cfg(test)]
#[derive(Clone)]
pub struct Mock {
    pub seed_seen: Arc<AtomicBool>,
    pub notarization_seen: Arc<AtomicBool>,
    pub finalization_seen: Arc<AtomicBool>,
}

#[cfg(test)]
impl Mock {
    pub fn new(_: &str, _: Identity) -> Self {
        Self {
            seed_seen: Arc::new(AtomicBool::new(false)),
            notarization_seen: Arc::new(AtomicBool::new(false)),
            finalization_seen: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(test)]
impl Indexer for Mock {
    type Error = std::io::Error;

    async fn seed_upload(&self, _: Seed) -> Result<(), Self::Error> {
        self.seed_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn notarized_upload(&self, _: Notarized) -> Result<(), Self::Error> {
        self.notarization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
        self.finalization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
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
}

/// A [Reporter] implementation that uploads consensus activity to an indexer.
///
/// Uses a disk-backed upload queue for reliable delivery. Uploads are persisted
/// to disk before returning, and a background worker retries until the indexer
/// acknowledges receipt.
#[derive(Clone)]
pub struct Pusher<E: Spawner + Clock + Metrics> {
    context: E,
    mailbox: Mailbox,
    marshal: marshal::Mailbox<Scheme, Block>,
}

impl<E: Spawner + Clock + Metrics> Pusher<E> {
    /// Create a new [Pusher] with an upload queue mailbox.
    pub fn new(context: E, mailbox: Mailbox, marshal: marshal::Mailbox<Scheme, Block>) -> Self {
        Self {
            context,
            mailbox,
            marshal,
        }
    }
}

/// Internal enum for handling both notarization and finalization uploads uniformly.
enum PendingUpload {
    Notarization(alto_types::Notarization),
    Finalization(alto_types::Finalization),
}

impl PendingUpload {
    fn view(&self) -> View {
        match self {
            Self::Notarization(n) => n.view(),
            Self::Finalization(f) => f.view(),
        }
    }

    fn seed(&self) -> Seed {
        match self {
            Self::Notarization(n) => n.seed(),
            Self::Finalization(f) => f.seed(),
        }
    }

    fn round(&self) -> Round {
        match self {
            Self::Notarization(n) => n.round(),
            Self::Finalization(f) => f.round(),
        }
    }

    fn payload(&self) -> Digest {
        match self {
            Self::Notarization(n) => n.proposal.payload,
            Self::Finalization(f) => f.proposal.payload,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Notarization(_) => "notarized_block",
            Self::Finalization(_) => "finalized_block",
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Notarization(_) => "notarization",
            Self::Finalization(_) => "finalization",
        }
    }

    async fn enqueue_with_block(
        self,
        block: Block,
        mailbox: &Mailbox,
    ) -> Result<u64, crate::upload_queue::EnqueueError> {
        match self {
            Self::Notarization(n) => mailbox.enqueue_notarization(Notarized::new(n, block)).await,
            Self::Finalization(f) => mailbox.enqueue_finalization(Finalized::new(f, block)).await,
        }
    }
}

impl<E: Spawner + Clock + Metrics> Pusher<E> {
    /// Spawns a task to wait for a block and enqueue the activity for upload.
    fn spawn_block_upload(&self, pending: PendingUpload) {
        let view = pending.view();
        let round = pending.round();
        let payload = pending.payload();
        let label = pending.label();
        let kind = pending.kind();
        let mailbox = self.mailbox.clone();
        let mut marshal = self.marshal.clone();

        self.context.with_label(label).spawn(move |_| async move {
            // Wait for block
            let block = marshal.subscribe(Some(round), payload).await.await;
            let Ok(block) = block else {
                warn!(%view, "subscription for block cancelled");
                return;
            };

            // Enqueue for upload
            if let Err(e) = pending.enqueue_with_block(block, &mailbox).await {
                warn!(%view, ?e, "failed to enqueue {kind} for upload");
                return;
            }
            debug!(%view, "{kind} enqueued for upload");
        });
    }
}

impl<E: Spawner + Clock + Metrics> Reporter for Pusher<E> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        let pending = match activity {
            Activity::Notarization(n) => PendingUpload::Notarization(n),
            Activity::Finalization(f) => PendingUpload::Finalization(f),
            _ => return,
        };

        let view = pending.view();

        // Enqueue seed
        if let Err(e) = self.mailbox.enqueue_seed(pending.seed()).await {
            warn!(%view, ?e, "failed to enqueue seed for upload");
            return;
        }
        debug!(%view, "seed enqueued for upload");

        // Spawn task to wait for block and enqueue the activity
        self.spawn_block_upload(pending);
    }
}
