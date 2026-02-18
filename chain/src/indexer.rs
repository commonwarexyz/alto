use crate::upload_queue::Mailbox;
#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seed, Seedable};
use commonware_consensus::types::{Round, View};
use commonware_consensus::{marshal, Reporter, Viewable};
use commonware_cryptography::sha256::Digest;
use commonware_parallel::Strategy;
use commonware_runtime::{Metrics, Spawner};
use std::future::Future;
#[cfg(test)]
use std::sync::{atomic::AtomicBool, Arc};
use tracing::{debug, warn};

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

/// An implementation of [Indexer] for the [Reporter] trait.
#[derive(Clone)]
pub struct Pusher<E: Spawner + Metrics> {
    context: E,
    upload_queue: Mailbox,
    marshal: marshal::Mailbox<Scheme, Block>,
}

impl<E: Spawner + Metrics> Pusher<E> {
    /// Create a new [Pusher].
    pub fn new(
        context: E,
        upload_queue: Mailbox,
        marshal: marshal::Mailbox<Scheme, Block>,
    ) -> Self {
        Self {
            context,
            upload_queue,
            marshal,
        }
    }

    fn spawn_block_upload(&self, pending: PendingUpload) {
        let view = pending.view();
        let round = pending.round();
        let payload = pending.payload();
        let label = pending.label();
        let kind = pending.kind();
        let upload_queue = self.upload_queue.clone();
        let mut marshal = self.marshal.clone();

        self.context.with_label(label).spawn(move |_| async move {
            // Wait for block
            let block = marshal.subscribe(Some(round), payload).await.await;
            let Ok(block) = block else {
                warn!(%view, "subscription for block cancelled");
                return;
            };

            // Enqueue for upload
            if let Err(e) = pending.enqueue_with_block(block, &upload_queue).await {
                warn!(%view, ?e, "failed to enqueue {kind} for upload");
                return;
            }
            debug!(%view, "{kind} enqueued for upload");
        });
    }
}

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
        upload_queue: &Mailbox,
    ) -> Result<u64, crate::upload_queue::EnqueueError> {
        match self {
            Self::Notarization(n) => {
                upload_queue
                    .enqueue_notarization(Notarized::new(n, block))
                    .await
            }
            Self::Finalization(f) => {
                upload_queue
                    .enqueue_finalization(Finalized::new(f, block))
                    .await
            }
        }
    }
}

impl<E: Spawner + Metrics> Reporter for Pusher<E> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        let pending = match activity {
            Activity::Notarization(n) => PendingUpload::Notarization(n),
            Activity::Finalization(f) => PendingUpload::Finalization(f),
            _ => return,
        };

        let view = pending.view();

        // Enqueue seed
        if let Err(e) = self.upload_queue.enqueue_seed(pending.seed()).await {
            warn!(%view, ?e, "failed to enqueue seed for upload");
            return;
        }
        debug!(%view, "seed enqueued for upload");

        // Enqueue block activity once the block is available
        self.spawn_block_upload(pending);
    }
}
