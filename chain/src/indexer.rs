#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seed, Seedable};
use commonware_consensus::{marshal, Reporter, Viewable};
use commonware_parallel::Strategy;
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use std::future::Future;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::upload_queue::UploadQueue;

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
pub struct Pusher<E: Spawner + Clock + Storage + Metrics> {
    context: E,
    queue: Arc<UploadQueue<E>>,
    marshal: marshal::Mailbox<Scheme, Block>,
}

impl<E: Spawner + Clock + Storage + Metrics> Pusher<E> {
    /// Create a new [Pusher] with an upload queue.
    pub fn new(
        context: E,
        queue: Arc<UploadQueue<E>>,
        marshal: marshal::Mailbox<Scheme, Block>,
    ) -> Self {
        Self {
            context,
            queue,
            marshal,
        }
    }
}

impl<E: Spawner + Clock + Storage + Metrics> Reporter for Pusher<E> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Activity::Notarization(notarization) => {
                let view = notarization.view();

                // Enqueue seed
                let seed = notarization.seed();
                self.queue
                    .enqueue_seed(seed)
                    .await
                    .expect("failed to enqueue seed");
                debug!(%view, "seed enqueued for upload");

                // Spawn task to wait for block and enqueue notarization
                self.context.with_label("notarized_block").spawn({
                    let queue = self.queue.clone();
                    let mut marshal = self.marshal.clone();
                    move |_| async move {
                        // Wait for block
                        let block = marshal
                            .subscribe(Some(notarization.round()), notarization.proposal.payload)
                            .await
                            .await;
                        let Ok(block) = block else {
                            warn!(%view, "subscription for block cancelled");
                            return;
                        };

                        // Enqueue notarization
                        let notarization = Notarized::new(notarization, block);
                        queue
                            .enqueue_notarization(notarization)
                            .await
                            .expect("failed to enqueue notarization");
                        debug!(%view, "notarization enqueued for upload");
                    }
                });
            }
            Activity::Finalization(finalization) => {
                let view = finalization.view();

                // Enqueue seed
                let seed = finalization.seed();
                self.queue
                    .enqueue_seed(seed)
                    .await
                    .expect("failed to enqueue seed");
                debug!(%view, "seed enqueued for upload");

                // Spawn task to wait for block and enqueue finalization
                self.context.with_label("finalized_block").spawn({
                    let queue = self.queue.clone();
                    let mut marshal = self.marshal.clone();
                    move |_| async move {
                        // Wait for block
                        let block = marshal
                            .subscribe(Some(finalization.round()), finalization.proposal.payload)
                            .await
                            .await;
                        let Ok(block) = block else {
                            warn!(%view, "subscription for block cancelled");
                            return;
                        };

                        // Enqueue finalization
                        let finalization = Finalized::new(finalization, block);
                        queue
                            .enqueue_finalization(finalization)
                            .await
                            .expect("failed to enqueue finalization");
                        debug!(%view, "finalization enqueued for upload");
                    }
                });
            }
            _ => {}
        }
    }
}
