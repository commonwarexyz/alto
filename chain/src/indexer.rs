use crate::upload_queue::Mailbox;
#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seed, Seedable};
use commonware_consensus::{marshal, Reporter, Viewable};
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
}

impl<E: Spawner + Metrics> Reporter for Pusher<E> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Activity::Notarization(notarization) => {
                // Enqueue seed
                let view = notarization.view();
                if let Err(e) = self.upload_queue.enqueue_seed(notarization.seed()).await {
                    warn!(%view, ?e, "failed to enqueue seed for upload");
                    return;
                }
                debug!(%view, "seed enqueued for upload");

                // Enqueue notarized block (once we have it)
                self.context.with_label("notarized_block").spawn({
                    let upload_queue = self.upload_queue.clone();
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

                        // Enqueue for upload once we have it
                        let notarization = Notarized::new(notarization, block);
                        if let Err(e) = upload_queue.enqueue_notarization(notarization).await {
                            warn!(%view, ?e, "failed to enqueue notarization for upload");
                            return;
                        }
                        debug!(%view, "notarization enqueued for upload");
                    }
                });
            }
            Activity::Finalization(finalization) => {
                // Enqueue seed
                let view = finalization.view();
                if let Err(e) = self.upload_queue.enqueue_seed(finalization.seed()).await {
                    warn!(%view, ?e, "failed to enqueue seed for upload");
                    return;
                }
                debug!(%view, "seed enqueued for upload");

                // Enqueue finalized block (once we have it)
                self.context.with_label("finalized_block").spawn({
                    let upload_queue = self.upload_queue.clone();
                    let mut marshal = self.marshal.clone();
                    move |_| async move {
                        let block = marshal
                            .subscribe(Some(finalization.round()), finalization.proposal.payload)
                            .await
                            .await;
                        let Ok(block) = block else {
                            warn!(%view, "subscription for block cancelled");
                            return;
                        };

                        // Enqueue for upload once we have it
                        let finalization = Finalized::new(finalization, block);
                        if let Err(e) = upload_queue.enqueue_finalization(finalization).await {
                            warn!(%view, ?e, "failed to enqueue finalization for upload");
                            return;
                        }
                        debug!(%view, "finalization enqueued for upload");
                    }
                });
            }
            _ => {}
        }
    }
}
