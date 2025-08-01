use alto_types::{Activity, Block, Finalized, Identity, Notarized, Seed};
use commonware_consensus::{marshal, threshold_simplex::types::Seedable, Reporter, Viewable};
use commonware_cryptography::bls12381::primitives::variant::MinSig;
use commonware_runtime::{Metrics, Spawner};
use std::future::Future;
#[cfg(test)]
use std::{sync::atomic::AtomicBool, sync::Arc};
use tracing::{debug, warn};

/// Trait for interacting with an indexer.
pub trait Indexer: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Create a new indexer with the given URI and public key.
    fn new(uri: &str, public: Identity) -> Self;

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

/// Mock is a simple indexer implementation for testing.
#[cfg(test)]
#[derive(Clone)]
pub struct Mock {
    pub seed_seen: Arc<AtomicBool>,
    pub notarization_seen: Arc<AtomicBool>,
    pub finalization_seen: Arc<AtomicBool>,
}

#[cfg(test)]
impl Indexer for Mock {
    type Error = std::io::Error;

    fn new(_: &str, _: Identity) -> Self {
        Mock {
            seed_seen: Arc::new(AtomicBool::new(false)),
            notarization_seen: Arc::new(AtomicBool::new(false)),
            finalization_seen: Arc::new(AtomicBool::new(false)),
        }
    }

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

impl Indexer for alto_client::Client {
    type Error = alto_client::Error;

    fn new(uri: &str, identity: Identity) -> Self {
        Self::new(uri, identity)
    }

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

#[derive(Clone)]
pub struct Pusher<E: Spawner + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: marshal::Mailbox<MinSig, Block>,
}

impl<E: Spawner + Metrics, I: Indexer> Pusher<E, I> {
    pub fn new(context: E, indexer: I, marshal: marshal::Mailbox<MinSig, Block>) -> Self {
        Self {
            context,
            indexer,
            marshal,
        }
    }
}

impl<E: Spawner + Metrics, I: Indexer> Reporter for Pusher<E, I> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Activity::Notarization(notarization) => {
                // Upload seed to indexer
                let view = notarization.view();
                self.context.with_label("notarized_seed").spawn({
                    let indexer = self.indexer.clone();
                    let seed = notarization.seed();
                    move |_| async move {
                        let result = indexer.seed_upload(seed).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload seed");
                            return;
                        }
                        debug!(view, "seed uploaded to indexer");
                    }
                });

                // Upload block to indexer (once we have it)
                self.context.with_label("notarized_block").spawn({
                    let indexer = self.indexer.clone();
                    let mut marshal = self.marshal.clone();
                    move |_| async move {
                        // Wait for block
                        let block = marshal
                            .subscribe(Some(notarization.view()), notarization.proposal.payload)
                            .await
                            .await;
                        let Ok(block) = block else {
                            warn!(view, "subscription for block cancelled");
                            return;
                        };

                        // Upload to indexer once we have it
                        let notarization = Notarized::new(notarization, block);
                        let result = indexer.notarized_upload(notarization).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload notarization");
                            return;
                        }
                        debug!(view, "notarization uploaded to indexer");
                    }
                });
            }
            Activity::Finalization(finalization) => {
                // Upload seed to indexer
                let view = finalization.view();
                self.context.with_label("finalized_seed").spawn({
                    let indexer = self.indexer.clone();
                    let seed = finalization.seed();
                    move |_| async move {
                        let result = indexer.seed_upload(seed).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload seed");
                            return;
                        }
                        debug!(view, "seed uploaded to indexer");
                    }
                });

                // Upload block to indexer (once we have it)
                self.context.with_label("finalized_block").spawn({
                    let indexer = self.indexer.clone();
                    let mut marshal = self.marshal.clone();
                    move |_| async move {
                        let block = marshal
                            .subscribe(Some(finalization.view()), finalization.proposal.payload)
                            .await
                            .await;
                        let Ok(block) = block else {
                            warn!(view, "subscription for block cancelled");
                            return;
                        };

                        // Upload to indexer once we have it
                        let finalization = Finalized::new(finalization, block);
                        let result = indexer.finalized_upload(finalization).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload finalization");
                            return;
                        }
                        debug!(view, "finalization uploaded to indexer");
                    }
                });
            }
            _ => {}
        }
    }
}
