use alto_types::{
    ActivityOf, Block, Finalized, FinalizedOf, Identity, Notarized, NotarizedOf, SchemeOf, Seed,
    Seedable,
};
use commonware_codec::{Encode, DecodeExt};
use commonware_consensus::{marshal, Reporter, Viewable};
use commonware_parallel::Strategy;
use commonware_runtime::{Metrics, Spawner};
use std::future::Future;
#[cfg(test)]
use std::{sync::atomic::AtomicBool, sync::Arc};
use tracing::{debug, warn};

/// Trait for interacting with an indexer.
///
/// The indexer receives consensus data and uploads it to an external service.
/// It is intentionally non-generic over the execution strategy since it only
/// handles serialized data and doesn't perform signature verification.
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

/// A mock indexer implementation for testing.
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
        alto_client::Client::seed_upload(self, seed)
    }

    fn notarized_upload(
        &self,
        notarized: Notarized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        alto_client::Client::notarized_upload(self, notarized)
    }

    fn finalized_upload(
        &self,
        finalized: Finalized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        alto_client::Client::finalized_upload(self, finalized)
    }
}

/// Converts a `NotarizedOf<S>` to the concrete `Notarized` type via serialization.
///
/// This is safe because the serialization format is identical regardless of the
/// strategy type parameter - only the verification parallelism differs.
fn convert_notarized<S: Strategy>(notarized: NotarizedOf<S>) -> Notarized {
    Notarized::decode(notarized.encode()).expect("notarized conversion failed")
}

/// Converts a `FinalizedOf<S>` to the concrete `Finalized` type via serialization.
///
/// This is safe because the serialization format is identical regardless of the
/// strategy type parameter - only the verification parallelism differs.
fn convert_finalized<S: Strategy>(finalized: FinalizedOf<S>) -> Finalized {
    Finalized::decode(finalized.encode()).expect("finalized conversion failed")
}

/// An implementation of [Reporter] that pushes consensus activity to an [Indexer].
///
/// The `Pusher` is generic over the execution strategy `S` to receive typed
/// consensus activity, but converts to concrete types before calling the
/// non-generic `Indexer` trait.
#[derive(Clone)]
pub struct Pusher<E: Spawner + Metrics, I: Indexer, S: Strategy> {
    context: E,
    indexer: I,
    marshal: marshal::Mailbox<SchemeOf<S>, Block>,
}

impl<E: Spawner + Metrics, I: Indexer, S: Strategy> Pusher<E, I, S> {
    /// Create a new [Pusher].
    pub fn new(context: E, indexer: I, marshal: marshal::Mailbox<SchemeOf<S>, Block>) -> Self {
        Self {
            context,
            indexer,
            marshal,
        }
    }
}

impl<E: Spawner + Metrics, I: Indexer, S: Strategy> Reporter for Pusher<E, I, S> {
    type Activity = ActivityOf<S>;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            ActivityOf::Notarization(notarization) => {
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
                        debug!(%view, "seed uploaded to indexer");
                    }
                });

                // Upload block to indexer (once we have it)
                self.context.with_label("notarized_block").spawn({
                    let indexer = self.indexer.clone();
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

                        // Convert to concrete type and upload to indexer
                        let notarized = convert_notarized(NotarizedOf::<S>::new(notarization, block));
                        let result = indexer.notarized_upload(notarized).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload notarization");
                            return;
                        }
                        debug!(%view, "notarization uploaded to indexer");
                    }
                });
            }
            ActivityOf::Finalization(finalization) => {
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
                        debug!(%view, "seed uploaded to indexer");
                    }
                });

                // Upload block to indexer (once we have it)
                self.context.with_label("finalized_block").spawn({
                    let indexer = self.indexer.clone();
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

                        // Convert to concrete type and upload to indexer
                        let finalized = convert_finalized(FinalizedOf::<S>::new(finalization, block));
                        let result = indexer.finalized_upload(finalized).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload finalization");
                            return;
                        }
                        debug!(%view, "finalization uploaded to indexer");
                    }
                });
            }
            _ => {}
        }
    }
}
