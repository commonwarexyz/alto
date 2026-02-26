#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme};
use commonware_consensus::{
    marshal::{self, standard::StandardMinimmit},
    Reporter, Viewable,
};
use commonware_parallel::Strategy;
use commonware_runtime::{Metrics, Spawner};
use std::future::Future;
#[cfg(test)]
use std::{sync::atomic::AtomicBool, sync::Arc};
use tracing::{debug, warn};

type MarshalMailbox = marshal::core::Mailbox<StandardMinimmit<Block, Scheme>>;

/// Trait for interacting with an indexer.
pub trait Indexer: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

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
    pub notarization_seen: Arc<AtomicBool>,
    pub finalization_seen: Arc<AtomicBool>,
}

#[cfg(test)]
impl Mock {
    pub fn new(_: &str, _: Identity) -> Self {
        Self {
            notarization_seen: Arc::new(AtomicBool::new(false)),
            finalization_seen: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(test)]
impl Indexer for Mock {
    type Error = std::io::Error;

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
pub struct Pusher<E: Spawner + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: MarshalMailbox,
}

impl<E: Spawner + Metrics, I: Indexer> Pusher<E, I> {
    /// Create a new [Pusher].
    pub fn new(context: E, indexer: I, marshal: MarshalMailbox) -> Self {
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
            Activity::MNotarization(notarization) => {
                let view = notarization.view();
                self.context.with_label("notarized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    move |_| async move {
                        let block = marshal
                            .subscribe_by_digest(
                                Some(notarization.round()),
                                notarization.proposal.payload,
                            )
                            .await
                            .await;
                        let Ok(block) = block else {
                            warn!(%view, "subscription for block cancelled");
                            return;
                        };

                        let notarization = Notarized::new(notarization, block);
                        let result = indexer.notarized_upload(notarization).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload notarization");
                            return;
                        }
                        debug!(%view, "notarization uploaded to indexer");
                    }
                });
            }
            Activity::Finalization(finalization) => {
                let view = finalization.view();
                self.context.with_label("finalized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    move |_| async move {
                        let block = marshal
                            .subscribe_by_digest(
                                Some(finalization.round()),
                                finalization.proposal.payload,
                            )
                            .await
                            .await;
                        let Ok(block) = block else {
                            warn!(%view, "subscription for block cancelled");
                            return;
                        };

                        let finalization = Finalized::new(finalization, block);
                        let result = indexer.finalized_upload(finalization).await;
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
