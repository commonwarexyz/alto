use alto_types::{Activity, Block, Notarized};
use commonware_consensus::{marshal, threshold_simplex::types::Seedable, Reporter, Viewable};
use commonware_cryptography::bls12381::primitives::variant::MinSig;
use commonware_runtime::{Metrics, Spawner};
use tracing::{debug, warn};

#[derive(Clone)]
pub struct Indexer<E: Spawner + Metrics, I: crate::Indexer> {
    context: E,
    indexer: I,
    marshal: marshal::ingress::mailbox::Mailbox<MinSig, Block>,
}

impl<E: Spawner + Metrics, I: crate::Indexer> Indexer<E, I> {
    pub fn new(
        context: E,
        indexer: I,
        marshal: marshal::ingress::mailbox::Mailbox<MinSig, Block>,
    ) -> Self {
        Self {
            context,
            indexer,
            marshal,
        }
    }
}

impl<E: Spawner + Metrics, I: crate::Indexer> Reporter for Indexer<E, I> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Activity::Notarization(notarization) => {
                // Upload seed to indexer
                let view = notarization.view();
                self.context.with_label("seed").spawn({
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
                self.context.with_label("notarized").spawn({
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
            _ => {}
        }
    }
}
