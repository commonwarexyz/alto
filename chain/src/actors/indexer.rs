use alto_types::{Activity, Block};
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
            }
            _ => {}
        }
    }
}
