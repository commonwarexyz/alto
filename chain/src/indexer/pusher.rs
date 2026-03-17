use super::{Indexer, SharedUploadState};
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seedable};
use commonware_consensus::{
    marshal::{core::Mailbox as MarshalMailbox, standard::Standard},
    Reporter, Viewable,
};
use commonware_cryptography::sha256::Digest;
use commonware_runtime::{Metrics, Spawner};
use tracing::{debug, warn};

/// An implementation of [Indexer] for the [Reporter] trait.
#[derive(Clone)]
pub(crate) struct Pusher<E: Spawner + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    uploads: SharedUploadState,
}

impl<E: Spawner + Metrics, I: Indexer> Pusher<E, I> {
    /// Create a new [Pusher].
    pub(crate) fn new(
        context: E,
        indexer: I,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        uploads: SharedUploadState,
    ) -> Self {
        Self {
            context,
            indexer,
            marshal,
            uploads,
        }
    }
}

struct CertificateUploadGuard {
    uploads: SharedUploadState,
    digest: Digest,
    uploaded_height: Option<u64>,
}

impl CertificateUploadGuard {
    fn new(uploads: SharedUploadState, digest: Digest) -> Self {
        uploads.lock().start_certificate_upload(digest);
        Self {
            uploads,
            digest,
            uploaded_height: None,
        }
    }

    fn cache_block(&self, block: Block) {
        self.uploads.lock().cache_block(block);
    }

    fn mark_uploaded(&mut self, height: u64) {
        self.uploaded_height = Some(height);
    }
}

impl Drop for CertificateUploadGuard {
    fn drop(&mut self) {
        let mut uploads = self.uploads.lock();
        if let Some(height) = self.uploaded_height {
            uploads.mark_uploaded(self.digest, height);
        }
        uploads.finish_certificate_upload(&self.digest);
    }
}

impl<E: Spawner + Metrics, I: Indexer> Reporter for Pusher<E, I> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Activity::Notarization(notarization) => {
                // Upload seed to indexer.
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

                // Upload certificate to indexer once the block is available.
                let digest = notarization.proposal.payload;
                self.context.with_label("notarized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    let uploads = self.uploads.clone();
                    move |_| async move {
                        let mut guard = CertificateUploadGuard::new(uploads, digest);

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

                        let height = block.height.get();
                        guard.cache_block(block.clone());
                        let notarized = Notarized::new(notarization, block);
                        let result = indexer.notarized_upload(notarized).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload notarization");
                            return;
                        }

                        guard.mark_uploaded(height);
                        debug!(%view, "notarization uploaded to indexer");
                    }
                });
            }
            Activity::Finalization(finalization) => {
                let view = finalization.view();

                // Upload seed to indexer.
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

                // Upload certificate to indexer once the block is available.
                let digest = finalization.proposal.payload;
                self.context.with_label("finalized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    let uploads = self.uploads.clone();
                    move |_| async move {
                        let mut guard = CertificateUploadGuard::new(uploads, digest);

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

                        let height = block.height.get();
                        guard.cache_block(block.clone());
                        let finalization = Finalized::new(finalization, block);
                        let result = indexer.finalized_upload(finalization).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload finalization");
                            return;
                        }

                        guard.mark_uploaded(height);
                        debug!(%view, "finalization uploaded to indexer");
                    }
                });
            }
            _ => {}
        }
    }
}
