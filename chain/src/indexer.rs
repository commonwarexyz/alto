#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seed, Seedable};
use commonware_consensus::{
    marshal::{
        core::Mailbox as MarshalMailbox, standard::Standard, Identifier,
    },
    Reporter, Viewable,
};
use commonware_cryptography::sha256::Digest;
use commonware_parallel::Strategy;
use commonware_runtime::{Clock, Handle, Metrics, Spawner, Storage};
use commonware_storage::queue;
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
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

    /// Upload a block (without certificate) to the indexer.
    fn block_upload(&self, block: Block) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A mock indexer implementation for testing.
#[cfg(test)]
#[derive(Clone)]
pub struct Mock {
    pub seed_seen: Arc<AtomicBool>,
    pub notarization_seen: Arc<AtomicBool>,
    pub finalization_seen: Arc<AtomicBool>,
    pub block_upload_seen: Arc<AtomicBool>,
}

#[cfg(test)]
impl Mock {
    pub fn new(_: &str, _: Identity) -> Self {
        Self {
            seed_seen: Arc::new(AtomicBool::new(false)),
            notarization_seen: Arc::new(AtomicBool::new(false)),
            finalization_seen: Arc::new(AtomicBool::new(false)),
            block_upload_seen: Arc::new(AtomicBool::new(false)),
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

    async fn block_upload(&self, _: Block) -> Result<(), Self::Error> {
        self.block_upload_seen
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

    fn block_upload(&self, block: Block) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.block_upload(block)
    }
}

pub type UploadedSet = Arc<Mutex<HashSet<Digest>>>;

/// An implementation of [Indexer] for the [Reporter] trait.
#[derive(Clone)]
pub struct Pusher<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    uploaded: UploadedSet,
    writer: queue::Writer<E, Digest>,
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> Pusher<E, I> {
    /// Create a new [Pusher].
    pub fn new(
        context: E,
        indexer: I,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        uploaded: UploadedSet,
        writer: queue::Writer<E, Digest>,
    ) -> Self {
        Self {
            context,
            indexer,
            marshal,
            uploaded,
            writer,
        }
    }

    /// Start the drainer loop that reads from the queue and uploads blocks.
    pub fn start_drainer(self, mut reader: queue::Reader<E, Digest>) -> Handle<()> {
        self.context.with_label("drainer").spawn(|_| async move {
            loop {
                let item = reader.recv().await;
                let Ok(Some((position, digest))) = item else {
                    warn!("drainer queue closed");
                    return;
                };

                // Skip if already uploaded
                let already_uploaded = self.uploaded.lock().unwrap().contains(&digest);
                if already_uploaded {
                    reader.ack(position).await.expect("failed to ack");
                    self.writer.sync().await.expect("failed to sync after ack");
                    debug!(?digest, "drainer skipping already-uploaded block");
                    continue;
                }

                // Get block from marshal
                let block = self.marshal.get_block(Identifier::Digest(digest)).await;
                let Some(block) = block else {
                    warn!(?digest, "drainer could not find block in marshal");
                    continue;
                };

                // Retry upload until success
                loop {
                    match self.indexer.block_upload(block.clone()).await {
                        Ok(()) => break,
                        Err(e) => {
                            warn!(?e, ?digest, "drainer failed to upload block, retrying");
                            self.context.sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }

                // Mark uploaded, ack, and sync
                self.uploaded.lock().unwrap().insert(digest);
                reader.ack(position).await.expect("failed to ack");
                self.writer.sync().await.expect("failed to sync after ack");
                debug!(?digest, "drainer uploaded block");
            }
        })
    }
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> Reporter for Pusher<E, I> {
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
                        debug!(%view, "seed uploaded to indexer");
                    }
                });

                // Upload block to indexer (once we have it)
                let digest = notarization.proposal.payload;
                self.context.with_label("notarized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    let uploaded = self.uploaded.clone();
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

                        let notarized = Notarized::new(notarization, block);
                        let result = indexer.notarized_upload(notarized).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload notarization");
                            return;
                        }

                        // Mark as uploaded so drainer can skip
                        {
                            let mut uploaded = uploaded.lock().unwrap();
                            uploaded.insert(digest);
                        }
                        debug!(%view, "notarization uploaded to indexer");
                    }
                });
            }
            Activity::Finalization(finalization) => {
                let digest = finalization.proposal.payload;

                // Ensure digest is durably enqueued before proceeding
                let already_uploaded = self.uploaded.lock().unwrap().contains(&digest);
                if !already_uploaded {
                    self.writer
                        .enqueue(digest)
                        .await
                        .expect("failed to enqueue finalized digest");
                    self.writer
                        .sync()
                        .await
                        .expect("failed to sync after enqueue");
                }

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
                    let marshal = self.marshal.clone();
                    let uploaded = self.uploaded.clone();
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

                        // Mark as uploaded so drainer can skip
                        {
                            let mut uploaded = uploaded.lock().unwrap();
                            uploaded.insert(digest);
                        }
                        debug!(%view, "finalization uploaded to indexer");
                    }
                });
            }
            _ => {}
        }
    }
}
