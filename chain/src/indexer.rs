#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seed, Seedable};
use commonware_consensus::{
    marshal::{core::Mailbox as MarshalMailbox, standard::Standard},
    Reporter, Viewable,
};
use commonware_cryptography::sha256::Digest;
#[cfg(test)]
use commonware_cryptography::Digestible;
use commonware_parallel::Strategy;
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use commonware_storage::queue;
#[cfg(test)]
use commonware_utils::channel::oneshot;
use commonware_utils::sync::Mutex;
use std::future::Future;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use tracing::{debug, warn};

mod durable;

pub(crate) use durable::{Drainer, Enqueuer, FinalizedEntry};
use durable::{DrainerMetrics, SharedUploadState, UploadState};

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
    pub block_upload_started: Arc<AtomicUsize>,
    pub block_upload_completed: Arc<AtomicUsize>,
    pub block_upload_max_inflight: Arc<AtomicUsize>,
    pub block_upload_started_digests: Arc<Mutex<Vec<Digest>>>,
    pub block_upload_completed_digests: Arc<Mutex<Vec<Digest>>>,
    cert_upload_inflight: Arc<AtomicUsize>,
    cert_upload_waiters: Arc<Mutex<Vec<oneshot::Receiver<()>>>>,
    block_upload_inflight: Arc<AtomicUsize>,
    block_upload_waiters: Arc<Mutex<Vec<oneshot::Receiver<()>>>>,
    pub fail_certs: bool,
}

#[cfg(test)]
impl Mock {
    pub fn new(_: &str, _: Identity) -> Self {
        Self {
            seed_seen: Arc::new(AtomicBool::new(false)),
            notarization_seen: Arc::new(AtomicBool::new(false)),
            finalization_seen: Arc::new(AtomicBool::new(false)),
            block_upload_started: Arc::new(AtomicUsize::new(0)),
            block_upload_completed: Arc::new(AtomicUsize::new(0)),
            block_upload_max_inflight: Arc::new(AtomicUsize::new(0)),
            block_upload_started_digests: Arc::new(Mutex::new(Vec::new())),
            block_upload_completed_digests: Arc::new(Mutex::new(Vec::new())),
            cert_upload_inflight: Arc::new(AtomicUsize::new(0)),
            cert_upload_waiters: Arc::new(Mutex::new(Vec::new())),
            block_upload_inflight: Arc::new(AtomicUsize::new(0)),
            block_upload_waiters: Arc::new(Mutex::new(Vec::new())),
            fail_certs: false,
        }
    }

    pub fn with_fail_certs(mut self) -> Self {
        self.fail_certs = true;
        self
    }

    pub fn with_block_upload_waiters(self, waiters: Vec<oneshot::Receiver<()>>) -> Self {
        *self.block_upload_waiters.lock() = waiters.into_iter().rev().collect();
        self
    }

    pub fn with_cert_upload_waiters(self, waiters: Vec<oneshot::Receiver<()>>) -> Self {
        *self.cert_upload_waiters.lock() = waiters.into_iter().rev().collect();
        self
    }

    pub fn current_cert_upload_inflight(&self) -> usize {
        self.cert_upload_inflight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn current_block_upload_inflight(&self) -> usize {
        self.block_upload_inflight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn wait_for_cert_upload(&self) {
        struct InflightGuard(Arc<AtomicUsize>);

        impl Drop for InflightGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        self.cert_upload_inflight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _guard = InflightGuard(self.cert_upload_inflight.clone());

        let waiter = self.cert_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
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
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.notarization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.finalization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn block_upload(&self, block: Block) -> Result<(), Self::Error> {
        struct InflightGuard(Arc<AtomicUsize>);

        impl Drop for InflightGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let digest = block.digest();
        self.block_upload_started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.block_upload_started_digests.lock().push(digest);
        let inflight = self
            .block_upload_inflight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.block_upload_max_inflight
            .fetch_max(inflight, std::sync::atomic::Ordering::SeqCst);
        let _guard = InflightGuard(self.block_upload_inflight.clone());

        let waiter = self.block_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
        }

        self.block_upload_completed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.block_upload_completed_digests.lock().push(digest);
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

/// Bundles the shared upload state and the actors built from it.
pub(crate) struct Uploads<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    enqueuer: Enqueuer<E>,
    pusher: Pusher<E, I>,
    drainer: (Drainer<E, I>, queue::Reader<E, FinalizedEntry>),
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> Uploads<E, I> {
    pub(crate) async fn new(
        context: E,
        indexer: I,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        durable_queue: (
            queue::Writer<E, FinalizedEntry>,
            queue::Reader<E, FinalizedEntry>,
        ),
    ) -> Self {
        let uploads: SharedUploadState = Arc::new(Mutex::new(UploadState::new()));
        let pusher = Pusher::new(
            context.clone(),
            indexer.clone(),
            marshal.clone(),
            uploads.clone(),
        );
        let (writer, reader) = durable_queue;
        let queue_size = writer.size().await;
        let ack_floor = reader.ack_floor().await;
        let pending = queue_size.saturating_sub(ack_floor);
        let metrics = DrainerMetrics::new(&context.with_label("queue"));
        metrics.depth.set(pending as i64);

        let enqueuer = Enqueuer::new(uploads.clone(), writer.clone(), metrics.clone());
        let drainer = (
            Drainer::new(context, indexer, marshal, metrics, uploads, writer),
            reader,
        );

        Self {
            enqueuer,
            pusher,
            drainer,
        }
    }

    pub(crate) fn enqueuer(&self) -> Enqueuer<E> {
        self.enqueuer.clone()
    }

    pub(crate) fn pusher(&self) -> Pusher<E, I> {
        self.pusher.clone()
    }

    pub(crate) fn into_drainer(self) -> (Drainer<E, I>, queue::Reader<E, FinalizedEntry>) {
        self.drainer
    }
}

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

                // Upload certificate to indexer (once we have it)
                let digest = notarization.proposal.payload;
                self.context.with_label("notarized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    let uploads = self.uploads.clone();
                    move |_| async move {
                        let mut guard = CertificateUploadGuard::new(uploads, digest);

                        // Wait for block.
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
                        // Upload to indexer once we have it.
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

                // Upload seed to indexer
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

                // Upload certificate to indexer (once we have it)
                let digest = finalization.proposal.payload;
                self.context.with_label("finalized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    let uploads = self.uploads.clone();
                    move |_| async move {
                        let mut guard = CertificateUploadGuard::new(uploads, digest);

                        // Wait for block.
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
                        // Upload to indexer once we have it.
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
