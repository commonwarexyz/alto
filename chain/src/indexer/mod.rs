//! Indexer upload integration for the chain engine.
//!
//! The indexer integration has two cooperating upload paths:
//! - the live path, where `Pusher` uploads seeds and certificate-bearing
//!   objects as consensus activity happens;
//! - the backfiller path, where `Producer` persists finalized block digests and
//!   `Consumer` retries block uploads from the backfill queue across
//!   restarts.
//!
//! `Indexer` is the top-level abstraction over those pieces. It owns
//! the shared `State` used to deduplicate uploads, cache blocks, and
//! coordinate the live and backfiller paths. The actors are still exposed
//! separately because they plug into three different integration points:
//! the application's finalized block stream, the consensus reporter, and a
//! background consumer task.

use alto_types::{Block, Finalized, Notarized, Scheme, Seed};
use commonware_consensus::marshal::{core::Mailbox as MarshalMailbox, standard::Standard};
#[cfg(test)]
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_parallel::Strategy;
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use commonware_storage::queue;
#[cfg(test)]
use commonware_utils::channel::oneshot;
use commonware_utils::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

mod backfiller;
mod pusher;

pub(crate) use backfiller::{Consumer, Entry, Producer};
use backfiller::{SharedState, State};
pub(crate) use pusher::Pusher;

/// Trait for interacting with an indexer backend.
pub trait Client: Clone + Send + Sync + 'static {
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

impl<S: Strategy> Client for alto_client::Client<S> {
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

/// Builds and owns the indexer's live and backfiller upload actors.
///
/// This is the high-level abstraction over the indexer upload subsystem. It
/// constructs the shared upload state once, then hands out the specific actor
/// handles needed by the engine:
/// - a producer for the application's finalized block stream;
/// - a pusher for consensus activity;
/// - a consumer for the background retry task.
pub(crate) struct Indexer<E: Spawner + Clock + Storage + Metrics, C: Client> {
    producer: Producer<E>,
    pusher: Pusher<E, C>,
    consumer: Consumer<E, C>,
}

impl<E: Spawner + Clock + Storage + Metrics, C: Client> Indexer<E, C> {
    pub(crate) async fn new(
        context: E,
        client: C,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        backfiller: (queue::Writer<E, Entry>, queue::Reader<E, Entry>),
        backfiller_max_in_flight: NonZeroUsize,
        backfiller_retry: Duration,
    ) -> Self {
        let uploads: SharedState = Arc::new(Mutex::new(State::new()));
        let pusher = Pusher::new(
            context.clone().with_label("pusher"),
            client.clone(),
            marshal.clone(),
            uploads.clone(),
        );
        let (writer, reader) = backfiller;
        let producer = Producer::new(uploads.clone(), writer.clone());
        let consumer = Consumer::new(
            context.with_label("consumer"),
            client,
            marshal,
            uploads,
            (writer, reader),
            backfiller_max_in_flight,
            backfiller_retry,
        );

        Self {
            producer,
            pusher,
            consumer,
        }
    }

    /// Consumes the runtime and returns the actor handles it constructed.
    pub(crate) fn split(self) -> (Producer<E>, Pusher<E, C>, Consumer<E, C>) {
        let Self {
            producer,
            pusher,
            consumer,
        } = self;
        (producer, pusher, consumer)
    }
}

#[cfg(test)]
struct InflightGuard(Arc<AtomicUsize>);

#[cfg(test)]
impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
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
    pub fn new() -> Self {
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
        self.cert_upload_inflight.load(Ordering::SeqCst)
    }

    pub fn current_block_upload_inflight(&self) -> usize {
        self.block_upload_inflight.load(Ordering::SeqCst)
    }

    async fn wait_for_cert_upload(&self) {
        self.cert_upload_inflight.fetch_add(1, Ordering::SeqCst);
        let _guard = InflightGuard(self.cert_upload_inflight.clone());

        let waiter = self.cert_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
        }
    }
}

#[cfg(test)]
impl Default for Mock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl Client for Mock {
    type Error = std::io::Error;

    async fn seed_upload(&self, _: Seed) -> Result<(), Self::Error> {
        self.seed_seen.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn notarized_upload(&self, _: Notarized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.notarization_seen.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.finalization_seen.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn block_upload(&self, block: Block) -> Result<(), Self::Error> {
        let digest = block.digest();
        self.block_upload_started.fetch_add(1, Ordering::SeqCst);
        self.block_upload_started_digests.lock().push(digest);
        let inflight = self.block_upload_inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.block_upload_max_inflight
            .fetch_max(inflight, Ordering::SeqCst);
        let _guard = InflightGuard(self.block_upload_inflight.clone());

        let waiter = self.block_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
        }

        self.block_upload_completed.fetch_add(1, Ordering::SeqCst);
        self.block_upload_completed_digests.lock().push(digest);
        Ok(())
    }
}
