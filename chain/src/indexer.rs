#[cfg(test)]
use alto_types::Identity;
use alto_types::{Activity, Block, Finalized, Notarized, Scheme, Seed, Seedable};
use bytes::{Buf, BufMut};
use commonware_codec::{self, FixedSize};
use commonware_consensus::{
    marshal::{core::Mailbox as MarshalMailbox, standard::Standard, Identifier},
    Reporter, Viewable,
};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_macros::select;
use commonware_parallel::Strategy;
use commonware_runtime::{
    telemetry::metrics::status::{self, CounterExt},
    Clock, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::queue;
#[cfg(test)]
use commonware_utils::channel::oneshot;
use commonware_utils::{
    futures::{OptionFuture, Pool},
    sync::Mutex,
    PrioritySet,
};
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use std::collections::BTreeMap;
use std::future::Future;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use tracing::{debug, warn};

const DRAINER_MAX_IN_FLIGHT: usize = 16;

#[derive(Clone)]
struct DrainerMetrics {
    depth: Gauge,
    enqueued: Counter,
    uploads: status::Counter,
    ack_floor: Gauge,
    in_flight: Gauge,
}

impl DrainerMetrics {
    fn new<E: Metrics>(context: &E) -> Self {
        let metrics = Self {
            depth: Gauge::default(),
            enqueued: Counter::default(),
            uploads: status::Counter::default(),
            ack_floor: Gauge::default(),
            in_flight: Gauge::default(),
        };

        context.register(
            "depth",
            "Current number of pending finalized block uploads in the durable queue",
            metrics.depth.clone(),
        );
        context.register(
            "enqueued",
            "Total number of finalized block uploads enqueued durably",
            metrics.enqueued.clone(),
        );
        context.register(
            "uploads",
            "Total number of finalized block upload attempt outcomes by status",
            metrics.uploads.clone(),
        );
        context.register(
            "ack_floor",
            "Durable queue positions below this value have been acknowledged and pruned",
            metrics.ack_floor.clone(),
        );
        context.register(
            "in_flight",
            "Current number of block uploads in flight from the durable queue",
            metrics.in_flight.clone(),
        );

        metrics
    }
}

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
    pub block_upload_started: Arc<AtomicUsize>,
    pub block_upload_completed: Arc<AtomicUsize>,
    pub block_upload_max_inflight: Arc<AtomicUsize>,
    pub block_upload_started_digests: Arc<Mutex<Vec<Digest>>>,
    pub block_upload_completed_digests: Arc<Mutex<Vec<Digest>>>,
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
            block_upload_seen: Arc::new(AtomicBool::new(false)),
            block_upload_started: Arc::new(AtomicUsize::new(0)),
            block_upload_completed: Arc::new(AtomicUsize::new(0)),
            block_upload_max_inflight: Arc::new(AtomicUsize::new(0)),
            block_upload_started_digests: Arc::new(Mutex::new(Vec::new())),
            block_upload_completed_digests: Arc::new(Mutex::new(Vec::new())),
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
        self.notarization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
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

        self.block_upload_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
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

/// Entry stored in the durable finalization queue.
pub struct FinalizedEntry {
    pub view: u64,
    pub digest: Digest,
}

impl FixedSize for FinalizedEntry {
    const SIZE: usize = u64::SIZE + Digest::SIZE;
}

impl commonware_codec::Write for FinalizedEntry {
    fn write(&self, buf: &mut impl BufMut) {
        self.view.write(buf);
        self.digest.write(buf);
    }
}

impl commonware_codec::Read for FinalizedEntry {
    type Cfg = ();
    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        let view = u64::read_cfg(buf, &())?;
        let digest = Digest::read_cfg(buf, &())?;
        Ok(Self { view, digest })
    }
}

/// Tracks raw block uploads and the oldest finalized view that still needs them.
pub(crate) struct UploadTracker {
    uploaded: PrioritySet<Digest, u64>,
    pending_finalized: BTreeMap<u64, u64>,
    latest_finalized: Option<u64>,
}

impl UploadTracker {
    pub(crate) fn new() -> Self {
        Self {
            uploaded: PrioritySet::new(),
            pending_finalized: BTreeMap::new(),
            latest_finalized: None,
        }
    }

    pub(crate) fn contains(&self, digest: &Digest) -> bool {
        self.uploaded.contains(digest)
    }

    pub(crate) fn mark_uploaded(&mut self, digest: Digest, view: u64) {
        self.uploaded.put(digest, view);
        self.prune_uploaded();
    }

    pub(crate) fn observe_finalization(&mut self, view: u64) {
        self.latest_finalized = Some(
            self.latest_finalized
                .map_or(view, |latest| latest.max(view)),
        );
        self.prune_uploaded();
    }

    pub(crate) fn queue_finalized(&mut self, position: u64, view: u64) {
        if let Some(previous) = self.pending_finalized.insert(position, view) {
            assert_eq!(previous, view, "pending finalized view changed");
        }
    }

    pub(crate) fn finish_finalized(&mut self, position: u64) {
        self.pending_finalized
            .remove(&position)
            .expect("missing pending finalized view");
        self.prune_uploaded();
    }

    fn prune_uploaded(&mut self) {
        let prune_before = match (
            self.pending_finalized
                .first_key_value()
                .map(|(_, &view)| view),
            self.latest_finalized,
        ) {
            (Some(pending), Some(latest)) => pending.min(latest),
            (Some(pending), None) => pending,
            (None, Some(latest)) => latest,
            (None, None) => return,
        };

        // Keep uploaded digests at or above the oldest finalized view that is
        // still queued (or, when the queue is empty, the latest finalized view
        // we have observed on the live path).
        while let Some((_, &view)) = self.uploaded.peek() {
            if view >= prune_before {
                break;
            }
            self.uploaded.pop();
        }
    }
}

pub(crate) type SharedUploadTracker = Arc<Mutex<UploadTracker>>;

struct DrainCompletion {
    position: u64,
    view: u64,
    digest: Option<Digest>,
}

/// An implementation of [Indexer] for the [Reporter] trait.
#[derive(Clone)]
pub(crate) struct Pusher<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    metrics: DrainerMetrics,
    uploaded: SharedUploadTracker,
    writer: queue::Writer<E, FinalizedEntry>,
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> Pusher<E, I> {
    /// Create a new [Pusher].
    pub(crate) fn new(
        context: E,
        indexer: I,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        initial_depth: u64,
        initial_ack_floor: u64,
        uploaded: SharedUploadTracker,
        writer: queue::Writer<E, FinalizedEntry>,
    ) -> Self {
        let metrics = DrainerMetrics::new(&context.with_label("queue"));
        metrics.depth.set(initial_depth as i64);
        metrics.ack_floor.set(initial_ack_floor as i64);
        metrics.in_flight.set(0);
        Self {
            context,
            indexer,
            marshal,
            metrics,
            uploaded,
            writer,
        }
    }

    /// Start the drainer loop that reads from the queue and uploads blocks.
    pub(crate) fn start_drainer(self, mut reader: queue::Reader<E, FinalizedEntry>) -> Handle<()> {
        let Self {
            context,
            indexer,
            marshal,
            metrics,
            uploaded,
            writer,
        } = self;
        context
            .with_label("drainer")
            .spawn(move |context| async move {
                let mut uploads: Pool<DrainCompletion> = Pool::default();
                let mut queue_closed = false;

                loop {
                    Self::fill_drainer_slots(
                        &context,
                        &indexer,
                        &marshal,
                        &metrics,
                        &uploaded,
                        &writer,
                        &mut reader,
                        &mut uploads,
                    )
                    .await;

                    if queue_closed {
                        if uploads.is_empty() {
                            warn!("drainer queue closed");
                            return;
                        }
                        let completion = uploads.next_completed().await;
                        Self::complete_drained(&metrics, &uploaded, &writer, &reader, completion)
                            .await;
                        continue;
                    }

                    if uploads.is_empty() {
                        let item = reader
                            .recv()
                            .await
                            .expect("failed to recv from finalized queue");
                        let Some((position, entry)) = item else {
                            queue_closed = true;
                            continue;
                        };
                        Self::start_drained_upload(
                            &context,
                            &indexer,
                            &marshal,
                            &metrics,
                            &uploaded,
                            &writer,
                            &reader,
                            &mut uploads,
                            position,
                            entry,
                        )
                        .await;
                        continue;
                    }

                    let wait_for_item = uploads.len() < DRAINER_MAX_IN_FLIGHT;
                    let item = OptionFuture::from(wait_for_item.then(|| reader.recv()));

                    select! {
                        completion = uploads.next_completed() => {
                            Self::complete_drained(&metrics, &uploaded, &writer, &reader, completion).await;
                        },
                        item = item => {
                            match item.expect("failed to recv from finalized queue") {
                                Some((position, entry)) => {
                                    Self::start_drained_upload(
                                        &context,
                                        &indexer,
                                        &marshal,
                                        &metrics,
                                        &uploaded,
                                        &writer,
                                        &reader,
                                        &mut uploads,
                                        position,
                                        entry,
                                    )
                                    .await;
                                }
                                None => {
                                    queue_closed = true;
                                }
                            }
                        }
                    }
                }
            })
    }

    async fn fill_drainer_slots(
        context: &E,
        indexer: &I,
        marshal: &MarshalMailbox<Scheme, Standard<Block>>,
        metrics: &DrainerMetrics,
        uploaded: &SharedUploadTracker,
        writer: &queue::Writer<E, FinalizedEntry>,
        reader: &mut queue::Reader<E, FinalizedEntry>,
        uploads: &mut Pool<DrainCompletion>,
    ) {
        let mut slots = DRAINER_MAX_IN_FLIGHT.saturating_sub(uploads.len());
        if slots == 0 {
            return;
        }

        while slots > 0 {
            let item = reader
                .try_recv()
                .await
                .expect("failed to recv from finalized queue");
            let Some((position, entry)) = item else {
                break;
            };

            Self::start_drained_upload(
                context, indexer, marshal, metrics, uploaded, writer, reader, uploads, position,
                entry,
            )
            .await;

            slots = DRAINER_MAX_IN_FLIGHT.saturating_sub(uploads.len());
        }
    }

    async fn start_drained_upload(
        context: &E,
        indexer: &I,
        marshal: &MarshalMailbox<Scheme, Standard<Block>>,
        metrics: &DrainerMetrics,
        uploaded: &SharedUploadTracker,
        writer: &queue::Writer<E, FinalizedEntry>,
        reader: &queue::Reader<E, FinalizedEntry>,
        uploads: &mut Pool<DrainCompletion>,
        position: u64,
        entry: FinalizedEntry,
    ) {
        let FinalizedEntry { view, digest } = entry;
        uploaded.lock().queue_finalized(position, view);

        // Skip queue entries that already succeeded through a live
        // notarization/finalization upload path.
        let already_uploaded = uploaded.lock().contains(&digest);
        if already_uploaded {
            // Persist the ack before moving on so a restart does not resurrect
            // work we already know completed.
            Self::complete_drained(
                metrics,
                uploaded,
                writer,
                reader,
                DrainCompletion {
                    position,
                    view,
                    digest: None,
                },
            )
            .await;
            debug!(?digest, "drainer skipping already-uploaded block");
            return;
        }

        metrics.in_flight.inc();
        uploads.push({
            let indexer = indexer.clone();
            let marshal = marshal.clone();
            let context = context.with_label("upload");
            let metrics = metrics.clone();
            async move {
                // The queue only stores the digest, so rehydrate the full block
                // from marshal before attempting the upload.
                let block = loop {
                    if let Some(block) = marshal.get_block(Identifier::Digest(digest)).await {
                        break block;
                    }
                    warn!(?digest, "drainer could not find block in marshal, retrying");
                    context.sleep(std::time::Duration::from_secs(1)).await;
                };

                // Upload block, retrying until success.
                loop {
                    match indexer.block_upload(block.clone()).await {
                        Ok(()) => {
                            metrics.uploads.inc(status::Status::Success);
                            debug!(?digest, "drainer uploaded block");
                            return DrainCompletion {
                                position,
                                view,
                                digest: Some(digest),
                            };
                        }
                        Err(e) => {
                            metrics.uploads.inc(status::Status::Failure);
                            warn!(?e, ?digest, "drainer failed to upload block, retrying");
                            context.sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        });
    }

    async fn complete_drained(
        metrics: &DrainerMetrics,
        uploaded: &SharedUploadTracker,
        writer: &queue::Writer<E, FinalizedEntry>,
        reader: &queue::Reader<E, FinalizedEntry>,
        completion: DrainCompletion,
    ) {
        if let Some(digest) = completion.digest {
            // Record the success before acking so the in-memory dedupe tracker
            // stays aligned with the durable queue state.
            uploaded.lock().mark_uploaded(digest, completion.view);
            metrics.in_flight.dec();
        }

        reader
            .ack(completion.position)
            .await
            .expect("failed to ack");
        writer.sync().await.expect("failed to sync after ack");
        metrics.depth.dec();
        metrics.ack_floor.set(reader.ack_floor().await as i64);
        uploaded.lock().finish_finalized(completion.position);
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
                let view_raw = view.get();
                self.context.with_label("notarized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    let uploaded = self.uploaded.clone();
                    move |_| async move {
                        // Wait for block
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

                        // Upload to indexer once we have it
                        let notarized = Notarized::new(notarization, block);
                        let result = indexer.notarized_upload(notarized).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload notarization");
                            return;
                        }

                        uploaded.lock().mark_uploaded(digest, view_raw);
                        debug!(%view, "notarization uploaded to indexer");
                    }
                });
            }
            Activity::Finalization(finalization) => {
                let digest = finalization.proposal.payload;
                let view = finalization.view();
                let view_raw = view.get();

                let already_uploaded = {
                    let mut uploaded = self.uploaded.lock();
                    uploaded.observe_finalization(view_raw);
                    uploaded.contains(&digest)
                };

                // Persist the finalized digest before spawning any uploads so
                // the drainer can recover the block after a crash or restart.
                if !already_uploaded {
                    let position = self
                        .writer
                        .enqueue(FinalizedEntry {
                            view: view_raw,
                            digest,
                        })
                        .await
                        .expect("failed to enqueue finalized digest");
                    self.metrics.enqueued.inc();
                    self.metrics.depth.inc();
                    self.uploaded.lock().queue_finalized(position, view_raw);
                    self.writer
                        .sync()
                        .await
                        .expect("failed to sync after enqueue");
                }

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

                // Upload block to indexer (once we have it)
                self.context.with_label("finalized_block").spawn({
                    let indexer = self.indexer.clone();
                    let marshal = self.marshal.clone();
                    let uploaded = self.uploaded.clone();
                    move |_| async move {
                        // Wait for block
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

                        // Upload to indexer once we have it
                        let finalization = Finalized::new(finalization, block);
                        let result = indexer.finalized_upload(finalization).await;
                        if let Err(e) = result {
                            warn!(?e, "failed to upload finalization");
                            return;
                        }

                        uploaded.lock().mark_uploaded(digest, view_raw);
                        debug!(%view, "finalization uploaded to indexer");
                    }
                });
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_cryptography::{Hasher, Sha256};

    #[test]
    fn test_upload_tracker_prunes_only_after_oldest_pending_completion() {
        let mut tracker = UploadTracker::new();

        let digest_10 = Sha256::hash(b"view-10");
        let digest_11 = Sha256::hash(b"view-11");
        let digest_12 = Sha256::hash(b"view-12");

        for (position, view) in [(3, 10), (4, 11), (5, 12)] {
            tracker.observe_finalization(view);
            tracker.queue_finalized(position, view);
        }

        // Later views may complete first, but they must remain in the dedupe set
        // until the oldest queued view has been retired.
        tracker.mark_uploaded(digest_11, 11);
        tracker.finish_finalized(4);
        tracker.mark_uploaded(digest_12, 12);
        tracker.finish_finalized(5);

        assert!(tracker.contains(&digest_11));
        assert!(tracker.contains(&digest_12));

        tracker.mark_uploaded(digest_10, 10);
        tracker.finish_finalized(3);

        assert!(!tracker.contains(&digest_10));
        assert!(!tracker.contains(&digest_11));
        assert!(tracker.contains(&digest_12));
    }
}
