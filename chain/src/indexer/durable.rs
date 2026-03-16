use super::Indexer;
use alto_types::{Block, Scheme};
use bytes::{Buf, BufMut};
use commonware_codec::{self, FixedSize};
use commonware_consensus::marshal::{core::Mailbox as MarshalMailbox, standard::Standard, Identifier};
use commonware_cryptography::sha256::Digest;
use commonware_macros::select;
use commonware_runtime::{
    telemetry::metrics::status::{self, CounterExt},
    Clock, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::queue;
use commonware_utils::{
    futures::{OptionFuture, Pool},
    sync::Mutex,
    PrioritySet,
};
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tracing::{debug, warn};

const DRAINER_MAX_IN_FLIGHT: usize = 16;
const DRAINER_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct DrainerMetrics {
    pub(crate) depth: Gauge,
    pub(crate) enqueued: Counter,
    pub(crate) uploads: status::Counter,
    pub(crate) ack_floor: Gauge,
    pub(crate) in_flight: Gauge,
}

impl DrainerMetrics {
    pub(crate) fn new<E: Metrics>(context: &E) -> Self {
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

/// Entry stored in the durable finalization queue.
#[derive(Clone, Copy)]
pub struct FinalizedEntry {
    pub height: u64,
    pub digest: Digest,
}

impl FixedSize for FinalizedEntry {
    const SIZE: usize = u64::SIZE + Digest::SIZE;
}

impl commonware_codec::Write for FinalizedEntry {
    fn write(&self, buf: &mut impl BufMut) {
        self.height.write(buf);
        self.digest.write(buf);
    }
}

impl commonware_codec::Read for FinalizedEntry {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        let height = u64::read_cfg(buf, &())?;
        let digest = Digest::read_cfg(buf, &())?;
        Ok(Self { height, digest })
    }
}

#[derive(Clone, Copy)]
struct PendingFinalized {
    height: u64,
    digest: Digest,
}

impl From<FinalizedEntry> for PendingFinalized {
    fn from(entry: FinalizedEntry) -> Self {
        Self {
            height: entry.height,
            digest: entry.digest,
        }
    }
}

enum QueueFinalized {
    KnownPosition,
    DuplicateDigest,
    NewPosition,
}

/// Tracks raw block uploads and the oldest finalized height that still needs them.
///
/// A digest can be in one of three states:
/// - not seen yet: a finalized block should enqueue a new durable row;
/// - pending: a durable row already exists (and may currently be draining), so
///   duplicate finalize notifications must not enqueue another row;
/// - uploaded: the block was already uploaded successfully, so further finalize
///   notifications can be ignored.
pub(crate) struct UploadTracker {
    uploaded: PrioritySet<Digest, u64>,
    pending_finalized: BTreeMap<u64, PendingFinalized>,
    pending_digests: BTreeMap<Digest, usize>,
    latest_finalized: Option<u64>,
}

impl UploadTracker {
    pub(crate) fn new() -> Self {
        Self {
            uploaded: PrioritySet::new(),
            pending_finalized: BTreeMap::new(),
            pending_digests: BTreeMap::new(),
            latest_finalized: None,
        }
    }

    pub(crate) fn contains(&self, digest: &Digest) -> bool {
        self.uploaded.contains(digest)
    }

    pub(crate) fn needs_enqueue(&mut self, digest: &Digest, height: u64) -> bool {
        self.observe_finalization(height);
        // A pending digest already has a durable queue row backing retries and
        // crash recovery, so a duplicate finalize notification must not enqueue
        // another row.
        !(self.contains(digest) || self.pending_digests.contains_key(digest))
    }

    pub(crate) fn mark_uploaded(&mut self, digest: Digest, height: u64) {
        self.uploaded.put(digest, height);
        self.prune_uploaded();
    }

    pub(crate) fn observe_finalization(&mut self, height: u64) {
        self.latest_finalized = Some(
            self.latest_finalized
                .map_or(height, |latest| latest.max(height)),
        );
        self.prune_uploaded();
    }

    fn queue_finalized(&mut self, position: u64, entry: FinalizedEntry) -> QueueFinalized {
        if let Some(previous) = self.pending_finalized.get(&position) {
            assert_eq!(previous.height, entry.height, "pending finalized height changed");
            assert_eq!(previous.digest, entry.digest, "pending finalized digest changed");
            return QueueFinalized::KnownPosition;
        }
        let already_pending = self.pending_digests.contains_key(&entry.digest);
        self.pending_finalized.insert(position, entry.into());
        *self.pending_digests.entry(entry.digest).or_default() += 1;
        if already_pending {
            QueueFinalized::DuplicateDigest
        } else {
            QueueFinalized::NewPosition
        }
    }

    pub(crate) fn finish_finalized(&mut self, position: u64) {
        let pending = self
            .pending_finalized
            .remove(&position)
            .expect("missing pending finalized height");
        let clear_digest = {
            let count = self
                .pending_digests
                .get_mut(&pending.digest)
                .expect("missing pending finalized digest");
            *count = count.saturating_sub(1);
            *count == 0
        };
        if clear_digest {
            self.pending_digests.remove(&pending.digest);
        }
        self.prune_uploaded();
    }

    fn prune_uploaded(&mut self) {
        let prune_before = match (
            self.pending_finalized
                .first_key_value()
                .map(|(_, pending)| pending.height),
            self.latest_finalized,
        ) {
            (Some(pending), Some(latest)) => pending.min(latest),
            (Some(pending), None) => pending,
            (None, Some(latest)) => latest,
            (None, None) => return,
        };

        while let Some((_, &height)) = self.uploaded.peek() {
            if height >= prune_before {
                break;
            }
            self.uploaded.pop();
        }
    }
}

pub(crate) type SharedUploadTracker = Arc<Mutex<UploadTracker>>;

/// Durably enqueues finalized block digests from the application's block stream.
#[derive(Clone)]
pub(crate) struct Enqueuer<E: Clock + Storage + Metrics> {
    pub(crate) uploaded: SharedUploadTracker,
    writer: queue::Writer<E, FinalizedEntry>,
    metrics: DrainerMetrics,
}

impl<E: Clock + Storage + Metrics> Enqueuer<E> {
    pub(crate) fn new(
        uploaded: SharedUploadTracker,
        writer: queue::Writer<E, FinalizedEntry>,
        metrics: DrainerMetrics,
    ) -> Self {
        Self {
            uploaded,
            writer,
            metrics,
        }
    }

    pub(crate) async fn enqueue_if_needed(&self, digest: Digest, height: u64) {
        if !self.uploaded.lock().needs_enqueue(&digest, height) {
            return;
        }

        // Persist exactly one queue row per digest while it is pending. The
        // drainer retries from this row until it either uploads successfully or
        // observes that the live certificate path already uploaded the block.
        let entry = FinalizedEntry { height, digest };
        let position = self
            .writer
            .enqueue(entry)
            .await
            .expect("failed to enqueue finalized digest");
        self.metrics.enqueued.inc();
        self.metrics.depth.inc();
        let _ = self.uploaded.lock().queue_finalized(position, entry);
        self.writer
            .sync()
            .await
            .expect("failed to sync after enqueue");
    }
}

struct DrainCompletion {
    position: u64,
    height: u64,
    digest: Option<Digest>,
    counted_in_flight: bool,
}

struct DrainerShared<'a, E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    context: &'a E,
    indexer: &'a I,
    marshal: &'a MarshalMailbox<Scheme, Standard<Block>>,
    metrics: &'a DrainerMetrics,
    uploaded: &'a SharedUploadTracker,
    writer: &'a queue::Writer<E, FinalizedEntry>,
}

#[derive(Clone)]
pub(crate) struct Drainer<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    metrics: DrainerMetrics,
    uploaded: SharedUploadTracker,
    writer: queue::Writer<E, FinalizedEntry>,
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> Drainer<E, I> {
    pub(crate) fn new(
        context: E,
        indexer: I,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        metrics: DrainerMetrics,
        uploaded: SharedUploadTracker,
        writer: queue::Writer<E, FinalizedEntry>,
    ) -> Self {
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
    pub(crate) fn start(self, mut reader: queue::Reader<E, FinalizedEntry>) -> Handle<()> {
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
                let shared = DrainerShared {
                    context: &context,
                    indexer: &indexer,
                    marshal: &marshal,
                    metrics: &metrics,
                    uploaded: &uploaded,
                    writer: &writer,
                };

                loop {
                    Self::fill_drainer_slots(&shared, &mut reader, &mut uploads).await;

                    if queue_closed {
                        if uploads.is_empty() {
                            warn!("drainer queue closed");
                            return;
                        }
                        let completion = uploads.next_completed().await;
                        Self::complete_drained(
                            shared.metrics,
                            shared.uploaded,
                            shared.writer,
                            &reader,
                            completion,
                        )
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
                        Self::start_drained_upload(&shared, &reader, &mut uploads, position, entry)
                            .await;
                        continue;
                    }

                    let wait_for_item = uploads.len() < DRAINER_MAX_IN_FLIGHT;
                    let item = OptionFuture::from(wait_for_item.then(|| reader.recv()));

                    select! {
                        completion = uploads.next_completed() => {
                            Self::complete_drained(
                                shared.metrics,
                                shared.uploaded,
                                shared.writer,
                                &reader,
                                completion,
                            )
                            .await;
                        },
                        item = item => {
                            match item.expect("failed to recv from finalized queue") {
                                Some((position, entry)) => {
                                    Self::start_drained_upload(
                                        &shared,
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
        shared: &DrainerShared<'_, E, I>,
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

            Self::start_drained_upload(shared, reader, uploads, position, entry).await;

            slots = DRAINER_MAX_IN_FLIGHT.saturating_sub(uploads.len());
        }
    }

    async fn start_drained_upload(
        shared: &DrainerShared<'_, E, I>,
        reader: &queue::Reader<E, FinalizedEntry>,
        uploads: &mut Pool<DrainCompletion>,
        position: u64,
        entry: FinalizedEntry,
    ) {
        let FinalizedEntry { height, digest } = entry;
        let queued = shared.uploaded.lock().queue_finalized(position, entry);
        if matches!(queued, QueueFinalized::DuplicateDigest) {
            // Another durable row for this digest is already pending. Keep that
            // original row as the single retry/recovery source of truth and
            // retire this duplicate entry.
            Self::complete_drained(
                shared.metrics,
                shared.uploaded,
                shared.writer,
                reader,
                DrainCompletion {
                    position,
                    height,
                    digest: None,
                    counted_in_flight: false,
                },
            )
            .await;
            debug!(?digest, "drainer skipping duplicate queued block");
            return;
        }

        // Skip queue entries that already succeeded through a live
        // notarization/finalization upload path.
        let already_uploaded = shared.uploaded.lock().contains(&digest);
        if already_uploaded {
            Self::complete_drained(
                shared.metrics,
                shared.uploaded,
                shared.writer,
                reader,
                DrainCompletion {
                    position,
                    height,
                    digest: None,
                    counted_in_flight: false,
                },
            )
            .await;
            debug!(?digest, "drainer skipping already-uploaded block");
            return;
        }

        shared.metrics.in_flight.inc();
        uploads.push({
            let indexer = (*shared.indexer).clone();
            let marshal = (*shared.marshal).clone();
            let context = shared.context.with_label("upload");
            let metrics = (*shared.metrics).clone();
            let uploaded = shared.uploaded.clone();
            async move {
                let block = loop {
                    if uploaded.lock().contains(&digest) {
                        debug!(?digest, "drainer observed live upload before fetching block");
                        return DrainCompletion {
                            position,
                            height,
                            digest: None,
                            counted_in_flight: true,
                        };
                    }
                    if let Some(block) = marshal.get_block(Identifier::Digest(digest)).await {
                        break block;
                    }
                    warn!(?digest, "drainer could not find block in marshal, retrying");
                    context.sleep(DRAINER_RETRY_DELAY).await;
                };

                loop {
                    // A live notarization/finalization upload may complete while this
                    // queue item is waiting for its block or retrying after failures.
                    if uploaded.lock().contains(&digest) {
                        debug!(?digest, "drainer observed live upload before raw upload");
                        return DrainCompletion {
                            position,
                            height,
                            digest: None,
                            counted_in_flight: true,
                        };
                    }

                    match indexer.block_upload(block.clone()).await {
                        Ok(()) => {
                            metrics.uploads.inc(status::Status::Success);
                            debug!(?digest, "drainer uploaded block");
                            return DrainCompletion {
                                position,
                                height,
                                digest: Some(digest),
                                counted_in_flight: true,
                            };
                        }
                        Err(e) => {
                            // Keep retrying from the original durable row. We do
                            // not ack the row until success or until the live
                            // certificate path proves the block was uploaded.
                            metrics.uploads.inc(status::Status::Failure);
                            warn!(?e, ?digest, "drainer failed to upload block, retrying");
                            context.sleep(DRAINER_RETRY_DELAY).await;
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
        if completion.counted_in_flight {
            metrics.in_flight.dec();
        }
        if let Some(digest) = completion.digest {
            // Record the success before acking so the in-memory dedupe tracker
            // stays aligned with the durable queue state.
            uploaded.lock().mark_uploaded(digest, completion.height);
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

        for (position, entry) in [
            (
                3,
                FinalizedEntry {
                    height: 10,
                    digest: digest_10,
                },
            ),
            (
                4,
                FinalizedEntry {
                    height: 11,
                    digest: digest_11,
                },
            ),
            (
                5,
                FinalizedEntry {
                    height: 12,
                    digest: digest_12,
                },
            ),
        ] {
            tracker.observe_finalization(entry.height);
            tracker.queue_finalized(position, entry);
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

    #[test]
    fn test_upload_tracker_dedupes_pending_digests() {
        let mut tracker = UploadTracker::new();
        let digest = Sha256::hash(b"view-10");
        let entry = FinalizedEntry { height: 10, digest };

        assert!(tracker.needs_enqueue(&digest, 10));
        assert!(matches!(
            tracker.queue_finalized(3, entry),
            QueueFinalized::NewPosition
        ));
        assert!(!tracker.needs_enqueue(&digest, 10));
        assert!(matches!(
            tracker.queue_finalized(3, entry),
            QueueFinalized::KnownPosition
        ));
        assert!(matches!(
            tracker.queue_finalized(4, entry),
            QueueFinalized::DuplicateDigest
        ));

        tracker.finish_finalized(3);
        assert!(!tracker.needs_enqueue(&digest, 10));

        tracker.finish_finalized(4);
        assert!(tracker.needs_enqueue(&digest, 10));
    }
}
