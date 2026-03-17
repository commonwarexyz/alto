use super::Indexer;
use alto_types::{Block, Scheme};
use bytes::{Buf, BufMut};
use commonware_codec::{self, FixedSize, Read, Write};
use commonware_consensus::marshal::{
    core::Mailbox as MarshalMailbox, standard::Standard, Identifier,
};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_macros::select_loop;
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
use prometheus_client::metrics::gauge::Gauge;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tracing::{debug, warn};

const DRAINER_MAX_IN_FLIGHT: usize = 16;
const DRAINER_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct DrainerMetrics {
    pub(crate) depth: Gauge,
    pub(crate) uploads: status::Counter,
    pub(crate) in_flight: Gauge,
}

impl DrainerMetrics {
    pub(crate) fn new<E: Metrics>(context: &E) -> Self {
        let metrics = Self {
            depth: Gauge::default(),
            uploads: status::Counter::default(),
            in_flight: Gauge::default(),
        };

        context.register(
            "depth",
            "Current number of pending finalized block uploads in the durable queue",
            metrics.depth.clone(),
        );
        context.register(
            "uploads",
            "Total number of finalized block upload attempt outcomes by status",
            metrics.uploads.clone(),
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
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FinalizedEntry {
    pub height: u64,
    pub digest: Digest,
}

impl FixedSize for FinalizedEntry {
    const SIZE: usize = u64::SIZE + Digest::SIZE;
}

impl Write for FinalizedEntry {
    fn write(&self, buf: &mut impl BufMut) {
        self.height.write(buf);
        self.digest.write(buf);
    }
}

impl Read for FinalizedEntry {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        let height = u64::read_cfg(buf, &())?;
        let digest = Digest::read_cfg(buf, &())?;
        Ok(Self { height, digest })
    }
}

/// Tracks raw block uploads and the oldest finalized height that still needs them.
///
/// A digest can be in one of three states:
/// - not seen yet: a finalized block should enqueue a new durable row;
/// - pending: a durable row already exists (and may currently be draining), so
///   duplicate finalize notifications must not enqueue another row;
/// - uploaded: the block was already uploaded successfully, so further finalize
///   notifications can be ignored.
struct UploadTracker {
    uploaded: PrioritySet<Digest, u64>,
    pending_finalized: BTreeMap<u64, FinalizedEntry>,
    pending_digests: BTreeMap<Digest, usize>,
    latest_finalized: Option<u64>,
}

impl UploadTracker {
    fn new() -> Self {
        Self {
            uploaded: PrioritySet::new(),
            pending_finalized: BTreeMap::new(),
            pending_digests: BTreeMap::new(),
            latest_finalized: None,
        }
    }

    fn contains(&self, digest: &Digest) -> bool {
        self.uploaded.contains(digest)
    }

    fn needs_enqueue(&mut self, digest: &Digest, height: u64) -> bool {
        self.observe_finalization(height);
        // A pending digest already has a durable queue row backing retries and
        // crash recovery, so a duplicate finalize notification must not enqueue
        // another row.
        !(self.contains(digest) || self.pending_digests.contains_key(digest))
    }

    fn mark_uploaded(&mut self, digest: Digest, height: u64) {
        self.uploaded.put(digest, height);
        self.prune_uploaded();
    }

    fn observe_finalization(&mut self, height: u64) {
        self.latest_finalized = Some(
            self.latest_finalized
                .map_or(height, |latest| latest.max(height)),
        );
        self.prune_uploaded();
    }

    fn register_finalized(&mut self, position: u64, entry: FinalizedEntry) -> bool {
        if let Some(previous) = self.pending_finalized.get(&position) {
            assert_eq!(
                previous.height, entry.height,
                "pending finalized height changed"
            );
            assert_eq!(
                previous.digest, entry.digest,
                "pending finalized digest changed"
            );
            return false;
        }
        let duplicate_digest = self.pending_digests.contains_key(&entry.digest);
        self.pending_finalized.insert(position, entry);
        *self.pending_digests.entry(entry.digest).or_default() += 1;
        duplicate_digest
    }

    fn finish_finalized(&mut self, position: u64) {
        let pending = self
            .pending_finalized
            .remove(&position)
            .expect("missing pending finalized height");
        let count = self
            .pending_digests
            .get_mut(&pending.digest)
            .expect("missing pending finalized digest");
        *count -= 1;
        if *count == 0 {
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

pub(crate) struct UploadState {
    tracker: UploadTracker,
    cached_blocks: BTreeMap<Digest, Block>,
    certificate_uploads: BTreeMap<Digest, usize>,
}

impl UploadState {
    pub(crate) fn new() -> Self {
        Self {
            tracker: UploadTracker::new(),
            cached_blocks: BTreeMap::new(),
            certificate_uploads: BTreeMap::new(),
        }
    }

    pub(crate) fn contains(&self, digest: &Digest) -> bool {
        self.tracker.contains(digest)
    }

    pub(crate) fn prepare_enqueue(&mut self, block: &Block) -> Option<FinalizedEntry> {
        let entry = FinalizedEntry {
            height: block.height.get(),
            digest: block.digest(),
        };
        let needs_enqueue = self.tracker.needs_enqueue(&entry.digest, entry.height);
        if needs_enqueue || self.tracker.pending_digests.contains_key(&entry.digest) {
            self.cache_block(block.clone());
        }
        needs_enqueue.then_some(entry)
    }

    pub(crate) fn register_finalized(&mut self, position: u64, entry: FinalizedEntry) -> bool {
        self.tracker.register_finalized(position, entry)
    }

    pub(crate) fn finish_finalized(&mut self, position: u64) {
        self.tracker.finish_finalized(position);
    }

    pub(crate) fn mark_uploaded(&mut self, digest: Digest, height: u64) {
        self.cached_blocks.remove(&digest);
        self.tracker.mark_uploaded(digest, height);
    }

    pub(crate) fn cache_block(&mut self, block: Block) {
        self.cached_blocks.entry(block.digest()).or_insert(block);
    }

    pub(crate) fn cached_block(&self, digest: &Digest) -> Option<Block> {
        self.cached_blocks.get(digest).cloned()
    }

    pub(crate) fn start_certificate_upload(&mut self, digest: Digest) {
        *self.certificate_uploads.entry(digest).or_default() += 1;
    }

    pub(crate) fn finish_certificate_upload(&mut self, digest: &Digest) {
        let count = self
            .certificate_uploads
            .get_mut(digest)
            .expect("missing in-flight certificate upload");
        *count -= 1;
        if *count == 0 {
            self.certificate_uploads.remove(digest);
        }
    }

    pub(crate) fn certificate_upload_in_flight(&self, digest: &Digest) -> bool {
        self.certificate_uploads.contains_key(digest)
    }
}

pub(crate) type SharedUploadState = Arc<Mutex<UploadState>>;

/// Durably enqueues finalized block digests from the application's block stream.
#[derive(Clone)]
pub(crate) struct Enqueuer<E: Clock + Storage + Metrics> {
    uploads: SharedUploadState,
    writer: queue::Writer<E, FinalizedEntry>,
    metrics: DrainerMetrics,
}

impl<E: Clock + Storage + Metrics> Enqueuer<E> {
    pub(crate) fn new(
        uploads: SharedUploadState,
        writer: queue::Writer<E, FinalizedEntry>,
        metrics: DrainerMetrics,
    ) -> Self {
        Self {
            uploads,
            writer,
            metrics,
        }
    }

    pub(crate) async fn enqueue_if_needed(&self, block: &Block) {
        let Some(entry) = self.uploads.lock().prepare_enqueue(block) else {
            return;
        };

        // Persist exactly one queue row per digest while it is pending. The
        // drainer retries from this row until it either uploads successfully or
        // observes that the live certificate path already uploaded the block.
        let position = self
            .writer
            .enqueue(entry)
            .await
            .expect("failed to enqueue finalized digest");
        self.metrics.depth.inc();
        let _ = self.uploads.lock().register_finalized(position, entry);
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
    uploads: &'a SharedUploadState,
    writer: &'a queue::Writer<E, FinalizedEntry>,
}

#[derive(Clone)]
pub(crate) struct Drainer<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    metrics: DrainerMetrics,
    uploads: SharedUploadState,
    writer: queue::Writer<E, FinalizedEntry>,
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> Drainer<E, I> {
    pub(crate) fn new(
        context: E,
        indexer: I,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        metrics: DrainerMetrics,
        uploads: SharedUploadState,
        writer: queue::Writer<E, FinalizedEntry>,
    ) -> Self {
        Self {
            context,
            indexer,
            marshal,
            metrics,
            uploads,
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
            uploads: shared_uploads,
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
                    uploads: &shared_uploads,
                    writer: &writer,
                };

                select_loop! {
                    context,
                    on_start => {
                        Self::fill_drainer_slots(&shared, &mut reader, &mut uploads).await;

                        if queue_closed && uploads.is_empty() {
                            warn!("drainer queue closed");
                            break;
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

                        let item = OptionFuture::from(
                            (!queue_closed && uploads.len() < DRAINER_MAX_IN_FLIGHT)
                                .then(|| reader.recv()),
                        );
                    },
                    on_stopped => {},
                    completion = uploads.next_completed() => {
                        Self::complete_drained(
                            shared.metrics,
                            shared.uploads,
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
                    },
                }
            })
    }

    async fn fill_drainer_slots(
        shared: &DrainerShared<'_, E, I>,
        reader: &mut queue::Reader<E, FinalizedEntry>,
        uploads: &mut Pool<DrainCompletion>,
    ) {
        if uploads.len() >= DRAINER_MAX_IN_FLIGHT {
            return;
        }

        while uploads.len() < DRAINER_MAX_IN_FLIGHT {
            let item = reader
                .try_recv()
                .await
                .expect("failed to recv from finalized queue");
            let Some((position, entry)) = item else {
                break;
            };

            Self::start_drained_upload(shared, reader, uploads, position, entry).await;
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
        if shared.uploads.lock().register_finalized(position, entry) {
            // Another durable row for this digest is already pending. Keep that
            // original row as the single retry/recovery source of truth and
            // retire this duplicate entry.
            Self::complete_drained(
                shared.metrics,
                shared.uploads,
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
        let already_uploaded = shared.uploads.lock().contains(&digest);
        if already_uploaded {
            Self::complete_drained(
                shared.metrics,
                shared.uploads,
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

        uploads.push({
            let indexer = (*shared.indexer).clone();
            let marshal = (*shared.marshal).clone();
            let context = shared.context.with_label("upload");
            let metrics = (*shared.metrics).clone();
            let uploads = shared.uploads.clone();
            async move {
                let Some(block) =
                    Self::wait_for_uploadable_block(&context, &marshal, &uploads, digest).await
                else {
                    debug!(?digest, "drainer observed live upload before raw upload");
                    return DrainCompletion {
                        position,
                        height,
                        digest: None,
                        counted_in_flight: false,
                    };
                };
                metrics.in_flight.inc();

                loop {
                    // A live notarization/finalization upload may complete while this
                    // queue item is waiting for its block or retrying after failures.
                    let wait_for_certificate = {
                        let uploads = uploads.lock();
                        if uploads.contains(&digest) {
                            debug!(?digest, "drainer observed live upload before raw upload");
                            return DrainCompletion {
                                position,
                                height,
                                digest: None,
                                counted_in_flight: true,
                            };
                        }
                        uploads.certificate_upload_in_flight(&digest)
                    };
                    if wait_for_certificate {
                        context.sleep(DRAINER_RETRY_DELAY).await;
                        continue;
                    }

                    if uploads.lock().contains(&digest) {
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

    async fn wait_for_uploadable_block(
        context: &E,
        marshal: &MarshalMailbox<Scheme, Standard<Block>>,
        uploads: &SharedUploadState,
        digest: Digest,
    ) -> Option<Block> {
        // Prefer the in-process block cache populated by the application and
        // certificate uploaders. On restart that cache is empty, so we fall
        // back to marshal storage. If a certificate upload is still in flight,
        // wait for it to either succeed or fail before starting the raw upload.
        enum NextBlock {
            AlreadyUploaded,
            WaitForCertificate,
            Ready(Box<Block>),
            FetchFromMarshal,
        }

        loop {
            let next = {
                let uploads = uploads.lock();
                if uploads.contains(&digest) {
                    NextBlock::AlreadyUploaded
                } else if uploads.certificate_upload_in_flight(&digest) {
                    NextBlock::WaitForCertificate
                } else if let Some(block) = uploads.cached_block(&digest) {
                    NextBlock::Ready(Box::new(block))
                } else {
                    NextBlock::FetchFromMarshal
                }
            };

            match next {
                NextBlock::AlreadyUploaded => return None,
                NextBlock::WaitForCertificate => {
                    context.sleep(DRAINER_RETRY_DELAY).await;
                }
                NextBlock::Ready(block) => return Some(*block),
                NextBlock::FetchFromMarshal => {
                    if let Some(block) = marshal.get_block(Identifier::Digest(digest)).await {
                        uploads.lock().cache_block(block.clone());
                        return Some(block);
                    }
                    warn!(?digest, "drainer could not find block in marshal, retrying");
                    context.sleep(DRAINER_RETRY_DELAY).await;
                }
            }
        }
    }

    async fn complete_drained(
        metrics: &DrainerMetrics,
        uploads: &SharedUploadState,
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
            uploads.lock().mark_uploaded(digest, completion.height);
        }

        reader
            .ack(completion.position)
            .await
            .expect("failed to ack");
        writer.sync().await.expect("failed to sync after ack");
        metrics.depth.dec();
        uploads.lock().finish_finalized(completion.position);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{Context, EPOCH};
    use commonware_consensus::types::{Height, Round, View};
    use commonware_cryptography::{ed25519, Digestible, Hasher, Sha256, Signer};

    fn test_block(view: u64, height: u64, label: &[u8]) -> Block {
        Block::new(
            Context {
                round: Round::new(EPOCH, View::new(view)),
                leader: ed25519::PrivateKey::from_seed(view).public_key(),
                parent: (
                    View::new(view.saturating_sub(1)),
                    Sha256::hash(format!("parent-{view}").as_bytes()),
                ),
            },
            Sha256::hash(label),
            Height::new(height),
            height,
        )
    }

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
            tracker.register_finalized(position, entry);
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
        assert!(!tracker.register_finalized(3, entry));
        assert!(!tracker.needs_enqueue(&digest, 10));
        assert!(!tracker.register_finalized(3, entry));
        assert!(tracker.register_finalized(4, entry));

        tracker.finish_finalized(3);
        assert!(!tracker.needs_enqueue(&digest, 10));

        tracker.finish_finalized(4);
        assert!(tracker.needs_enqueue(&digest, 10));
    }

    #[test]
    fn test_upload_state_does_not_recache_already_uploaded_blocks() {
        let mut uploads = UploadState::new();
        let block = test_block(7, 7, b"view-7");
        let digest = block.digest();

        assert!(uploads.prepare_enqueue(&block).is_some());
        uploads.mark_uploaded(digest, block.height.get());
        assert!(uploads.cached_block(&digest).is_none());
        assert!(uploads.prepare_enqueue(&block).is_none());
        assert!(uploads.cached_block(&digest).is_none());
    }
}
