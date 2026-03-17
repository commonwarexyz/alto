use alto_types::Block;
use bytes::{Buf, BufMut};
use commonware_codec::{self, FixedSize, Read, Write};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_runtime::{telemetry::metrics::status, Clock, Metrics, Storage};
use commonware_storage::queue;
use commonware_utils::{sync::Mutex, PrioritySet};
use prometheus_client::metrics::gauge::Gauge;
use std::{collections::BTreeMap, sync::Arc};

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
            "Current number of occupied upload slots in the durable drainer",
            metrics.in_flight.clone(),
        );

        metrics
    }
}

/// What the durable drainer should do next for a digest.
pub(crate) enum RawUploadDecision {
    /// The block is already uploaded, so the durable row can be retired.
    Retire,
    /// The live certificate path is still handling this digest, so the drainer
    /// should wait instead of racing it.
    Wait,
    /// The drainer should proceed with a raw upload attempt.
    Proceed,
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
pub(crate) struct UploadState {
    // Successfully uploaded digests stay in the dedupe set until the oldest
    // pending finalized height advances past them.
    uploaded: PrioritySet<Digest, u64>,
    // Pending durable queue rows keyed by queue position so replay and acking
    // follow the queue's actual cursor model.
    pending_finalized: BTreeMap<u64, FinalizedEntry>,
    // Counts pending queue rows per digest to suppress duplicate enqueues while
    // the durable row is still the retry source of truth.
    pending_digests: BTreeMap<Digest, usize>,
    // Highest finalized height observed from the live application stream.
    latest_finalized: Option<u64>,
    // Blocks cached for the live certificate upload path and the raw drainer.
    cached_blocks: BTreeMap<Digest, Block>,
    // Number of in-flight certificate uploads per digest so the drainer can
    // wait for the live path instead of racing it.
    certificate_uploads: BTreeMap<Digest, usize>,
}

impl UploadState {
    pub(crate) fn new() -> Self {
        Self {
            uploaded: PrioritySet::new(),
            pending_finalized: BTreeMap::new(),
            pending_digests: BTreeMap::new(),
            latest_finalized: None,
            cached_blocks: BTreeMap::new(),
            certificate_uploads: BTreeMap::new(),
        }
    }

    fn contains(&self, digest: &Digest) -> bool {
        self.uploaded.contains(digest)
    }

    pub(crate) fn prepare_enqueue(&mut self, block: &Block) -> Option<FinalizedEntry> {
        let entry = FinalizedEntry {
            height: block.height.get(),
            digest: block.digest(),
        };
        let needs_enqueue = self.needs_enqueue(&entry.digest, entry.height);
        if needs_enqueue || self.pending_digests.contains_key(&entry.digest) {
            self.cache_block(block.clone());
        }
        needs_enqueue.then_some(entry)
    }

    pub(crate) fn register_finalized(&mut self, position: u64, entry: FinalizedEntry) -> bool {
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

    pub(crate) fn finish_finalized(&mut self, position: u64) {
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
        self.prune();
    }

    pub(crate) fn mark_uploaded(&mut self, digest: Digest, height: u64) {
        self.cached_blocks.remove(&digest);
        self.uploaded.put(digest, height);
        self.prune();
    }

    pub(crate) fn cache_block(&mut self, block: Block) {
        self.cached_blocks.entry(block.digest()).or_insert(block);
    }

    pub(crate) fn cached_block(&self, digest: &Digest) -> Option<Block> {
        self.cached_blocks.get(digest).cloned()
    }

    pub(crate) fn raw_upload_decision(&self, digest: &Digest) -> RawUploadDecision {
        if self.contains(digest) {
            RawUploadDecision::Retire
        } else if self.certificate_uploads.contains_key(digest) {
            RawUploadDecision::Wait
        } else {
            RawUploadDecision::Proceed
        }
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
        self.prune();
    }

    fn needs_enqueue(&mut self, digest: &Digest, height: u64) -> bool {
        self.observe_finalization(height);
        // A pending digest already has a durable queue row backing retries and
        // crash recovery, so a duplicate finalize notification must not enqueue
        // another row.
        !(self.contains(digest) || self.pending_digests.contains_key(digest))
    }

    fn observe_finalization(&mut self, height: u64) {
        self.latest_finalized = Some(
            self.latest_finalized
                .map_or(height, |latest| latest.max(height)),
        );
        self.prune();
    }

    fn prune(&mut self) {
        let uploaded_prune_before = match (
            self.pending_finalized
                .first_key_value()
                .map(|(_, pending)| pending.height),
            self.latest_finalized,
        ) {
            (Some(pending), Some(latest)) => Some(pending.min(latest)),
            (Some(pending), None) => Some(pending),
            (None, Some(latest)) => Some(latest),
            (None, None) => None,
        };

        if let Some(prune_before) = uploaded_prune_before {
            while let Some((_, &height)) = self.uploaded.peek() {
                if height >= prune_before {
                    break;
                }
                self.uploaded.pop();
            }
        }

        let mut cached_prune_before = self
            .pending_finalized
            .values()
            .map(|pending| pending.height)
            .min();
        for digest in self.certificate_uploads.keys() {
            let Some(block) = self.cached_blocks.get(digest) else {
                continue;
            };
            cached_prune_before = Some(cached_prune_before.map_or(block.height.get(), |current| {
                current.min(block.height.get())
            }));
        }
        if cached_prune_before.is_none() {
            cached_prune_before = self.latest_finalized;
        }

        if let Some(prune_before) = cached_prune_before {
            self.cached_blocks
                .retain(|_, block| block.height.get() >= prune_before);
        }
    }
}

/// State shared by the live certificate path and the durable raw-block path.
pub(crate) type SharedUploadState = Arc<Mutex<UploadState>>;

/// Records finalized block digests in the durable queue from the application's
/// block stream.
#[derive(Clone)]
pub(crate) struct Recorder<E: Clock + Storage + Metrics> {
    uploads: SharedUploadState,
    writer: queue::Writer<E, FinalizedEntry>,
    metrics: DrainerMetrics,
}

impl<E: Clock + Storage + Metrics> Recorder<E> {
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

    pub(crate) async fn record(&self, block: &Block) {
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
    fn test_upload_state_prunes_only_after_oldest_pending_completion() {
        let mut uploads = UploadState::new();

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
            uploads.observe_finalization(entry.height);
            uploads.register_finalized(position, entry);
        }

        // Later views may complete first, but they must remain in the dedupe set
        // until the oldest queued view has been retired.
        uploads.mark_uploaded(digest_11, 11);
        uploads.finish_finalized(4);
        uploads.mark_uploaded(digest_12, 12);
        uploads.finish_finalized(5);

        assert!(uploads.contains(&digest_11));
        assert!(uploads.contains(&digest_12));

        uploads.mark_uploaded(digest_10, 10);
        uploads.finish_finalized(3);

        assert!(!uploads.contains(&digest_10));
        assert!(!uploads.contains(&digest_11));
        assert!(uploads.contains(&digest_12));
    }

    #[test]
    fn test_upload_state_dedupes_pending_digests() {
        let mut uploads = UploadState::new();
        let digest = Sha256::hash(b"view-10");
        let entry = FinalizedEntry { height: 10, digest };

        assert!(uploads.needs_enqueue(&digest, 10));
        assert!(!uploads.register_finalized(3, entry));
        assert!(!uploads.needs_enqueue(&digest, 10));
        assert!(!uploads.register_finalized(3, entry));
        assert!(uploads.register_finalized(4, entry));

        uploads.finish_finalized(3);
        assert!(!uploads.needs_enqueue(&digest, 10));

        uploads.finish_finalized(4);
        assert!(uploads.needs_enqueue(&digest, 10));
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

    #[test]
    fn test_upload_state_eventually_drops_cached_block_after_failed_certificate_upload() {
        let mut uploads = UploadState::new();
        let block = test_block(7, 7, b"view-7");
        let digest = block.digest();

        uploads.start_certificate_upload(digest);
        uploads.cache_block(block);
        uploads.finish_certificate_upload(&digest);

        assert!(uploads.cached_block(&digest).is_some());

        uploads.observe_finalization(8);
        assert!(uploads.cached_block(&digest).is_none());
    }

    #[test]
    fn test_upload_state_keeps_cached_block_while_durable_row_is_pending() {
        let mut uploads = UploadState::new();
        let block = test_block(7, 7, b"view-7");
        let entry = FinalizedEntry {
            height: block.height.get(),
            digest: block.digest(),
        };

        uploads.start_certificate_upload(entry.digest);
        uploads.cache_block(block);
        uploads.register_finalized(3, entry);
        uploads.finish_certificate_upload(&entry.digest);

        assert!(uploads.cached_block(&entry.digest).is_some());

        uploads.finish_finalized(3);
        assert!(uploads.cached_block(&entry.digest).is_some());

        uploads.observe_finalization(entry.height + 1);
        assert!(uploads.cached_block(&entry.digest).is_none());
    }
}
