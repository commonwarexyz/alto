//! A disk-backed upload queue for reliable delivery to the indexer.
//!
//! This module provides crash-safe upload delivery using a journal for storing
//! upload items. Uploads are appended atomically to the journal, and a background
//! worker processes them with parallel upload support.
//!
//! ## Design
//!
//! - **Journal**: Stores upload items in an append-only log with atomic appends
//! - **Parallel uploads**: Multiple items can be uploaded concurrently
//! - **Idempotent uploads**: Assumes indexer can handle duplicate uploads safely
//! - **Pruning**: Completed items are pruned from the journal to reclaim space
//!
//! On restart, processing resumes from the logical pruning boundary. The journal
//! uses section-based pruning, so items may be re-uploaded if their section hasn't
//! been fully reclaimed. Since the indexer is idempotent, this is safe.

use crate::indexer::Indexer;
use alto_types::{Finalized, Notarized, Seed};
use bytes::{Buf, BufMut};
use commonware_codec::{EncodeSize, Error as CodecError, Read, ReadExt, Write};
use commonware_macros::select_loop;
use commonware_runtime::{buffer::PoolRef, Clock, Metrics, Spawner, Storage};
use commonware_storage::journal::{
    contiguous::variable::{Config as JournalConfig, Journal},
    Error as JournalError,
};
use commonware_utils::{NZUsize, NZU16, NZU64};
use futures::lock::Mutex;
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Errors that can occur in the upload queue.
#[derive(Debug, Error)]
pub enum Error {
    #[error("journal error: {0}")]
    Journal(#[from] JournalError),
}

/// A queued upload with its type encoded in the data.
#[derive(Clone)]
pub enum QueuedUpload {
    Seed(Seed),
    Notarization(Notarized),
    Finalization(Finalized),
}

impl QueuedUpload {
    fn kind_str(&self) -> &'static str {
        match self {
            QueuedUpload::Seed(_) => "seed",
            QueuedUpload::Notarization(_) => "notarization",
            QueuedUpload::Finalization(_) => "finalization",
        }
    }
}

impl Write for QueuedUpload {
    fn write(&self, writer: &mut impl BufMut) {
        match self {
            QueuedUpload::Seed(seed) => {
                0u8.write(writer);
                seed.write(writer);
            }
            QueuedUpload::Notarization(notarized) => {
                1u8.write(writer);
                notarized.write(writer);
            }
            QueuedUpload::Finalization(finalized) => {
                2u8.write(writer);
                finalized.write(writer);
            }
        }
    }
}

impl Read for QueuedUpload {
    type Cfg = ();

    fn read_cfg(reader: &mut impl Buf, _: &Self::Cfg) -> Result<Self, CodecError> {
        let tag = u8::read(reader)?;
        match tag {
            0 => Ok(QueuedUpload::Seed(Seed::read(reader)?)),
            1 => Ok(QueuedUpload::Notarization(Notarized::read(reader)?)),
            2 => Ok(QueuedUpload::Finalization(Finalized::read(reader)?)),
            _ => Err(CodecError::Invalid("QueuedUpload", "unknown tag")),
        }
    }
}

impl EncodeSize for QueuedUpload {
    fn encode_size(&self) -> usize {
        1 + match self {
            QueuedUpload::Seed(seed) => seed.encode_size(),
            QueuedUpload::Notarization(notarized) => notarized.encode_size(),
            QueuedUpload::Finalization(finalized) => finalized.encode_size(),
        }
    }
}

/// Metrics for the upload queue.
#[derive(Clone)]
struct QueueMetrics {
    /// Current number of pending uploads in the queue.
    queue_depth: Gauge,
    /// Total number of uploads enqueued.
    uploads_enqueued: Counter,
    /// Total number of successful uploads.
    uploads_succeeded: Counter,
    /// Total number of failed upload attempts (includes retries).
    uploads_failed: Counter,
    /// Current pruning boundary (items before this have been uploaded and pruned).
    pruning_boundary: Gauge,
    /// Current journal size.
    journal_size: Gauge,
}

impl QueueMetrics {
    fn new<E: Metrics>(context: &E) -> Self {
        let metrics = Self {
            queue_depth: Gauge::default(),
            uploads_enqueued: Counter::default(),
            uploads_succeeded: Counter::default(),
            uploads_failed: Counter::default(),
            pruning_boundary: Gauge::default(),
            journal_size: Gauge::default(),
        };

        context.register(
            "queue_depth",
            "Current number of pending uploads",
            metrics.queue_depth.clone(),
        );
        context.register(
            "uploads_enqueued",
            "Total number of uploads enqueued",
            metrics.uploads_enqueued.clone(),
        );
        context.register(
            "uploads_succeeded",
            "Total number of successful uploads",
            metrics.uploads_succeeded.clone(),
        );
        context.register(
            "uploads_failed",
            "Total number of failed upload attempts",
            metrics.uploads_failed.clone(),
        );
        context.register(
            "pruning_boundary",
            "Items before this position have been uploaded and pruned",
            metrics.pruning_boundary.clone(),
        );
        context.register(
            "journal_size",
            "Total items appended to journal",
            metrics.journal_size.clone(),
        );

        metrics
    }
}

/// Configuration for the upload queue.
#[derive(Clone)]
pub struct Config {
    /// Partition name prefix for storage.
    pub partition: String,
    /// Maximum number of concurrent uploads.
    pub max_concurrent_uploads: usize,
    /// How often to check for new items to process.
    pub poll_interval: Duration,
    /// Base delay before retrying after a failure.
    pub retry_delay: Duration,
    /// Maximum delay between retries (caps exponential backoff).
    pub max_retry_delay: Duration,
    /// Number of items per journal section.
    pub items_per_section: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            partition: "upload-queue".to_string(),
            max_concurrent_uploads: 8,
            poll_interval: Duration::from_millis(50),
            retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(30),
            items_per_section: 1024,
        }
    }
}

/// A disk-backed queue for reliable upload delivery.
///
/// Uses a journal for atomic appends. On successful upload, items are pruned.
/// On restart, processing resumes from the logical pruning boundary. Items may
/// be re-uploaded if the journal hasn't physically reclaimed their section yet,
/// which is safe since the indexer is idempotent.
pub struct UploadQueue<E: Spawner + Clock + Storage + Metrics> {
    context: E,
    config: Config,
    metrics: QueueMetrics,

    /// The journal storing upload items.
    journal: Mutex<Journal<E, QueuedUpload>>,

    /// Positions currently being uploaded (in-flight).
    in_flight: Mutex<HashSet<u64>>,

    /// Positions that completed successfully, waiting for contiguous pruning.
    completed: Mutex<HashSet<u64>>,

    /// Consecutive batch failures for exponential backoff.
    consecutive_failures: AtomicU32,

    /// Logical pruning boundary - tracks what we've pruned (may differ from journal's
    /// reported boundary which updates asynchronously).
    logical_pruning_boundary: AtomicU64,
}

impl<E: Spawner + Clock + Storage + Metrics> UploadQueue<E> {
    /// Create a new upload queue, recovering state from disk if available.
    pub async fn new(context: E, config: Config) -> Result<Self, Error> {
        let metrics = QueueMetrics::new(&context);

        // Initialize journal
        let journal_config = JournalConfig {
            partition: format!("{}-journal", config.partition),
            items_per_section: NZU64!(config.items_per_section),
            compression: None,
            codec_config: (),
            buffer_pool: PoolRef::new(NZU16!(64), NZUsize!(64 * 1024)),
            write_buffer: NZUsize!(64 * 1024),
        };
        let journal: Journal<E, QueuedUpload> =
            Journal::init(context.clone(), journal_config).await?;

        // Calculate pending count from journal state
        let journal_size = journal.size();
        let pruning_boundary = journal.pruning_boundary();
        let oldest_retained = journal.oldest_retained_pos();

        // Use the max of pruning_boundary and oldest_retained for restart recovery.
        // pruning_boundary may not persist across restarts, but oldest_retained reflects
        // what's actually on disk.
        let effective_boundary =
            oldest_retained.map_or(pruning_boundary, |pos| pos.max(pruning_boundary));
        let pending = journal_size.saturating_sub(effective_boundary);

        debug!(
            pruning_boundary,
            ?oldest_retained,
            effective_boundary,
            journal_size,
            pending,
            "journal state on init"
        );

        if pending > 0 {
            info!(
                effective_boundary,
                journal_size, pending, "recovered upload queue state (may re-upload on restart)"
            );
        }

        // Update metrics
        metrics.queue_depth.set(pending as i64);
        metrics.pruning_boundary.set(effective_boundary as i64);
        metrics.journal_size.set(journal_size as i64);

        Ok(Self {
            context,
            config,
            metrics,
            journal: Mutex::new(journal),
            in_flight: Mutex::new(HashSet::new()),
            completed: Mutex::new(HashSet::new()),
            consecutive_failures: AtomicU32::new(0),
            logical_pruning_boundary: AtomicU64::new(effective_boundary),
        })
    }

    /// Enqueue a seed for upload.
    pub async fn enqueue_seed(&self, seed: Seed) -> Result<u64, Error> {
        self.enqueue(QueuedUpload::Seed(seed)).await
    }

    /// Enqueue a notarization for upload.
    pub async fn enqueue_notarization(&self, notarized: Notarized) -> Result<u64, Error> {
        self.enqueue(QueuedUpload::Notarization(notarized)).await
    }

    /// Enqueue a finalization for upload.
    pub async fn enqueue_finalization(&self, finalized: Finalized) -> Result<u64, Error> {
        self.enqueue(QueuedUpload::Finalization(finalized)).await
    }

    /// Enqueue an upload item. Returns the position in the journal.
    async fn enqueue(&self, upload: QueuedUpload) -> Result<u64, Error> {
        let kind = upload.kind_str();

        let mut journal = self.journal.lock().await;
        let position = journal.append(upload).await?;
        journal.sync().await?;

        self.metrics.uploads_enqueued.inc();
        self.metrics.queue_depth.inc();
        self.metrics.journal_size.set(journal.size() as i64);

        debug!(position, kind, "enqueued upload");
        Ok(position)
    }

    /// Mark a position as successfully uploaded.
    async fn mark_complete(&self, position: u64) {
        self.in_flight.lock().await.remove(&position);
        self.completed.lock().await.insert(position);

        self.metrics.uploads_succeeded.inc();
        self.metrics.queue_depth.dec();

        debug!(position, "marked upload complete");
    }

    /// Try to prune contiguous completed positions from the journal.
    ///
    /// This finds the highest contiguous completed position starting from
    /// the current pruning boundary and prunes up to that point.
    async fn try_prune(&self) -> Result<(), Error> {
        let mut journal = self.journal.lock().await;
        let mut completed = self.completed.lock().await;

        // Use our logical boundary which updates immediately after prune
        let boundary = self.logical_pruning_boundary.load(Ordering::Acquire);
        let mut prune_to = boundary;

        // Find contiguous completed positions from boundary
        while completed.remove(&prune_to) {
            prune_to += 1;
        }

        if prune_to > boundary {
            journal.prune(prune_to).await?;
            journal.sync().await?;
            // Update our logical boundary immediately (journal's boundary updates async)
            self.logical_pruning_boundary
                .store(prune_to, Ordering::Release);
            self.metrics.pruning_boundary.set(prune_to as i64);
            debug!(
                old_boundary = boundary,
                new_boundary = prune_to,
                "pruned journal"
            );
        }

        Ok(())
    }

    /// Get the next positions to process (up to max_concurrent).
    async fn get_positions_to_process(&self) -> Vec<u64> {
        let journal_size = self.journal.lock().await.size();

        // Start from our logical pruning boundary - items before this have been successfully uploaded
        let start = self.logical_pruning_boundary.load(Ordering::Acquire);

        let in_flight = self.in_flight.lock().await;
        let completed = self.completed.lock().await;

        let mut positions = Vec::new();
        let mut pos = start;

        while positions.len() < self.config.max_concurrent_uploads && pos < journal_size {
            // Skip if already in-flight or completed (waiting for prune)
            if !in_flight.contains(&pos) && !completed.contains(&pos) {
                positions.push(pos);
            }
            pos += 1;
        }

        positions
    }

    /// Mark positions as in-flight.
    async fn mark_in_flight(&self, positions: &[u64]) {
        let mut in_flight = self.in_flight.lock().await;
        for &pos in positions {
            in_flight.insert(pos);
        }
    }

    /// Mark a position as failed (remove from in-flight so it can be retried).
    async fn mark_failed(&self, position: u64) {
        self.in_flight.lock().await.remove(&position);
        self.metrics.uploads_failed.inc();
    }

    /// Start the background worker that processes the queue.
    pub fn start_worker<I: Indexer>(self: Arc<Self>, indexer: I) {
        let queue = self.clone();
        self.context
            .with_label("upload_worker")
            .spawn(move |context| async move {
                info!("upload queue worker started");

                select_loop! {
                    context,
                    on_stopped => {
                        info!("upload queue worker shutting down");
                    },
                    _ = async {
                        queue.process_batch(&indexer).await;
                        context.sleep(queue.config.poll_interval).await;
                    } => {},
                }
            });
    }

    /// Process a batch of uploads in parallel.
    async fn process_batch<I: Indexer>(&self, indexer: &I) {
        // Get positions to process
        let positions = self.get_positions_to_process().await;
        if positions.is_empty() {
            return;
        }

        // Mark them as in-flight
        self.mark_in_flight(&positions).await;

        // Read all items first (holding lock once), then upload in parallel
        let items: Vec<_> = {
            let journal = self.journal.lock().await;
            let mut items = Vec::with_capacity(positions.len());
            for &pos in &positions {
                match journal.read(pos).await {
                    Ok(item) => items.push((pos, Some(item))),
                    Err(e) => {
                        warn!(?e, pos, "failed to read item from journal");
                        items.push((pos, None));
                    }
                }
            }
            items
        };

        // Upload in parallel (no lock held)
        let mut any_succeeded = false;
        let mut any_failed = false;
        let futures: Vec<_> = items
            .into_iter()
            .map(|(position, item)| async move {
                let result = match item {
                    Some(item) => self.do_upload(indexer, position, item).await,
                    None => Err("failed to read from journal".into()),
                };
                (position, result)
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        for (position, result) in results {
            match result {
                Ok(()) => {
                    self.mark_complete(position).await;
                    any_succeeded = true;
                }
                Err(e) => {
                    warn!(?e, position, "upload failed, will retry");
                    self.mark_failed(position).await;
                    any_failed = true;
                }
            }
        }

        // Update backoff state and sleep if needed.
        // Reset on any success; sleep on any failure (even partial success triggers delay).
        if any_succeeded {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }
        if any_failed {
            let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
            // Cap exponent at 6 (64x multiplier) to avoid overflow
            let multiplier = 1u32 << failures.min(6);
            let delay = (self.config.retry_delay * multiplier).min(self.config.max_retry_delay);
            debug!(failures = failures + 1, ?delay, "backing off after failure");
            self.context.sleep(delay).await;
        }

        // Prune completed entries
        if let Err(e) = self.try_prune().await {
            warn!(?e, "failed to prune journal");
        }
    }

    /// Upload a single item to the indexer (item already read from journal).
    async fn do_upload<I: Indexer>(
        &self,
        indexer: &I,
        position: u64,
        item: QueuedUpload,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let kind = item.kind_str();

        match item {
            QueuedUpload::Seed(seed) => {
                indexer.seed_upload(seed).await?;
            }
            QueuedUpload::Notarization(notarized) => {
                indexer.notarized_upload(notarized).await?;
            }
            QueuedUpload::Finalization(finalized) => {
                indexer.finalized_upload(finalized).await?;
            }
        }

        debug!(position, kind, "upload succeeded");
        Ok(())
    }

    /// Get queue statistics (test-only).
    #[cfg(test)]
    async fn stats(&self) -> QueueStats {
        let journal_size = self.journal.lock().await.size();
        let pruning_boundary = self.logical_pruning_boundary.load(Ordering::Acquire);
        let in_flight = self.in_flight.lock().await.len();
        let completed_pending_prune = self.completed.lock().await.len();

        // Items retained in journal (not yet pruned)
        let retained = journal_size.saturating_sub(pruning_boundary);
        // Items actually needing upload = retained - completed - in_flight
        let pending = retained.saturating_sub((completed_pending_prune + in_flight) as u64);

        QueueStats { retained, pending }
    }
}

/// Statistics about the upload queue (test-only).
#[cfg(test)]
struct QueueStats {
    /// Items retained in journal (not yet pruned).
    retained: u64,
    /// Items needing upload (retained - completed - in_flight).
    pending: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{Block, Seedable};
    use commonware_consensus::{
        simplex::{
            scheme::bls12381_threshold,
            types::{Finalize, Notarize, Proposal},
        },
        types::{Epoch, Height, Round, View},
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, Digestible, Hasher,
        Sha256,
    };
    use commonware_macros::test_traced;
    use commonware_parallel::Sequential;
    use commonware_runtime::{
        deterministic::{self, Runner},
        Runner as _,
    };
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    const EPOCH: Epoch = Epoch::new(0);

    /// Test indexer that tracks upload counts.
    #[derive(Clone)]
    struct TestIndexer {
        seed_count: Arc<AtomicUsize>,
        notarization_count: Arc<AtomicUsize>,
        finalization_count: Arc<AtomicUsize>,
    }

    impl TestIndexer {
        fn new() -> Self {
            Self {
                seed_count: Arc::new(AtomicUsize::new(0)),
                notarization_count: Arc::new(AtomicUsize::new(0)),
                finalization_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn total_uploads(&self) -> usize {
            self.seed_count.load(AtomicOrdering::SeqCst)
                + self.notarization_count.load(AtomicOrdering::SeqCst)
                + self.finalization_count.load(AtomicOrdering::SeqCst)
        }
    }

    impl Indexer for TestIndexer {
        type Error = std::io::Error;

        async fn seed_upload(&self, _: Seed) -> Result<(), Self::Error> {
            self.seed_count.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }

        async fn notarized_upload(&self, _: Notarized) -> Result<(), Self::Error> {
            self.notarization_count.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }

        async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
            self.finalization_count.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    /// Create test fixtures for seeds/notarizations/finalizations.
    fn create_test_fixtures(
        context: &mut deterministic::Context,
    ) -> (Vec<Seed>, Vec<Notarized>, Vec<Finalized>) {
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(context, alto_types::NAMESPACE, 4);

        let mut seeds = Vec::new();
        let mut notarizations = Vec::new();
        let mut finalizations = Vec::new();

        for i in 0u64..3 {
            // Create a block
            let parent = Sha256::hash(&i.to_be_bytes());
            let block = Block::new(parent, Height::new(i + 1), 1000 + i);
            let proposal = Proposal::new(
                Round::new(EPOCH, View::new(i)),
                View::new(i.saturating_sub(1)),
                block.digest(),
            );

            // Create notarization
            let notarizes: Vec<_> = schemes
                .iter()
                .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
                .collect();
            let notarization =
                alto_types::Notarization::from_notarizes(&schemes[0], &notarizes, &Sequential)
                    .unwrap();

            // Create finalization
            let finalizes: Vec<_> = schemes
                .iter()
                .map(|scheme| Finalize::sign(scheme, proposal.clone()).unwrap())
                .collect();
            let finalization =
                alto_types::Finalization::from_finalizes(&schemes[0], &finalizes, &Sequential)
                    .unwrap();

            seeds.push(notarization.seed());
            notarizations.push(Notarized::new(notarization, block.clone()));
            finalizations.push(Finalized::new(finalization, block));
        }

        (seeds, notarizations, finalizations)
    }

    fn test_config() -> Config {
        Config {
            partition: "test-queue".to_string(),
            max_concurrent_uploads: 4,
            poll_interval: Duration::from_millis(10),
            retry_delay: Duration::from_millis(50),
            max_retry_delay: Duration::from_millis(200),
            items_per_section: 16,
        }
    }

    #[test_traced]
    fn test_basic_upload() {
        // Test that enqueued items are uploaded
        let cfg = deterministic::Config::default().with_seed(42);
        let executor = Runner::from(cfg);
        executor.start(|mut context| async move {
            let (seeds, _, _) = create_test_fixtures(&mut context);
            let indexer = TestIndexer::new();
            let config = test_config();

            // Create queue and start worker
            let queue = Arc::new(UploadQueue::new(context.clone(), config).await.unwrap());
            queue.clone().start_worker(indexer.clone());

            // Enqueue seeds
            for seed in &seeds {
                queue.enqueue_seed(seed.clone()).await.unwrap();
            }

            // Wait for uploads to complete
            for _ in 0..100 {
                context.sleep(Duration::from_millis(10)).await;
                if indexer.seed_count.load(AtomicOrdering::SeqCst) >= seeds.len() {
                    break;
                }
            }

            assert_eq!(
                indexer.seed_count.load(AtomicOrdering::SeqCst),
                seeds.len(),
                "all seeds should be uploaded"
            );
        });
    }

    #[test_traced]
    fn test_restart_recovery() {
        // Test that items enqueued before "crash" are uploaded after restart
        let cfg = deterministic::Config::default().with_seed(123);
        let executor = Runner::from(cfg);
        executor.start(|mut context| async move {
            let (seeds, _, _) = create_test_fixtures(&mut context);
            let config = test_config();

            // Phase 1: Enqueue items but don't process them (no worker started)
            {
                let queue = UploadQueue::new(context.clone(), config.clone())
                    .await
                    .unwrap();

                for seed in &seeds {
                    queue.enqueue_seed(seed.clone()).await.unwrap();
                }

                let stats = queue.stats().await;
                assert_eq!(
                    stats.retained,
                    seeds.len() as u64,
                    "items should be in journal"
                );
                assert_eq!(stats.pending, seeds.len() as u64, "items should be pending");

                // Queue is dropped here (simulates crash)
            }

            // Phase 2: Restart - create new queue with same partition, start worker
            let indexer = TestIndexer::new();
            {
                let queue = Arc::new(
                    UploadQueue::new(context.clone(), config.clone())
                        .await
                        .unwrap(),
                );

                // Verify items recovered
                let stats = queue.stats().await;
                assert_eq!(
                    stats.retained,
                    seeds.len() as u64,
                    "items should be recovered from journal"
                );

                // Start worker and wait for uploads
                queue.clone().start_worker(indexer.clone());

                for _ in 0..100 {
                    context.sleep(Duration::from_millis(10)).await;
                    if indexer.seed_count.load(AtomicOrdering::SeqCst) >= seeds.len() {
                        break;
                    }
                }

                assert_eq!(
                    indexer.seed_count.load(AtomicOrdering::SeqCst),
                    seeds.len(),
                    "all seeds should be uploaded after restart"
                );
            }
        });
    }

    #[test_traced]
    fn test_partial_upload_then_restart() {
        // Test: upload some items, "crash", restart, verify all items eventually uploaded
        let cfg = deterministic::Config::default().with_seed(456);
        let executor = Runner::from(cfg);
        executor.start(|mut context| async move {
            let (seeds, notarizations, _) = create_test_fixtures(&mut context);
            let config = test_config();

            let indexer = TestIndexer::new();
            let total_items = seeds.len() + notarizations.len();

            // Phase 1: Enqueue all, upload some
            {
                let queue = Arc::new(
                    UploadQueue::new(context.clone(), config.clone())
                        .await
                        .unwrap(),
                );
                queue.clone().start_worker(indexer.clone());

                // Enqueue seeds
                for seed in &seeds {
                    queue.enqueue_seed(seed.clone()).await.unwrap();
                }

                // Wait for seeds to upload
                for _ in 0..50 {
                    context.sleep(Duration::from_millis(10)).await;
                    if indexer.seed_count.load(AtomicOrdering::SeqCst) >= seeds.len() {
                        break;
                    }
                }

                // Enqueue notarizations (may or may not upload before "crash")
                for notarization in &notarizations {
                    queue
                        .enqueue_notarization(notarization.clone())
                        .await
                        .unwrap();
                }

                // Small delay - some notarizations might upload
                context.sleep(Duration::from_millis(20)).await;

                // Queue dropped here (simulates crash)
            }

            let uploads_before_crash = indexer.total_uploads();
            info!(uploads_before_crash, "uploads before simulated crash");

            // Phase 2: Restart and verify remaining items upload
            // Note: Some items may be re-uploaded (idempotent), that's okay
            {
                let queue = Arc::new(
                    UploadQueue::new(context.clone(), config.clone())
                        .await
                        .unwrap(),
                );
                queue.clone().start_worker(indexer.clone());

                // Wait for all items to upload
                for _ in 0..200 {
                    context.sleep(Duration::from_millis(10)).await;
                    let total = indexer.total_uploads();
                    // At minimum, we need total_items uploads (possibly more due to re-uploads)
                    if total >= total_items {
                        break;
                    }
                }

                let final_uploads = indexer.total_uploads();
                assert!(
                    final_uploads >= total_items,
                    "all items should be uploaded (got {}, expected at least {})",
                    final_uploads,
                    total_items
                );
            }
        });
    }

    #[test_traced]
    fn test_restart_recovery_with_pruning() {
        // Test that the queue handles restart correctly.
        // Note: The journal uses section-based pruning, so items may be re-uploaded
        // after restart if their section hasn't been fully reclaimed. This is safe
        // because the indexer is idempotent (as documented in the module header).
        let cfg = deterministic::Config::default().with_seed(789);
        let executor = Runner::from(cfg);
        executor.start(|mut context| async move {
            let (seeds, _, _) = create_test_fixtures(&mut context);
            let config = test_config();
            let indexer = TestIndexer::new();

            // Phase 1: Upload all items and mark them as pruned
            {
                let queue = Arc::new(
                    UploadQueue::new(context.clone(), config.clone())
                        .await
                        .unwrap(),
                );
                queue.clone().start_worker(indexer.clone());

                for seed in &seeds {
                    queue.enqueue_seed(seed.clone()).await.unwrap();
                }

                // Wait for uploads to complete
                for _ in 0..200 {
                    context.sleep(Duration::from_millis(10)).await;
                    if indexer.seed_count.load(AtomicOrdering::SeqCst) >= seeds.len() {
                        break;
                    }
                }

                assert_eq!(
                    indexer.seed_count.load(AtomicOrdering::SeqCst),
                    seeds.len(),
                    "all seeds should be uploaded in phase 1"
                );

                // Wait for pruning to complete (updates logical boundary)
                for _ in 0..100 {
                    context.sleep(Duration::from_millis(10)).await;
                    let stats = queue.stats().await;
                    if stats.retained == 0 {
                        break;
                    }
                }

                let stats = queue.stats().await;
                assert_eq!(
                    stats.retained, 0,
                    "logical boundary should show all items pruned"
                );
            }

            // Phase 2: Restart - items may be re-uploaded since journal uses
            // section-based pruning. Verify the queue recovers and completes.
            {
                let queue = Arc::new(
                    UploadQueue::new(context.clone(), config.clone())
                        .await
                        .unwrap(),
                );

                // Check that the queue recovered some state
                let stats = queue.stats().await;
                info!(
                    retained = stats.retained,
                    pending = stats.pending,
                    "queue state after restart"
                );

                queue.clone().start_worker(indexer.clone());

                // If there are items to process, wait for them
                if stats.retained > 0 {
                    for _ in 0..200 {
                        context.sleep(Duration::from_millis(10)).await;
                        let stats = queue.stats().await;
                        if stats.retained == 0 {
                            break;
                        }
                    }
                }

                // Verify the queue eventually completes (all items processed)
                let final_stats = queue.stats().await;
                assert_eq!(
                    final_stats.retained, 0,
                    "queue should eventually process all items"
                );
            }
        });
    }
}
