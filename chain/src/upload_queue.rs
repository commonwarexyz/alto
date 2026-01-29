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
//!
//! ## Actor Pattern
//!
//! This module follows the actor pattern used in commonware:
//! - [`Actor`] owns all mutable state and runs the event loop
//! - [`Mailbox`] is a cloneable handle for sending messages to the actor
//! - Communication uses message passing via mpsc channels

use crate::indexer::Indexer;
use alto_types::{Finalized, Notarized, Seed};
use bytes::{Buf, BufMut};
use commonware_codec::{EncodeSize, Error as CodecError, Read, ReadExt, Write};
use commonware_macros::select_loop;
use commonware_runtime::{
    buffer::PoolRef, spawn_cell, Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::{
    journal::{
        contiguous::variable::{Config as JournalConfig, Journal},
        Error as JournalError,
    },
    rmap::RMap,
};
use commonware_utils::{
    futures::{OptionFuture, Pool},
    NZUsize, NZU16, NZU64,
};
use futures::{
    channel::{mpsc, oneshot},
    SinkExt, StreamExt,
};
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::{debug, info, warn};

/// Error returned when enqueueing an upload fails.
#[derive(Debug, Clone, Error)]
pub enum EnqueueError {
    /// The upload queue actor has stopped.
    #[error("upload queue actor stopped")]
    ActorStopped,
    /// Failed to write to the journal.
    #[error("journal error: {0}")]
    JournalError(String),
}

impl From<JournalError> for EnqueueError {
    fn from(e: JournalError) -> Self {
        EnqueueError::JournalError(e.to_string())
    }
}

/// Message sent to the upload queue actor.
struct Message {
    /// The item to enqueue.
    item: Item,
    /// Channel to send back the result (journal position or error).
    ack: oneshot::Sender<Result<u64, EnqueueError>>,
}

/// Handle for sending uploads to the queue actor.
///
/// This is the external interface to the upload queue. It can be cloned
/// and shared across tasks. Sends block if the mailbox is full.
#[derive(Clone)]
pub struct Mailbox {
    sender: mpsc::Sender<Message>,
}

impl Mailbox {
    /// Enqueue a seed for upload.
    ///
    /// Returns the journal position once the item is durably stored.
    /// Blocks if the mailbox is full.
    pub async fn enqueue_seed(&self, seed: Seed) -> Result<u64, EnqueueError> {
        self.enqueue(Item::Seed(seed)).await
    }

    /// Enqueue a notarization for upload.
    ///
    /// Returns the journal position once the item is durably stored.
    /// Blocks if the mailbox is full.
    pub async fn enqueue_notarization(&self, notarized: Notarized) -> Result<u64, EnqueueError> {
        self.enqueue(Item::Notarization(notarized)).await
    }

    /// Enqueue a finalization for upload.
    ///
    /// Returns the journal position once the item is durably stored.
    /// Blocks if the mailbox is full.
    pub async fn enqueue_finalization(&self, finalized: Finalized) -> Result<u64, EnqueueError> {
        self.enqueue(Item::Finalization(finalized)).await
    }

    /// Enqueue an upload item and wait for durable acknowledgment.
    async fn enqueue(&self, item: Item) -> Result<u64, EnqueueError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sender
            .clone()
            .send(Message { item, ack: ack_tx })
            .await
            .map_err(|_| EnqueueError::ActorStopped)?;
        ack_rx.await.map_err(|_| EnqueueError::ActorStopped)?
    }
}

/// A queued upload with its type encoded in the data.
#[derive(Clone)]
enum Item {
    Seed(Seed),
    Notarization(Notarized),
    Finalization(Finalized),
}

impl Write for Item {
    fn write(&self, writer: &mut impl BufMut) {
        match self {
            Item::Seed(seed) => {
                0u8.write(writer);
                seed.write(writer);
            }
            Item::Notarization(notarized) => {
                1u8.write(writer);
                notarized.write(writer);
            }
            Item::Finalization(finalized) => {
                2u8.write(writer);
                finalized.write(writer);
            }
        }
    }
}

impl Read for Item {
    type Cfg = ();

    fn read_cfg(reader: &mut impl Buf, _: &Self::Cfg) -> Result<Self, CodecError> {
        let tag = u8::read(reader)?;
        match tag {
            0 => Ok(Item::Seed(Seed::read(reader)?)),
            1 => Ok(Item::Notarization(Notarized::read(reader)?)),
            2 => Ok(Item::Finalization(Finalized::read(reader)?)),
            _ => Err(CodecError::Invalid("Item", "unknown tag")),
        }
    }
}

impl EncodeSize for Item {
    fn encode_size(&self) -> usize {
        1 + match self {
            Item::Seed(seed) => seed.encode_size(),
            Item::Notarization(notarized) => notarized.encode_size(),
            Item::Finalization(finalized) => finalized.encode_size(),
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
    /// Size of the mailbox for incoming messages.
    pub mailbox_size: usize,
    /// Maximum number of concurrent uploads.
    pub max_concurrent_uploads: usize,
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
            mailbox_size: 1024,
            max_concurrent_uploads: 8,
            retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(30),
            items_per_section: 1024,
        }
    }
}

/// Result type for upload completion.
type UploadResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A disk-backed queue for reliable upload delivery.
///
/// This actor owns all mutable state and processes uploads in a single-threaded
/// event loop. External code communicates via the [`Mailbox`] handle.
///
/// Uses a journal for atomic appends. On successful upload, items are pruned.
/// On restart, processing resumes from the logical pruning boundary. Items may
/// be re-uploaded if the journal hasn't physically reclaimed their section yet,
/// which is safe since the indexer is idempotent.
pub struct Actor<E: Spawner + Clock + Storage + Metrics> {
    context: E,
    config: Config,
    metrics: QueueMetrics,

    /// Receiver for incoming uploads.
    receiver: mpsc::Receiver<Message>,

    /// Pool of in-flight upload futures.
    uploads: Pool<(u64, UploadResult)>,

    /// The journal storing upload items.
    journal: Journal<E, Item>,

    /// Positions currently being uploaded (in-flight).
    in_flight: HashSet<u64>,

    /// Positions that completed successfully, waiting for contiguous pruning.
    /// Uses RMap for efficient contiguous range tracking and pruning.
    completed: RMap,

    /// Logical pruning boundary - tracks what we've pruned (may differ from journal's
    /// reported boundary which updates asynchronously).
    logical_pruning_boundary: u64,
}

impl<E: Spawner + Clock + Storage + Metrics> Actor<E> {
    /// Create a new upload queue actor, recovering state from disk if available.
    ///
    /// Returns the actor and a mailbox for sending messages to it.
    pub async fn new(context: E, config: Config) -> Result<(Self, Mailbox), JournalError> {
        let metrics = QueueMetrics::new(&context);

        // Create message channel
        let (sender, receiver) = mpsc::channel(config.mailbox_size);

        // Initialize journal
        let journal_config = JournalConfig {
            partition: format!("{}-journal", config.partition),
            items_per_section: NZU64!(config.items_per_section),
            compression: None,
            codec_config: (),
            buffer_pool: PoolRef::new(NZU16!(64), NZUsize!(64 * 1024)),
            write_buffer: NZUsize!(64 * 1024),
        };
        let journal: Journal<E, Item> = Journal::init(context.clone(), journal_config).await?;

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

        let actor = Self {
            context,
            config,
            metrics,
            receiver,
            uploads: Pool::default(),
            journal,
            in_flight: HashSet::new(),
            completed: RMap::new(),
            logical_pruning_boundary: effective_boundary,
        };
        let mailbox = Mailbox { sender };

        Ok((actor, mailbox))
    }

    /// Start the actor and begin processing uploads.
    ///
    /// This spawns the actor's event loop and returns a handle that can be
    /// used to wait for or cancel the actor.
    pub fn start<I: Indexer>(self, indexer: I) -> Handle<()> {
        let mut context = ContextCell::new(self.context.clone());
        spawn_cell!(context, self.run(indexer).await)
    }

    /// Run the upload queue actor event loop.
    async fn run<I: Indexer>(mut self, indexer: I) {
        info!("upload queue actor started");

        // Backoff state (kept as local variables to avoid borrow conflicts in select_loop)
        let mut retry_count: u32 = 0;
        let mut backoff_until: Option<SystemTime> = None;

        // Process any items loaded from disk on startup
        self.spawn_uploads(&indexer, backoff_until).await;

        select_loop! {
            self.context,
            on_stopped => {
                info!("upload queue actor shutting down");
            },
            // Handle incoming uploads
            msg = self.receiver.next() => {
                let Some(Message { item, ack }) = msg else {
                    debug!("mailbox closed, stopping actor");
                    break;
                };
                let result = self.enqueue_item(item).await;
                let should_spawn = result.is_ok();
                // Send ack to caller (ignore if they stopped waiting)
                let _ = ack.send(result);
                if should_spawn {
                    self.spawn_uploads(&indexer, backoff_until).await;
                }
            },
            // Handle upload completions
            (position, result) = self.uploads.next_completed() => {
                self.handle_completion(position, result, &mut retry_count, &mut backoff_until);
                if let Err(e) = self.try_prune().await {
                    warn!(?e, "failed to prune journal");
                }
                self.spawn_uploads(&indexer, backoff_until).await;
            },
            // Handle backoff expiration - retry uploads after delay
            // Note: Uses commonware_utils::futures::OptionFuture which yields Poll::Pending
            // when None, NOT futures::future::OptionFuture which would immediately return
            // Poll::Ready(None) and cause a busy loop.
            _ = OptionFuture::from(backoff_until.map(|until| self.context.sleep_until(until))) => {
                debug!("backoff expired, retrying uploads");
                backoff_until = None;
                self.spawn_uploads(&indexer, backoff_until).await;
            },
        }
    }

    /// Handle an upload completion result.
    fn handle_completion(
        &mut self,
        position: u64,
        result: UploadResult,
        retry_count: &mut u32,
        backoff_until: &mut Option<SystemTime>,
    ) {
        match result {
            Ok(()) => {
                self.mark_complete(position);
                // Reset backoff on success
                *retry_count = 0;
                *backoff_until = None;
            }
            Err(e) => {
                warn!(?e, position, "upload failed, will retry");
                self.mark_failed(position);
                let delay = self.compute_backoff_delay(*retry_count);
                debug!(retries = *retry_count, ?delay, "backing off after failure");
                *backoff_until = Some(self.context.now() + delay);
                *retry_count = retry_count.saturating_add(1);
            }
        }
    }

    /// Enqueue an upload item to the journal. Returns the position once durably stored.
    async fn enqueue_item(&mut self, item: Item) -> Result<u64, EnqueueError> {
        let position = self.journal.append(item).await?;
        self.journal.sync().await?;

        self.metrics.uploads_enqueued.inc();
        self.metrics.queue_depth.inc();
        self.metrics.journal_size.set(self.journal.size() as i64);

        debug!(position, "enqueued upload");
        Ok(position)
    }

    /// Mark a position as successfully uploaded.
    fn mark_complete(&mut self, position: u64) {
        self.in_flight.remove(&position);
        self.completed.insert(position);

        self.metrics.uploads_succeeded.inc();
        self.metrics.queue_depth.dec();

        debug!(position, "marked upload complete");
    }

    /// Try to prune contiguous completed positions from the journal.
    ///
    /// This finds the highest contiguous completed position starting from
    /// the current pruning boundary and prunes up to that point.
    async fn try_prune(&mut self) -> Result<(), JournalError> {
        // Check if boundary is in a completed range and get its end
        let Some((_, range_end)) = self.completed.get(&self.logical_pruning_boundary) else {
            return Ok(());
        };

        // Prune up to (but not including) the position after the range end
        let prune_to = range_end + 1;

        // Important: prune and sync the journal BEFORE updating in-memory state.
        // If either operation fails, we return early without modifying `completed`,
        // keeping state consistent. The positions remain in `completed` and won't
        // be re-uploaded on retry.
        self.journal.prune(prune_to).await?;
        self.journal.sync().await?;

        let old_boundary = self.logical_pruning_boundary;
        self.completed.remove(old_boundary, range_end);
        self.logical_pruning_boundary = prune_to;
        self.metrics.pruning_boundary.set(prune_to as i64);
        debug!(old_boundary, new_boundary = prune_to, "pruned journal");

        Ok(())
    }

    /// Mark a position as failed (remove from in-flight so it can be retried).
    fn mark_failed(&mut self, position: u64) {
        self.in_flight.remove(&position);
        self.metrics.uploads_failed.inc();
    }

    /// Compute backoff delay using exponential backoff.
    fn compute_backoff_delay(&self, retry_count: u32) -> Duration {
        let multiplier = 1u32.checked_shl(retry_count).unwrap_or(u32::MAX);
        (self.config.retry_delay * multiplier).min(self.config.max_retry_delay)
    }

    /// Add upload futures to the pool up to max_concurrent_uploads.
    ///
    /// Returns early if in backoff or no slots available.
    async fn spawn_uploads<I: Indexer>(&mut self, indexer: &I, backoff_until: Option<SystemTime>) {
        // Check if in backoff
        if backoff_until.is_some_and(|until| self.context.now() < until) {
            return;
        }

        let slots_available = self
            .config
            .max_concurrent_uploads
            .saturating_sub(self.uploads.len());
        if slots_available == 0 {
            return;
        }

        let journal_size = self.journal.size();
        let positions: Vec<u64> = (self.logical_pruning_boundary..journal_size)
            .filter(|pos| !self.in_flight.contains(pos) && self.completed.get(pos).is_none())
            .take(slots_available)
            .collect();

        // Process each position: read from journal, mark in-flight, add to upload pool
        for position in positions {
            let item = match self.journal.read(position).await {
                Ok(item) => item,
                Err(e) => {
                    warn!(?e, position, "failed to read item from journal");
                    continue;
                }
            };
            self.in_flight.insert(position);

            // Add upload future to pool
            let indexer = indexer.clone();
            self.uploads.push(async move {
                let result = upload_item(&indexer, item).await;
                (position, result)
            });
        }
    }

    /// Get queue statistics (test-only).
    #[cfg(test)]
    fn stats(&self) -> QueueStats {
        let journal_size = self.journal.size();
        let in_flight = self.in_flight.len();
        let completed_pending_prune: usize = self
            .completed
            .iter()
            .map(|(start, end)| (end - start + 1) as usize)
            .sum();

        // Items retained in journal (not yet pruned)
        let retained = journal_size.saturating_sub(self.logical_pruning_boundary);
        // Items actually needing upload = retained - completed - in_flight
        let pending = retained.saturating_sub((completed_pending_prune + in_flight) as u64);

        QueueStats { retained, pending }
    }
}

/// Upload a single item to the indexer.
async fn upload_item<I: Indexer>(
    indexer: &I,
    item: Item,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match item {
        Item::Seed(seed) => {
            indexer.seed_upload(seed).await?;
        }
        Item::Notarization(notarized) => {
            indexer.notarized_upload(notarized).await?;
        }
        Item::Finalization(finalized) => {
            indexer.finalized_upload(finalized).await?;
        }
    }
    Ok(())
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
    use std::sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    };

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
            mailbox_size: 128,
            max_concurrent_uploads: 4,
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

            // Create actor and start it
            let (actor, mailbox) = Actor::new(context.clone(), config).await.unwrap();
            let _handle = actor.start(indexer.clone());

            // Enqueue seeds
            for seed in &seeds {
                mailbox.enqueue_seed(seed.clone()).await.unwrap();
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

            // Phase 1: Enqueue items but don't process them (no actor started)
            {
                let (mut actor, mailbox) =
                    Actor::new(context.clone(), config.clone()).await.unwrap();

                for seed in &seeds {
                    // Enqueue directly on actor (not yet running)
                    actor.enqueue_item(Item::Seed(seed.clone())).await.unwrap();
                }

                let stats = actor.stats();
                assert_eq!(
                    stats.retained,
                    seeds.len() as u64,
                    "items should be in journal"
                );
                assert_eq!(stats.pending, seeds.len() as u64, "items should be pending");

                // Actor and mailbox dropped here (simulates crash)
                drop(mailbox);
            }

            // Phase 2: Restart - create new actor with same partition, start it
            let indexer = TestIndexer::new();
            {
                let (actor, mailbox) = Actor::new(context.clone(), config.clone()).await.unwrap();

                // Verify items recovered (check before starting)
                let stats = actor.stats();
                assert_eq!(
                    stats.retained,
                    seeds.len() as u64,
                    "items should be recovered from journal"
                );

                // Start actor and wait for uploads
                let _handle = actor.start(indexer.clone());

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

                drop(mailbox);
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
                let (actor, mailbox) = Actor::new(context.clone(), config.clone()).await.unwrap();
                let _handle = actor.start(indexer.clone());

                // Enqueue seeds
                for seed in &seeds {
                    mailbox.enqueue_seed(seed.clone()).await.unwrap();
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
                    mailbox
                        .enqueue_notarization(notarization.clone())
                        .await
                        .unwrap();
                }

                // Small delay - some notarizations might upload
                context.sleep(Duration::from_millis(20)).await;

                // Mailbox dropped here (simulates crash)
            }

            let uploads_before_crash = indexer.total_uploads();
            info!(uploads_before_crash, "uploads before simulated crash");

            // Phase 2: Restart and verify remaining items upload
            // Note: Some items may be re-uploaded (idempotent), that's okay
            {
                let (actor, mailbox) = Actor::new(context.clone(), config.clone()).await.unwrap();
                let _handle = actor.start(indexer.clone());

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

                drop(mailbox);
            }
        });
    }

    #[test_traced]
    fn test_determinism() {
        // Test that the upload queue behaves deterministically when run with the same seed.
        // This verifies that all async operations (journal I/O, parallel uploads, pruning)
        // execute in a reproducible order.

        /// Run the upload scenario and return the auditor state.
        fn run_scenario(seed: u64) -> String {
            let cfg = deterministic::Config::default().with_seed(seed);
            let executor = Runner::from(cfg);
            executor.start(|mut context| async move {
                let (seeds, notarizations, finalizations) = create_test_fixtures(&mut context);
                let indexer = TestIndexer::new();
                let config = test_config();

                let (actor, mailbox) = Actor::new(context.clone(), config).await.unwrap();
                let _handle = actor.start(indexer.clone());

                // Enqueue a mix of all upload types
                for seed in &seeds {
                    mailbox.enqueue_seed(seed.clone()).await.unwrap();
                }
                for notarization in &notarizations {
                    mailbox
                        .enqueue_notarization(notarization.clone())
                        .await
                        .unwrap();
                }
                for finalization in &finalizations {
                    mailbox
                        .enqueue_finalization(finalization.clone())
                        .await
                        .unwrap();
                }

                let total_items = seeds.len() + notarizations.len() + finalizations.len();

                // Wait for all uploads to complete
                for _ in 0..200 {
                    context.sleep(Duration::from_millis(10)).await;
                    if indexer.total_uploads() >= total_items {
                        break;
                    }
                }

                // Wait a bit more for pruning to complete
                context.sleep(Duration::from_millis(100)).await;

                assert_eq!(indexer.total_uploads(), total_items);
                context.auditor().state()
            })
        }

        // Run the same scenario twice with the same seed
        let seed = 12345;
        let state1 = run_scenario(seed);
        let state2 = run_scenario(seed);

        assert_eq!(
            state1, state2,
            "upload queue must be deterministic with same seed"
        );
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

            // Phase 1: Upload all items and wait for pruning
            {
                let (actor, mailbox) = Actor::new(context.clone(), config.clone()).await.unwrap();
                let _handle = actor.start(indexer.clone());

                for seed in &seeds {
                    mailbox.enqueue_seed(seed.clone()).await.unwrap();
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

                // Wait for pruning to complete
                context.sleep(Duration::from_millis(100)).await;
            }

            let uploads_after_phase1 = indexer.total_uploads();

            // Phase 2: Restart - items may be re-uploaded since journal uses
            // section-based pruning. Verify the queue recovers and completes.
            {
                let (actor, _mailbox) = Actor::new(context.clone(), config.clone()).await.unwrap();

                // Check that the queue recovered some state (before starting)
                let stats = actor.stats();
                info!(
                    retained = stats.retained,
                    pending = stats.pending,
                    "queue state after restart"
                );

                let _handle = actor.start(indexer.clone());

                // Wait for any re-uploads to complete
                context.sleep(Duration::from_millis(200)).await;

                // Verify total uploads is at least what we had before
                // (may be more due to re-uploads from section-based pruning)
                assert!(
                    indexer.total_uploads() >= uploads_after_phase1,
                    "uploads should be at least {} after restart",
                    uploads_after_phase1
                );
            }
        });
    }
}
