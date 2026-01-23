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
//! On restart, processing resumes from `oldest_retained_pos()`. Since the indexer
//! is idempotent, re-uploading items after a crash is safe.

use crate::indexer::Indexer;
use alto_types::{Finalized, Notarized, Seed};
use bytes::{Buf, BufMut};
use commonware_codec::{EncodeSize, Error as CodecError, Read, ReadExt, Write};
use commonware_runtime::{buffer::PoolRef, Clock, Metrics, Spawner, Storage};
use commonware_storage::journal::{
    contiguous::variable::{Config as JournalConfig, Journal},
    Error as JournalError,
};
use commonware_utils::{NZU16, NZU64, NZUsize};
use futures::lock::Mutex;
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, warn};

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
    /// How long to wait before retrying after a failure.
    pub retry_delay: Duration,
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
            items_per_section: 1024,
        }
    }
}

/// A disk-backed queue for reliable upload delivery.
///
/// Uses a journal for atomic appends. On successful upload, items are pruned.
/// On restart, processing resumes from `oldest_retained_pos()` since the
/// indexer is idempotent and can handle duplicate uploads.
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
        let oldest = journal.oldest_retained_pos().unwrap_or(journal_size);
        let pending = journal_size.saturating_sub(oldest);

        if pending > 0 {
            info!(
                oldest_retained = oldest,
                journal_size,
                pending,
                "recovered upload queue state (may re-upload on restart)"
            );
        }

        // Update metrics
        let pruning_boundary = journal.pruning_boundary();
        metrics.queue_depth.set(pending as i64);
        metrics.pruning_boundary.set(pruning_boundary as i64);
        metrics.journal_size.set(journal_size as i64);

        Ok(Self {
            context,
            config,
            metrics,
            journal: Mutex::new(journal),
            in_flight: Mutex::new(HashSet::new()),
            completed: Mutex::new(HashSet::new()),
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

        let mut journal: futures::lock::MutexGuard<'_, Journal<E, QueuedUpload>> =
            self.journal.lock().await;
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
        // Remove from in-flight
        {
            let mut in_flight: futures::lock::MutexGuard<'_, HashSet<u64>> =
                self.in_flight.lock().await;
            in_flight.remove(&position);
        }

        // Add to completed set
        {
            let mut completed: futures::lock::MutexGuard<'_, HashSet<u64>> =
                self.completed.lock().await;
            completed.insert(position);
        }

        self.metrics.uploads_succeeded.inc();
        self.metrics.queue_depth.dec();

        debug!(position, "marked upload complete");
    }

    /// Try to prune contiguous completed positions from the journal.
    ///
    /// This finds the highest contiguous completed position starting from
    /// the current pruning boundary and prunes up to that point.
    async fn try_prune(&self) -> Result<(), Error> {
        let mut journal: futures::lock::MutexGuard<'_, Journal<E, QueuedUpload>> =
            self.journal.lock().await;
        let mut completed: futures::lock::MutexGuard<'_, HashSet<u64>> =
            self.completed.lock().await;

        let boundary = journal.pruning_boundary();
        let mut prune_to = boundary;

        // Find contiguous completed positions from boundary
        while completed.remove(&prune_to) {
            prune_to += 1;
        }

        if prune_to > boundary {
            journal.prune(prune_to).await?;
            journal.sync().await?;
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
        let journal: futures::lock::MutexGuard<'_, Journal<E, QueuedUpload>> =
            self.journal.lock().await;
        let journal_size = journal.size();
        let start = journal.oldest_retained_pos().unwrap_or(journal_size);
        drop(journal);

        let in_flight: futures::lock::MutexGuard<'_, HashSet<u64>> = self.in_flight.lock().await;
        let completed: futures::lock::MutexGuard<'_, HashSet<u64>> = self.completed.lock().await;

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
        let mut in_flight = self.in_flight.lock().await;
        in_flight.remove(&position);
        self.metrics.uploads_failed.inc();
    }

    /// Read an item from the journal.
    async fn read_item(&self, position: u64) -> Result<QueuedUpload, Error> {
        let journal: futures::lock::MutexGuard<'_, Journal<E, QueuedUpload>> =
            self.journal.lock().await;
        Ok(journal.read(position).await?)
    }

    /// Prune completed items from the journal.
    /// This is called automatically after processing batches.
    pub async fn prune(&self) -> Result<(), Error> {
        self.try_prune().await
    }

    /// Start the background worker that processes the queue.
    pub fn start_worker<I: Indexer>(self: Arc<Self>, indexer: I) {
        let queue = self.clone();
        self.context
            .with_label("upload_worker")
            .spawn(move |context| async move {
                info!("upload queue worker started");

                loop {
                    queue.process_batch(&indexer).await;
                    context.sleep(queue.config.poll_interval).await;
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

        // Process positions in parallel
        let mut any_failed = false;
        let futures: Vec<_> = positions
            .iter()
            .map(|&position| async move {
                let result = self.upload_item(indexer, position).await;
                (position, result)
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        for (position, result) in results {
            match result {
                Ok(()) => {
                    self.mark_complete(position).await;
                }
                Err(e) => {
                    warn!(?e, position, "upload failed, will retry");
                    self.mark_failed(position).await;
                    any_failed = true;
                }
            }
        }

        // If any uploads failed, wait before next batch to avoid hammering indexer
        if any_failed {
            self.context.sleep(self.config.retry_delay).await;
        }

        // Prune completed entries
        if let Err(e) = self.prune().await {
            warn!(?e, "failed to prune journal");
        }
    }

    /// Upload a single item to the indexer.
    async fn upload_item<I: Indexer>(
        &self,
        indexer: &I,
        position: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let item = self.read_item(position).await?;
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

    /// Get queue statistics.
    pub async fn stats(&self) -> QueueStats {
        let journal: futures::lock::MutexGuard<'_, Journal<E, QueuedUpload>> =
            self.journal.lock().await;
        let journal_size = journal.size();
        let oldest_retained = journal.oldest_retained_pos();
        let pruning_boundary = journal.pruning_boundary();
        drop(journal);

        let in_flight = self.in_flight.lock().await.len();
        let completed_pending_prune = self.completed.lock().await.len();

        QueueStats {
            pruning_boundary,
            journal_size,
            pending: journal_size.saturating_sub(oldest_retained.unwrap_or(journal_size)),
            in_flight,
            completed_pending_prune,
            oldest_retained,
        }
    }
}

/// Statistics about the upload queue.
#[derive(Debug, Clone)]
pub struct QueueStats {
    /// Position before which all items have been pruned.
    pub pruning_boundary: u64,
    /// Total items appended to journal.
    pub journal_size: u64,
    /// Number of pending uploads (not yet completed).
    pub pending: u64,
    /// Number of uploads currently in-flight.
    pub in_flight: usize,
    /// Number of completed uploads waiting to be pruned.
    pub completed_pending_prune: usize,
    /// Oldest retained position in journal (None if empty/fully pruned).
    pub oldest_retained: Option<u64>,
}

/// A handle to the upload queue for enqueueing uploads.
///
/// This is a lightweight clone of the queue that can be passed around.
#[derive(Clone)]
pub struct QueueHandle<E: Spawner + Clock + Storage + Metrics> {
    queue: Arc<UploadQueue<E>>,
}

impl<E: Spawner + Clock + Storage + Metrics> QueueHandle<E> {
    /// Create a new queue handle.
    pub fn new(queue: Arc<UploadQueue<E>>) -> Self {
        Self { queue }
    }

    /// Enqueue a seed for upload.
    ///
    /// This awaits the journal append to ensure crash safety.
    pub async fn enqueue_seed(&self, seed: Seed) {
        if let Err(e) = self.queue.enqueue_seed(seed).await {
            error!(?e, "failed to enqueue seed");
        }
    }

    /// Enqueue a notarization for upload.
    ///
    /// This awaits the journal append to ensure crash safety.
    pub async fn enqueue_notarization(&self, notarized: Notarized) {
        if let Err(e) = self.queue.enqueue_notarization(notarized).await {
            error!(?e, "failed to enqueue notarization");
        }
    }

    /// Enqueue a finalization for upload.
    ///
    /// This awaits the journal append to ensure crash safety.
    pub async fn enqueue_finalization(&self, finalized: Finalized) {
        if let Err(e) = self.queue.enqueue_finalization(finalized).await {
            error!(?e, "failed to enqueue finalization");
        }
    }
}
