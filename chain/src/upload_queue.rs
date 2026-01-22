//! A disk-backed upload queue for reliable delivery to the indexer.
//!
//! This module provides crash-safe upload delivery. Uploads are persisted to
//! disk (with sync) before the enqueue call returns, ensuring no data loss on
//! crash. A background worker continuously retries uploads until the indexer
//! acknowledges receipt.

use crate::indexer::Indexer;
use alto_types::{Finalized, Notarized, Seed};
use bytes::{Buf, BufMut};
use commonware_codec::{DecodeExt, Encode, EncodeSize, Error, Read, ReadExt, Write};
use commonware_runtime::{Blob, Clock, Metrics, Spawner, Storage};
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime};
use tracing::{debug, error, info, warn};

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

    fn read_cfg(reader: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let tag = u8::read(reader)?;
        match tag {
            0 => Ok(QueuedUpload::Seed(Seed::read(reader)?)),
            1 => Ok(QueuedUpload::Notarization(Notarized::read(reader)?)),
            2 => Ok(QueuedUpload::Finalization(Finalized::read(reader)?)),
            _ => Err(Error::Invalid("QueuedUpload", "unknown tag")),
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
    /// Number of times we entered backoff due to failures.
    backoff_events: Counter,
}

impl QueueMetrics {
    fn new<E: Metrics>(context: &E) -> Self {
        let metrics = Self {
            queue_depth: Gauge::default(),
            uploads_enqueued: Counter::default(),
            uploads_succeeded: Counter::default(),
            uploads_failed: Counter::default(),
            backoff_events: Counter::default(),
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
            "backoff_events",
            "Number of times backoff was triggered",
            metrics.backoff_events.clone(),
        );

        metrics
    }
}

/// Configuration for the upload queue.
#[derive(Clone)]
pub struct Config {
    /// Partition name for storing pending uploads.
    /// This will be created within the validator's configured storage directory.
    pub partition: String,
    /// Initial backoff duration for retries.
    pub initial_backoff: Duration,
    /// Maximum backoff duration for retries.
    pub max_backoff: Duration,
    /// How often to scan for pending uploads.
    pub scan_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            partition: "upload-queue".to_string(),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            scan_interval: Duration::from_millis(50),
        }
    }
}

/// Result of attempting to read a pending upload.
enum ReadPendingResult {
    /// Successfully read the data.
    Ok(Vec<u8>),
    /// Blob exists but has size 0 (likely still being written).
    EmptyBlob,
    /// Failed to open or read the blob.
    Error,
}

/// A disk-backed queue for reliable upload delivery.
///
/// Uploads are persisted to disk as individual blobs within a storage partition.
/// A background worker continuously processes the queue, retrying failed uploads
/// with exponential backoff.
pub struct UploadQueue<E: Spawner + Clock + Storage + Metrics> {
    context: E,
    config: Config,
    metrics: QueueMetrics,
    counter: AtomicU64,
    /// Global retry state - consecutive failures and next retry time (millis since epoch).
    consecutive_failures: AtomicU32,
    next_retry_ms: AtomicU64,
}

impl<E: Spawner + Clock + Storage + Metrics> UploadQueue<E> {
    /// Create a new upload queue.
    ///
    /// This will scan for any pending uploads from previous runs.
    pub async fn new(context: E, config: Config) -> Self {
        // Initialize metrics
        let metrics = QueueMetrics::new(&context);

        // Count existing pending uploads
        let pending_count = match context.scan(&config.partition).await {
            Ok(blobs) => blobs.len(),
            Err(_) => 0, // Partition doesn't exist yet, no pending uploads
        };

        if pending_count > 0 {
            info!(
                count = pending_count,
                partition = config.partition,
                "found pending uploads from previous run"
            );
            // Set initial queue depth from recovered uploads
            metrics.queue_depth.set(pending_count as i64);
        }

        Self {
            context,
            config,
            metrics,
            counter: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
            next_retry_ms: AtomicU64::new(0),
        }
    }

    /// Generate a unique blob name for an upload.
    fn generate_name(&self) -> Vec<u8> {
        let timestamp = self
            .context
            .current()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}", timestamp, counter).into_bytes()
    }

    /// Enqueue a seed for upload.
    pub async fn enqueue_seed(&self, seed: Seed) {
        self.enqueue(QueuedUpload::Seed(seed)).await;
    }

    /// Enqueue a notarization for upload.
    pub async fn enqueue_notarization(&self, notarized: Notarized) {
        self.enqueue(QueuedUpload::Notarization(notarized)).await;
    }

    /// Enqueue a finalization for upload.
    pub async fn enqueue_finalization(&self, finalized: Finalized) {
        self.enqueue(QueuedUpload::Finalization(finalized)).await;
    }

    /// Enqueue an upload.
    async fn enqueue(&self, upload: QueuedUpload) {
        let name = self.generate_name();
        let data = upload.encode().to_vec();
        self.enqueue_raw(&name, data, upload.kind_str()).await;
    }

    /// Write raw data to the queue as a blob.
    async fn enqueue_raw(&self, name: &[u8], data: Vec<u8>, kind: &str) {
        let name_str = String::from_utf8_lossy(name);

        // Open/create the blob
        let (blob, _) = match self.context.open(&self.config.partition, name).await {
            Ok(b) => b,
            Err(e) => {
                error!(?e, name = %name_str, kind, "failed to open blob for enqueue");
                return;
            }
        };

        // Resize and write
        if let Err(e) = blob.resize(data.len() as u64).await {
            error!(?e, name = %name_str, kind, "failed to resize blob");
            return;
        }

        if let Err(e) = blob.write_at(data, 0).await {
            error!(?e, name = %name_str, kind, "failed to write blob");
            return;
        }

        // Sync to ensure durability
        if let Err(e) = blob.sync().await {
            error!(?e, name = %name_str, kind, "failed to sync blob");
            return;
        }

        // Update metrics
        self.metrics.uploads_enqueued.inc();
        self.metrics.queue_depth.inc();

        debug!(name = %name_str, kind, "enqueued upload");
    }

    /// Remove a completed upload from the queue.
    async fn dequeue(&self, name: &[u8]) {
        let name_str = String::from_utf8_lossy(name);

        if let Err(e) = self
            .context
            .remove(&self.config.partition, Some(name))
            .await
        {
            warn!(?e, name = %name_str, "failed to dequeue upload");
        } else {
            self.metrics.queue_depth.dec();
            debug!(name = %name_str, "dequeued upload");
        }
    }

    /// Get all pending upload blob names.
    async fn list_pending(&self) -> Vec<Vec<u8>> {
        self.context
            .scan(&self.config.partition)
            .await
            .unwrap_or_default()
    }

    /// Read a pending upload's data.
    async fn read_pending(&self, name: &[u8]) -> ReadPendingResult {
        let (blob, size) = match self.context.open(&self.config.partition, name).await {
            Ok(b) => b,
            Err(_) => return ReadPendingResult::Error,
        };

        if size == 0 {
            return ReadPendingResult::EmptyBlob;
        }

        let buf = vec![0u8; size as usize];
        match blob.read_at(buf, 0).await {
            Ok(data) => ReadPendingResult::Ok(data.into()),
            Err(_) => ReadPendingResult::Error,
        }
    }

    /// Calculate backoff duration for a given failure count.
    ///
    /// Backoff sequence: initial, initial*2, initial*4, ... up to max_backoff.
    fn calculate_backoff(&self, failures: u32) -> Duration {
        // failures=1 → initial, failures=2 → initial*2, etc.
        let multiplier = 2u32.saturating_pow(failures.saturating_sub(1));
        let backoff = self.config.initial_backoff * multiplier;
        backoff.min(self.config.max_backoff)
    }

    /// Start the background worker that processes the queue.
    ///
    /// This spawns an async task that continuously:
    /// 1. Scans for pending uploads
    /// 2. Attempts to upload each one
    /// 3. On success, removes from queue
    /// 4. On failure, schedules retry with exponential backoff
    pub fn start_worker<I: Indexer>(self: Arc<Self>, indexer: I) {
        let queue = self.clone();
        self.context
            .with_label("upload_worker")
            .spawn(move |context| async move {
                info!("upload queue worker started");

                loop {
                    queue.process_queue(&indexer).await;
                    context.sleep(queue.config.scan_interval).await;
                }
            });
    }

    /// Process all pending uploads in the queue.
    async fn process_queue<I: Indexer>(&self, indexer: &I) {
        let now = self.context.current();
        let now_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Check global backoff - if we're in backoff period, skip this cycle
        let next_retry_ms = self.next_retry_ms.load(Ordering::Relaxed);
        if now_ms < next_retry_ms {
            return; // Still in backoff period
        }

        let pending = self.list_pending().await;
        let mut had_success = false;
        let mut had_failure = false;

        for name in pending {
            let name_str = String::from_utf8_lossy(&name).to_string();

            // Read the data
            let data = match self.read_pending(&name).await {
                ReadPendingResult::Ok(data) => data,
                ReadPendingResult::EmptyBlob => {
                    // Blob has size=0, likely still being written by enqueue_raw.
                    // Skip it and let the next scan pick it up once complete.
                    debug!(name = %name_str, "skipping empty blob (likely being written)");
                    continue;
                }
                ReadPendingResult::Error => {
                    // Failed to open or read the blob. Skip rather than delete to
                    // avoid data loss in case this is a transient error.
                    warn!(name = %name_str, "failed to read pending upload, skipping");
                    continue;
                }
            };

            // Decode the queued upload (type is encoded in the data)
            let upload = match QueuedUpload::decode(data.as_slice()) {
                Ok(upload) => upload,
                Err(e) => {
                    error!(?e, name = %name_str, "failed to decode queued upload, removing");
                    self.dequeue(&name).await;
                    continue;
                }
            };

            let kind = upload.kind_str();

            // Attempt upload
            let result = match upload {
                QueuedUpload::Seed(seed) => {
                    indexer.seed_upload(seed).await.map_err(|e| e.to_string())
                }
                QueuedUpload::Notarization(notarized) => indexer
                    .notarized_upload(notarized)
                    .await
                    .map_err(|e| e.to_string()),
                QueuedUpload::Finalization(finalized) => indexer
                    .finalized_upload(finalized)
                    .await
                    .map_err(|e| e.to_string()),
            };

            match result {
                Ok(()) => {
                    debug!(name = %name_str, kind, "upload succeeded");
                    self.metrics.uploads_succeeded.inc();
                    self.dequeue(&name).await;
                    had_success = true;
                }
                Err(e) => {
                    warn!(?e, name = %name_str, kind, "upload failed");
                    self.metrics.uploads_failed.inc();
                    had_failure = true;
                    // On first failure, enter backoff and stop processing this cycle
                    break;
                }
            }
        }

        // Update global backoff state
        if had_failure {
            let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
            let backoff = self.calculate_backoff(failures);
            let next_retry_ms = now_ms + backoff.as_millis() as u64;
            self.next_retry_ms.store(next_retry_ms, Ordering::Relaxed);
            self.metrics.backoff_events.inc();
            warn!(
                consecutive_failures = failures,
                backoff_ms = backoff.as_millis(),
                "entering backoff after upload failure"
            );
        } else if had_success {
            // Reset backoff on success
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }
    }
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
    /// This awaits the disk write to ensure crash safety.
    pub async fn enqueue_seed(&self, seed: Seed) {
        self.queue.enqueue_seed(seed).await;
    }

    /// Enqueue a notarization for upload.
    ///
    /// This awaits the disk write to ensure crash safety.
    pub async fn enqueue_notarization(&self, notarized: Notarized) {
        self.queue.enqueue_notarization(notarized).await;
    }

    /// Enqueue a finalization for upload.
    ///
    /// This awaits the disk write to ensure crash safety.
    pub async fn enqueue_finalization(&self, finalized: Finalized) {
        self.queue.enqueue_finalization(finalized).await;
    }
}
