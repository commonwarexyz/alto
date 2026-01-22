//! A disk-backed upload queue for reliable delivery to the indexer.
//!
//! This module provides crash-safe, fully async upload delivery. Uploads are
//! persisted to disk before being sent, and a background worker retries until
//! the indexer acknowledges receipt.

use crate::indexer::Indexer;
use alto_types::{Finalized, Notarized, Seed};
use commonware_codec::{DecodeExt, Encode};
use commonware_runtime::{Blob, Clock, Metrics, Spawner, Storage};
use std::sync::Arc;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, SystemTime},
};
use tracing::{debug, error, info, warn};

/// The kind of upload (determines which endpoint to use).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadKind {
    Seed,
    Notarization,
    Finalization,
}

impl UploadKind {
    fn as_str(&self) -> &'static str {
        match self {
            UploadKind::Seed => "seed",
            UploadKind::Notarization => "notarization",
            UploadKind::Finalization => "finalization",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "seed" => Some(UploadKind::Seed),
            "notarization" => Some(UploadKind::Notarization),
            "finalization" => Some(UploadKind::Finalization),
            _ => None,
        }
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

/// Tracks retry state for a pending upload.
struct RetryState {
    attempts: u32,
    next_retry: SystemTime,
}

/// A disk-backed queue for reliable upload delivery.
///
/// Uploads are persisted to disk as individual blobs within a storage partition.
/// A background worker continuously processes the queue, retrying failed uploads
/// with exponential backoff.
pub struct UploadQueue<E: Spawner + Clock + Storage + Metrics> {
    context: E,
    config: Config,
    counter: AtomicU64,
    /// Tracks retry state for pending uploads (keyed by blob name).
    retry_state: Mutex<HashMap<String, RetryState>>,
}

impl<E: Spawner + Clock + Storage + Metrics> UploadQueue<E> {
    /// Create a new upload queue.
    ///
    /// This will scan for any pending uploads from previous runs.
    pub async fn new(context: E, config: Config) -> Self {
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
        }

        Self {
            context,
            config,
            counter: AtomicU64::new(0),
            retry_state: Mutex::new(HashMap::new()),
        }
    }

    /// Generate a unique blob name for an upload.
    fn generate_name(&self, kind: UploadKind) -> Vec<u8> {
        let timestamp = self
            .context
            .current()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}-{}", timestamp, counter, kind.as_str()).into_bytes()
    }

    /// Parse the kind from a blob name.
    fn parse_name(name: &[u8]) -> Option<UploadKind> {
        let name_str = std::str::from_utf8(name).ok()?;
        // Format: {timestamp}-{counter}-{kind}
        let kind_str = name_str.rsplit('-').next()?;
        UploadKind::from_str(kind_str)
    }

    /// Enqueue a seed for upload.
    pub async fn enqueue_seed(&self, seed: Seed) {
        let name = self.generate_name(UploadKind::Seed);
        let data = seed.encode().to_vec();
        self.enqueue_raw(&name, data).await;
    }

    /// Enqueue a notarization for upload.
    pub async fn enqueue_notarization(&self, notarized: Notarized) {
        let name = self.generate_name(UploadKind::Notarization);
        let data = notarized.encode().to_vec();
        self.enqueue_raw(&name, data).await;
    }

    /// Enqueue a finalization for upload.
    pub async fn enqueue_finalization(&self, finalized: Finalized) {
        let name = self.generate_name(UploadKind::Finalization);
        let data = finalized.encode().to_vec();
        self.enqueue_raw(&name, data).await;
    }

    /// Write raw data to the queue as a blob.
    async fn enqueue_raw(&self, name: &[u8], data: Vec<u8>) {
        let name_str = String::from_utf8_lossy(name);

        // Open/create the blob
        let (blob, _) = match self.context.open(&self.config.partition, name).await {
            Ok(b) => b,
            Err(e) => {
                error!(?e, name = %name_str, "failed to open blob for enqueue");
                return;
            }
        };

        // Resize and write
        if let Err(e) = blob.resize(data.len() as u64).await {
            error!(?e, name = %name_str, "failed to resize blob");
            return;
        }

        if let Err(e) = blob.write_at(data, 0).await {
            error!(?e, name = %name_str, "failed to write blob");
            return;
        }

        // Sync to ensure durability
        if let Err(e) = blob.sync().await {
            error!(?e, name = %name_str, "failed to sync blob");
            return;
        }

        debug!(name = %name_str, "enqueued upload");
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
            // Clean up retry state
            if let Ok(mut state) = self.retry_state.lock() {
                state.remove(name_str.as_ref());
            }
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
    async fn read_pending(&self, name: &[u8]) -> Option<Vec<u8>> {
        let (blob, size) = self.context.open(&self.config.partition, name).await.ok()?;

        if size == 0 {
            return None;
        }

        let buf = vec![0u8; size as usize];
        let result = blob.read_at(buf, 0).await.ok()?;
        Some(result.into())
    }

    /// Calculate backoff duration for a given attempt count.
    fn calculate_backoff(&self, attempts: u32) -> Duration {
        let backoff = self.config.initial_backoff * 2u32.saturating_pow(attempts);
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
            .spawn(move |context| {
                let queue = queue.clone();
                let indexer = indexer.clone();
                async move {
                    info!("upload queue worker started");

                    loop {
                        queue.process_queue(&indexer).await;
                        context.sleep(queue.config.scan_interval).await;
                    }
                }
            });
    }

    /// Process all pending uploads in the queue.
    async fn process_queue<I: Indexer>(&self, indexer: &I) {
        let pending = self.list_pending().await;
        let now = self.context.current();

        for name in pending {
            let name_str = String::from_utf8_lossy(&name).to_string();

            // Check if we should retry this upload yet
            {
                if let Ok(state) = self.retry_state.lock() {
                    if let Some(rs) = state.get(&name_str) {
                        if now < rs.next_retry {
                            continue; // Not ready to retry yet
                        }
                    }
                }
            }

            // Parse the kind from blob name
            let Some(kind) = Self::parse_name(&name) else {
                warn!(name = %name_str, "invalid upload blob name, skipping");
                continue;
            };

            // Read the data
            let Some(data) = self.read_pending(&name).await else {
                warn!(name = %name_str, "failed to read pending upload");
                continue;
            };

            // Attempt upload
            let result = match kind {
                UploadKind::Seed => match Seed::decode(data.as_slice()) {
                    Ok(seed) => indexer.seed_upload(seed).await.map_err(|e| e.to_string()),
                    Err(e) => {
                        error!(?e, name = %name_str, "failed to decode seed, removing");
                        self.dequeue(&name).await;
                        continue;
                    }
                },
                UploadKind::Notarization => match Notarized::decode(data.as_slice()) {
                    Ok(notarized) => indexer
                        .notarized_upload(notarized)
                        .await
                        .map_err(|e| e.to_string()),
                    Err(e) => {
                        error!(?e, name = %name_str, "failed to decode notarization, removing");
                        self.dequeue(&name).await;
                        continue;
                    }
                },
                UploadKind::Finalization => match Finalized::decode(data.as_slice()) {
                    Ok(finalized) => indexer
                        .finalized_upload(finalized)
                        .await
                        .map_err(|e| e.to_string()),
                    Err(e) => {
                        error!(?e, name = %name_str, "failed to decode finalization, removing");
                        self.dequeue(&name).await;
                        continue;
                    }
                },
            };

            match result {
                Ok(()) => {
                    debug!(name = %name_str, "upload succeeded");
                    self.dequeue(&name).await;
                }
                Err(e) => {
                    // Update retry state
                    if let Ok(mut state) = self.retry_state.lock() {
                        let rs = state.entry(name_str.clone()).or_insert(RetryState {
                            attempts: 0,
                            next_retry: now,
                        });
                        rs.attempts += 1;
                        let backoff = self.calculate_backoff(rs.attempts);
                        rs.next_retry = now + backoff;

                        warn!(
                            ?e,
                            name = %name_str,
                            attempts = rs.attempts,
                            next_retry_ms = backoff.as_millis(),
                            "upload failed, will retry"
                        );
                    }
                }
            }
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
    pub fn enqueue_seed(&self, seed: Seed) {
        let queue = self.queue.clone();
        // Spawn the async enqueue operation
        self.queue.context.with_label("enqueue_seed").spawn({
            move |_| async move {
                queue.enqueue_seed(seed).await;
            }
        });
    }

    /// Enqueue a notarization for upload.
    pub fn enqueue_notarization(&self, notarized: Notarized) {
        let queue = self.queue.clone();
        self.queue
            .context
            .with_label("enqueue_notarization")
            .spawn({
                move |_| async move {
                    queue.enqueue_notarization(notarized).await;
                }
            });
    }

    /// Enqueue a finalization for upload.
    pub fn enqueue_finalization(&self, finalized: Finalized) {
        let queue = self.queue.clone();
        self.queue
            .context
            .with_label("enqueue_finalization")
            .spawn({
                move |_| async move {
                    queue.enqueue_finalization(finalized).await;
                }
            });
    }
}
