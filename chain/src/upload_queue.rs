//! A disk-backed upload queue for reliable delivery to the indexer.
//!
//! This module provides crash-safe, fully async upload delivery. Uploads are
//! persisted to disk before being sent, and a background worker retries until
//! the indexer acknowledges receipt.

use crate::indexer::Indexer;
use alto_types::{Finalized, Notarized, Seed};
use commonware_codec::{DecodeExt, Encode};
use commonware_runtime::{Clock, Metrics, Spawner};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
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
    /// Directory where pending uploads are stored.
    pub queue_dir: PathBuf,
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
            queue_dir: PathBuf::from("upload_queue"),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            scan_interval: Duration::from_millis(50),
        }
    }
}

/// Tracks retry state for a pending upload.
struct RetryState {
    attempts: u32,
    next_retry: Instant,
}

/// A disk-backed queue for reliable upload delivery.
///
/// Uploads are persisted to disk as individual files. A background worker
/// continuously processes the queue, retrying failed uploads with exponential
/// backoff.
pub struct UploadQueue<E: Spawner + Clock + Metrics> {
    context: E,
    config: Config,
    counter: AtomicU64,
    /// Tracks retry state for pending uploads (keyed by filename).
    retry_state: Mutex<HashMap<String, RetryState>>,
}

impl<E: Spawner + Clock + Metrics> UploadQueue<E> {
    /// Create a new upload queue.
    ///
    /// This will create the queue directory if it doesn't exist.
    pub fn new(context: E, config: Config) -> Self {
        // Create queue directory if it doesn't exist
        if let Err(e) = fs::create_dir_all(&config.queue_dir) {
            warn!(?e, path = ?config.queue_dir, "failed to create queue directory");
        }

        // Count existing pending uploads
        let pending_count = fs::read_dir(&config.queue_dir)
            .map(|entries| entries.count())
            .unwrap_or(0);

        if pending_count > 0 {
            info!(
                count = pending_count,
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

    /// Generate a unique filename for an upload.
    fn generate_filename(&self, kind: UploadKind) -> String {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}-{}", timestamp, counter, kind.as_str())
    }

    /// Parse the kind from a filename.
    fn parse_filename(filename: &str) -> Option<UploadKind> {
        // Format: {timestamp}-{counter}-{kind}
        let kind_str = filename.rsplit('-').next()?;
        UploadKind::from_str(kind_str)
    }

    /// Enqueue a seed for upload.
    pub fn enqueue_seed(&self, seed: Seed) {
        let filename = self.generate_filename(UploadKind::Seed);
        let data = seed.encode().to_vec();
        self.enqueue_raw(&filename, data);
    }

    /// Enqueue a notarization for upload.
    pub fn enqueue_notarization(&self, notarized: Notarized) {
        let filename = self.generate_filename(UploadKind::Notarization);
        let data = notarized.encode().to_vec();
        self.enqueue_raw(&filename, data);
    }

    /// Enqueue a finalization for upload.
    pub fn enqueue_finalization(&self, finalized: Finalized) {
        let filename = self.generate_filename(UploadKind::Finalization);
        let data = finalized.encode().to_vec();
        self.enqueue_raw(&filename, data);
    }

    /// Write raw data to the queue.
    fn enqueue_raw(&self, filename: &str, data: Vec<u8>) {
        let path = self.config.queue_dir.join(filename);
        if let Err(e) = fs::write(&path, data) {
            error!(?e, ?path, "failed to enqueue upload");
        } else {
            debug!(?path, "enqueued upload");
        }
    }

    /// Remove a completed upload from the queue.
    fn dequeue(&self, filename: &str) {
        let path = self.config.queue_dir.join(filename);
        if let Err(e) = fs::remove_file(&path) {
            warn!(?e, ?path, "failed to dequeue upload");
        } else {
            // Clean up retry state
            if let Ok(mut state) = self.retry_state.lock() {
                state.remove(filename);
            }
            debug!(?path, "dequeued upload");
        }
    }

    /// Get all pending upload filenames.
    fn list_pending(&self) -> Vec<String> {
        match fs::read_dir(&self.config.queue_dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().into_string().ok())
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Read a pending upload's data.
    fn read_pending(&self, filename: &str) -> Option<Vec<u8>> {
        let path = self.config.queue_dir.join(filename);
        fs::read(&path).ok()
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
        let pending = self.list_pending();
        let now = Instant::now();

        for filename in pending {
            // Check if we should retry this upload yet
            {
                if let Ok(state) = self.retry_state.lock() {
                    if let Some(rs) = state.get(&filename) {
                        if now < rs.next_retry {
                            continue; // Not ready to retry yet
                        }
                    }
                }
            }

            // Parse the kind from filename
            let Some(kind) = Self::parse_filename(&filename) else {
                warn!(filename, "invalid upload filename, skipping");
                continue;
            };

            // Read the data
            let Some(data) = self.read_pending(&filename) else {
                warn!(filename, "failed to read pending upload");
                continue;
            };

            // Attempt upload
            let result = match kind {
                UploadKind::Seed => match Seed::decode(data.as_slice()) {
                    Ok(seed) => indexer.seed_upload(seed).await.map_err(|e| e.to_string()),
                    Err(e) => {
                        error!(?e, filename, "failed to decode seed, removing");
                        self.dequeue(&filename);
                        continue;
                    }
                },
                UploadKind::Notarization => match Notarized::decode(data.as_slice()) {
                    Ok(notarized) => indexer
                        .notarized_upload(notarized)
                        .await
                        .map_err(|e| e.to_string()),
                    Err(e) => {
                        error!(?e, filename, "failed to decode notarization, removing");
                        self.dequeue(&filename);
                        continue;
                    }
                },
                UploadKind::Finalization => match Finalized::decode(data.as_slice()) {
                    Ok(finalized) => indexer
                        .finalized_upload(finalized)
                        .await
                        .map_err(|e| e.to_string()),
                    Err(e) => {
                        error!(?e, filename, "failed to decode finalization, removing");
                        self.dequeue(&filename);
                        continue;
                    }
                },
            };

            match result {
                Ok(()) => {
                    debug!(filename, "upload succeeded");
                    self.dequeue(&filename);
                }
                Err(e) => {
                    // Update retry state
                    if let Ok(mut state) = self.retry_state.lock() {
                        let rs = state.entry(filename.clone()).or_insert(RetryState {
                            attempts: 0,
                            next_retry: now,
                        });
                        rs.attempts += 1;
                        let backoff = self.calculate_backoff(rs.attempts);
                        rs.next_retry = now + backoff;

                        warn!(
                            ?e,
                            filename,
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
pub struct QueueHandle<E: Spawner + Clock + Metrics> {
    queue: Arc<UploadQueue<E>>,
}

impl<E: Spawner + Clock + Metrics> QueueHandle<E> {
    /// Create a new queue handle.
    pub fn new(queue: Arc<UploadQueue<E>>) -> Self {
        Self { queue }
    }

    /// Enqueue a seed for upload.
    pub fn enqueue_seed(&self, seed: Seed) {
        self.queue.enqueue_seed(seed);
    }

    /// Enqueue a notarization for upload.
    pub fn enqueue_notarization(&self, notarized: Notarized) {
        self.queue.enqueue_notarization(notarized);
    }

    /// Enqueue a finalization for upload.
    pub fn enqueue_finalization(&self, finalized: Finalized) {
        self.queue.enqueue_finalization(finalized);
    }
}
