//! A durable upload queue for reliable delivery to the indexer.
//!
//! This module uses `commonware_storage::queue` for at-least-once delivery.
//! Uploads are durably enqueued before acking callers, then uploaded by a
//! background actor. Failed uploads are retried with exponential backoff.

use crate::indexer::Indexer;
use alto_types::{Finalized, Notarized, Seed};
use bytes::{Buf, BufMut};
use commonware_codec::{EncodeSize, Error as CodecError, Read, ReadExt, Write};
use commonware_macros::select_loop;
use commonware_runtime::telemetry::metrics::status::CounterExt;
use commonware_runtime::{
    buffer::paged::CacheRef, spawn_cell, telemetry::metrics::status, BufferPooler, Clock,
    ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::{queue, queue::shared};
use commonware_utils::{
    channel::{mpsc, oneshot},
    futures::{OptionFuture, Pool},
    NZUsize, NZU16, NZU64,
};
use prometheus_client::metrics::{counter::Counter, gauge::Gauge};
use std::{
    collections::HashSet,
    num::NonZero,
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tracing::{debug, info, warn};

/// Error returned when enqueueing an upload fails.
#[derive(Debug, Clone, Error)]
pub enum EnqueueError {
    /// The upload queue actor has stopped.
    #[error("upload queue actor stopped")]
    ActorStopped,
    /// Failed to write to the persistent queue.
    #[error("queue error: {0}")]
    QueueError(String),
}

impl From<queue::Error> for EnqueueError {
    fn from(e: queue::Error) -> Self {
        EnqueueError::QueueError(e.to_string())
    }
}

/// Message sent to the upload queue actor.
struct Message {
    /// The item to enqueue.
    item: Item,
    /// Channel to send back the result (queue position or error).
    ack: oneshot::Sender<Result<u64, EnqueueError>>,
}

/// Handle for sending uploads to the queue actor.
#[derive(Clone)]
pub struct Mailbox {
    sender: mpsc::Sender<Message>,
}

impl Mailbox {
    /// Enqueue a seed for upload.
    pub async fn enqueue_seed(&self, seed: Seed) -> Result<u64, EnqueueError> {
        self.enqueue(Item::Seed(seed)).await
    }

    /// Enqueue a notarization for upload.
    pub async fn enqueue_notarization(&self, notarized: Notarized) -> Result<u64, EnqueueError> {
        self.enqueue(Item::Notarization(notarized)).await
    }

    /// Enqueue a finalization for upload.
    pub async fn enqueue_finalization(&self, finalized: Finalized) -> Result<u64, EnqueueError> {
        self.enqueue(Item::Finalization(finalized)).await
    }

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
    depth: Gauge,
    enqueued: Counter,
    uploads: status::Counter,
    ack_floor: Gauge,
}

impl QueueMetrics {
    fn new<E: Metrics>(context: &E) -> Self {
        let metrics = Self {
            depth: Gauge::default(),
            enqueued: Counter::default(),
            uploads: status::Counter::default(),
            ack_floor: Gauge::default(),
        };

        context.register(
            "depth",
            "Current number of pending uploads",
            metrics.depth.clone(),
        );
        context.register(
            "enqueued",
            "Total number of uploads enqueued",
            metrics.enqueued.clone(),
        );
        context.register(
            "uploads",
            "Total number of upload outcomes by status",
            metrics.uploads.clone(),
        );
        context.register(
            "ack_floor",
            "Items before this position have been uploaded and pruned",
            metrics.ack_floor.clone(),
        );

        metrics
    }
}

/// Configuration for the upload queue.
#[derive(Clone)]
pub struct Config {
    /// Partition name for storage.
    pub partition: String,
    /// Size of the mailbox for incoming messages.
    pub mailbox_size: usize,
    /// Maximum number of concurrent uploads.
    pub max_concurrent_uploads: usize,
    /// Base delay before retrying after a failure.
    pub retry_delay: Duration,
    /// Maximum delay between retries.
    pub max_retry_delay: Duration,
    /// Number of items per queue section.
    pub items_per_section: NonZero<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            partition: "upload-queue".to_string(),
            mailbox_size: 1024,
            max_concurrent_uploads: 8,
            retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(30),
            items_per_section: NZU64!(1024),
        }
    }
}

type UploadResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const PAGE_CACHE_PAGE_SIZE: NonZero<u16> = NZU16!(4_096); // 4KB
const PAGE_CACHE_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
const WRITE_BUFFER_SIZE: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB

/// A durable queue actor for reliable upload delivery.
pub struct Actor<E: BufferPooler + Spawner + Clock + Storage + Metrics> {
    context: E,
    config: Config,
    metrics: QueueMetrics,
    receiver: mpsc::Receiver<Message>,
    uploads: Pool<(u64, UploadResult)>,
    writer: shared::Writer<E, Item>,
    reader: shared::Reader<E, Item>,
    in_flight: HashSet<u64>,
}

impl<E: BufferPooler + Spawner + Clock + Storage + Metrics> Actor<E> {
    /// Create a new upload queue actor, recovering state from disk if available.
    pub async fn new(context: E, config: Config) -> Result<(Self, Mailbox), queue::Error> {
        let metrics = QueueMetrics::new(&context);

        let (sender, receiver) = mpsc::channel(config.mailbox_size);

        let (writer, reader) = shared::init(
            context.clone(),
            queue::Config {
                partition: config.partition.clone(),
                items_per_section: config.items_per_section,
                compression: None,
                codec_config: (),
                page_cache: CacheRef::from_pooler(
                    &context,
                    PAGE_CACHE_PAGE_SIZE,
                    PAGE_CACHE_CAPACITY,
                ),
                write_buffer: WRITE_BUFFER_SIZE,
            },
        )
        .await?;

        let queue_size = writer.size().await;
        let pruning_boundary = reader.ack_floor().await;
        let pending = queue_size.saturating_sub(pruning_boundary);

        if pending > 0 {
            info!(
                queue_size,
                pruning_boundary,
                pending,
                "recovered upload queue state (may re-upload on restart)"
            );
        }

        metrics.depth.set(pending as i64);
        metrics.ack_floor.set(pruning_boundary as i64);

        let actor = Self {
            context,
            config,
            metrics,
            receiver,
            uploads: Pool::default(),
            writer,
            reader,
            in_flight: HashSet::new(),
        };
        let mailbox = Mailbox { sender };

        Ok((actor, mailbox))
    }

    /// Start the actor and begin processing uploads.
    pub fn start<I: Indexer>(self, indexer: I) -> Handle<()> {
        let mut context = ContextCell::new(self.context.clone());
        spawn_cell!(context, self.run(indexer).await)
    }

    async fn run<I: Indexer>(mut self, indexer: I) {
        info!("upload queue actor started");

        let mut retry_count: u32 = 0;
        let mut backoff_until: Option<SystemTime> = None;

        if !self.spawn_uploads(&indexer, backoff_until).await {
            return;
        }

        select_loop! {
            self.context,
            on_stopped => {
                info!("upload queue actor shutting down");
            },
            msg = self.receiver.recv() => {
                let Some(Message { item, ack }) = msg else {
                    debug!("mailbox closed, stopping actor");
                    break;
                };

                match self.writer.enqueue(item).await {
                    Ok(position) => {
                        self.metrics.enqueued.inc();
                        self.metrics.depth.inc();
                        let _ = ack.send(Ok(position));

                        if !self.spawn_uploads(&indexer, backoff_until).await {
                            break;
                        }
                    }
                    Err(e) => {
                        // Mutable storage errors are unrecoverable for this actor instance.
                        let error = e.to_string();
                        warn!(error = %error, "fatal queue enqueue error, stopping actor");
                        let _ = ack.send(Err(EnqueueError::QueueError(error)));
                        break;
                    }
                }
            },
            (position, result) = self.uploads.next_completed() => {
                let ok = self.handle_completion(position, result, &mut retry_count, &mut backoff_until).await;
                if !ok {
                    break;
                }
                if !self.spawn_uploads(&indexer, backoff_until).await {
                    break;
                }
            },
            _ = OptionFuture::from(backoff_until.map(|until| self.context.sleep_until(until))) => {
                debug!("backoff expired, retrying uploads");
                backoff_until = None;
                if !self.spawn_uploads(&indexer, backoff_until).await {
                    break;
                }
            },
        }
    }

    async fn handle_completion(
        &mut self,
        position: u64,
        result: UploadResult,
        retry_count: &mut u32,
        backoff_until: &mut Option<SystemTime>,
    ) -> bool {
        self.in_flight.remove(&position);

        match result {
            Ok(()) => {
                if let Err(e) = self.reader.ack(position).await {
                    // Mutable storage errors are unrecoverable for this actor instance.
                    warn!(?e, position, "fatal queue ack error, stopping actor");
                    return false;
                }

                if let Err(e) = self.writer.sync().await {
                    // Mutable storage errors are unrecoverable for this actor instance.
                    warn!(?e, "fatal queue sync error, stopping actor");
                    return false;
                }

                self.metrics.uploads.inc(status::Status::Success);
                self.metrics.depth.dec();
                let ack_floor = self.reader.ack_floor().await;
                self.metrics.ack_floor.set(ack_floor as i64);

                // Only clear retry/backoff state once the queue is fully drained.
                // This avoids a successful completion canceling backoff that was
                // scheduled due to another failing position.
                let queue_size = self.writer.size().await;
                if self.in_flight.is_empty() && ack_floor >= queue_size {
                    *retry_count = 0;
                    *backoff_until = None;
                }
                true
            }
            Err(e) => {
                warn!(?e, position, "upload failed, will retry");
                self.metrics.uploads.inc(status::Status::Failure);
                // Advance backoff once per retry round (not per failed item).
                // Concurrent failures in the same round should share one delay.
                if backoff_until.is_none() {
                    let delay = self.compute_backoff_delay(*retry_count);
                    *backoff_until = Some(self.context.now() + delay);
                    *retry_count = retry_count.saturating_add(1);
                }

                // Re-deliver unacked positions from the queue floor.
                self.reader.reset().await;
                true
            }
        }
    }

    fn compute_backoff_delay(&self, retry_count: u32) -> Duration {
        let multiplier = 1u32.checked_shl(retry_count).unwrap_or(u32::MAX);
        (self.config.retry_delay * multiplier).min(self.config.max_retry_delay)
    }

    async fn spawn_uploads<I: Indexer>(
        &mut self,
        indexer: &I,
        backoff_until: Option<SystemTime>,
    ) -> bool {
        if backoff_until.is_some_and(|until| self.context.now() < until) {
            return true;
        }

        let mut slots = self
            .config
            .max_concurrent_uploads
            .saturating_sub(self.uploads.len());
        if slots == 0 {
            return true;
        }

        while slots > 0 {
            let next = match self.reader.try_recv().await {
                Ok(item) => item,
                Err(e) => {
                    // Mutable storage errors are unrecoverable for this actor instance.
                    warn!(?e, "fatal queue dequeue error, stopping actor");
                    return false;
                }
            };

            let Some((position, item)) = next else {
                break;
            };

            if !self.in_flight.insert(position) {
                continue;
            }

            let indexer = indexer.clone();
            self.uploads.push(async move {
                let result = upload_item(&indexer, item).await;
                (position, result)
            });

            slots -= 1;
        }
        true
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{Notarization, Seedable, EPOCH, NAMESPACE};
    use commonware_consensus::simplex::{
        scheme::bls12381_threshold::vrf as bls12381_threshold,
        types::{Notarize, Proposal},
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, sha256::Digest,
        Digest as _,
    };
    use commonware_macros::test_traced;
    use commonware_parallel::Sequential;
    use commonware_runtime::{deterministic::Runner, Clock, Runner as _};
    use rand::{rngs::StdRng, SeedableRng};
    use std::io;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Default)]
    struct UploadState {
        attempts: usize,
        successes: usize,
    }

    #[derive(Clone)]
    struct TestIndexer {
        state: Arc<Mutex<UploadState>>,
        remaining_failures: Arc<Mutex<usize>>,
    }

    impl TestIndexer {
        fn fail_first(state: Arc<Mutex<UploadState>>, n: usize) -> Self {
            Self {
                state,
                remaining_failures: Arc::new(Mutex::new(n)),
            }
        }

        fn always_fail(state: Arc<Mutex<UploadState>>) -> Self {
            Self::fail_first(state, usize::MAX)
        }

        fn always_ok(state: Arc<Mutex<UploadState>>) -> Self {
            Self::fail_first(state, 0)
        }
    }

    impl Indexer for TestIndexer {
        type Error = io::Error;

        async fn seed_upload(&self, _: Seed) -> Result<(), Self::Error> {
            {
                let mut state = self.state.lock().unwrap();
                state.attempts += 1;
            }

            let should_fail = {
                let mut remaining = self.remaining_failures.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };

            if should_fail {
                return Err(io::Error::other("forced failure"));
            }

            let mut state = self.state.lock().unwrap();
            state.successes += 1;
            Ok(())
        }

        async fn notarized_upload(&self, _: Notarized) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn test_seed() -> Seed {
        let mut rng = StdRng::seed_from_u64(7);
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        let proposal = Proposal::new(
            commonware_consensus::types::Round::new(
                EPOCH,
                commonware_consensus::types::View::new(1),
            ),
            commonware_consensus::types::View::new(0),
            Digest::EMPTY,
        );
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let notarization =
            Notarization::from_notarizes(&schemes[0], &notarizes, &Sequential).unwrap();
        notarization.seed()
    }

    #[test_traced]
    fn retries_after_failure() {
        let runner = Runner::timed(Duration::from_secs(10));
        runner.start(|context| async move {
            let state = Arc::new(Mutex::new(UploadState::default()));
            let (actor, mailbox) = Actor::new(
                context.with_label("queue"),
                Config {
                    partition: "upload-queue-retry".to_string(),
                    retry_delay: Duration::from_millis(1),
                    max_retry_delay: Duration::from_millis(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            actor.start(TestIndexer::fail_first(state.clone(), 1));
            mailbox.enqueue_seed(test_seed()).await.unwrap();

            let deadline = context.current() + Duration::from_millis(200);
            loop {
                let done = {
                    let state = state.lock().unwrap();
                    state.successes >= 1 && state.attempts >= 2
                };
                if done {
                    break;
                }
                assert!(context.current() < deadline, "upload did not retry in time");
                context.sleep(Duration::from_millis(5)).await;
            }
        });
    }

    #[test_traced]
    fn replays_pending_upload_after_restart() {
        let state = Arc::new(Mutex::new(UploadState::default()));
        let mut checkpoint = None;

        for phase in 0..2 {
            let shared = state.clone();
            let partition = "upload-queue-restart".to_string();
            let (complete, recovered) = if let Some(prev) = checkpoint {
                Runner::from(prev)
            } else {
                Runner::timed(Duration::from_secs(10))
            }
            .start_and_recover(move |context| async move {
                let (actor, mailbox) = Actor::new(
                    context.with_label("queue"),
                    Config {
                        partition,
                        retry_delay: Duration::from_millis(1),
                        max_retry_delay: Duration::from_millis(1),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

                if phase == 0 {
                    actor.start(TestIndexer::always_fail(shared));
                    mailbox.enqueue_seed(test_seed()).await.unwrap();
                    context.sleep(Duration::from_millis(20)).await;
                    false
                } else {
                    actor.start(TestIndexer::always_ok(shared.clone()));
                    let deadline = context.current() + Duration::from_millis(250);
                    loop {
                        if shared.lock().unwrap().successes >= 1 {
                            return true;
                        }
                        if context.current() >= deadline {
                            return false;
                        }
                        context.sleep(Duration::from_millis(5)).await;
                    }
                }
            });

            if complete {
                break;
            }
            checkpoint = Some(recovered);
        }

        let state = state.lock().unwrap();
        assert!(state.successes >= 1, "pending upload was not replayed");
    }
}
