use super::{DrainerMetrics, FinalizedEntry, SharedUploadState};
use crate::indexer::Indexer;
use alto_types::{Block, Scheme};
use commonware_consensus::marshal::{
    core::Mailbox as MarshalMailbox, standard::Standard, Identifier,
};
use commonware_cryptography::sha256::Digest;
use commonware_macros::select_loop;
use commonware_runtime::{
    telemetry::metrics::status::{self, CounterExt},
    Clock, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::queue;
use commonware_utils::futures::{OptionFuture, Pool};
use std::time::Duration;
use tracing::{debug, warn};

const DRAINER_MAX_IN_FLIGHT: usize = 16;
const DRAINER_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Final outcome for one durable queue row.
///
/// `digest` is `Some` only when this row's raw block upload actually succeeded.
/// When it is `None`, the row is being retired because it was duplicate work or
/// because the live certificate path already uploaded the block first.
struct DrainCompletion {
    position: u64,
    height: u64,
    digest: Option<Digest>,
    counted_in_flight: bool,
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
    pub(crate) fn start(self, reader: queue::Reader<E, FinalizedEntry>) -> Handle<()> {
        let Self {
            context,
            indexer,
            marshal,
            metrics,
            uploads,
            writer,
        } = self;
        context
            .with_label("drainer")
            .spawn(move |context| async move {
                DrainerRunner {
                    context,
                    indexer,
                    marshal,
                    metrics,
                    uploads,
                    writer,
                    reader,
                    in_flight: Pool::default(),
                    queue_closed: false,
                }
                .run()
                .await;
            })
    }
}

/// Owns the mutable state of the drainer event loop so the queue-processing
/// steps can be expressed as instance methods rather than long helper calls.
struct DrainerRunner<E: Spawner + Clock + Storage + Metrics, I: Indexer> {
    context: E,
    indexer: I,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    metrics: DrainerMetrics,
    uploads: SharedUploadState,
    writer: queue::Writer<E, FinalizedEntry>,
    reader: queue::Reader<E, FinalizedEntry>,
    in_flight: Pool<DrainCompletion>,
    queue_closed: bool,
}

impl<E: Spawner + Clock + Storage + Metrics, I: Indexer> DrainerRunner<E, I> {
    async fn run(mut self) {
        select_loop! {
            self.context,
            on_start => {
                // Drain any backlog already sitting in the durable queue before
                // blocking so restarts resume with full parallelism immediately.
                self.fill_drainer_slots().await;

                if self.queue_closed && self.in_flight.is_empty() {
                    warn!("drainer queue closed");
                    break;
                }

                if self.in_flight.is_empty() {
                    // If there is no work in flight, block for the next queue
                    // row instead of polling an optional future in a tight loop.
                    let item = self
                        .reader
                        .recv()
                        .await
                        .expect("failed to recv from finalized queue");
                    let Some((position, entry)) = item else {
                        self.queue_closed = true;
                        continue;
                    };
                    self.start_drained_upload(position, entry).await;
                    continue;
                }

                // Once the drainer is busy, race newly dequeued rows against
                // completions from already-running uploads.
                let item = OptionFuture::from(
                    (!self.queue_closed && self.in_flight.len() < DRAINER_MAX_IN_FLIGHT)
                        .then(|| self.reader.recv()),
                );
            },
            on_stopped => {},
            completion = self.in_flight.next_completed() => {
                self.complete_drained(completion).await;
            },
            item = item => {
                match item.expect("failed to recv from finalized queue") {
                    Some((position, entry)) => {
                        self.start_drained_upload(position, entry).await;
                    }
                    None => {
                        self.queue_closed = true;
                    }
                }
            },
        }
    }

    async fn fill_drainer_slots(&mut self) {
        // Consume all queue rows that are already available without waiting so
        // the drainer keeps as many upload slots busy as it can.
        while self.in_flight.len() < DRAINER_MAX_IN_FLIGHT {
            let item = self
                .reader
                .try_recv()
                .await
                .expect("failed to recv from finalized queue");
            let Some((position, entry)) = item else {
                break;
            };

            self.start_drained_upload(position, entry).await;
        }
    }

    async fn start_drained_upload(&mut self, position: u64, entry: FinalizedEntry) {
        let FinalizedEntry { height, digest } = entry;
        let skip = {
            let mut uploads = self.uploads.lock();
            // Re-register every dequeued row in shared state before deciding
            // what to do with it. That keeps crash recovery idempotent: replayed
            // rows rebuild the same pending state the original process had.
            if uploads.register_finalized(position, entry) {
                Some("drainer skipping duplicate queued block")
            } else if uploads.contains(&digest) {
                Some("drainer skipping already-uploaded block")
            } else {
                None
            }
        };
        if let Some(reason) = skip {
            self.complete_drained(DrainCompletion {
                position,
                height,
                digest: None,
                counted_in_flight: false,
            })
            .await;
            debug!(?digest, reason);
            return;
        }

        // Hand the upload/retry loop off to the in-flight pool so the drainer
        // can continue dequeuing and retiring other durable rows concurrently.
        self.in_flight.push({
            let indexer = self.indexer.clone();
            let marshal = self.marshal.clone();
            let context = self.context.with_label("upload");
            let metrics = self.metrics.clone();
            let uploads = self.uploads.clone();
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
                // Count only active raw block upload attempts. Waiting for a
                // block to appear in cache/marshal is not yet an in-flight upload.
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

    async fn complete_drained(&mut self, completion: DrainCompletion) {
        if completion.counted_in_flight {
            self.metrics.in_flight.dec();
        }
        if let Some(digest) = completion.digest {
            // Record the success before acking so the in-memory dedupe tracker
            // stays aligned with the durable queue state.
            self.uploads.lock().mark_uploaded(digest, completion.height);
        }

        // Persist retirement of the durable row before dropping the corresponding
        // pending state from memory so replay after a crash sees a consistent
        // queue/UploadState pairing.
        self.reader
            .ack(completion.position)
            .await
            .expect("failed to ack");
        self.writer.sync().await.expect("failed to sync after ack");
        self.metrics.depth.dec();
        self.uploads.lock().finish_finalized(completion.position);
    }
}
