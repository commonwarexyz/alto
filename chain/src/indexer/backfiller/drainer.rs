use super::{FinalizedEntry, SharedUploadState, UploadDecision};
use crate::indexer::Client;
use alto_types::{Block, Scheme};
use commonware_consensus::marshal::{
    core::Mailbox as MarshalMailbox, standard::Standard, Identifier,
};
use commonware_cryptography::sha256::Digest;
use commonware_macros::select_loop;
use commonware_runtime::{
    spawn_cell,
    telemetry::metrics::status::{self, CounterExt},
    Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::queue;
use commonware_utils::futures::{OptionFuture, Pool};
use prometheus_client::metrics::gauge::Gauge;
use std::time::Duration;
use tracing::{debug, warn};

const DRAINER_MAX_IN_FLIGHT: usize = 16;
const DRAINER_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Final outcome for one backfill queue row.
enum DrainCompletion {
    /// The drainer uploaded the block itself and must mark the digest
    /// uploaded before retiring the queue row.
    Uploaded {
        position: u64,
        height: u64,
        digest: Digest,
    },
    /// The queue row became redundant because it was duplicate work or
    /// because the live certificate path uploaded the block first.
    Retired { position: u64 },
}

pub struct Drainer<E: Spawner + Clock + Storage + Metrics, C: Client> {
    context: ContextCell<E>,
    client: C,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    upload_results: status::Counter,
    in_flight_uploads: Gauge,
    uploads: SharedUploadState,
    writer: queue::Writer<E, FinalizedEntry>,
    reader: queue::Reader<E, FinalizedEntry>,
    in_flight: Pool<DrainCompletion>,
    queue_closed: bool,
}

impl<E: Spawner + Clock + Storage + Metrics, C: Client> Drainer<E, C> {
    pub fn new(
        context: E,
        client: C,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        uploads: SharedUploadState,
        writer: queue::Writer<E, FinalizedEntry>,
        reader: queue::Reader<E, FinalizedEntry>,
    ) -> Self {
        let queue_metrics = context.with_label("queue");
        let upload_results = status::Counter::default();
        queue_metrics.register(
            "uploads",
            "Total number of finalized block upload attempt outcomes by status",
            upload_results.clone(),
        );
        let in_flight_uploads = Gauge::default();
        queue_metrics.register(
            "in_flight",
            "Current number of occupied upload slots in the backfiller",
            in_flight_uploads.clone(),
        );
        Self {
            context: ContextCell::new(context.with_label("drainer")),
            client,
            marshal,
            upload_results,
            in_flight_uploads,
            uploads,
            writer,
            reader,
            in_flight: Pool::default(),
            queue_closed: false,
        }
    }

    /// Start the drainer loop that reads from the queue and uploads blocks.
    pub fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run().await)
    }

    async fn run(mut self) {
        select_loop! {
            self.context,
            on_start => {
                // Drain any backlog already sitting in the backfill queue before
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
            } else if matches!(uploads.upload_decision(&digest), UploadDecision::Retire) {
                Some("drainer skipping already-uploaded block")
            } else {
                None
            }
        };
        if let Some(reason) = skip {
            self.complete_drained(DrainCompletion::Retired { position })
                .await;
            debug!(?digest, reason);
            return;
        }

        // Hand the upload/retry loop off to the in-flight pool so the drainer
        // can continue dequeuing and retiring other queue rows concurrently.
        self.in_flight_uploads.inc();
        self.in_flight.push({
            let client = self.client.clone();
            let marshal = self.marshal.clone();
            let context = self.context.with_label("upload");
            let upload_results = self.upload_results.clone();
            let uploads = self.uploads.clone();
            async move {
                let Some(block) =
                    Self::wait_for_uploadable_block(&context, &marshal, &uploads, digest).await
                else {
                    debug!(?digest, "drainer observed live upload before block upload");
                    return DrainCompletion::Retired { position };
                };

                loop {
                    // A live notarization/finalization upload may complete while this
                    // queue item is waiting for its block or retrying after failures.
                    let decision = {
                        let uploads = uploads.lock();
                        uploads.upload_decision(&digest)
                    };
                    match decision {
                        UploadDecision::Retire => {
                            debug!(?digest, "drainer observed live upload before block upload");
                            return DrainCompletion::Retired { position };
                        }
                        UploadDecision::Wait => {
                            context.sleep(DRAINER_RETRY_DELAY).await;
                            continue;
                        }
                        UploadDecision::Proceed => {}
                    }

                    match client.block_upload(block.clone()).await {
                        Ok(()) => {
                            upload_results.inc(status::Status::Success);
                            debug!(?digest, "drainer uploaded block");
                            return DrainCompletion::Uploaded {
                                position,
                                height,
                                digest,
                            };
                        }
                        Err(e) => {
                            // Keep retrying from the original queue row. We do
                            // not ack the row until success or until the live
                            // certificate path proves the block was uploaded.
                            upload_results.inc(status::Status::Failure);
                            warn!(?e, ?digest, "drainer failed to upload block, retrying");
                            context.sleep(DRAINER_RETRY_DELAY).await;
                        }
                    }
                }
            }
        });
    }

    async fn wait_for_uploadable_block(
        context: &ContextCell<E>,
        marshal: &MarshalMailbox<Scheme, Standard<Block>>,
        uploads: &SharedUploadState,
        digest: Digest,
    ) -> Option<Block> {
        // Prefer the in-process block cache populated by the application and
        // certificate uploaders. On restart that cache is empty, so we fall
        // back to marshal storage. If a certificate upload is still in flight,
        // wait for it to either succeed or fail before starting the block upload.
        enum NextBlock {
            AlreadyUploaded,
            WaitForCertificate,
            Ready(Box<Block>),
            FetchFromMarshal,
        }

        loop {
            let next = {
                let uploads = uploads.lock();
                match uploads.upload_decision(&digest) {
                    UploadDecision::Retire => NextBlock::AlreadyUploaded,
                    UploadDecision::Wait => NextBlock::WaitForCertificate,
                    UploadDecision::Proceed => {
                        if let Some(block) = uploads.cached_block(&digest) {
                            NextBlock::Ready(Box::new(block))
                        } else {
                            NextBlock::FetchFromMarshal
                        }
                    }
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
        self.in_flight_uploads.dec();
        let position = match completion {
            // Record the success before acking so the in-memory dedupe tracker
            // stays aligned with the queue state.
            DrainCompletion::Uploaded {
                position,
                height,
                digest,
            } => {
                self.uploads.lock().mark_uploaded(digest, height);
                position
            }
            DrainCompletion::Retired { position } => position,
        };

        // Persist retirement of the queue row before dropping the corresponding
        // pending state from memory so replay after a crash sees a consistent
        // queue/UploadState pairing.
        self.reader.ack(position).await.expect("failed to ack");
        self.writer.sync().await.expect("failed to sync after ack");
        self.uploads.lock().finish_finalized(position);
    }
}
