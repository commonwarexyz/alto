use super::{Decision, Entry, SharedState};
use crate::indexer::Client;
use alto_types::{Block, Scheme};
use commonware_consensus::marshal::{
    core::Mailbox as MarshalMailbox, standard::Standard, Identifier,
};
use commonware_cryptography::sha256::Digest;
use commonware_macros::select_loop;
use commonware_runtime::{
    spawn_cell, telemetry::metrics::status, Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::queue;
use commonware_utils::futures::{OptionFuture, Pool};
use std::{num::NonZeroUsize, time::Duration};
use tracing::{debug, warn};

/// Final outcome for one backfill queue entry.
enum Completion {
    /// The consumer uploaded the block itself and must mark the digest
    /// uploaded before retiring the queue entry.
    Uploaded {
        position: u64,
        height: u64,
        digest: Digest,
    },
    /// The queue entry was skipped because the live certificate path
    /// uploaded the block first.
    Skipped { position: u64, height: u64 },
}

pub struct Consumer<E: Spawner + Clock + Storage + Metrics, C: Client> {
    context: ContextCell<E>,
    client: C,
    marshal: MarshalMailbox<Scheme, Standard<Block>>,
    upload_results: status::Counter,
    uploads: SharedState,
    writer: queue::Writer<E, Entry>,
    reader: queue::Reader<E, Entry>,
    active: Pool<Completion>,
    max_active: NonZeroUsize,
    retry: Duration,
}

impl<E: Spawner + Clock + Storage + Metrics, C: Client> Consumer<E, C> {
    pub fn new(
        context: E,
        client: C,
        marshal: MarshalMailbox<Scheme, Standard<Block>>,
        uploads: SharedState,
        backfiller: (queue::Writer<E, Entry>, queue::Reader<E, Entry>),
        max_active: NonZeroUsize,
        retry: Duration,
    ) -> Self {
        let upload_results = context.register(
            "uploads",
            "Total number of finalized block upload attempt outcomes by status",
            status::Raw::default(),
        );
        let (writer, reader) = backfiller;
        Self {
            context: ContextCell::new(context),
            client,
            marshal,
            upload_results,
            uploads,
            writer,
            reader,
            active: Pool::default(),
            max_active,
            retry,
        }
    }

    /// Start the consumer loop that reads from the queue and uploads blocks.
    pub fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    async fn run(mut self) {
        select_loop! {
            self.context,
            on_start => {
                // Drain any backlog already sitting in the backfill queue before
                // blocking so restarts resume with full parallelism immediately.
                self.fill_slots().await;

                // If there is no work in flight, block for the next queue
                // row instead of polling an optional future in a tight loop.
                if self.active.is_empty() {
                    let item = self
                        .reader
                        .recv()
                        .await
                        .expect("failed to recv from finalized queue");
                    let Some((position, entry)) = item else {
                        warn!("consumer queue closed");
                        break;
                    };
                    self.start_upload(position, entry).await;
                    continue;
                }

                // Once the consumer is busy, race newly dequeued rows against
                // completions from already-running uploads.
                let item = OptionFuture::from(
                    (self.active.len() < self.max_active.get()).then(|| self.reader.recv()),
                );
            },
            on_stopped => {},
            completion = self.active.next_completed() => {
                self.complete(completion).await;
            },
            item = item => {
                match item.expect("failed to recv from finalized queue") {
                    Some((position, entry)) => {
                        self.start_upload(position, entry).await;
                    }
                    None => {
                        warn!("consumer queue closed");
                        break;
                    }
                }
            },
        }
    }

    async fn fill_slots(&mut self) {
        // Consume all queue entries that are already available without waiting so
        // the consumer keeps as many upload slots busy as it can.
        while self.active.len() < self.max_active.get() {
            let item = self
                .reader
                .try_recv()
                .await
                .expect("failed to recv from finalized queue");
            let Some((position, entry)) = item else {
                break;
            };

            self.start_upload(position, entry).await;
        }
    }

    async fn start_upload(&mut self, position: u64, entry: Entry) {
        let Entry { height, digest } = entry;
        if matches!(self.uploads.lock().should_upload(&digest), Decision::Skip) {
            self.complete(Completion::Skipped { position, height })
                .await;
            debug!(?digest, "consumer skipping already-uploaded block");
            return;
        }

        // Hand the upload/retry loop off to the in-flight pool so the consumer
        // can continue dequeuing and retiring other queue entries concurrently.
        self.active.push({
            let context = self
                .context
                .child("upload")
                .with_attribute("digest", digest)
                .with_attribute("height", height);
            let client = self.client.clone();
            let marshal = self.marshal.clone();
            let upload_results = self.upload_results.clone();
            let uploads = self.uploads.clone();
            let retry = self.retry;
            async move {
                let Some(block) =
                    Self::wait_for_uploadable_block(&context, &marshal, &uploads, digest, retry)
                        .await
                else {
                    debug!(?digest, "skipping previously uploaded block");
                    return Completion::Skipped { position, height };
                };

                loop {
                    // A live notarization/finalization upload may complete while this
                    // queue item is waiting for its block or retrying after failures.
                    let decision = {
                        let uploads = uploads.lock();
                        uploads.should_upload(&digest)
                    };
                    match decision {
                        Decision::Skip => {
                            debug!(?digest, "skipping previously uploaded block");
                            return Completion::Skipped { position, height };
                        }
                        Decision::Wait => {
                            context.sleep(retry).await;
                            continue;
                        }
                        Decision::Proceed => {}
                    }

                    match client.block_upload(block.clone()).await {
                        Ok(()) => {
                            upload_results.inc(status::Status::Success);
                            debug!(?digest, "uploaded block by digest");
                            return Completion::Uploaded {
                                position,
                                height,
                                digest,
                            };
                        }
                        Err(e) => {
                            // Keep retrying from the original queue entry. We do
                            // not ack the entry until success or until the live
                            // certificate path proves the block was uploaded.
                            upload_results.inc(status::Status::Failure);
                            warn!(?e, ?digest, "retrying block upload by digest");
                            context.sleep(retry).await;
                        }
                    }
                }
            }
        });
    }

    async fn wait_for_uploadable_block(
        context: &E,
        marshal: &MarshalMailbox<Scheme, Standard<Block>>,
        uploads: &SharedState,
        digest: Digest,
        retry: Duration,
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
                match uploads.should_upload(&digest) {
                    Decision::Skip => NextBlock::AlreadyUploaded,
                    Decision::Wait => NextBlock::WaitForCertificate,
                    Decision::Proceed => uploads
                        .cached_block(&digest)
                        .map(|block| NextBlock::Ready(Box::new(block)))
                        .unwrap_or(NextBlock::FetchFromMarshal),
                }
            };

            match next {
                NextBlock::AlreadyUploaded => return None,
                NextBlock::WaitForCertificate => {
                    context.sleep(retry).await;
                }
                NextBlock::Ready(block) => return Some(*block),
                NextBlock::FetchFromMarshal => {
                    if let Some(block) = marshal.get_block(Identifier::Digest(digest)).await {
                        uploads.lock().cache_block(block.clone());
                        return Some(block);
                    }
                    warn!(
                        ?digest,
                        "consumer could not find block in marshal, retrying"
                    );
                    context.sleep(retry).await;
                }
            }
        }
    }

    async fn complete(&mut self, completion: Completion) {
        // Record the success before acking so the in-memory dedupe tracker
        // stays aligned with the queue state.
        let (position, height) = match completion {
            Completion::Uploaded {
                position,
                height,
                digest,
            } => {
                self.uploads.lock().mark_uploaded(digest, height);
                (position, height)
            }
            Completion::Skipped { position, height } => (position, height),
        };

        // Acknowledge the queue entry and advance the queue floor if needed.
        let floor = self.reader.ack_floor().await;
        self.reader.ack(position).await.expect("failed to ack");
        let floor_advanced = self.reader.ack_floor().await > floor;
        self.writer.sync().await.expect("failed to sync after ack");
        if floor_advanced {
            self.uploads.lock().advance_queue_floor(height);
        }
    }
}
