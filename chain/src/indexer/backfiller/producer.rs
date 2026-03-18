use super::{Entry, SharedState};
use alto_types::Block;
use commonware_runtime::{Clock, Metrics, Storage};
use commonware_storage::queue;

/// Records finalized block digests in the backfill queue from the application's
/// block stream.
#[derive(Clone)]
pub struct Producer<E: Clock + Storage + Metrics> {
    uploads: SharedState,
    writer: queue::Writer<E, Entry>,
}

impl<E: Clock + Storage + Metrics> Producer<E> {
    pub fn new(uploads: SharedState, writer: queue::Writer<E, Entry>) -> Self {
        Self { uploads, writer }
    }

    pub async fn record(&self, block: &Block) {
        let Some(entry) = self.uploads.lock().record(block) else {
            return;
        };

        // Persist a queue entry for each finalized block that is not already
        // known uploaded. The backfiller retries from durable queue state until
        // it either uploads successfully or observes that the live certificate
        // path already uploaded the block.
        self.writer
            .enqueue(entry)
            .await
            .expect("failed to enqueue finalized digest");
    }
}
