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
        let Some(entry) = self.uploads.lock().prepare_enqueue(block) else {
            return;
        };

        // Persist exactly one queue entry per digest while it is pending. The
        // backfiller retries from this row until it either uploads successfully or
        // observes that the live certificate path already uploaded the block.
        let position = self
            .writer
            .enqueue(entry)
            .await
            .expect("failed to enqueue finalized digest");
        let _ = self.uploads.lock().register_finalized(position, entry);
        self.writer
            .sync()
            .await
            .expect("failed to sync after enqueue");
    }
}
