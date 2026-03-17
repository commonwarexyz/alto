use super::{FinalizedEntry, SharedUploadState};
use alto_types::Block;
use commonware_runtime::{Clock, Metrics, Storage};
use commonware_storage::queue;

/// Records finalized block digests in the backfill queue from the application's
/// block stream.
#[derive(Clone)]
pub struct Recorder<E: Clock + Storage + Metrics> {
    uploads: SharedUploadState,
    writer: queue::Writer<E, FinalizedEntry>,
}

impl<E: Clock + Storage + Metrics> Recorder<E> {
    pub fn new(uploads: SharedUploadState, writer: queue::Writer<E, FinalizedEntry>) -> Self {
        Self { uploads, writer }
    }

    pub async fn record(&self, block: &Block) {
        let Some(entry) = self.uploads.lock().prepare_enqueue(block) else {
            return;
        };

        // Persist exactly one queue row per digest while it is pending. The
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
