use super::{Entry, SharedState};
use alto_types::Block;
use commonware_consensus::marshal::Update;
use commonware_runtime::{BufferPooler, Clock, Metrics, Spawner, Storage};
use commonware_storage::queue;
use commonware_utils::Acknowledgement;

/// Records finalized block digests in the backfill queue from the application's
/// block stream.
pub struct Producer<E: Clock + Storage + Metrics + Spawner + BufferPooler> {
    context: E,
    uploads: SharedState,
    writer: queue::Writer<E, Entry>,
}

impl<E: Clock + Storage + Metrics + Spawner + BufferPooler> Clone for Producer<E> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.child("producer"),
            uploads: self.uploads.clone(),
            writer: self.writer.clone(),
        }
    }
}

impl<E: Clock + Storage + Metrics + Spawner + BufferPooler> Producer<E> {
    pub fn new(context: E, uploads: SharedState, writer: queue::Writer<E, Entry>) -> Self {
        Self {
            context,
            uploads,
            writer,
        }
    }

    pub fn record(&self, update: Update<Block>) {
        self.context.child("record").spawn({
            let uploads = self.uploads.clone();
            let writer = self.writer.clone();
            move |_| async move {
                let Update::Block(block, ack) = update else {
                    return;
                };
                let Some(entry) = uploads.lock().record(&block) else {
                    ack.acknowledge();
                    return;
                };

                // Persist a queue entry before acking so the backfiller retries
                // from durable queue state after restarts.
                writer
                    .enqueue(entry)
                    .await
                    .expect("failed to enqueue finalized digest");
                ack.acknowledge();
            }
        });
    }
}
