use super::{Entry, SharedState};
use alto_types::Block;
use commonware_actor::{
    mailbox::{self, Policy},
    Feedback,
};
use commonware_consensus::{marshal::Update, Reporter};
use commonware_runtime::{
    spawn_cell, BufferPooler, Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::queue;
use commonware_utils::{acknowledgement::Exact, channel::oneshot, Acknowledgement};
use std::{collections::VecDeque, num::NonZeroUsize, sync::Arc};

/// Records finalized block digests in the backfill queue from the application's
/// block stream.
#[derive(Clone)]
pub struct Producer {
    sender: mailbox::Sender<Message>,
}

enum Message {
    // Hold the marshal ack until the finalized block is durably queued.
    Block { block: Arc<Block>, ack: Exact },
    // Complete only after the reader's acknowledgements have been synced.
    Sync(oneshot::Sender<()>),
}

impl Policy for Message {
    type Overflow = VecDeque<Self>;

    fn handle(overflow: &mut Self::Overflow, message: Self) {
        overflow.push_back(message);
    }
}

struct Actor<E: Clock + Storage + Metrics + BufferPooler> {
    context: ContextCell<E>,
    uploads: SharedState,
    queue: queue::Queue<E, Entry>,
    receiver: mailbox::Receiver<Message>,
}

impl Reporter for Producer {
    type Activity = Update<Block>;

    fn report(&mut self, activity: Self::Activity) -> Feedback {
        match activity {
            Update::Block(block, ack) => self.sender.enqueue(Message::Block { block, ack }),
            Update::Tip(_, _, _) => Feedback::Ok,
        }
    }
}

impl Producer {
    pub(super) async fn sync(&self) {
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.enqueue(Message::Sync(sender));
        receiver.await.expect("failed to sync finalized queue");
    }
}

impl<E: Clock + Storage + Metrics + Spawner + BufferPooler> Actor<E> {
    pub fn new(
        context: E,
        uploads: SharedState,
        queue: queue::Queue<E, Entry>,
        mailbox_size: NonZeroUsize,
    ) -> (Self, Producer) {
        let (sender, receiver) = mailbox::new(context.child("mailbox"), mailbox_size);
        let actor = Self {
            context: ContextCell::new(context),
            uploads,
            queue,
            receiver,
        };
        (actor, Producer { sender })
    }

    pub fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    async fn run(mut self) {
        let mut queue = self.queue;
        while let Some(message) = self.receiver.recv().await {
            match message {
                Message::Block { block, ack } => {
                    let entry = self.uploads.lock().record(&block);
                    if let Some(entry) = entry {
                        (queue, _) = queue
                            .enqueue(entry)
                            .await
                            .expect("failed to enqueue finalized digest");
                    }
                    ack.acknowledge();
                }
                Message::Sync(sender) => {
                    queue = queue.sync().await.expect("failed to sync after ack");
                    let _ = sender.send(());
                }
            }
        }
    }
}

pub fn init<E>(
    context: E,
    uploads: SharedState,
    queue: queue::Queue<E, Entry>,
    mailbox_size: NonZeroUsize,
) -> Producer
where
    E: Clock + Storage + Metrics + Spawner + BufferPooler,
{
    let (actor, producer) = Actor::new(context, uploads, queue, mailbox_size);
    actor.start();
    producer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::backfiller::State;
    use commonware_cryptography::Digestible;
    use commonware_runtime::{
        buffer::paged::CacheRef,
        deterministic,
        mocks::{drive_pending_syncs, next_pending_sync, DelayedSyncContext, PendingSyncs},
        Runner as _, Supervisor as _,
    };
    use commonware_utils::{sync::Mutex, NZUsize, NZU16, NZU64};
    use futures::poll;
    use std::time::Duration;

    fn config(pooler: &impl BufferPooler) -> queue::Config<()> {
        queue::Config {
            partition: "producer-test".into(),
            items_per_section: NZU64!(1),
            compression: None,
            codec_config: (),
            page_cache: CacheRef::from_pooler(pooler, NZU16!(1024), NZUsize!(10)),
            write_buffer: NZUsize!(4096),
            replay_buffer: NZUsize!(4096),
        }
    }

    #[test]
    fn test_producer_holds_ack_until_durable_enqueue() {
        for cancel in [false, true] {
            deterministic::Runner::timed(Duration::from_secs(10)).start(|context| async move {
                let pending = PendingSyncs::default();
                let context = DelayedSyncContext {
                    inner: context,
                    pending: pending.clone(),
                };
                let cfg = queue::Config {
                    items_per_section: NZU64!(4),
                    ..config(&context)
                };
                let (queue, mut reader) = drive_pending_syncs(
                    &pending,
                    queue::Queue::init(context.child("queue"), cfg.clone()),
                )
                .await
                .unwrap();
                let (actor, mut producer) = Actor::new(
                    context.child("producer"),
                    Arc::new(Mutex::new(State::new())),
                    queue,
                    NZUsize!(1),
                );
                let block = Arc::new(Block::genesis());
                let expected = Entry {
                    height: block.height.get(),
                    digest: block.digest(),
                };
                let (ack, mut waiter) = Exact::handle();
                assert!(producer.report(Update::Block(block, ack)).accepted());

                pending.unblock();
                pending.arm();
                let gate = next_pending_sync(&pending);
                let handle = actor.start();
                gate.blocked.await.unwrap();
                assert!(poll!(&mut waiter).is_pending());
                assert!(reader.try_recv().await.unwrap().is_none());

                if cancel {
                    handle.abort();
                    assert!(handle.await.is_err());
                    assert!(waiter.await.is_err());
                    assert!(reader.recv().await.unwrap().is_none());
                } else {
                    gate.release.send(Ok(())).unwrap();
                    waiter.await.unwrap();
                    let (position, entry) = reader.recv().await.unwrap().unwrap();
                    assert_eq!(position, 0);
                    assert!(entry == expected);
                    drop(producer);
                    handle.await.unwrap();
                    drop(reader);

                    // Reopen without a separate sync to prove the marshal ack covers recovery.
                    let (queue, mut reader) =
                        queue::Queue::<_, Entry>::init(context.child("recovered"), cfg)
                            .await
                            .unwrap();
                    assert!(reader.recv().await.unwrap().unwrap().1 == expected);
                    drop(reader);
                    queue.destroy().await.unwrap();
                }
            });
        }
    }

    #[test]
    fn test_producer_sync_receipt_covers_pruning() {
        deterministic::Runner::timed(Duration::from_secs(10)).start(|context| async move {
            let pending = PendingSyncs::default();
            let context = DelayedSyncContext {
                inner: context,
                pending: pending.clone(),
            };
            let cfg = config(&context);
            let (queue, mut reader) = drive_pending_syncs(
                &pending,
                queue::Queue::init(context.child("queue"), cfg.clone()),
            )
            .await
            .unwrap();
            let entry = Entry {
                height: 0,
                digest: Block::genesis().digest(),
            };
            let (queue, _) = drive_pending_syncs(&pending, queue.enqueue_bulk([entry; 2]))
                .await
                .unwrap();
            let (position, _) = reader.recv().await.unwrap().unwrap();
            reader.ack(position).unwrap();
            let (actor, producer) = Actor::new(
                context.child("producer"),
                Arc::new(Mutex::new(State::new())),
                queue,
                NZUsize!(1),
            );

            pending.unblock();
            pending.arm();
            let gate = next_pending_sync(&pending);
            let handle = actor.start();
            let mut sync = Box::pin(producer.sync());
            assert!(poll!(&mut sync).is_pending());
            gate.blocked.await.unwrap();
            assert!(poll!(&mut sync).is_pending());
            gate.release.send(Ok(())).unwrap();
            sync.await;
            drop(producer);
            handle.await.unwrap();
            drop(reader);

            let (queue, mut reader) =
                queue::Queue::<_, Entry>::init(context.child("recovered"), cfg)
                    .await
                    .unwrap();
            assert_eq!(reader.ack_floor(), 1);
            assert_eq!(reader.recv().await.unwrap().unwrap().0, 1);
            drop(reader);
            queue.destroy().await.unwrap();
        });
    }
}
