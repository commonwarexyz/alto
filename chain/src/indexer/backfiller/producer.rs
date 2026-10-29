use super::{Entry, SharedState};
use alto_types::Block;
use commonware_actor::{
    mailbox::{self, Policy},
    Feedback,
};
use commonware_consensus::{marshal::Update, Reporter};
use commonware_cryptography::Digested;
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Metrics, Spawner, Storage};
use commonware_storage::queue;
use commonware_utils::{acknowledgement::Exact, Acknowledgement};
use std::{collections::VecDeque, num::NonZeroUsize};

/// Records finalized block digests in the backfill queue from the application's
/// block stream.
#[derive(Clone)]
pub struct Producer {
    sender: mailbox::Sender<Message>,
}

// Carries a finalized block while holding its marshal ack until the block is
// durably queued.
struct Message {
    block: Digested<Block>,
    ack: Exact,
}

impl Policy for Message {
    type Overflow = VecDeque<Self>;

    fn handle(overflow: &mut Self::Overflow, message: Self) {
        overflow.push_back(message);
    }
}

struct Actor<E: Clock + Storage + Metrics> {
    context: ContextCell<E>,
    uploads: SharedState,
    receiver: mailbox::Receiver<Message>,
}

impl Reporter for Producer {
    type Activity = Update<Block>;

    fn report(&mut self, activity: Self::Activity) -> Feedback {
        match activity {
            Update::Block(block, ack) => self.sender.enqueue(Message { block, ack }),
            Update::Tip(_, _, _) => Feedback::Ok,
        }
    }
}

impl<E: Clock + Storage + Metrics + Spawner> Actor<E> {
    pub fn new(context: E, uploads: SharedState, mailbox_size: NonZeroUsize) -> (Self, Producer) {
        let (sender, receiver) = mailbox::new(context.child("mailbox"), mailbox_size);
        let actor = Self {
            context: ContextCell::new(context),
            uploads,
            receiver,
        };
        (actor, Producer { sender })
    }

    pub fn start(mut self, queue: queue::Queue<E, Entry>) -> Handle<()> {
        spawn_cell!(self.context, self.run(queue))
    }

    async fn run(mut self, mut queue: queue::Queue<E, Entry>) {
        while let Some(Message { block, ack }) = self.receiver.recv().await {
            queue = self.record(queue, block.inner()).await;
            ack.acknowledge();
        }
    }

    async fn record(
        &mut self,
        queue: queue::Queue<E, Entry>,
        block: &Block,
    ) -> queue::Queue<E, Entry> {
        let Some(entry) = self.uploads.lock().record(block) else {
            return queue;
        };

        // Persist a queue entry for each finalized block that is not already
        // known uploaded. The backfiller retries from durable queue state until
        // it either uploads successfully or observes that the live certificate
        // path already uploaded the block.
        //
        // The producer owns the queue, so its sync also prunes the sections the
        // consumer has acknowledged.
        let (queue, _) = queue
            .append(&entry)
            .await
            .expect("failed to append finalized digest");
        queue.sync().await.expect("failed to sync finalized queue")
    }
}

pub fn init<E>(
    context: E,
    uploads: SharedState,
    queue: queue::Queue<E, Entry>,
    mailbox_size: NonZeroUsize,
) -> Producer
where
    E: Clock + Storage + Metrics + Spawner,
{
    let (actor, producer) = Actor::new(context, uploads, mailbox_size);
    actor.start(queue);
    producer
}
