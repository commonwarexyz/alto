use super::{Entry, SharedState};
use alto_types::Block;
use commonware_actor::{
    mailbox::{self, Policy},
    Feedback,
};
use commonware_consensus::{marshal::Update, Reporter};
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
    block: Block,
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
    writer: queue::Writer<E, Entry>,
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
    pub fn new(
        context: E,
        uploads: SharedState,
        writer: queue::Writer<E, Entry>,
        mailbox_size: NonZeroUsize,
    ) -> (Self, Producer) {
        let (sender, receiver) = mailbox::new(context.child("mailbox"), mailbox_size);
        let actor = Self {
            context: ContextCell::new(context),
            uploads,
            writer,
            receiver,
        };
        (actor, Producer { sender })
    }

    pub fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    async fn run(mut self) {
        while let Some(Message { block, ack }) = self.receiver.recv().await {
            self.record(&block).await;
            ack.acknowledge();
        }
    }

    async fn record(&mut self, block: &Block) {
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

pub fn init<E>(
    context: E,
    uploads: SharedState,
    writer: queue::Writer<E, Entry>,
    mailbox_size: NonZeroUsize,
) -> Producer
where
    E: Clock + Storage + Metrics + Spawner,
{
    let (actor, producer) = Actor::new(context, uploads, writer, mailbox_size);
    actor.start();
    producer
}
