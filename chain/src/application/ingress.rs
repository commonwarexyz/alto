use alto_types::{Block, PublicKey};
use commonware_consensus::{
    marshal::Update,
    simplex::types::Context,
    types::{Epoch, Round, View},
    Automaton, Relay, Reporter,
};
use commonware_cryptography::sha256::Digest;
use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};

/// Messages sent to the application.
pub enum Message {
    Genesis {
        response: oneshot::Sender<Digest>,
    },
    Propose {
        round: Round,
        parent: (View, Digest),
        response: oneshot::Sender<Digest>,
    },
    Broadcast {
        payload: Digest,
    },
    Verify {
        round: Round,
        parent: (View, Digest),
        payload: Digest,
        response: oneshot::Sender<bool>,
    },
    Finalized {
        block: Block,
    },
}

/// Mailbox for the application.
#[derive(Clone)]
pub struct Mailbox {
    sender: mpsc::Sender<Message>,
}

impl Mailbox {
    pub(super) fn new(sender: mpsc::Sender<Message>) -> Self {
        Self { sender }
    }
}

impl Automaton for Mailbox {
    type Digest = Digest;
    type Context = Context<Self::Digest, PublicKey>;

    async fn genesis(&mut self, _epoch: Epoch) -> Self::Digest {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Message::Genesis { response })
            .await
            .expect("Failed to send genesis");
        receiver.await.expect("Failed to receive genesis")
    }

    async fn propose(
        &mut self,
        context: Context<Self::Digest, PublicKey>,
    ) -> oneshot::Receiver<Self::Digest> {
        // If we linked payloads to their parent, we would include
        // the parent in the `Context` in the payload.
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Message::Propose {
                round: context.round,
                parent: context.parent,
                response,
            })
            .await
            .expect("Failed to send propose");
        receiver
    }

    async fn verify(
        &mut self,
        context: Context<Self::Digest, PublicKey>,
        payload: Self::Digest,
    ) -> oneshot::Receiver<bool> {
        // If we linked payloads to their parent, we would verify
        // the parent included in the payload matches the provided `Context`.
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Message::Verify {
                round: context.round,
                parent: context.parent,
                payload,
                response,
            })
            .await
            .expect("Failed to send verify");
        receiver
    }
}

impl Relay for Mailbox {
    type Digest = Digest;

    async fn broadcast(&mut self, digest: Self::Digest) {
        self.sender
            .send(Message::Broadcast { payload: digest })
            .await
            .expect("Failed to send broadcast");
    }
}

impl Reporter for Mailbox {
    type Activity = Update<Block>;

    async fn report(&mut self, update: Self::Activity) {
        let Update::Block(block) = update else {
            return;
        };
        self.sender
            .send(Message::Finalized { block })
            .await
            .expect("Failed to send finalized");
    }
}
