use commonware_consensus::{
    threshold_simplex::{Context, View},
    Automaton as Au, Committer as Co, Proof, Relay as Re,
};
use commonware_cryptography::sha256::Digest;
use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};

pub enum Message {
    Genesis {
        response: oneshot::Sender<Digest>,
    },
    Propose {
        view: View,
        parent: (View, Digest),
        response: oneshot::Sender<Digest>,
    },
    Broadcast {
        payload: Digest,
    },
    Verify {
        view: View,
        parent: (View, Digest),
        payload: Digest,
        response: oneshot::Sender<bool>,
    },
    Prepared {
        proof: Proof,
        payload: Digest,
    },
    Finalized {
        proof: Proof,
        payload: Digest,
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

impl Au for Mailbox {
    type Digest = Digest;
    type Context = Context<Self::Digest>;

    async fn genesis(&mut self) -> Self::Digest {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Message::Genesis { response })
            .await
            .expect("Failed to send genesis");
        receiver.await.expect("Failed to receive genesis")
    }

    async fn propose(&mut self, context: Context<Self::Digest>) -> oneshot::Receiver<Self::Digest> {
        // If we linked payloads to their parent, we would include
        // the parent in the `Context` in the payload.
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Message::Propose {
                view: context.view,
                parent: context.parent,
                response,
            })
            .await
            .expect("Failed to send propose");
        receiver
    }

    async fn verify(
        &mut self,
        context: Context<Self::Digest>,
        payload: Self::Digest,
    ) -> oneshot::Receiver<bool> {
        // If we linked payloads to their parent, we would verify
        // the parent included in the payload matches the provided `Context`.
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Message::Verify {
                view: context.view,
                parent: context.parent,
                payload,
                response,
            })
            .await
            .expect("Failed to send verify");
        receiver
    }
}

impl Re for Mailbox {
    type Digest = Digest;

    async fn broadcast(&mut self, digest: Self::Digest) {
        self.sender
            .send(Message::Broadcast { payload: digest })
            .await
            .expect("Failed to send broadcast");
    }
}

impl Co for Mailbox {
    type Digest = Digest;

    async fn prepared(&mut self, proof: Proof, payload: Self::Digest) {
        self.sender
            .send(Message::Prepared { proof, payload })
            .await
            .expect("Failed to send notarized");
    }

    async fn finalized(&mut self, proof: Proof, payload: Self::Digest) {
        self.sender
            .send(Message::Finalized { proof, payload })
            .await
            .expect("Failed to send finalized");
    }
}
