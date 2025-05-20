use alto_types::{Activity, Block, Finalization, Notarization};
use commonware_consensus::Reporter;
use commonware_cryptography::sha256::Digest;
use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};

pub enum Message {
    Get {
        // Only populated if parent (notarized)
        view: Option<u64>,
        payload: Digest,
        response: oneshot::Sender<Block>,
    },
    Broadcast {
        payload: Block,
    },
    Verified {
        view: u64,
        payload: Block,
    },
    Notarization {
        notarization: Notarization,
    },
    Finalization {
        finalization: Finalization,
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

    /// Get is a best-effort attempt to retrieve a given payload from the syncer. It is not an indication to go
    /// fetch the payload from the network.
    pub async fn get(&mut self, view: Option<u64>, payload: Digest) -> oneshot::Receiver<Block> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Message::Get {
                view,
                payload,
                response,
            })
            .await
            .expect("Failed to send get");
        receiver
    }

    /// Broadcast indicates that a payload should be sent to all peers.
    pub async fn broadcast(&mut self, payload: Block) {
        self.sender
            .send(Message::Broadcast { payload })
            .await
            .expect("Failed to send broadcast");
    }

    pub async fn verified(&mut self, view: u64, payload: Block) {
        self.sender
            .send(Message::Verified { view, payload })
            .await
            .expect("Failed to send lock");
    }
}

impl Reporter for Mailbox {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Activity::Notarization(notarization) => {
                self.sender
                    .send(Message::Notarization { notarization })
                    .await
                    .expect("Failed to send notarization");
            }
            Activity::Finalization(finalization) => {
                self.sender
                    .send(Message::Finalization { finalization })
                    .await
                    .expect("Failed to send finalization");
            }
            _ => {
                // Ignore other activity types
            }
        }
    }
}

/// Enum representing the different types of messages that the `Finalizer` loop
/// can send to the inner actor loop.
///
/// We break this into a separate enum to establish a separate priority for consensus messages.
pub enum Orchestration {
    Get {
        next: u64,
        result: oneshot::Sender<Option<Block>>,
    },
    Processed {
        next: u64,
        digest: Digest,
    },
    Repair {
        next: u64,
        result: oneshot::Sender<bool>,
    },
}

#[derive(Clone)]
pub struct Orchestrator {
    sender: mpsc::Sender<Orchestration>,
}

impl Orchestrator {
    pub fn new(sender: mpsc::Sender<Orchestration>) -> Self {
        Self { sender }
    }

    pub async fn get(&mut self, next: u64) -> Option<Block> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Orchestration::Get {
                next,
                result: response,
            })
            .await
            .expect("Failed to send get");
        receiver.await.unwrap()
    }

    pub async fn processed(&mut self, next: u64, digest: Digest) {
        self.sender
            .send(Orchestration::Processed { next, digest })
            .await
            .expect("Failed to send processed");
    }

    pub async fn repair(&mut self, next: u64) -> bool {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(Orchestration::Repair {
                next,
                result: response,
            })
            .await
            .expect("Failed to send repair");
        receiver.await.unwrap()
    }
}
