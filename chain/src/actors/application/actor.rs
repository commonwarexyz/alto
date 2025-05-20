use super::{
    ingress::{Mailbox, Message},
    supervisor::Supervisor,
    Config,
};
use crate::actors::syncer;
use alto_types::Block;
use commonware_cryptography::{Digestible, Hasher, Sha256};
use commonware_macros::select;
use commonware_runtime::{Clock, Handle, Metrics, Spawner};
use commonware_utils::SystemTimeExt;
use futures::StreamExt;
use futures::{channel::mpsc, future::try_join};
use futures::{channel::oneshot, future};
use futures::{
    future::Either,
    task::{Context, Poll},
};
use rand::Rng;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};
use tracing::{info, warn};

// Define a future that checks if the oneshot channel is closed using a mutable reference
struct ChannelClosedFuture<'a, T> {
    sender: &'a mut oneshot::Sender<T>,
}

impl<T> futures::Future for ChannelClosedFuture<'_, T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Use poll_canceled to check if the receiver has dropped the channel
        match self.sender.poll_canceled(cx) {
            Poll::Ready(()) => Poll::Ready(()), // Receiver dropped, channel closed
            Poll::Pending => Poll::Pending,     // Channel still open
        }
    }
}

// Helper function to create the future using a mutable reference
fn oneshot_closed_future<T>(sender: &mut oneshot::Sender<T>) -> ChannelClosedFuture<T> {
    ChannelClosedFuture { sender }
}

/// Genesis message to use during initialization.
const GENESIS: &[u8] = b"commonware is neat";

/// Milliseconds in the future to allow for block timestamps.
const SYNCHRONY_BOUND: u64 = 500;

/// Application actor.
pub struct Actor<R: Rng + Spawner + Metrics + Clock> {
    context: R,
    hasher: Sha256,
    mailbox: mpsc::Receiver<Message>,
}

impl<R: Rng + Spawner + Metrics + Clock> Actor<R> {
    /// Create a new application actor.
    pub fn new(context: R, config: Config) -> (Self, Supervisor, Mailbox) {
        let (sender, mailbox) = mpsc::channel(config.mailbox_size);
        (
            Self {
                context,
                hasher: Sha256::new(),
                mailbox,
            },
            Supervisor::new(config.polynomial, config.participants, config.share),
            Mailbox::new(sender),
        )
    }

    pub fn start(mut self, syncer: syncer::Mailbox) -> Handle<()> {
        self.context.spawn_ref()(self.run(syncer))
    }

    /// Run the application actor.
    async fn run(mut self, mut syncer: syncer::Mailbox) {
        // Compute genesis digest
        self.hasher.update(GENESIS);
        let genesis_parent = self.hasher.finalize();
        let genesis = Block::new(genesis_parent, 0, 0);
        let genesis_digest = genesis.digest();
        let built: Option<Block> = None;
        let built = Arc::new(Mutex::new(built));
        while let Some(message) = self.mailbox.next().await {
            match message {
                Message::Genesis { response } => {
                    // Use the digest of the genesis message as the initial
                    // payload.
                    let _ = response.send(genesis_digest);
                }
                Message::Propose {
                    view,
                    parent,
                    mut response,
                } => {
                    // Get the parent block
                    let parent_request = if parent.1 == genesis_digest {
                        Either::Left(future::ready(Ok(genesis.clone())))
                    } else {
                        Either::Right(syncer.get(Some(parent.0), parent.1).await)
                    };

                    // Wait for the parent block to be available or the request to be cancelled in a separate task (to
                    // continue processing other messages)
                    self.context.with_label("propose").spawn({
                        let built = built.clone();
                        move |context| async move {
                            let response_closed = oneshot_closed_future(&mut response);
                            select! {
                                parent = parent_request => {
                                    // Get the parent block
                                    let parent = parent.unwrap();

                                    // Create a new block
                                    let mut current = context.current().epoch_millis();
                                    if current <= parent.timestamp {
                                        current = parent.timestamp + 1;
                                    }
                                    let block = Block::new(parent.digest(), parent.height+1, current);
                                    let digest = block.digest();
                                    {
                                        let mut built = built.lock().unwrap();
                                        *built = Some(block);
                                    }

                                    // Send the digest to the consensus
                                    let result = response.send(digest);
                                    info!(view, ?digest, success=result.is_ok(), "proposed new block");
                                },
                                _ = response_closed => {
                                    // The response was cancelled
                                    warn!(view, "propose aborted");
                                }
                            }
                        }
                    });
                }
                Message::Broadcast { payload } => {
                    // Check if the last built is equal
                    let Some(built) = built.lock().unwrap().clone() else {
                        warn!(?payload, "missing block to broadcast");
                        continue;
                    };

                    // Send the block to the syncer
                    info!(?payload, "broadcast requested");
                    syncer.broadcast(built.clone()).await;
                }
                Message::Verify {
                    view,
                    parent,
                    payload,
                    mut response,
                } => {
                    // Get the parent and current block
                    let parent_request = if parent.1 == genesis_digest {
                        Either::Left(future::ready(Ok(genesis.clone())))
                    } else {
                        Either::Right(syncer.get(Some(parent.0), parent.1).await)
                    };

                    // Wait for the blocks to be available or the request to be cancelled in a separate task (to
                    // continue processing other messages)
                    self.context.with_label("verify").spawn({
                        let mut syncer = syncer.clone();
                        move |context| async move {
                            let requester =
                                try_join(parent_request, syncer.get(None, payload).await);
                            let response_closed = oneshot_closed_future(&mut response);
                            select! {
                                result = requester => {
                                    // Unwrap the results
                                    let (parent, block) = result.unwrap();

                                    // Verify the block
                                    if block.height != parent.height + 1 {
                                        let _ = response.send(false);
                                        return;
                                    }
                                    if block.parent != parent.digest() {
                                        let _ = response.send(false);
                                        return;
                                    }
                                    if block.timestamp <= parent.timestamp {
                                        let _ = response.send(false);
                                        return;
                                    }
                                    let current = context.current().epoch_millis();
                                    if block.timestamp > current + SYNCHRONY_BOUND {
                                        let _ = response.send(false);
                                        return;
                                    }

                                    // Persist the verified block
                                    syncer.verified(view, block).await;

                                    // Send the verification result to the consensus
                                    let _ = response.send(true);
                                },
                                _ = response_closed => {
                                    // The response was cancelled
                                    warn!(view, "verify aborted");
                                }
                            }
                        }
                    });
                }
            }
        }
    }
}
