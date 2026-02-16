use crate::Source;
use alto_client::consensus::Payload;
use alto_client::IndexQuery;
use alto_types::Block;
use bytes::Bytes;
use commonware_codec::Encode;
use commonware_consensus::{marshal::ingress::handler, types::Height};
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest};
use commonware_macros::select_loop;
use commonware_resolver::Consumer;
use commonware_runtime::{spawn_cell, ContextCell, Handle, Spawner};
use commonware_utils::channel::mpsc as tokio_mpsc;
use commonware_utils::{
    futures::{AbortablePool, Aborter},
    vec::NonEmptyVec,
};
use futures::{channel::mpsc, SinkExt, StreamExt};
use std::collections::HashMap;
use tracing::{debug, info, trace, warn};

/// Messages sent from the [Resolver] handle to the [Actor].
#[allow(clippy::type_complexity)]
pub enum ResolverMessage {
    Fetch(handler::Request<Block>),
    Cancel(handler::Request<Block>),
    Clear,
    Retain(Box<dyn Fn(&handler::Request<Block>) -> bool + Send>),
}

/// Handle to the [Actor] that implements [Resolver] for marshal.
///
/// All operations are forwarded as messages to the actor via a channel.
#[derive(Clone)]
pub struct Resolver {
    mailbox_tx: mpsc::Sender<ResolverMessage>,
}

impl commonware_resolver::Resolver for Resolver {
    type Key = handler::Request<Block>;
    type PublicKey = PublicKey;

    async fn fetch(&mut self, key: Self::Key) {
        let msg = ResolverMessage::Fetch(key);
        if let Err(e) = self.mailbox_tx.send(msg).await {
            warn!(error = ?e, "failed to send fetch request to resolver actor");
        }
    }

    async fn fetch_all(&mut self, keys: Vec<Self::Key>) {
        for key in keys {
            self.fetch(key).await;
        }
    }

    async fn fetch_targeted(&mut self, key: Self::Key, _targets: NonEmptyVec<Self::PublicKey>) {
        self.fetch(key).await;
    }

    async fn fetch_all_targeted(
        &mut self,
        requests: Vec<(Self::Key, NonEmptyVec<Self::PublicKey>)>,
    ) {
        for (key, _) in requests {
            self.fetch(key).await;
        }
    }

    async fn cancel(&mut self, key: Self::Key) {
        let msg = ResolverMessage::Cancel(key);
        if let Err(e) = self.mailbox_tx.send(msg).await {
            warn!(error = ?e, "failed to send cancel request to resolver actor");
        }
    }

    async fn clear(&mut self) {
        let msg = ResolverMessage::Clear;
        if let Err(e) = self.mailbox_tx.send(msg).await {
            warn!(error = ?e, "failed to send clear request to resolver actor");
        }
    }

    async fn retain(&mut self, f: impl Fn(&Self::Key) -> bool + Send + 'static) {
        let msg = ResolverMessage::Retain(Box::new(f));
        if let Err(e) = self.mailbox_tx.send(msg).await {
            warn!(error = ?e, "failed to send retain request to resolver actor");
        }
    }
}

/// Actor that fetches blocks and certificates from a [Source] on behalf of marshal.
///
/// This replaces the p2p-based resolver used by validators. When marshal needs
/// a block or certificate it does not have locally, it asks this actor to fetch
/// it from the HTTP source. In-flight requests are deduplicated.
///
/// The [Source] (client) is constructed without verification because marshal's
/// Deliver handler verifies all signatures before accepting resolved data.
pub struct Actor<E: Spawner, C: Source> {
    context: ContextCell<E>,
    client: C,
    mailbox_rx: mpsc::Receiver<ResolverMessage>,
    handler: handler::Handler<Block>,
    in_flight: AbortablePool<handler::Request<Block>>,
    in_flight_keys: HashMap<handler::Request<Block>, Aborter>,
}

impl<E: Spawner, C: Source> Actor<E, C> {
    /// Create a new [Actor] and its corresponding [Resolver] handle.
    pub fn new(
        context: E,
        client: C,
        ingress_tx: tokio_mpsc::Sender<handler::Message<Block>>,
        mailbox_size: usize,
    ) -> (Self, Resolver) {
        let (mailbox_tx, mailbox_rx) = mpsc::channel(mailbox_size);

        let actor = Self {
            context: ContextCell::new(context),
            client,
            mailbox_rx,
            handler: handler::Handler::new(ingress_tx),
            in_flight: AbortablePool::default(),
            in_flight_keys: HashMap::new(),
        };

        let handle = Resolver { mailbox_tx };

        (actor, handle)
    }

    /// Start the [Actor] in a background task.
    pub fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run().await)
    }

    /// Run the actor loop, processing fetch/cancel/clear/retain messages.
    async fn run(mut self) {
        info!("resolver actor started");

        select_loop! {
            self.context,
            on_stopped => {
                info!("resolver actor stopped");
            },
            Ok(key) = self.in_flight.next_completed() else continue => {
                self.in_flight_keys.remove(&key);
            },
            Some(msg) = self.mailbox_rx.next() else break => {
                match msg {
                    ResolverMessage::Fetch(key) => {
                        if self.in_flight_keys.contains_key(&key) {
                            trace!(?key, "skipping duplicate fetch request");
                            continue;
                        }
                        let future = Self::process_fetch(
                            key.clone(),
                            self.client.clone(),
                            self.handler.clone(),
                        );
                        let aborter = self.in_flight.push(future);
                        self.in_flight_keys.insert(key, aborter);
                    }
                    ResolverMessage::Cancel(key) => {
                        if self.in_flight_keys.remove(&key).is_some() {
                            debug!(?key, "cancelled in-flight request");
                        }
                    }
                    ResolverMessage::Clear => {
                        let count = self.in_flight_keys.len();
                        self.in_flight_keys.clear();
                        debug!(count, "cleared all in-flight requests");
                    }
                    ResolverMessage::Retain(f) => {
                        let before = self.in_flight_keys.len();
                        self.in_flight_keys.retain(|key, _| f(key));
                        let removed = before - self.in_flight_keys.len();
                        debug!(removed, remaining = self.in_flight_keys.len(), "retained in-flight requests");
                    }
                }
            },
        }
    }

    async fn process_fetch(
        key: handler::Request<Block>,
        client: C,
        handler: handler::Handler<Block>,
    ) -> handler::Request<Block> {
        match &key {
            handler::Request::Block(digest) => {
                Self::fetch_block_by_digest(*digest, client, handler).await;
            }
            handler::Request::Finalized { height } => {
                Self::fetch_finalized_by_height(*height, client, handler).await;
            }
            handler::Request::Notarized { round } => {
                Self::fetch_notarized_by_round(*round, client, handler).await;
            }
        }
        key
    }

    async fn fetch_block_by_digest(
        digest: Digest,
        client: C,
        mut handler: handler::Handler<Block>,
    ) {
        debug!(?digest, "fetching block by digest");

        match client.block(alto_client::Query::Digest(digest)).await {
            Ok(Payload::Block(block)) => {
                let key = handler::Request::Block(digest);
                let value = Bytes::from(block.encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(?digest, "failed to deliver block to marshal");
                }
                debug!(?digest, "fetched block by digest");
            }
            Ok(_) => {
                warn!(?digest, "wrong payload returned for block by digest");
            }
            Err(e) => {
                warn!(?digest, error=?e, "failed to fetch block by digest");
            }
        }
    }

    async fn fetch_finalized_by_height(
        height: Height,
        client: C,
        mut handler: handler::Handler<Block>,
    ) {
        debug!(height = height.get(), "fetching finalized block by height");

        match client.block(alto_client::Query::Index(height.get())).await {
            Ok(Payload::Finalized(finalized)) => {
                let key = handler::Request::Finalized { height };
                let finalization = finalized.proof.clone();
                let block = finalized.block.clone();
                let value = Bytes::from((finalization, block).encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(
                        height = height.get(),
                        "failed to deliver finalized block to marshal"
                    );
                }
                debug!(height = height.get(), "fetched finalized block by height");
            }
            Ok(_) => {
                warn!(
                    height = height.get(),
                    "wrong payload returned for finalized block by height"
                );
            }
            Err(e) => {
                warn!(height = height.get(), error=?e, "failed to fetch finalized block by height");
            }
        }
    }

    async fn fetch_notarized_by_round(
        round: commonware_consensus::types::Round,
        client: C,
        mut handler: handler::Handler<Block>,
    ) {
        let view = round.view().get();
        debug!(view, "fetching notarized block by round");

        match client.notarized(IndexQuery::Index(view)).await {
            Ok(notarized) => {
                let key = handler::Request::Notarized { round };
                let notarization = notarized.proof.clone();
                let block = notarized.block.clone();
                let value = Bytes::from((notarization, block).encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(view, "failed to deliver notarized block to marshal");
                }
                debug!(view, "fetched notarized block by round");
            }
            Err(e) => {
                warn!(view, error=?e, "failed to fetch notarized block by round");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockSource, TestFixture};
    use alto_client::consensus::Payload;
    use alto_types::Block;
    use commonware_consensus::{
        marshal::ingress::handler,
        types::{Height, Round, View},
    };
    use commonware_cryptography::Digestible;
    use commonware_macros::test_traced;
    use commonware_resolver::Resolver as _;
    use commonware_runtime::{deterministic::Runner, Clock, Metrics, Runner as _};
    use commonware_utils::channel::mpsc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Exercises the full Resolver trait surface (fetch, cancel, clear,
    /// retain) to ensure messages reach the actor without error.
    #[test_traced]
    fn fetch_cancel_clear_retain() {
        Runner::default().start(|context| async move {
            let source = MockSource::new();
            let (ingress_tx, _ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) =
                Actor::new(context.with_label("resolver"), source, ingress_tx, 16);

            let _actor_handle = actor.start();

            let key = handler::Request::<Block>::Finalized {
                height: Height::new(1),
            };

            resolver.fetch(key.clone()).await;
            resolver.cancel(key).await;
            resolver.clear().await;
            resolver.retain(|_| true).await;

            // Allow the actor to process all queued messages
            context.sleep(Duration::from_millis(100)).await;
        });
    }

    /// Verifies that the actor fetches a block by digest from the source
    /// and delivers it to marshal's ingress channel.
    #[test_traced]
    fn fetches_block_by_digest() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();

        // Configure the mock to return the block for any query
        let source = MockSource::new();
        *source.block_handler.lock().unwrap() = Some(Box::new(move |_| {
            Some(Payload::Block(Box::new(block.clone())))
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) =
                Actor::new(context.with_label("resolver"), source, ingress_tx, 16);

            let _actor_handle = actor.start();

            resolver.fetch(handler::Request::Block(digest)).await;

            // Verify the actor delivered the block with the correct key
            let msg = ingress_rx.recv().await.unwrap();
            match msg {
                handler::Message::Deliver { key, .. } => {
                    assert!(matches!(key, handler::Request::Block(d) if d == digest));
                }
                _ => panic!("expected Deliver message"),
            }
        });
    }

    /// Verifies that the actor fetches a finalized block by height from
    /// the source and delivers it to marshal's ingress channel.
    #[test_traced]
    fn fetches_finalized_by_height() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(5, 5);
        let height = Height::new(5);

        let source = MockSource::new();
        *source.block_handler.lock().unwrap() = Some(Box::new(move |_| {
            Some(Payload::Finalized(Box::new(finalized.clone())))
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) =
                Actor::new(context.with_label("resolver"), source, ingress_tx, 16);

            let _actor_handle = actor.start();

            resolver.fetch(handler::Request::Finalized { height }).await;

            let msg = ingress_rx.recv().await.unwrap();
            match msg {
                handler::Message::Deliver { key, .. } => {
                    assert!(
                        matches!(key, handler::Request::Finalized { height: h } if h == height)
                    );
                }
                _ => panic!("expected Deliver message"),
            }
        });
    }

    /// Verifies that the actor fetches a notarized block by round from
    /// the source and delivers it to marshal's ingress channel.
    #[test_traced]
    fn fetches_notarized_by_round() {
        let fixture = TestFixture::new();
        let notarized = fixture.create_notarized(3, 3);
        let round = Round::new(alto_types::EPOCH, View::new(3));

        let source = MockSource::new();
        *source.notarized_handler.lock().unwrap() =
            Some(Box::new(move |_| Some(notarized.clone())));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) =
                Actor::new(context.with_label("resolver"), source, ingress_tx, 16);

            let _actor_handle = actor.start();

            resolver.fetch(handler::Request::Notarized { round }).await;

            let msg = ingress_rx.recv().await.unwrap();
            match msg {
                handler::Message::Deliver { key, .. } => {
                    assert!(matches!(key, handler::Request::Notarized { round: r } if r == round));
                }
                _ => panic!("expected Deliver message"),
            }
        });
    }

    /// Verifies that duplicate fetch requests for the same key are
    /// deduplicated -- the source handler should only be called once.
    #[test_traced]
    fn dedup() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();

        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_inner = call_count.clone();

        let source = MockSource::new();
        *source.block_handler.lock().unwrap() = Some(Box::new(move |_| {
            *call_count_inner.lock().unwrap() += 1;
            Some(Payload::Block(Box::new(block.clone())))
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) =
                Actor::new(context.with_label("resolver"), source, ingress_tx, 16);

            let _actor_handle = actor.start();

            // Send the same request twice
            let key = handler::Request::<Block>::Block(digest);
            resolver.fetch(key.clone()).await;
            resolver.fetch(key).await;

            // Wait for the single fetch to complete
            let _msg = ingress_rx.recv().await.unwrap();
            context.sleep(Duration::from_millis(100)).await;

            // Source should have been called exactly once
            assert_eq!(*call_count.lock().unwrap(), 1);
        });
    }
}
