use crate::Source;
use alto_client::consensus::Payload;
use alto_client::IndexQuery;
use alto_types::Block;
use bytes::Bytes;
use commonware_codec::Encode;
use commonware_consensus::{marshal::ingress::handler, types::Height};
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest};
use commonware_macros::select;
use commonware_resolver::{Consumer, Resolver};
use commonware_utils::channel::mpsc as tokio_mpsc;
use commonware_utils::{
    futures::{AbortablePool, Aborter},
    vec::NonEmptyVec,
};
use futures::{channel::mpsc, SinkExt, StreamExt};
use std::collections::HashMap;
use tracing::{debug, info, trace, warn};

#[allow(clippy::type_complexity)]
pub enum ResolverMessage {
    Fetch(handler::Request<Block>),
    Cancel(handler::Request<Block>),
    Clear,
    Retain(Box<dyn Fn(&handler::Request<Block>) -> bool + Send>),
}

#[derive(Clone)]
pub struct HttpResolver {
    mailbox_tx: mpsc::Sender<ResolverMessage>,
}

impl Resolver for HttpResolver {
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

pub struct HttpResolverActor<C: Source> {
    client: C,
    mailbox_rx: mpsc::Receiver<ResolverMessage>,
    handler: handler::Handler<Block>,
    in_flight: AbortablePool<handler::Request<Block>>,
    in_flight_keys: HashMap<handler::Request<Block>, Aborter>,
}

impl<C: Source> HttpResolverActor<C> {
    pub fn new(
        client: C,
        ingress_tx: tokio_mpsc::Sender<handler::Message<Block>>,
        mailbox_size: usize,
    ) -> (Self, HttpResolver) {
        let (mailbox_tx, mailbox_rx) = mpsc::channel(mailbox_size);

        let actor = Self {
            client,
            mailbox_rx,
            handler: handler::Handler::new(ingress_tx),
            in_flight: AbortablePool::default(),
            in_flight_keys: HashMap::new(),
        };

        let handle = HttpResolver { mailbox_tx };

        (actor, handle)
    }

    pub async fn run(mut self) {
        info!("resolver actor started");

        loop {
            select! {
                result = self.in_flight.next_completed() => {
                    let Ok(key) = result else {
                        continue;
                    };
                    self.in_flight_keys.remove(&key);
                },
                msg = self.mailbox_rx.next() => {
                    let Some(msg) = msg else {
                        warn!("mailbox closed");
                        break;
                    };
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
            };
        }

        info!("resolver actor stopped");
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
                info!(?digest, "fetched block by digest");
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
                info!(height = height.get(), "fetched finalized block by height");
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

        match client.notarized_get(IndexQuery::Index(view)).await {
            Ok(notarized) => {
                let key = handler::Request::Notarized { round };
                let notarization = notarized.proof.clone();
                let block = notarized.block.clone();
                let value = Bytes::from((notarization, block).encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(view, "failed to deliver notarized block to marshal");
                }
                info!(view, "fetched notarized block by round");
            }
            Err(e) => {
                warn!(view, error=?e, "failed to fetch notarized block by round");
            }
        }
    }
}
