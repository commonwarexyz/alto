use crate::Source;
use alto_client::consensus::Payload;
use alto_client::{IndexQuery, Query};
use bytes::Bytes;
use commonware_codec::Encode;
use commonware_consensus::{marshal::resolver::handler, types::Height};
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest};
use commonware_macros::select_loop;
use commonware_resolver::Consumer;
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Spawner};
use commonware_utils::channel::mpsc;
use commonware_utils::{
    futures::{AbortablePool, Aborter},
    vec::NonEmptyVec,
    SystemTimeExt,
};
use rand::{CryptoRng, RngCore};
use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, SystemTime},
};
use tracing::{debug, trace, warn};

/// Messages sent from the [Resolver] handle to the [Actor].
#[allow(clippy::type_complexity)]
pub enum Message {
    Fetch(handler::Request<Digest>),
    Cancel(handler::Request<Digest>),
    Clear,
    Retain(Box<dyn Fn(&handler::Request<Digest>) -> bool + Send>),
}

/// Handle to the [Actor] that implements [Resolver] for marshal.
///
/// All operations are forwarded as messages to the actor via a channel.
#[derive(Clone)]
pub struct Resolver {
    mailbox_tx: mpsc::Sender<Message>,
}

impl commonware_resolver::Resolver for Resolver {
    type Key = handler::Request<Digest>;
    type PublicKey = PublicKey;

    async fn fetch(&mut self, key: Self::Key) {
        let msg = Message::Fetch(key);
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
        let msg = Message::Cancel(key);
        if let Err(e) = self.mailbox_tx.send(msg).await {
            warn!(error = ?e, "failed to send cancel request to resolver actor");
        }
    }

    async fn clear(&mut self) {
        let msg = Message::Clear;
        if let Err(e) = self.mailbox_tx.send(msg).await {
            warn!(error = ?e, "failed to send clear request to resolver actor");
        }
    }

    async fn retain(&mut self, f: impl Fn(&Self::Key) -> bool + Send + 'static) {
        let msg = Message::Retain(Box::new(f));
        if let Err(e) = self.mailbox_tx.send(msg).await {
            warn!(error = ?e, "failed to send retain request to resolver actor");
        }
    }
}

/// Actor that fetches blocks and certificates from a [Source] on behalf of marshal.
///
/// This replaces the p2p-based resolver used by validators. When marshal needs
/// a block or certificate it does not have locally, it asks this actor to fetch
/// it from the HTTP source.
///
/// The [Source] (client) should be constructed without verification because marshal's
/// Deliver handler verifies all signatures before accepting resolved data.
/// Rejections are logged as warnings and retried.
pub struct Actor<E: Spawner, C: Source> {
    context: ContextCell<E>,
    client: C,
    mailbox_rx: mpsc::Receiver<Message>,
    handler: handler::Handler<Digest>,
    active: AbortablePool<Result>,
    requests: HashMap<handler::Request<Digest>, State>,
    retry_schedule: BTreeSet<(SystemTime, handler::Request<Digest>)>,
    fetch_retry_timeout: Duration,
    next_id: u64,
}

enum State {
    // A fetch is currently running.
    //
    // The id lets us ignore stale completions from an earlier attempt for
    // the same key, and dropping the aborter cancels the current attempt.
    // This ensures we only have 1 fetch in flight for a given key (regardless
    // of the order of fetch, cancel, clear, retain messages).
    Active { id: u64, aborter: Aborter },
    // A retry is queued for the recorded deadline.
    Scheduled(SystemTime),
}

struct Result {
    key: handler::Request<Digest>,
    id: u64,
    retry: bool,
}

impl<E: Spawner + Clock + CryptoRng + RngCore, C: Source> Actor<E, C> {
    /// Create a new [Actor] and its corresponding [Resolver] handle.
    pub fn new(
        context: E,
        client: C,
        ingress_tx: mpsc::Sender<handler::Message<Digest>>,
        mailbox_size: usize,
        fetch_retry_timeout: Duration,
    ) -> (Self, Resolver) {
        let (mailbox_tx, mailbox_rx) = mpsc::channel(mailbox_size);

        let actor = Self {
            context: ContextCell::new(context),
            client,
            mailbox_rx,
            handler: handler::Handler::new(ingress_tx),
            active: AbortablePool::default(),
            requests: HashMap::new(),
            retry_schedule: BTreeSet::new(),
            fetch_retry_timeout,
            next_id: 0,
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
        select_loop! {
            self.context,
            on_stopped => {},
            Ok(result) = self.active.next_completed() else continue => {
                self.handle_completed(result);
            },
            _ = match self.retry_schedule.first() {
                Some((deadline, _)) => futures::future::Either::Left(self.context.sleep_until(*deadline)),
                None => futures::future::Either::Right(futures::future::pending()),
            } => {
                self.process_retries();
            },
            Some(msg) = self.mailbox_rx.recv() else break => {
                match msg {
                    Message::Fetch(key) => {
                        if let Some(state) = self.requests.get(&key) {
                            match state {
                                State::Active { .. } => {
                                    trace!(?key, "ignoring fetch request for active key");
                                }
                                State::Scheduled(_) => {
                                    trace!(?key, "ignoring fetch request for scheduled key");
                                }
                            }
                            continue;
                        }
                        self.start_fetch(key);
                    }
                    Message::Cancel(key) => {
                        if let Some(state) = self.requests.remove(&key) {
                            match state {
                                State::Active { aborter, .. } => {
                                    drop(aborter);
                                    debug!(?key, "cancelled active request");
                                }
                                State::Scheduled(deadline) => {
                                    let removed = self.retry_schedule.remove(&(deadline, key.clone()));
                                    assert!(removed, "scheduled retry entry missing");
                                    debug!(?key, ?deadline, "cancelled scheduled request");
                                }
                            }
                        }
                    }
                    Message::Clear => {
                        let active = self
                            .requests
                            .values()
                            .filter(|state| matches!(state, State::Active { .. }))
                            .count();
                        let scheduled = self.requests.len() - active;
                        self.requests.clear();
                        self.retry_schedule.clear();
                        debug!(active, scheduled, "cleared all pending requests");
                    }
                    Message::Retain(f) => {
                        let to_remove = self
                            .requests
                            .keys()
                            .filter(|key| !f(key))
                            .cloned()
                            .collect::<Vec<_>>();
                        let removed = to_remove.len();
                        for key in to_remove {
                            if let Some(State::Scheduled(deadline)) = self.requests.remove(&key) {
                                let removed = self.retry_schedule.remove(&(deadline, key.clone()));
                                assert!(removed, "scheduled retry entry missing");
                            }
                        }
                        debug!(removed, remaining = self.requests.len(), "retained pending requests");
                    }
                }
            },
        }
    }

    /// Start a fetch for the given key.
    fn start_fetch(&mut self, key: handler::Request<Digest>) {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let future =
            Self::process_fetch(key.clone(), id, self.client.clone(), self.handler.clone());
        let aborter = self.active.push(future);
        let previous = self.requests.insert(key, State::Active { id, aborter });
        assert!(previous.is_none(), "request state already existed");
    }

    /// Handle a completed fetch.
    fn handle_completed(&mut self, result: Result) {
        // If the request has been removed or updated, ignore the completion.
        let Some(state) = self.requests.get(&result.key) else {
            trace!(
                ?result.key,
                id = result.id,
                "ignoring stale fetch completion for removed request"
            );
            return;
        };
        match state {
            State::Active { id, .. } if *id == result.id => {}
            State::Active { id, .. } => {
                trace!(
                    ?result.key,
                    completed_id = result.id,
                    active_id = *id,
                    "ignoring stale fetch completion for replaced request"
                );
                return;
            }
            State::Scheduled(deadline) => {
                trace!(
                    ?result.key,
                    id = result.id,
                    ?deadline,
                    "ignoring stale fetch completion for scheduled request"
                );
                return;
            }
        }

        // Remove the request and (optionally) schedule a retry.
        let removed = self.requests.remove(&result.key);
        assert!(
            matches!(
                removed,
                Some(State::Active { id, .. }) if id == result.id
            ),
            "active request state missing for completed fetch"
        );
        if result.retry {
            self.schedule_retry(result.key);
        }
    }

    /// Schedule a retry for the given key.
    fn schedule_retry(&mut self, key: handler::Request<Digest>) {
        let deadline = self
            .context
            .current()
            .add_jittered(&mut self.context, self.fetch_retry_timeout);
        let previous = self
            .requests
            .insert(key.clone(), State::Scheduled(deadline));
        assert!(
            matches!(previous, None | Some(State::Active { .. })),
            "request was already scheduled"
        );
        self.retry_schedule.insert((deadline, key.clone()));
        debug!(?key, ?deadline, "scheduled fetch retry");
    }

    /// Process any due retries.
    fn process_retries(&mut self) {
        let now = self.context.current();
        while let Some((deadline, key)) = self.retry_schedule.pop_first() {
            if deadline > now {
                self.retry_schedule.insert((deadline, key));
                break;
            }
            match self.requests.get(&key) {
                Some(State::Scheduled(state_deadline)) if *state_deadline == deadline => {
                    self.requests.remove(&key);
                    debug!(?key, "retrying fetch request");
                    self.start_fetch(key);
                }
                Some(State::Active { .. }) => {
                    trace!(
                        ?key,
                        "skipping stale retry because request is already in flight"
                    );
                }
                Some(State::Scheduled(state_deadline)) => {
                    trace!(
                        ?key,
                        ?deadline,
                        ?state_deadline,
                        "skipping stale retry with outdated deadline"
                    );
                }
                None => {
                    trace!(?key, ?deadline, "skipping stale retry for removed request");
                }
            }
        }
    }

    /// Process a fetch request.
    async fn process_fetch(
        key: handler::Request<Digest>,
        id: u64,
        client: C,
        handler: handler::Handler<Digest>,
    ) -> Result {
        let retry = match &key {
            handler::Request::Block(digest) => {
                Self::fetch_block_by_digest(*digest, client, handler).await
            }
            handler::Request::Finalized { height } => {
                Self::fetch_finalized_by_height(*height, client, handler).await
            }
            handler::Request::Notarized { round } => {
                Self::fetch_notarized_by_round(*round, client, handler).await
            }
        };
        Result { key, id, retry }
    }

    /// Fetch a block by digest.
    async fn fetch_block_by_digest(
        digest: Digest,
        client: C,
        mut handler: handler::Handler<Digest>,
    ) -> bool {
        debug!(?digest, "fetching block by digest");

        match client.block(alto_client::Query::Digest(digest)).await {
            Ok(Payload::Block(block)) => {
                let key = handler::Request::Block(digest);
                let value = Bytes::from(block.encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(?digest, "failed to deliver block to marshal");
                    return true;
                }
                debug!(?digest, "fetched block by digest");
                false
            }
            Ok(_) => {
                warn!(?digest, "wrong payload returned for block by digest");
                true
            }
            Err(e) => {
                warn!(?digest, error=?e, "failed to fetch block by digest");
                true
            }
        }
    }

    /// Fetch a finalized block by height.
    async fn fetch_finalized_by_height(
        height: Height,
        client: C,
        mut handler: handler::Handler<Digest>,
    ) -> bool {
        debug!(height = height.get(), "fetching finalized block by height");

        match client.block(Query::Index(height.get())).await {
            Ok(Payload::Finalized(finalized)) => {
                let key = handler::Request::Finalized { height };
                let finalization = finalized.proof.clone();
                let block = finalized.block.clone();
                let value = Bytes::from((finalization, block).encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(height = height.get(), "marshal rejected finalized block");
                    return true;
                }
                debug!(height = height.get(), "fetched finalized block by height");
                false
            }
            Ok(_) => {
                warn!(
                    height = height.get(),
                    "wrong payload returned for finalized block by height"
                );
                true
            }
            Err(e) => {
                warn!(height = height.get(), error=?e, "failed to fetch finalized block by height");
                true
            }
        }
    }

    /// Fetch a notarized block by round.
    async fn fetch_notarized_by_round(
        round: commonware_consensus::types::Round,
        client: C,
        mut handler: handler::Handler<Digest>,
    ) -> bool {
        let view = round.view().get();
        debug!(view, "fetching notarized block by round");

        match client.notarized(IndexQuery::Index(view)).await {
            Ok(notarized) => {
                let key = handler::Request::Notarized { round };
                let notarization = notarized.proof.clone();
                let block = notarized.block.clone();
                let value = Bytes::from((notarization, block).encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(view, "marshal rejected notarized block");
                    return true;
                }
                debug!(view, "fetched notarized block by round");
                false
            }
            Err(e) => {
                warn!(view, error=?e, "failed to fetch notarized block by round");
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockSource, TestFixture};
    use alto_client::{consensus::Payload, Query};
    use commonware_consensus::{
        marshal::resolver::handler,
        types::{Height, Round, View},
    };
    use commonware_cryptography::{sha256::Digest, Digestible};
    use commonware_macros::test_traced;
    use commonware_resolver::Resolver as _;
    use commonware_runtime::{deterministic::Runner, Clock, Metrics, Runner as _};
    use commonware_utils::channel::mpsc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const DEFAULT_FETCH_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

    /// Exercises the full Resolver trait surface (fetch, cancel, clear,
    /// retain) to ensure messages reach the actor without error.
    #[test_traced]
    fn fetch_cancel_clear_retain() {
        Runner::default().start(|context| async move {
            let source = MockSource::new();
            let (ingress_tx, _ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let _actor_handle = actor.start();

            let key = handler::Request::<Digest>::Finalized {
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
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let _actor_handle = actor.start();

            // Verify the actor delivered the block with the correct key
            resolver.fetch(handler::Request::Block(digest)).await;
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
        *source.block_handler.lock().unwrap() = Some(Box::new(move |query| match query {
            Query::Index(index) if index == height.get() => {
                Some(Payload::Finalized(Box::new(finalized.clone())))
            }
            _ => None,
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
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

    /// Verifies that finalized backfill uses a height-indexed block query
    /// (not a view-indexed finalization query) so diverged view/height still
    /// resolves correctly.
    #[test_traced]
    fn fetches_finalized_by_height_uses_height_indexed_block_query() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(5, 8);
        let height = Height::new(5);
        let expected_height = height.get();

        let block_call_count = Arc::new(Mutex::new(0u32));
        let block_call_count_inner = block_call_count.clone();
        let finalized_call_count = Arc::new(Mutex::new(0u32));
        let finalized_call_count_inner = finalized_call_count.clone();

        let source = MockSource::new();
        *source.block_handler.lock().unwrap() = Some(Box::new(move |query| {
            *block_call_count_inner.lock().unwrap() += 1;
            match query {
                Query::Index(index) if index == expected_height => {
                    Some(Payload::Finalized(Box::new(finalized.clone())))
                }
                _ => None,
            }
        }));
        *source.finalized_handler.lock().unwrap() = Some(Box::new(move |_| {
            *finalized_call_count_inner.lock().unwrap() += 1;
            None
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let _actor_handle = actor.start();

            // Allow the fetch to run to completion.
            resolver.fetch(handler::Request::Finalized { height }).await;
            context.sleep(Duration::from_millis(100)).await;

            assert_eq!(
                *block_call_count.lock().unwrap(),
                1,
                "expected finalized backfill to query block endpoint by height"
            );
            assert_eq!(
                *finalized_call_count.lock().unwrap(),
                0,
                "expected finalized backfill to avoid view-indexed finalization endpoint"
            );

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
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
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

    /// Verifies that marshal rejecting a finalized delivery causes the
    /// resolver to retry and redeliver it.
    #[test_traced]
    fn retries_when_marshal_rejects_finalized_delivery() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(1, 1);
        let height = Height::new(1);
        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_inner = call_count.clone();

        let source = MockSource::new();
        *source.block_handler.lock().unwrap() = Some(Box::new(move |query| match query {
            Query::Index(index) if index == height.get() => {
                *call_count_inner.lock().unwrap() += 1;
                Some(Payload::Finalized(Box::new(finalized.clone())))
            }
            _ => None,
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let _actor_handle = actor.start();

            resolver.fetch(handler::Request::Finalized { height }).await;
            let msg = ingress_rx.recv().await.unwrap();
            match msg {
                handler::Message::Deliver { response, .. } => {
                    response.send(false).expect("deliver response dropped");
                }
                _ => panic!("expected Deliver message"),
            }

            let max_retry_wait = DEFAULT_FETCH_RETRY_TIMEOUT * 2 + Duration::from_millis(10);
            context.sleep(max_retry_wait).await;

            let retry = ingress_rx.recv().await.unwrap();
            match retry {
                handler::Message::Deliver { key, response, .. } => {
                    assert!(
                        matches!(key, handler::Request::Finalized { height: h } if h == height)
                    );
                    response.send(true).expect("deliver response dropped");
                }
                _ => panic!("expected Deliver message"),
            }

            assert_eq!(
                *call_count.lock().unwrap(),
                2,
                "expected marshal rejection to trigger one retry"
            );
        });
    }

    /// Verifies that marshal rejecting a notarized delivery causes the
    /// resolver to retry and redeliver it.
    #[test_traced]
    fn retries_when_marshal_rejects_notarized_delivery() {
        let fixture = TestFixture::new();
        let notarized = fixture.create_notarized(3, 3);
        let round = Round::new(alto_types::EPOCH, View::new(3));
        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_inner = call_count.clone();

        let source = MockSource::new();
        *source.notarized_handler.lock().unwrap() = Some(Box::new(move |_| {
            *call_count_inner.lock().unwrap() += 1;
            Some(notarized.clone())
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let _actor_handle = actor.start();

            resolver.fetch(handler::Request::Notarized { round }).await;
            let msg = ingress_rx.recv().await.unwrap();
            match msg {
                handler::Message::Deliver { response, .. } => {
                    response.send(false).expect("deliver response dropped");
                }
                _ => panic!("expected Deliver message"),
            }

            let max_retry_wait = DEFAULT_FETCH_RETRY_TIMEOUT * 2 + Duration::from_millis(10);
            context.sleep(max_retry_wait).await;

            let retry = ingress_rx.recv().await.unwrap();
            match retry {
                handler::Message::Deliver { key, response, .. } => {
                    assert!(matches!(key, handler::Request::Notarized { round: r } if r == round));
                    response.send(true).expect("deliver response dropped");
                }
                _ => panic!("expected Deliver message"),
            }

            assert_eq!(
                *call_count.lock().unwrap(),
                2,
                "expected marshal rejection to trigger one retry"
            );
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
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let _actor_handle = actor.start();

            // Send the same request twice
            let key = handler::Request::<Digest>::Block(digest);
            resolver.fetch(key.clone()).await;
            resolver.fetch(key).await;

            // Wait for the single fetch to complete
            let _msg = ingress_rx.recv().await.unwrap();
            context.sleep(Duration::from_millis(100)).await;

            // Source should have been called exactly once
            assert_eq!(*call_count.lock().unwrap(), 1);
        });
    }

    /// Verifies that repeated fetch failures continue retrying until a later
    /// attempt succeeds and delivers the requested payload.
    #[test_traced]
    fn failed_fetch_eventually_resolves_after_multiple_retries() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();

        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_inner = call_count.clone();

        let source = MockSource::new();
        *source.block_handler.lock().unwrap() = Some(Box::new(move |_| {
            let mut calls = call_count_inner.lock().unwrap();
            *calls += 1;
            if *calls >= 3 {
                Some(Payload::Block(Box::new(block.clone())))
            } else {
                None
            }
        }));

        Runner::default().start(|context| async move {
            let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
            let (actor, mut resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let _actor_handle = actor.start();

            resolver.fetch(handler::Request::Block(digest)).await;
            let max_retry_wait = DEFAULT_FETCH_RETRY_TIMEOUT * 2 + Duration::from_millis(10);
            for _ in 0..3 {
                if *call_count.lock().unwrap() >= 3 {
                    break;
                }
                context.sleep(max_retry_wait).await;
            }

            let msg = ingress_rx.recv().await.unwrap();
            match msg {
                handler::Message::Deliver { key, .. } => {
                    assert!(matches!(key, handler::Request::Block(d) if d == digest));
                }
                _ => panic!("expected Deliver message"),
            }

            assert_eq!(
                *call_count.lock().unwrap(),
                3,
                "expected fetch to succeed on the third attempt"
            );
        });
    }

    /// Verifies that a stale completion from an earlier fetch attempt cannot
    /// remove or reschedule a newer fetch for the same key after the original
    /// request was removed and re-fetched.
    #[test_traced]
    fn stale_completion_does_not_mutate_replaced_request() {
        let fixture = TestFixture::new();
        let digest = fixture.create_block(1, 1).digest();

        Runner::default().start(|context| async move {
            let source = MockSource::new();
            let (ingress_tx, _ingress_rx) = mpsc::channel(16);
            let (mut actor, _resolver) = Actor::new(
                context.with_label("resolver"),
                source,
                ingress_tx,
                16,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );

            let key = handler::Request::<Digest>::Block(digest);
            actor.start_fetch(key.clone());
            let Some(State::Active { id: first_id, .. }) = actor.requests.remove(&key) else {
                panic!("expected first fetch attempt to be active");
            };

            actor.start_fetch(key.clone());
            let Some(State::Active { id: second_id, .. }) = actor.requests.get(&key) else {
                panic!("expected second fetch attempt to be active");
            };
            let second_id = *second_id;

            actor.handle_completed(Result {
                key: key.clone(),
                id: first_id,
                retry: true,
            });

            assert!(matches!(
                actor.requests.get(&key),
                Some(State::Active { id, .. }) if *id == second_id
            ));
            assert!(
                actor.retry_schedule.is_empty(),
                "stale completion should not schedule a retry for the replaced request"
            );
        });
    }
}
