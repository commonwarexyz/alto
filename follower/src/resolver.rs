use crate::Source;
use alto_client::{consensus::Payload, IndexQuery, Query};
use bytes::Bytes;
use commonware_actor::{mailbox, Feedback};
use commonware_codec::Encode;
use commonware_consensus::{
    marshal::resolver::handler,
    types::{Height, Round},
};
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest};
use commonware_macros::select_loop;
use commonware_resolver::{
    delivery::{Completion as DeliveryCompletion, Tracker as DeliveryTracker},
    ingress,
    subscribers::Tracker as SubscriberTracker,
    Consumer, Delivery, Fetch,
};
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Metrics, Spawner};
use commonware_utils::{
    futures::{AbortablePool, Aborter},
    vec::NonEmptyVec,
};
use futures::future::{self, Either};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    time::{Duration, SystemTime},
};
use tracing::{debug, trace, warn};

type Key = handler::Key<Digest>;
type Subscriber = handler::Annotation;
type FetchKey = ingress::FetchKey<Key, Subscriber>;
type Message = ingress::Message<Key, Subscriber>;
type RetainPredicate = ingress::Predicate<Key, Subscriber>;

/// Handle to the source-backed resolver actor used by marshal.
#[derive(Clone)]
pub struct Resolver {
    mailbox: mailbox::Sender<Message>,
}

impl commonware_resolver::Resolver for Resolver {
    type Key = Key;
    type Subscriber = Subscriber;
    type PublicKey = PublicKey;

    fn fetch<F>(&mut self, fetch: F) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.send(Message::Fetch(vec![FetchKey::from(fetch.into())]))
    }

    fn fetch_all<F>(&mut self, fetches: Vec<F>) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.send(Message::Fetch(
            fetches
                .into_iter()
                .map(|fetch| FetchKey::from(fetch.into()))
                .collect(),
        ))
    }

    fn fetch_targeted(
        &mut self,
        fetch: impl Into<Fetch<Self::Key, Self::Subscriber>> + Send,
        _targets: NonEmptyVec<Self::PublicKey>,
    ) -> Feedback {
        self.fetch(fetch)
    }

    fn fetch_all_targeted<F>(&mut self, fetches: Vec<(F, NonEmptyVec<Self::PublicKey>)>) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.fetch_all(fetches.into_iter().map(|(fetch, _)| fetch).collect())
    }

    fn retain(
        &mut self,
        predicate: impl Fn(&Self::Key, &Self::Subscriber) -> bool + Send + 'static,
    ) -> Feedback {
        self.send(Message::Retain {
            predicate: Box::new(predicate),
        })
    }
}

impl Resolver {
    /// Submit a resolver message to the actor mailbox and return mailbox feedback.
    fn send(&self, message: Message) -> Feedback {
        self.mailbox.enqueue(message)
    }
}

/// Actor that fetches blocks and certificates from a [Source] on behalf of marshal.
///
/// The [Source] should be constructed without verification because marshal
/// validates all signatures before accepting resolved data. Rejections are
/// logged and retried.
struct Actor<
    E: Clock + Spawner,
    C: Source,
    H: Consumer<Key = Key, Value = Bytes, Subscriber = Subscriber>,
> {
    context: ContextCell<E>,
    client: C,
    mailbox: mailbox::Receiver<Message>,
    // Runs source fetches concurrently. The per-request Attempt owns each
    // aborter, so removing/replacing the Attempt cancels the corresponding
    // fetch future.
    fetches: AbortablePool<FetchCompletion>,
    // Runs marshal delivery validation and caches accepted source bytes for
    // subscribers that arrive while validation is in flight.
    deliveries: DeliveryTracker<H, u64>,
    // Keys with a source fetch, marshal delivery, or retry currently outstanding.
    requests: BTreeMap<Key, Attempt>,
    // Local subscribers still waiting for each key. This is separate from
    // Attempt so retain predicates can prune subscribers without depending on
    // whether the key is fetching, delivering, or scheduled for retry.
    subscribers: SubscriberTracker<Key, Subscriber>,
    // Mirrors Attempt::Scheduled deadlines so the actor can sleep until the next
    // retry without scanning every request.
    retry_schedule: BTreeSet<(SystemTime, Key)>,
    fetch_retry_timeout: Duration,
    next_id: u64,
}

enum Attempt {
    // A source fetch is currently running.
    //
    // The id lets us ignore stale completions from an earlier attempt, and
    // dropping the aborter cancels the current attempt.
    Fetching { id: u64, _aborter: Aborter },
    // Marshal is currently validating a source response.
    Delivering { id: u64 },
    // A retry is queued for the recorded deadline.
    Scheduled(SystemTime),
}

struct FetchCompletion {
    key: Key,
    id: u64,
    result: FetchResult,
}

enum FetchResult {
    // Encoded bytes returned by the Source. Marshal validates the bytes before
    // they are considered accepted.
    Value(Bytes),
    // Any source error or wrong payload type is retried while subscribers remain.
    Retry,
}

pub fn init<E, C>(
    context: E,
    client: C,
    mailbox_size: NonZeroUsize,
    fetch_retry_timeout: Duration,
) -> (handler::Receiver<Digest>, Resolver)
where
    E: Clock + Spawner + Metrics,
    C: Source,
{
    let (handler_rx, handler) = handler::init(context.child("handler"), mailbox_size);
    let (mailbox_tx, mailbox_rx) = mailbox::new(context.child("mailbox"), mailbox_size);
    Actor::new(
        context.child("actor"),
        client,
        mailbox_rx,
        handler,
        fetch_retry_timeout,
    )
    .start();
    (
        handler_rx,
        Resolver {
            mailbox: mailbox_tx,
        },
    )
}

impl<E, C, H> Actor<E, C, H>
where
    E: Clock + Spawner,
    C: Source,
    H: Consumer<Key = Key, Value = Bytes, Subscriber = Subscriber>,
{
    fn new(
        context: E,
        client: C,
        mailbox: mailbox::Receiver<Message>,
        handler: H,
        fetch_retry_timeout: Duration,
    ) -> Self {
        Self {
            context: ContextCell::new(context),
            client,
            mailbox,
            deliveries: DeliveryTracker::new(handler),
            fetches: AbortablePool::default(),
            requests: BTreeMap::new(),
            subscribers: SubscriberTracker::new(),
            retry_schedule: BTreeSet::new(),
            fetch_retry_timeout,
            next_id: 0,
        }
    }

    /// Spawn the resolver actor on its runtime context.
    fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    /// Drive mailbox messages, source fetch completions, delivery completions, and retries.
    async fn run(mut self) {
        select_loop! {
            self.context,
            on_stopped => {},
            // Aborted futures also complete through this pool. They are ignored
            // by the `Ok(...)` pattern and by the id checks below if stale.
            Ok(result) = self.fetches.next_completed() else continue => {
                self.handle_fetch_completed(result);
            },
            delivery = self.deliveries.next_completion() => {
                let delivery = match delivery {
                    Ok(delivery) => delivery,
                    Err(_) => continue,
                };
                self.handle_delivery_completed(delivery);
            },
            _ = match self.retry_schedule.first() {
                Some((deadline, _)) => Either::Left(self.context.sleep_until(*deadline)),
                None => Either::Right(future::pending()),
            } => {
                self.process_retries();
            },
            Some(message) = self.mailbox.recv() else break => {
                self.handle_message(message);
            },
        }
    }

    /// Apply a single actor message to resolver state.
    fn handle_message(&mut self, message: Message) {
        match message {
            Message::Fetch(fetches) => {
                for fetch in fetches {
                    self.add_fetch(fetch);
                }
            }
            Message::Retain { predicate } => self.retain(predicate),
        }
    }

    /// Add subscribers for a key and start the first source fetch if needed.
    fn add_fetch(&mut self, fetch: FetchKey) {
        let FetchKey { key, subscribers } = fetch;
        let is_new = self.subscribers.insert(key, subscribers);

        if is_new {
            assert!(self.deliveries.insert(key), "delivery entry");
            // Insert a scheduled placeholder before starting the fetch so
            // start_fetch can atomically replace it with an active attempt.
            self.requests
                .insert(key, Attempt::Scheduled(self.context.current()));
            self.start_fetch(key);
        }
    }

    /// Prune subscribers that no longer satisfy marshal's retain predicate.
    fn retain(&mut self, predicate: RetainPredicate) {
        for key in self
            .subscribers
            .retain(|key, subscriber| predicate(key, subscriber))
        {
            self.deliveries.remove(&key);
            // Removing an active Attempt drops any source fetch aborter. The
            // delivery tracker cancels marshal validation for delivering keys.
            if let Some(Attempt::Scheduled(deadline)) = self.requests.remove(&key) {
                self.retry_schedule.remove(&(deadline, key));
            }
        }
    }

    /// Start a source fetch attempt for an already-registered key.
    fn start_fetch(&mut self, key: Key) {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let future = Self::fetch(key, id, self.client.clone());
        let aborter = self.fetches.push(future);
        // The aborter is kept for its Drop behavior. Dropping/replacing this
        // Attempt aborts the fetch; dropping it immediately would cancel the
        // newly pushed future.
        self.requests.insert(
            key,
            Attempt::Fetching {
                id,
                _aborter: aborter,
            },
        );
    }

    /// Start delivery of fetched bytes to marshal for the current subscriber batch.
    fn start_delivery(&mut self, key: Key, value: Bytes, delivered: NonEmptyVec<Subscriber>) {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.deliveries.deliver(
            Delivery {
                key,
                subscribers: delivered,
            },
            id,
            value,
        );
        self.requests.insert(key, Attempt::Delivering { id });
    }

    /// Redeliver accepted source bytes to another subscriber batch.
    fn redeliver(&mut self, key: Key, delivered: NonEmptyVec<Subscriber>) {
        self.deliveries.redeliver(Delivery {
            key,
            subscribers: delivered,
        });
    }

    /// Dispatch a source fetch completion after rejecting stale attempt ids.
    fn handle_fetch_completed(&mut self, completion: FetchCompletion) {
        let FetchCompletion { key, id, result } = completion;
        // A completion can race with retain cancellation or a replacement
        // attempt. Only the id currently recorded for the key may mutate state.
        if !self.current_fetch(key, id) {
            return;
        }
        self.handle_fetched(key, result);
    }

    /// Dispatch a marshal delivery completion after rejecting stale attempt ids.
    fn handle_delivery_completed(&mut self, completion: DeliveryCompletion<Key, Subscriber, u64>) {
        let DeliveryCompletion {
            context: id,
            delivery,
            valid,
        } = completion;
        let Delivery {
            key,
            subscribers: delivered,
        } = delivery;
        // Delivery completions are id-checked because retain or retry handling
        // may have replaced the attempt while marshal was validating.
        if !self.current_delivery(key, id) {
            return;
        }
        self.handle_delivered(key, delivered, valid);
    }

    /// Return whether a source fetch completion belongs to the current attempt.
    fn current_fetch(&self, key: Key, id: u64) -> bool {
        let Some(attempt) = self.requests.get(&key) else {
            trace!(?key, id, "ignoring stale fetch completion");
            return false;
        };
        match attempt {
            Attempt::Fetching { id: active_id, .. } if *active_id == id => true,
            Attempt::Fetching { id: active_id, .. } => {
                trace!(
                    ?key,
                    completed_id = id,
                    active_id,
                    "ignoring replaced fetch completion",
                );
                false
            }
            Attempt::Delivering { id: active_id } => {
                trace!(
                    ?key,
                    completed_id = id,
                    active_id,
                    "ignoring fetch completion for delivery attempt",
                );
                false
            }
            Attempt::Scheduled(deadline) => {
                trace!(?key, id, ?deadline, "ignoring scheduled fetch completion",);
                false
            }
        }
    }

    /// Return whether a marshal delivery completion belongs to the current attempt.
    fn current_delivery(&self, key: Key, id: u64) -> bool {
        let Some(attempt) = self.requests.get(&key) else {
            trace!(?key, id, "ignoring stale delivery completion");
            return false;
        };
        match attempt {
            Attempt::Delivering { id: active_id } if *active_id == id => true,
            Attempt::Delivering { id: active_id } => {
                trace!(
                    ?key,
                    completed_id = id,
                    active_id,
                    "ignoring replaced delivery completion",
                );
                false
            }
            Attempt::Fetching { id: active_id, .. } => {
                trace!(
                    ?key,
                    completed_id = id,
                    active_id,
                    "ignoring delivery completion for fetch attempt",
                );
                false
            }
            Attempt::Scheduled(deadline) => {
                trace!(
                    ?key,
                    id,
                    ?deadline,
                    "ignoring scheduled delivery completion",
                );
                false
            }
        }
    }

    /// Transition a completed source lookup into retry, delivery, or cleanup.
    fn handle_fetched(&mut self, key: Key, result: FetchResult) {
        match result {
            FetchResult::Retry => self.schedule_retry(key),
            FetchResult::Value(value) => {
                if let Some(subscribers) = self.subscribers.pending(&key) {
                    // Source data is not accepted until marshal validates it, so
                    // transition into delivery rather than completing the request.
                    self.start_delivery(key, value, subscribers);
                } else {
                    self.requests.remove(&key);
                    self.subscribers.remove(&key);
                    self.deliveries.remove(&key);
                }
            }
        }
    }

    /// Update subscriber state after marshal accepts or rejects a delivery batch.
    fn handle_delivered(&mut self, key: Key, delivered: NonEmptyVec<Subscriber>, valid: bool) {
        let accepted = self.deliveries.response_accepted(&key);

        if valid {
            // Marshal accepted this value for the delivered subscriber set. Remove
            // those subscribers, then redeliver the same value to any subscribers
            // that arrived while validation was in flight.
            let remaining = self.subscribers.remove_delivered(&key, delivered);

            if let Some(subscribers) = remaining {
                if !accepted {
                    self.deliveries.accept_response(&key);
                }
                self.redeliver(key, subscribers);
            } else {
                self.requests.remove(&key);
                self.subscribers.remove(&key);
                self.deliveries.remove(&key);
            }
            return;
        }

        if accepted {
            // The same bytes were already accepted for an earlier subscriber. A
            // later local rejection should not cause an external refetch for data
            // known to be peer-valid, so drop the remaining local demand.
            warn!(
                ?key,
                "previously accepted source resolver response rejected during local redelivery",
            );
            self.requests.remove(&key);
            self.subscribers.remove(&key);
            self.deliveries.remove(&key);
            return;
        }

        warn!(?key, "marshal rejected source resolver delivery");
        self.deliveries.discard_response(&key);
        self.schedule_retry(key);
    }

    /// Queue a retry for a key that still has active local demand.
    fn schedule_retry(&mut self, key: Key) {
        let deadline = self.context.current() + self.fetch_retry_timeout;
        let Some(attempt) = self.requests.get_mut(&key) else {
            return;
        };
        // Replacing a Fetching attempt drops the fetch aborter. Delivery
        // attempts have already completed before they are scheduled for retry.
        *attempt = Attempt::Scheduled(deadline);
        self.retry_schedule.insert((deadline, key));
        debug!(?key, ?deadline, "scheduled source resolver retry");
    }

    /// Start all retry attempts whose deadlines have elapsed.
    fn process_retries(&mut self) {
        let now = self.context.current();
        while let Some((deadline, key)) = self.retry_schedule.pop_first() {
            if deadline > now {
                self.retry_schedule.insert((deadline, key));
                break;
            }

            let Some(state) = self.requests.get(&key) else {
                continue;
            };
            match state {
                Attempt::Scheduled(state_deadline) if *state_deadline == deadline => {
                    debug!(?key, "retrying source resolver fetch");
                    self.start_fetch(key);
                }
                // A stale schedule entry may remain after the request was
                // rescheduled. Active attempts do not need retry work yet.
                Attempt::Scheduled(_) | Attempt::Fetching { .. } | Attempt::Delivering { .. } => {}
            }
        }
    }

    /// Fetch and encode a value from the source for a resolver key.
    async fn fetch(key: Key, id: u64, client: C) -> FetchCompletion {
        // Source calls intentionally only fetch and encode. Signature and chain
        // validation are left to marshal through the delivery tracker.
        let result = match key {
            handler::Key::Block(digest) => Self::fetch_block_by_digest(digest, client).await,
            handler::Key::Finalized { height } => {
                Self::fetch_finalized_by_height(height, client).await
            }
            handler::Key::Notarized { round } => {
                Self::fetch_notarized_by_round(round, client).await
            }
        };
        FetchCompletion { key, id, result }
    }

    /// Fetch and encode a block response by digest.
    async fn fetch_block_by_digest(digest: Digest, client: C) -> FetchResult {
        debug!(?digest, "fetching block by digest");
        match client.block(Query::Digest(digest)).await {
            Ok(Payload::Block(block)) => {
                let value = Bytes::from(block.encode().to_vec());
                FetchResult::Value(value)
            }
            Ok(_) => {
                warn!(?digest, "wrong payload returned for block by digest");
                FetchResult::Retry
            }
            Err(error) => {
                warn!(?digest, ?error, "failed to fetch block by digest");
                FetchResult::Retry
            }
        }
    }

    /// Fetch and encode a finalization plus block by finalized height.
    async fn fetch_finalized_by_height(height: Height, client: C) -> FetchResult {
        debug!(height = height.get(), "fetching finalized block by height");
        match client.block(Query::Index(height.get())).await {
            Ok(Payload::Finalized(finalized)) => {
                let value = Bytes::from(
                    (finalized.proof.clone(), finalized.block.clone())
                        .encode()
                        .to_vec(),
                );
                FetchResult::Value(value)
            }
            Ok(_) => {
                warn!(
                    height = height.get(),
                    "wrong payload returned for finalized block by height"
                );
                FetchResult::Retry
            }
            Err(error) => {
                warn!(
                    height = height.get(),
                    ?error,
                    "failed to fetch finalized block by height"
                );
                FetchResult::Retry
            }
        }
    }

    /// Fetch and encode a notarization plus block by consensus round.
    async fn fetch_notarized_by_round(round: Round, client: C) -> FetchResult {
        let view = round.view().get();
        debug!(view, "fetching notarized block by round");
        match client.notarized(IndexQuery::Index(view)).await {
            Ok(notarized) => {
                let value = Bytes::from(
                    (notarized.proof.clone(), notarized.block.clone())
                        .encode()
                        .to_vec(),
                );
                FetchResult::Value(value)
            }
            Err(error) => {
                warn!(view, ?error, "failed to fetch notarized block by round");
                FetchResult::Retry
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockError, MockSource, TestFixture};
    use alto_client::Query;
    use commonware_cryptography::{ed25519::PrivateKey, Digestible, Signer};
    use commonware_macros::test_traced;
    use commonware_resolver::Resolver as _;
    use commonware_runtime::{deterministic, Clock, Runner as _, Supervisor as _};
    use commonware_utils::{channel::oneshot, non_empty_vec, sync::Mutex, NZUsize};
    use futures::stream;
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
    };

    const DEFAULT_FETCH_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

    struct CapturedDelivery {
        delivery: Delivery<Key, Subscriber>,
        value: Bytes,
        response: oneshot::Sender<bool>,
    }

    #[derive(Clone, Default)]
    struct TestConsumer {
        deliveries: Arc<Mutex<VecDeque<CapturedDelivery>>>,
    }

    impl TestConsumer {
        fn pop(&self) -> Option<CapturedDelivery> {
            self.deliveries.lock().pop_front()
        }

        fn len(&self) -> usize {
            self.deliveries.lock().len()
        }
    }

    impl Consumer for TestConsumer {
        type Key = Key;
        type Value = Bytes;
        type Subscriber = Subscriber;

        fn deliver(
            &mut self,
            delivery: Delivery<Self::Key, Self::Subscriber>,
            value: Self::Value,
        ) -> oneshot::Receiver<bool> {
            let (response, receiver) = oneshot::channel();
            self.deliveries.lock().push_back(CapturedDelivery {
                delivery,
                value,
                response,
            });
            receiver
        }
    }

    struct DropSignal(Arc<Mutex<Option<oneshot::Sender<()>>>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.lock().take() {
                let _ = sender.send(());
            }
        }
    }

    #[derive(Clone)]
    struct BlockingSource {
        started: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        dropped: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    }

    impl BlockingSource {
        fn new() -> (Self, oneshot::Receiver<()>, oneshot::Receiver<()>) {
            let (started_tx, started_rx) = oneshot::channel();
            let (dropped_tx, dropped_rx) = oneshot::channel();
            (
                Self {
                    started: Arc::new(Mutex::new(Some(started_tx))),
                    dropped: Arc::new(Mutex::new(Some(dropped_tx))),
                },
                started_rx,
                dropped_rx,
            )
        }
    }

    impl Source for BlockingSource {
        type Error = MockError;

        async fn health(&self) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn block(&self, _query: Query) -> Result<Payload, Self::Error> {
            if let Some(sender) = self.started.lock().take() {
                let _ = sender.send(());
            }
            let _drop_signal = DropSignal(self.dropped.clone());
            std::future::pending::<Result<Payload, Self::Error>>().await
        }

        async fn notarized(
            &self,
            _query: IndexQuery,
        ) -> Result<alto_types::Notarized, Self::Error> {
            Err(MockError("notarized not supported".to_string()))
        }

        async fn finalized(
            &self,
            _query: IndexQuery,
        ) -> Result<alto_types::Finalized, Self::Error> {
            Err(MockError("finalized not supported".to_string()))
        }

        async fn listen(
            &self,
        ) -> Result<
            impl futures::Stream<Item = Result<alto_client::consensus::Message, Self::Error>>
                + Send
                + Unpin,
            Self::Error,
        > {
            Ok(stream::empty())
        }
    }

    fn start_resolver<C: Source>(
        context: deterministic::Context,
        source: C,
        consumer: TestConsumer,
    ) -> Resolver {
        let (mailbox_tx, mailbox_rx) = mailbox::new(context.child("mailbox"), NZUsize!(16));
        Actor::new(
            context.child("actor"),
            source,
            mailbox_rx,
            consumer,
            DEFAULT_FETCH_RETRY_TIMEOUT,
        )
        .start();
        Resolver {
            mailbox: mailbox_tx,
        }
    }

    async fn wait_for_delivery(
        context: &deterministic::Context,
        consumer: &TestConsumer,
    ) -> CapturedDelivery {
        for _ in 0..50 {
            if let Some(delivery) = consumer.pop() {
                return delivery;
            }
            context.sleep(Duration::from_millis(100)).await;
        }
        panic!("timed out waiting for delivery");
    }

    #[test_traced]
    fn fetches_block_by_digest() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();

        let source = MockSource::new();
        *source.block_handler.lock() = Some(Box::new(move |_| {
            Some(Payload::Block(Box::new(block.clone())))
        }));

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());
            let height = Height::new(1);

            assert!(resolver
                .fetch(handler::Request::certified_block(digest, height))
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;

            assert!(matches!(delivery.delivery.key, handler::Key::Block(d) if d == digest));
            assert!(delivery
                .delivery
                .subscribers
                .contains(&handler::Annotation::Certified { height }));
            assert!(!delivery.value.is_empty());
            delivery.response.send(true).expect("response dropped");
        });
    }

    #[test_traced]
    fn fetches_finalized_by_height_uses_height_indexed_block_query() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(5, 8);
        let height = Height::new(5);
        let block_calls = Arc::new(AtomicU32::new(0));
        let finalized_calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let block_calls = block_calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |query| {
                block_calls.fetch_add(1, Ordering::Relaxed);
                match query {
                    Query::Index(index) if index == height.get() => {
                        Some(Payload::Finalized(Box::new(finalized.clone())))
                    }
                    _ => None,
                }
            }));
        }
        {
            let finalized_calls = finalized_calls.clone();
            *source.finalized_handler.lock() = Some(Box::new(move |_| {
                finalized_calls.fetch_add(1, Ordering::Relaxed);
                None
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver =
                start_resolver(context.child("resolver"), source, consumer.clone());

            assert!(resolver.fetch(handler::Request::finalized(height)).accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            assert!(
                matches!(delivery.delivery.key, handler::Key::Finalized { height: h } if h == height)
            );
            delivery.response.send(true).expect("response dropped");

            assert_eq!(block_calls.load(Ordering::Relaxed), 1);
            assert_eq!(finalized_calls.load(Ordering::Relaxed), 0);
        });
    }

    #[test_traced]
    fn fetches_notarized_by_round() {
        let fixture = TestFixture::new();
        let notarized = fixture.create_notarized(3, 3);
        let round = Round::new(alto_types::EPOCH, commonware_consensus::types::View::new(3));

        let source = MockSource::new();
        *source.notarized_handler.lock() = Some(Box::new(move |_| Some(notarized.clone())));

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());

            assert!(resolver
                .fetch(handler::Request::notarized(round))
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            assert!(
                matches!(delivery.delivery.key, handler::Key::Notarized { round: r } if r == round)
            );
            delivery.response.send(true).expect("response dropped");
        });
    }

    #[test_traced]
    fn retries_when_marshal_rejects_finalized_delivery() {
        let fixture = TestFixture::new();
        let finalized = fixture.create_finalized(1, 1);
        let height = Height::new(1);
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |query| match query {
                Query::Index(index) if index == height.get() => {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Some(Payload::Finalized(Box::new(finalized.clone())))
                }
                _ => None,
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());

            assert!(resolver
                .fetch(handler::Request::finalized(height))
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            delivery.response.send(false).expect("response dropped");

            context
                .sleep(DEFAULT_FETCH_RETRY_TIMEOUT + Duration::from_millis(100))
                .await;
            let retry = wait_for_delivery(&context, &consumer).await;
            assert!(
                matches!(retry.delivery.key, handler::Key::Finalized { height: h } if h == height)
            );
            retry.response.send(true).expect("response dropped");

            assert_eq!(calls.load(Ordering::Relaxed), 2);
        });
    }

    #[test_traced]
    fn retries_when_marshal_rejects_notarized_delivery() {
        let fixture = TestFixture::new();
        let notarized = fixture.create_notarized(3, 3);
        let round = Round::new(alto_types::EPOCH, commonware_consensus::types::View::new(3));
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.notarized_handler.lock() = Some(Box::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(notarized.clone())
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());

            assert!(resolver
                .fetch(handler::Request::notarized(round))
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            delivery.response.send(false).expect("response dropped");

            context
                .sleep(DEFAULT_FETCH_RETRY_TIMEOUT + Duration::from_millis(100))
                .await;
            let retry = wait_for_delivery(&context, &consumer).await;
            assert!(
                matches!(retry.delivery.key, handler::Key::Notarized { round: r } if r == round)
            );
            retry.response.send(true).expect("response dropped");

            assert_eq!(calls.load(Ordering::Relaxed), 2);
        });
    }

    #[test_traced]
    fn deduplicates_identical_subscribers() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(Payload::Block(Box::new(block.clone())))
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());
            let request = handler::Request::certified_block(digest, Height::new(1));

            assert!(resolver.fetch(request).accepted());
            assert!(resolver.fetch(request).accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            assert_eq!(delivery.delivery.subscribers.len().get(), 1);
            delivery.response.send(true).expect("response dropped");
            context.sleep(Duration::from_millis(100)).await;

            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(consumer.len(), 0);
        });
    }

    #[test_traced]
    fn failed_fetch_eventually_resolves_after_multiple_retries() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |_| {
                let attempt = calls.fetch_add(1, Ordering::Relaxed) + 1;
                (attempt >= 3).then(|| Payload::Block(Box::new(block.clone())))
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());

            assert!(resolver
                .fetch(handler::Request::certified_block(digest, Height::new(1)))
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            assert!(matches!(delivery.delivery.key, handler::Key::Block(d) if d == digest));
            delivery.response.send(true).expect("response dropped");

            assert_eq!(calls.load(Ordering::Relaxed), 3);
        });
    }

    #[test_traced]
    fn fetch_during_validation_reuses_response_after_success() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(Payload::Block(Box::new(block.clone())))
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());
            let height = Height::new(1);

            assert!(resolver
                .fetch(handler::Request::certified_block(digest, height))
                .accepted());
            let first = wait_for_delivery(&context, &consumer).await;

            assert!(resolver
                .fetch(handler::Request::finalized_block_by_height(digest, height))
                .accepted());
            context.sleep(Duration::from_millis(100)).await;
            first.response.send(true).expect("response dropped");

            let second = wait_for_delivery(&context, &consumer).await;
            assert!(matches!(second.delivery.key, handler::Key::Block(d) if d == digest));
            assert!(second
                .delivery
                .subscribers
                .contains(&handler::Annotation::Finalized(
                    handler::Finalized::ByHeight { height }
                )));
            second.response.send(true).expect("response dropped");

            context.sleep(Duration::from_millis(100)).await;
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        });
    }

    #[test_traced]
    fn accepted_redelivery_rejection_does_not_refetch() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(Payload::Block(Box::new(block.clone())))
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());
            let height = Height::new(1);

            assert!(resolver
                .fetch(handler::Request::certified_block(digest, height))
                .accepted());
            let first = wait_for_delivery(&context, &consumer).await;

            assert!(resolver
                .fetch(handler::Request::finalized_block_by_height(digest, height))
                .accepted());
            context.sleep(Duration::from_millis(100)).await;
            first.response.send(true).expect("response dropped");

            let second = wait_for_delivery(&context, &consumer).await;
            assert!(matches!(second.delivery.key, handler::Key::Block(d) if d == digest));
            second.response.send(false).expect("response dropped");

            context
                .sleep(DEFAULT_FETCH_RETRY_TIMEOUT + Duration::from_millis(100))
                .await;
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(consumer.len(), 0);
        });
    }

    #[test_traced]
    fn retain_keeps_active_delivery_for_retained_subscriber() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(2, 2);
        let digest = block.digest();
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(Payload::Block(Box::new(block.clone())))
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());
            let subscriber = handler::Annotation::Certified {
                height: Height::new(2),
            };

            assert!(resolver
                .fetch(handler::Request::certified_block(digest, Height::new(2)))
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            assert!(delivery.delivery.subscribers.contains(&subscriber));

            assert!(resolver
                .retain(move |_, candidate| *candidate == subscriber)
                .accepted());
            context.sleep(Duration::from_millis(100)).await;

            delivery.response.send(true).expect("response dropped");
            context.sleep(Duration::from_millis(100)).await;

            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(consumer.len(), 0);
        });
    }

    #[test_traced]
    fn retain_cancels_active_delivery_when_no_subscribers_remain() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(2, 2);
        let digest = block.digest();

        let source = MockSource::new();
        *source.block_handler.lock() = Some(Box::new(move |_| {
            Some(Payload::Block(Box::new(block.clone())))
        }));

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());

            assert!(resolver
                .fetch(handler::Request::certified_block(digest, Height::new(2)))
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;

            assert!(resolver.retain(|_, _| false).accepted());
            context.sleep(Duration::from_millis(100)).await;

            assert!(delivery.response.send(true).is_err());
            assert_eq!(consumer.len(), 0);
        });
    }

    #[test_traced]
    fn retain_cancels_active_fetch_when_no_subscribers_remain() {
        let fixture = TestFixture::new();
        let digest = fixture.create_block(2, 2).digest();

        deterministic::Runner::default().start(|context| async move {
            let (source, started, dropped) = BlockingSource::new();
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());

            assert!(resolver
                .fetch(handler::Request::certified_block(digest, Height::new(2)))
                .accepted());
            started.await.expect("source fetch did not start");

            assert!(resolver.retain(|_, _| false).accepted());
            dropped.await.expect("source fetch was not aborted");

            context.sleep(Duration::from_millis(100)).await;
            assert_eq!(consumer.len(), 0);
        });
    }

    #[test_traced]
    fn retain_removes_unwanted_subscribers() {
        let fixture = TestFixture::new();
        let digest = fixture.create_block(1, 1).digest();

        deterministic::Runner::default().start(|context| async move {
            let source = MockSource::new();
            let consumer = TestConsumer::default();
            let (mailbox_tx, mailbox_rx) = mailbox::new(context.child("mailbox"), NZUsize!(16));
            let mut actor = Actor::new(
                context.child("actor"),
                source,
                mailbox_rx,
                consumer,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );
            let mut resolver = Resolver {
                mailbox: mailbox_tx,
            };
            let keep = handler::Annotation::Certified {
                height: Height::new(2),
            };
            let discard = handler::Annotation::Certified {
                height: Height::new(1),
            };
            let key = handler::Key::Block(digest);
            actor
                .requests
                .insert(key, Attempt::Scheduled(context.current()));
            actor.subscribers.insert(key, non_empty_vec![keep, discard]);

            assert!(resolver
                .retain(move |_, subscriber| *subscriber == keep)
                .accepted());
            let message = actor.mailbox.recv().await.expect("missing retain");
            actor.handle_message(message);

            let subscribers = actor
                .subscribers
                .pending(&key)
                .expect("request should be retained")
                .into_vec();
            assert_eq!(subscribers, vec![keep]);
        });
    }

    #[test_traced]
    fn overflow_retain_prunes_queued_fetches_before_delivery() {
        let fixture = TestFixture::new();
        let ready_digest = fixture.create_block(1, 1).digest();
        let queued_digest = fixture.create_block(2, 2).digest();

        deterministic::Runner::default().start(|context| async move {
            let (mailbox_tx, mut mailbox_rx) = mailbox::new(context.child("mailbox"), NZUsize!(1));
            let mut resolver = Resolver {
                mailbox: mailbox_tx,
            };
            let discard = handler::Annotation::Certified {
                height: Height::new(2),
            };
            let keep = handler::Annotation::Finalized(handler::Finalized::ByHeight {
                height: Height::new(2),
            });

            assert!(resolver
                .fetch(handler::Request::certified_block(
                    ready_digest,
                    Height::new(1)
                ))
                .accepted());
            assert!(resolver
                .fetch(handler::Request::certified_block(
                    queued_digest,
                    Height::new(2)
                ))
                .accepted());
            assert!(resolver
                .fetch(handler::Request::finalized_block_by_height(
                    queued_digest,
                    Height::new(2)
                ))
                .accepted());
            assert!(resolver
                .retain(move |key, subscriber| {
                    !matches!(key, handler::Key::Block(digest) if *digest == queued_digest)
                        || *subscriber == keep
                })
                .accepted());

            let ready = mailbox_rx.recv().await.expect("missing ready fetch");
            let Message::Fetch(fetches) = ready else {
                panic!("expected initial ready fetch");
            };
            assert_eq!(fetches.len(), 1);
            assert!(
                matches!(fetches[0].key, handler::Key::Block(digest) if digest == ready_digest)
            );

            let retained = mailbox_rx.recv().await.expect("missing retained predicate");
            let Message::Retain { .. } = retained else {
                panic!("expected retain to drain before queued fetch");
            };

            let queued = mailbox_rx.recv().await.expect("missing queued fetch");
            let Message::Fetch(fetches) = queued else {
                panic!("expected retained fetch");
            };
            assert_eq!(fetches.len(), 1);
            assert!(
                matches!(fetches[0].key, handler::Key::Block(digest) if digest == queued_digest)
            );
            assert_eq!(fetches[0].subscribers.len().get(), 1);
            assert!(fetches[0].subscribers.contains(&keep));
            assert!(!fetches[0].subscribers.contains(&discard));
        });
    }

    #[test_traced]
    fn stale_completion_does_not_mutate_replaced_request() {
        let fixture = TestFixture::new();
        let digest = fixture.create_block(1, 1).digest();

        deterministic::Runner::default().start(|context| async move {
            let source = MockSource::new();
            let consumer = TestConsumer::default();
            let (_, mailbox_rx) = mailbox::new(context.child("mailbox"), NZUsize!(16));
            let mut actor = Actor::new(
                context.child("actor"),
                source,
                mailbox_rx,
                consumer,
                DEFAULT_FETCH_RETRY_TIMEOUT,
            );

            let key = handler::Key::Block(digest);
            let subscriber = handler::Annotation::Certified {
                height: Height::new(1),
            };
            actor
                .requests
                .insert(key, Attempt::Scheduled(context.current()));
            actor.subscribers.insert(key, non_empty_vec![subscriber]);
            actor.start_fetch(key);
            let first_state = actor.requests.remove(&key).expect("missing first state");
            let Attempt::Fetching { id: first_id, .. } = first_state else {
                panic!("expected first fetch attempt to be active");
            };

            actor
                .requests
                .insert(key, Attempt::Scheduled(context.current()));
            actor.subscribers.insert(key, non_empty_vec![subscriber]);
            actor.start_fetch(key);
            let Some(Attempt::Fetching { id: second_id, .. }) = actor.requests.get(&key) else {
                panic!("expected second fetch attempt to be active");
            };
            let second_id = *second_id;

            actor.handle_fetch_completed(FetchCompletion {
                key,
                id: first_id,
                result: FetchResult::Retry,
            });

            assert!(matches!(
                actor.requests.get(&key),
                Some(Attempt::Fetching { id, .. }) if *id == second_id
            ));
            assert!(actor.retry_schedule.is_empty());
        });
    }

    #[test_traced]
    fn targeted_fetch_variants_use_same_source_path() {
        let fixture = TestFixture::new();
        let block = fixture.create_block(1, 1);
        let digest = block.digest();
        let calls = Arc::new(AtomicU32::new(0));

        let source = MockSource::new();
        {
            let calls = calls.clone();
            *source.block_handler.lock() = Some(Box::new(move |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(Payload::Block(Box::new(block.clone())))
            }));
        }

        deterministic::Runner::default().start(|context| async move {
            let consumer = TestConsumer::default();
            let mut resolver = start_resolver(context.child("resolver"), source, consumer.clone());
            let target = PrivateKey::from_seed(7).public_key();

            assert!(resolver
                .fetch_targeted(
                    handler::Request::certified_block(digest, Height::new(1)),
                    NonEmptyVec::new(target)
                )
                .accepted());
            let delivery = wait_for_delivery(&context, &consumer).await;
            delivery.response.send(true).expect("response dropped");

            assert_eq!(calls.load(Ordering::Relaxed), 1);
        });
    }
}
