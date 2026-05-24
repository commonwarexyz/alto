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
use commonware_resolver::{Consumer, Delivery, Fetch};
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Metrics, Spawner};
use commonware_utils::{
    futures::{AbortablePool, Aborter},
    sync::Mutex,
    vec::NonEmptyVec,
};
use futures::future::{self, Either};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::{debug, trace, warn};

type Key = handler::Key<Digest>;
type Subscriber = handler::Annotation;
type FetchRequest = Fetch<Key, Subscriber>;
type RetainPredicate = Box<dyn Fn(&Key, &Subscriber) -> bool + Send>;
type Subscribers = Arc<Mutex<Vec<Subscriber>>>;

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
        self.send(Message::Fetch(fetch.into()))
    }

    fn fetch_all<F>(&mut self, fetches: Vec<F>) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.send(Message::FetchAll(
            fetches.into_iter().map(Into::into).collect(),
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
        self.send(Message::Retain(Box::new(predicate)))
    }
}

impl Resolver {
    fn send(&self, message: Message) -> Feedback {
        self.mailbox.enqueue(message)
    }
}

enum Message {
    Fetch(FetchRequest),
    FetchAll(Vec<FetchRequest>),
    Retain(RetainPredicate),
}

impl mailbox::Policy for Message {
    type Overflow = VecDeque<Self>;

    fn handle(overflow: &mut Self::Overflow, message: Self) {
        overflow.push_back(message);
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
    handler: H,
    active: AbortablePool<Result>,
    requests: BTreeMap<Key, RequestState>,
    retry_schedule: BTreeSet<(SystemTime, Key)>,
    fetch_retry_timeout: Duration,
    next_id: u64,
}

struct RequestState {
    // Subscribers that keep the request alive, including active deliveries.
    subscribers: Subscribers,
    attempt: Attempt,
}

enum Attempt {
    // A source fetch or marshal delivery is currently running.
    //
    // The id lets us ignore stale completions from an earlier attempt for the
    // same key, and dropping the aborter cancels the current attempt.
    Active { id: u64, _aborter: Aborter },
    // A retry is queued for the recorded deadline.
    Scheduled(SystemTime),
}

struct Result {
    key: Key,
    id: u64,
    retry: bool,
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
            handler,
            active: AbortablePool::default(),
            requests: BTreeMap::new(),
            retry_schedule: BTreeSet::new(),
            fetch_retry_timeout,
            next_id: 0,
        }
    }

    fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    async fn run(mut self) {
        select_loop! {
            self.context,
            on_stopped => {},
            Ok(result) = self.active.next_completed() else continue => {
                self.handle_completed(result);
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

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::Fetch(fetch) => self.add_fetch(fetch),
            Message::FetchAll(fetches) => {
                for fetch in fetches {
                    self.add_fetch(fetch);
                }
            }
            Message::Retain(predicate) => self.retain(predicate),
        }
    }

    fn add_fetch(&mut self, fetch: FetchRequest) {
        let Fetch { key, subscriber } = fetch;
        if let Some(state) = self.requests.get_mut(&key) {
            let mut subscribers = state.subscribers.lock();
            if !subscribers.contains(&subscriber) {
                subscribers.push(subscriber);
            }
            return;
        }

        let subscribers = Arc::new(Mutex::new(vec![subscriber]));
        self.requests.insert(
            key,
            RequestState {
                subscribers,
                attempt: Attempt::Scheduled(self.context.current()),
            },
        );
        self.start_fetch(key);
    }

    fn retain(&mut self, predicate: RetainPredicate) {
        let mut removed = Vec::new();
        for (key, state) in self.requests.iter_mut() {
            let mut subscribers = state.subscribers.lock();
            subscribers.retain(|subscriber| predicate(key, subscriber));
            if subscribers.is_empty() {
                removed.push(*key);
            }
        }

        for key in removed {
            if let Some(state) = self.requests.remove(&key) {
                if let Attempt::Scheduled(deadline) = state.attempt {
                    self.retry_schedule.remove(&(deadline, key));
                }
            }
        }
    }

    fn start_fetch(&mut self, key: Key) {
        let subscribers = self
            .requests
            .get(&key)
            .expect("request missing")
            .subscribers
            .clone();
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let future = Self::process_fetch(
            key,
            id,
            self.client.clone(),
            self.handler.clone(),
            subscribers,
        );
        let aborter = self.active.push(future);
        let state = self.requests.get_mut(&key).expect("request missing");
        state.attempt = Attempt::Active {
            id,
            _aborter: aborter,
        };
    }

    fn handle_completed(&mut self, result: Result) {
        let Some(state) = self.requests.get(&result.key) else {
            trace!(?result.key, id = result.id, "ignoring stale fetch completion");
            return;
        };
        match state.attempt {
            Attempt::Active { id, .. } if id == result.id => {}
            Attempt::Active { id, .. } => {
                trace!(
                    ?result.key,
                    completed_id = result.id,
                    active_id = id,
                    "ignoring replaced fetch completion"
                );
                return;
            }
            Attempt::Scheduled(deadline) => {
                trace!(
                    ?result.key,
                    id = result.id,
                    ?deadline,
                    "ignoring scheduled fetch completion"
                );
                return;
            }
        }

        if result.retry {
            self.schedule_retry(result.key);
        } else if self
            .requests
            .get(&result.key)
            .is_some_and(|state| !state.subscribers.lock().is_empty())
        {
            self.start_fetch(result.key);
        } else {
            self.requests.remove(&result.key);
        }
    }

    fn schedule_retry(&mut self, key: Key) {
        let deadline = self.context.current() + self.fetch_retry_timeout;
        let Some(state) = self.requests.get_mut(&key) else {
            return;
        };
        state.attempt = Attempt::Scheduled(deadline);
        self.retry_schedule.insert((deadline, key));
        debug!(?key, ?deadline, "scheduled source resolver retry");
    }

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
            match state.attempt {
                Attempt::Scheduled(state_deadline) if state_deadline == deadline => {
                    debug!(?key, "retrying source resolver fetch");
                    self.start_fetch(key);
                }
                Attempt::Scheduled(_) | Attempt::Active { .. } => {}
            }
        }
    }

    async fn process_fetch(
        key: Key,
        id: u64,
        client: C,
        handler: H,
        subscribers: Subscribers,
    ) -> Result {
        let retry = match key {
            handler::Key::Block(digest) => {
                Self::fetch_block_by_digest(key, digest, client, handler, subscribers).await
            }
            handler::Key::Finalized { height } => {
                Self::fetch_finalized_by_height(key, height, client, handler, subscribers).await
            }
            handler::Key::Notarized { round } => {
                Self::fetch_notarized_by_round(key, round, client, handler, subscribers).await
            }
        };
        Result { key, id, retry }
    }

    async fn fetch_block_by_digest(
        key: Key,
        digest: Digest,
        client: C,
        handler: H,
        subscribers: Subscribers,
    ) -> bool {
        debug!(?digest, "fetching block by digest");
        match client.block(Query::Digest(digest)).await {
            Ok(Payload::Block(block)) => {
                let value = Bytes::from(block.encode().to_vec());
                Self::deliver(key, value, handler, subscribers).await
            }
            Ok(_) => {
                warn!(?digest, "wrong payload returned for block by digest");
                true
            }
            Err(error) => {
                warn!(?digest, ?error, "failed to fetch block by digest");
                true
            }
        }
    }

    async fn fetch_finalized_by_height(
        key: Key,
        height: Height,
        client: C,
        handler: H,
        subscribers: Subscribers,
    ) -> bool {
        debug!(height = height.get(), "fetching finalized block by height");
        match client.block(Query::Index(height.get())).await {
            Ok(Payload::Finalized(finalized)) => {
                let value = Bytes::from(
                    (finalized.proof.clone(), finalized.block.clone())
                        .encode()
                        .to_vec(),
                );
                Self::deliver(key, value, handler, subscribers).await
            }
            Ok(_) => {
                warn!(
                    height = height.get(),
                    "wrong payload returned for finalized block by height"
                );
                true
            }
            Err(error) => {
                warn!(
                    height = height.get(),
                    ?error,
                    "failed to fetch finalized block by height"
                );
                true
            }
        }
    }

    async fn fetch_notarized_by_round(
        key: Key,
        round: Round,
        client: C,
        handler: H,
        subscribers: Subscribers,
    ) -> bool {
        let view = round.view().get();
        debug!(view, "fetching notarized block by round");
        match client.notarized(IndexQuery::Index(view)).await {
            Ok(notarized) => {
                let value = Bytes::from(
                    (notarized.proof.clone(), notarized.block.clone())
                        .encode()
                        .to_vec(),
                );
                Self::deliver(key, value, handler, subscribers).await
            }
            Err(error) => {
                warn!(view, ?error, "failed to fetch notarized block by round");
                true
            }
        }
    }

    async fn deliver(key: Key, value: Bytes, mut handler: H, subscribers: Subscribers) -> bool {
        loop {
            let pending = {
                let subscribers = subscribers.lock();
                NonEmptyVec::try_from(subscribers.clone()).ok()
            };
            let Some(delivered) = pending else {
                return false;
            };
            let response = handler.deliver(
                Delivery {
                    key,
                    subscribers: delivered.clone(),
                },
                value.clone(),
            );
            match response.await {
                Ok(true) => {
                    let mut pending = subscribers.lock();
                    pending.retain(|subscriber| !delivered.contains(subscriber));
                    if pending.is_empty() {
                        return false;
                    }
                }
                Ok(false) => {
                    warn!(?key, "marshal rejected source resolver delivery");
                    return true;
                }
                Err(error) => {
                    warn!(
                        ?key,
                        ?error,
                        "marshal dropped source resolver delivery response"
                    );
                    return true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockSource, TestFixture};
    use alto_client::Query;
    use commonware_cryptography::{ed25519::PrivateKey, Digestible, Signer};
    use commonware_macros::test_traced;
    use commonware_resolver::Resolver as _;
    use commonware_runtime::{deterministic, Clock, Runner as _, Supervisor as _};
    use commonware_utils::{channel::oneshot, NZUsize};
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
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

    fn start_resolver(
        context: deterministic::Context,
        source: MockSource,
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
            actor.requests.insert(
                key,
                RequestState {
                    subscribers: Arc::new(Mutex::new(vec![keep, discard])),
                    attempt: Attempt::Scheduled(context.current()),
                },
            );

            assert!(resolver
                .retain(move |_, subscriber| *subscriber == keep)
                .accepted());
            let message = actor.mailbox.recv().await.expect("missing retain");
            actor.handle_message(message);

            let subscribers = actor
                .requests
                .get(&key)
                .expect("request should be retained")
                .subscribers
                .lock()
                .clone();
            assert_eq!(subscribers, vec![keep]);
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
            actor.requests.insert(
                key,
                RequestState {
                    subscribers: Arc::new(Mutex::new(vec![subscriber])),
                    attempt: Attempt::Scheduled(context.current()),
                },
            );
            actor.start_fetch(key);
            let first_state = actor.requests.remove(&key).expect("missing first state");
            let Attempt::Active { id: first_id, .. } = first_state.attempt else {
                panic!("expected first fetch attempt to be active");
            };

            actor.requests.insert(
                key,
                RequestState {
                    subscribers: Arc::new(Mutex::new(vec![subscriber])),
                    attempt: Attempt::Scheduled(context.current()),
                },
            );
            actor.start_fetch(key);
            let Some(RequestState {
                attempt: Attempt::Active { id: second_id, .. },
                ..
            }) = actor.requests.get(&key)
            else {
                panic!("expected second fetch attempt to be active");
            };
            let second_id = *second_id;

            actor.handle_completed(Result {
                key,
                id: first_id,
                retry: true,
            });

            assert!(matches!(
                actor.requests.get(&key),
                Some(RequestState {
                    attempt: Attempt::Active { id, .. },
                    ..
                }) if *id == second_id
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
