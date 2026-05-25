use crate::Source;
use alto_client::{consensus::Payload, IndexQuery, Query};
use bytes::Bytes;
use commonware_codec::Encode;
use commonware_consensus::{
    marshal::resolver::handler,
    types::{Height, Round},
};
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest};
use commonware_resolver::opaque;
use commonware_runtime::{Clock, Metrics, Spawner};
use std::{future::Future, num::NonZeroUsize, time::Duration};
use tracing::{debug, warn};

type Key = handler::Key<Digest>;
type Subscriber = handler::Annotation;
pub type Resolver = opaque::Resolver<Key, Subscriber, PublicKey>;

/// Start the follower resolver and marshal handler backed by `client`.
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
    let resolver = opaque::init::<_, _, _, PublicKey>(
        context.child("resolver"),
        Fetcher::new(client),
        handler,
        mailbox_size,
        fetch_retry_timeout,
    );
    (handler_rx, resolver)
}

/// Fetches and encodes marshal resolver payloads from an Alto source client.
#[derive(Clone)]
struct Fetcher<C>(C);

impl<C> Fetcher<C> {
    const fn new(client: C) -> Self {
        Self(client)
    }
}

impl<C> opaque::Fetcher for Fetcher<C>
where
    C: Source,
{
    type Key = Key;
    type Value = Bytes;

    fn fetch(&self, key: Self::Key) -> impl Future<Output = Option<Self::Value>> + Send {
        let client = self.0.clone();
        async move {
            match key {
                handler::Key::Block(digest) => Self::fetch_block_by_digest(digest, client).await,
                handler::Key::Finalized { height } => {
                    Self::fetch_finalized_by_height(height, client).await
                }
                handler::Key::Notarized { round } => {
                    Self::fetch_notarized_by_round(round, client).await
                }
            }
        }
    }
}

impl<C> Fetcher<C>
where
    C: Source,
{
    /// Fetch and encode a block response by digest.
    async fn fetch_block_by_digest(digest: Digest, client: C) -> Option<Bytes> {
        debug!(?digest, "fetching block by digest");
        match client.block(Query::Digest(digest)).await {
            Ok(Payload::Block(block)) => Some(Bytes::from(block.encode().to_vec())),
            Ok(_) => {
                warn!(?digest, "wrong payload returned for block by digest");
                None
            }
            Err(error) => {
                warn!(?digest, ?error, "failed to fetch block by digest");
                None
            }
        }
    }

    /// Fetch and encode a finalization plus block by finalized height.
    async fn fetch_finalized_by_height(height: Height, client: C) -> Option<Bytes> {
        debug!(height = height.get(), "fetching finalized block by height");
        match client.block(Query::Index(height.get())).await {
            Ok(Payload::Finalized(finalized)) => Some(Bytes::from(
                (finalized.proof.clone(), finalized.block.clone())
                    .encode()
                    .to_vec(),
            )),
            Ok(_) => {
                warn!(
                    height = height.get(),
                    "wrong payload returned for finalized block by height"
                );
                None
            }
            Err(error) => {
                warn!(
                    height = height.get(),
                    ?error,
                    "failed to fetch finalized block by height"
                );
                None
            }
        }
    }

    /// Fetch and encode a notarization plus block by consensus round.
    async fn fetch_notarized_by_round(round: Round, client: C) -> Option<Bytes> {
        let view = round.view().get();
        debug!(view, "fetching notarized block by round");
        match client.notarized(IndexQuery::Index(view)).await {
            Ok(notarized) => Some(Bytes::from(
                (notarized.proof.clone(), notarized.block.clone())
                    .encode()
                    .to_vec(),
            )),
            Err(error) => {
                warn!(view, ?error, "failed to fetch notarized block by round");
                None
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
    use commonware_resolver::{Consumer, Delivery, Resolver as _, TargetedResolver as _};
    use commonware_runtime::{deterministic, Clock, Runner as _, Supervisor as _};
    use commonware_utils::{channel::oneshot, sync::Mutex, vec::NonEmptyVec, NZUsize};
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
        opaque::init::<_, _, _, PublicKey>(
            context,
            Fetcher::new(source),
            consumer,
            NZUsize!(16),
            DEFAULT_FETCH_RETRY_TIMEOUT,
        )
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
