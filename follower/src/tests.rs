use crate::feeder::{CertificateFeeder, FeederError};
use crate::resolver::HttpResolverActor;
use crate::Source;
use alto_client::consensus::{Message, Payload};
use alto_client::{IndexQuery, Query};
use alto_types::{Block, Context, Finalized, Notarized, Scheme, EPOCH, NAMESPACE};
use commonware_consensus::{
    marshal::ingress::handler,
    simplex::{
        scheme::bls12381_threshold::vrf as bls12381_threshold,
        types::{Finalize, Notarize, Proposal},
    },
    types::{Height, Round, View},
};
use commonware_cryptography::{
    bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, ed25519, sha256, Digest,
    Digestible, Hasher, Sha256, Signer,
};
use commonware_macros::test_traced;
use commonware_parallel::Sequential;
use commonware_resolver::Resolver;
use commonware_runtime::{deterministic::Runner, Clock, Runner as _, Spawner};
use commonware_utils::channel::mpsc;
use futures::StreamExt;
use rand::{rngs::StdRng, SeedableRng};
use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug)]
struct MockError(String);

impl fmt::Display for MockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MockError {}

type BlockHandler = Arc<Mutex<Option<Box<dyn Fn(Query) -> Option<Payload> + Send + Sync>>>>;
type FinalizedHandler =
    Arc<Mutex<Option<Box<dyn Fn(IndexQuery) -> Option<Finalized> + Send + Sync>>>>;
type NotarizedHandler =
    Arc<Mutex<Option<Box<dyn Fn(IndexQuery) -> Option<Notarized> + Send + Sync>>>>;

#[derive(Clone)]
struct MockSource {
    block_handler: BlockHandler,
    finalized_handler: FinalizedHandler,
    notarized_handler: NotarizedHandler,
    messages: Arc<Mutex<Vec<Message>>>,
}

impl MockSource {
    fn new() -> Self {
        Self {
            block_handler: Arc::new(Mutex::new(None)),
            finalized_handler: Arc::new(Mutex::new(None)),
            notarized_handler: Arc::new(Mutex::new(None)),
            messages: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Source for MockSource {
    type Error = MockError;

    async fn health(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn block(&self, query: Query) -> Result<Payload, Self::Error> {
        let handler = self.block_handler.clone();
        let guard = handler.lock().unwrap();
        match guard.as_ref().and_then(|f| f(query)) {
            Some(payload) => Ok(payload),
            None => Err(MockError("block not found".to_string())),
        }
    }

    async fn finalized(&self, query: IndexQuery) -> Result<Finalized, Self::Error> {
        let handler = self.finalized_handler.clone();
        let guard = handler.lock().unwrap();
        match guard.as_ref().and_then(|f| f(query)) {
            Some(finalized) => Ok(finalized),
            None => Err(MockError("finalized not found".to_string())),
        }
    }

    async fn notarized_get(&self, query: IndexQuery) -> Result<Notarized, Self::Error> {
        let handler = self.notarized_handler.clone();
        let guard = handler.lock().unwrap();
        match guard.as_ref().and_then(|f| f(query)) {
            Some(notarized) => Ok(notarized),
            None => Err(MockError("notarized not found".to_string())),
        }
    }

    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl futures::Stream<Item = Result<Message, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send {
        let messages = self.messages.clone();
        async move {
            let msgs = messages.lock().unwrap().drain(..).collect::<Vec<_>>();
            Ok(futures::stream::iter(msgs.into_iter().map(Ok)))
        }
    }
}

struct TestFixture {
    schemes: Vec<Scheme>,
}

impl TestFixture {
    fn new() -> Self {
        let mut rng = StdRng::seed_from_u64(0);
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        Self { schemes }
    }

    fn create_block(&self, height: u64, view: u64) -> Block {
        let context = Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(view.saturating_sub(1)), sha256::Digest::EMPTY),
        };
        let parent_digest = Sha256::hash(format!("parent-{height}").as_bytes());
        Block::new(context, parent_digest, Height::new(height), height * 100)
    }

    fn create_finalized(&self, height: u64, view: u64) -> Finalized {
        let block = self.create_block(height, view);
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(view)),
            View::new(view.saturating_sub(1)),
            block.digest(),
        );
        let finalizes: Vec<_> = self
            .schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let finalization =
            alto_types::Finalization::from_finalizes(&self.schemes[0], &finalizes, &Sequential)
                .unwrap();
        Finalized::new(finalization, block)
    }

    fn create_notarized(&self, height: u64, view: u64) -> Notarized {
        let block = self.create_block(height, view);
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(view)),
            View::new(view.saturating_sub(1)),
            block.digest(),
        );
        let notarizes: Vec<_> = self
            .schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let notarization =
            alto_types::Notarization::from_notarizes(&self.schemes[0], &notarizes, &Sequential)
                .unwrap();
        Notarized::new(notarization, block)
    }

    fn verifier_scheme(&self) -> Scheme {
        let identity = *self.schemes[0].polynomial().public();
        Scheme::certificate_verifier(NAMESPACE, identity)
    }

    fn wrong_verifier_scheme(&self) -> Scheme {
        let mut rng = StdRng::seed_from_u64(42);
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        let wrong_identity = *schemes[0].polynomial().public();
        Scheme::certificate_verifier(NAMESPACE, wrong_identity)
    }
}

#[test_traced]
fn resolver_fetch_cancel_clear_retain() {
    Runner::default().start(|context| async move {
        let source = MockSource::new();
        let (ingress_tx, _ingress_rx) = mpsc::channel(16);
        let (actor, mut resolver) = HttpResolverActor::new(source, ingress_tx, 16);

        let _actor_handle = context.clone().spawn(|_| actor.run());

        let key = handler::Request::<Block>::Finalized {
            height: Height::new(1),
        };

        resolver.fetch(key.clone()).await;
        resolver.cancel(key).await;
        resolver.clear().await;
        resolver.retain(|_| true).await;

        context.sleep(Duration::from_millis(100)).await;
    });
}

#[test_traced]
fn resolver_actor_fetches_block_by_digest() {
    let fixture = TestFixture::new();
    let block = fixture.create_block(1, 1);
    let digest = block.digest();

    let source = MockSource::new();
    *source.block_handler.lock().unwrap() = Some(Box::new(move |_| {
        Some(Payload::Block(Box::new(block.clone())))
    }));

    Runner::default().start(|context| async move {
        let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
        let (actor, mut resolver) = HttpResolverActor::new(source, ingress_tx, 16);

        let _actor_handle = context.clone().spawn(|_| actor.run());

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

#[test_traced]
fn resolver_actor_fetches_finalized_by_height() {
    let fixture = TestFixture::new();
    let finalized = fixture.create_finalized(5, 5);
    let height = Height::new(5);

    let source = MockSource::new();
    *source.block_handler.lock().unwrap() = Some(Box::new(move |_| {
        Some(Payload::Finalized(Box::new(finalized.clone())))
    }));

    Runner::default().start(|context| async move {
        let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
        let (actor, mut resolver) = HttpResolverActor::new(source, ingress_tx, 16);

        let _actor_handle = context.clone().spawn(|_| actor.run());

        resolver.fetch(handler::Request::Finalized { height }).await;

        let msg = ingress_rx.recv().await.unwrap();
        match msg {
            handler::Message::Deliver { key, .. } => {
                assert!(matches!(key, handler::Request::Finalized { height: h } if h == height));
            }
            _ => panic!("expected Deliver message"),
        }
    });
}

#[test_traced]
fn resolver_actor_fetches_notarized_by_round() {
    let fixture = TestFixture::new();
    let notarized = fixture.create_notarized(3, 3);
    let round = Round::new(EPOCH, View::new(3));

    let source = MockSource::new();
    *source.notarized_handler.lock().unwrap() = Some(Box::new(move |_| Some(notarized.clone())));

    Runner::default().start(|context| async move {
        let (ingress_tx, mut ingress_rx) = mpsc::channel(16);
        let (actor, mut resolver) = HttpResolverActor::new(source, ingress_tx, 16);

        let _actor_handle = context.clone().spawn(|_| actor.run());

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

#[test_traced]
fn resolver_actor_dedup() {
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
        let (actor, mut resolver) = HttpResolverActor::new(source, ingress_tx, 16);

        let _actor_handle = context.clone().spawn(|_| actor.run());

        let key = handler::Request::<Block>::Block(digest);
        resolver.fetch(key.clone()).await;
        resolver.fetch(key).await;

        let _msg = ingress_rx.recv().await.unwrap();
        context.sleep(Duration::from_millis(100)).await;

        assert_eq!(*call_count.lock().unwrap(), 1);
    });
}

#[test_traced]
fn finalization_verify_valid() {
    let fixture = TestFixture::new();
    let finalized = fixture.create_finalized(5, 5);
    let verifier = fixture.verifier_scheme();
    assert!(finalized.verify(&verifier, &Sequential));
}

#[test_traced]
fn finalization_verify_invalid_scheme() {
    let fixture = TestFixture::new();
    let finalized = fixture.create_finalized(5, 5);
    let wrong_verifier = fixture.wrong_verifier_scheme();
    assert!(!finalized.verify(&wrong_verifier, &Sequential));
}

#[test_traced]
fn source_mock_health() {
    let source = MockSource::new();
    Runner::default().start(|_| async move {
        assert!(source.health().await.is_ok());
    });
}

#[test_traced]
fn source_mock_listen() {
    let fixture = TestFixture::new();
    let finalized = fixture.create_finalized(1, 1);

    let source = MockSource::new();
    source
        .messages
        .lock()
        .unwrap()
        .push(Message::Finalization(finalized));

    Runner::default().start(|_| async move {
        let mut stream = source.listen().await.unwrap();
        let msg = stream.next().await.unwrap().unwrap();
        assert!(matches!(msg, Message::Finalization(_)));
        assert!(stream.next().await.is_none());
    });
}

#[test_traced]
fn feeder_accepts_valid_finalization() {
    let fixture = TestFixture::new();
    let finalized = fixture.create_finalized(1, 1);
    let verifier = fixture.verifier_scheme();

    Runner::default().start(|context| async move {
        let engine = crate::engine::Engine::new(context.clone(), verifier.clone(), 16).await;
        let marshal_mailbox = engine.mailbox();
        let engine_buffer = engine.buffer();

        let source = MockSource::new();
        let mut feeder = CertificateFeeder::new(
            context.clone(),
            source,
            verifier,
            marshal_mailbox,
            engine_buffer,
        );

        let result = feeder
            .handle_message(Message::Finalization(finalized))
            .await;
        assert!(result.is_ok());
    });
}

#[test_traced]
fn feeder_rejects_invalid_finalization() {
    let fixture = TestFixture::new();
    let finalized = fixture.create_finalized(1, 1);
    let wrong_verifier = fixture.wrong_verifier_scheme();

    Runner::default().start(|context| async move {
        let engine = crate::engine::Engine::new(context.clone(), wrong_verifier.clone(), 16).await;
        let marshal_mailbox = engine.mailbox();
        let engine_buffer = engine.buffer();

        let source = MockSource::new();
        let mut feeder = CertificateFeeder::new(
            context.clone(),
            source,
            wrong_verifier,
            marshal_mailbox,
            engine_buffer,
        );

        let result = feeder
            .handle_message(Message::Finalization(finalized))
            .await;
        assert!(matches!(
            result,
            Err(FeederError::InvalidSignature { height: 1 })
        ));
    });
}

#[test_traced]
fn feeder_ignores_notarization() {
    let fixture = TestFixture::new();
    let notarized = fixture.create_notarized(1, 1);
    let verifier = fixture.verifier_scheme();

    Runner::default().start(|context| async move {
        let engine = crate::engine::Engine::new(context.clone(), verifier.clone(), 16).await;
        let marshal_mailbox = engine.mailbox();
        let engine_buffer = engine.buffer();

        let source = MockSource::new();
        let mut feeder = CertificateFeeder::new(
            context.clone(),
            source,
            verifier,
            marshal_mailbox,
            engine_buffer,
        );

        let result = feeder
            .handle_message(Message::Notarization(notarized))
            .await;
        assert!(result.is_ok());
    });
}
