//! Shared test utilities for the follower crate (mock source and fixture helpers).

use crate::Source;
use alto_client::consensus::{Message, Payload};
use alto_client::{IndexQuery, Query};
use alto_types::{Block, Context, Finalized, Notarized, Scheme, EPOCH, NAMESPACE};
use commonware_consensus::{
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
use commonware_parallel::Sequential;
use rand::{rngs::StdRng, SeedableRng};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct MockError(pub String);

pub type BlockHandler = Arc<Mutex<Option<Box<dyn Fn(Query) -> Option<Payload> + Send + Sync>>>>;
pub type FinalizedHandler =
    Arc<Mutex<Option<Box<dyn Fn(IndexQuery) -> Option<Finalized> + Send + Sync>>>>;
pub type NotarizedHandler =
    Arc<Mutex<Option<Box<dyn Fn(IndexQuery) -> Option<Notarized> + Send + Sync>>>>;

#[derive(Clone)]
pub struct MockSource {
    pub block_handler: BlockHandler,
    pub finalized_handler: FinalizedHandler,
    pub notarized_handler: NotarizedHandler,
    pub messages: Arc<Mutex<Vec<Message>>>,
}

impl MockSource {
    pub fn new() -> Self {
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

    async fn notarized(&self, query: IndexQuery) -> Result<Notarized, Self::Error> {
        let handler = self.notarized_handler.clone();
        let guard = handler.lock().unwrap();
        match guard.as_ref().and_then(|f| f(query)) {
            Some(notarized) => Ok(notarized),
            None => Err(MockError("notarized not found".to_string())),
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

pub struct TestFixture {
    pub schemes: Vec<Scheme>,
}

impl TestFixture {
    pub fn new() -> Self {
        let mut rng = StdRng::seed_from_u64(0);
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        Self { schemes }
    }

    pub fn create_block(&self, height: u64, view: u64) -> Block {
        let context = Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(view.saturating_sub(1)), sha256::Digest::EMPTY),
        };
        let parent_digest = Sha256::hash(format!("parent-{height}").as_bytes());
        Block::new(context, parent_digest, Height::new(height), height * 100)
    }

    pub fn create_finalized(&self, height: u64, view: u64) -> Finalized {
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

    pub fn create_notarized(&self, height: u64, view: u64) -> Notarized {
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

    pub fn verifier_scheme(&self) -> Scheme {
        let identity = *self.schemes[0].polynomial().public();
        Scheme::certificate_verifier(NAMESPACE, identity)
    }

    pub fn wrong_verifier_scheme(&self) -> Scheme {
        let mut rng = StdRng::seed_from_u64(42);
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        let wrong_identity = *schemes[0].polynomial().public();
        Scheme::certificate_verifier(NAMESPACE, wrong_identity)
    }
}
