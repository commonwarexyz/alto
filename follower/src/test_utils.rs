//! Shared test utilities for the follower crate (mock source and fixture helpers).

use crate::Source;
use alto_client::consensus::{Message, Payload};
use alto_client::{IndexQuery, Query};
use alto_types::{
    Block, ConsensusScheme, Context, Finalized, Identity, Notarized, PrivateKey, Scheme, EPOCH,
    NAMESPACE,
};
use bytes::Bytes;
use commonware_consensus::{
    simplex::types::{Finalize, Notarize, Proposal},
    types::{Height, Round, View},
};
use commonware_cryptography::{sha256, Digest, Digestible, Hasher, Sha256, Signer};
use commonware_parallel::Sequential;
use commonware_utils::{
    non_empty,
    ordered::{BiMap, Set},
    sync::Mutex,
};
use std::{future::Future, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct MockError(pub String);

pub type BlockHandler =
    Arc<Mutex<Option<Box<dyn Fn(Query) -> Option<Payload<ConsensusScheme>> + Send + Sync>>>>;
pub type NotarizedHandler =
    Arc<Mutex<Option<Box<dyn Fn(IndexQuery) -> Option<Notarized<ConsensusScheme>> + Send + Sync>>>>;

#[derive(Clone)]
pub struct MockSource {
    pub block_handler: BlockHandler,
    pub notarized_handler: NotarizedHandler,
    pub messages: Arc<Mutex<Vec<Message<ConsensusScheme>>>>,
}

impl MockSource {
    pub fn new() -> Self {
        Self {
            block_handler: Arc::new(Mutex::new(None)),
            notarized_handler: Arc::new(Mutex::new(None)),
            messages: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Source for MockSource {
    type Scheme = ConsensusScheme;
    type Error = MockError;

    async fn block(&self, query: Query) -> Result<Payload<ConsensusScheme>, Self::Error> {
        let handler = self.block_handler.clone();
        let guard = handler.lock();
        match guard.as_ref().and_then(|f| f(query)) {
            Some(payload) => Ok(payload),
            None => Err(MockError("block not found".to_string())),
        }
    }

    async fn notarized(
        &self,
        query: IndexQuery,
    ) -> Result<Notarized<ConsensusScheme>, Self::Error> {
        let handler = self.notarized_handler.clone();
        let guard = handler.lock();
        match guard.as_ref().and_then(|f| f(query)) {
            Some(notarized) => Ok(notarized),
            None => Err(MockError("notarized not found".to_string())),
        }
    }

    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl futures::Stream<Item = Result<Message<ConsensusScheme>, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send {
        let messages = self.messages.clone();
        async move {
            let msgs = messages.lock().drain(..).collect::<Vec<_>>();
            Ok(futures::stream::iter(msgs.into_iter().map(Ok)))
        }
    }
}

/// Returns the participant set of four validators whose keys are seeded from `first`.
fn participants(first: u64) -> (Vec<PrivateKey>, Identity) {
    let keys: Vec<_> = (first..first + 4).map(PrivateKey::from_seed).collect();
    let identity = Set::from_iter_dedup(keys.iter().map(|key| key.public_key()));
    (keys, identity)
}

pub struct TestFixture {
    pub schemes: Vec<ConsensusScheme>,
    identity: Identity,
}

impl TestFixture {
    pub fn new() -> Self {
        let (keys, identity) = participants(0);
        let signers = BiMap::try_from(
            identity
                .iter()
                .map(|key| (key.clone(), key.clone()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let schemes = keys
            .into_iter()
            .map(|key| ConsensusScheme::signer(NAMESPACE, signers.clone(), key).unwrap())
            .collect();
        Self { schemes, identity }
    }

    pub fn create_block(&self, height: u64, view: u64) -> Block {
        let context = Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: PrivateKey::from_seed(0).public_key(),
            parent: (View::new(view.saturating_sub(1)), sha256::Digest::EMPTY),
        };
        let parent_digest = Sha256::hash(&[format!("parent-{height}").as_bytes()]);
        Block::new(
            context,
            parent_digest,
            Height::new(height),
            height * 100,
            Bytes::new(),
        )
    }

    pub fn create_finalized(&self, height: u64, view: u64) -> Finalized<ConsensusScheme> {
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
        let finalization = alto_types::Finalization::from_finalizes(
            &self.schemes[0],
            non_empty![@&finalizes],
            &Sequential,
        )
        .unwrap();
        Finalized::new(finalization, block)
    }

    pub fn create_notarized(&self, height: u64, view: u64) -> Notarized<ConsensusScheme> {
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
        let notarization = alto_types::Notarization::from_notarizes(
            &self.schemes[0],
            non_empty![@&notarizes],
            &Sequential,
        )
        .unwrap();
        Notarized::new(notarization, block)
    }

    pub fn verifier_scheme(&self) -> ConsensusScheme {
        ConsensusScheme::certificate_verifier(NAMESPACE, self.identity.clone())
    }

    pub fn wrong_verifier_scheme(&self) -> ConsensusScheme {
        let (_, wrong_identity) = participants(42);
        ConsensusScheme::certificate_verifier(NAMESPACE, wrong_identity)
    }
}
