use alto_types::{Block, PublicKey, Scheme};
use commonware_consensus::{
    marshal::{ingress::mailbox::AncestorStream, Update},
    simplex::types::Context,
    Block as _, Reporter,
};
use commonware_cryptography::{sha256::Digest, Digestible, Hasher, Sha256};
use commonware_runtime::{Clock, Metrics, Spawner};
use commonware_utils::{Acknowledgement, SystemTimeExt};
use futures::StreamExt;
use rand::Rng;
use std::sync::Arc;
use tracing::info;

/// Genesis message to use during initialization.
const GENESIS: &[u8] = b"commonware is neat";

/// Milliseconds in the future to allow for block timestamps.
const SYNCHRONY_BOUND: u64 = 500;

#[derive(Clone)]
pub struct Application {
    genesis: Arc<Block>,
}

impl Application {
    pub fn new() -> Self {
        let genesis = Block::new(Sha256::hash(GENESIS), 0, 0);
        Self {
            genesis: Arc::new(genesis),
        }
    }
}

impl Default for Application {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> commonware_consensus::Application<E> for Application
where
    E: Rng + Spawner + Metrics + Clock,
{
    type SigningScheme = Scheme;
    type Context = Context<Digest, PublicKey>;
    type Block = Block;

    async fn genesis(&mut self) -> Self::Block {
        self.genesis.as_ref().clone()
    }

    async fn propose(
        &mut self,
        (runtime_context, _context): (E, Self::Context),
        mut ancestry: AncestorStream<Self::SigningScheme, Self::Block>,
    ) -> Option<Self::Block> {
        let parent = ancestry.next().await?;

        // Create a new block
        let mut current = runtime_context.current().epoch_millis();
        if current <= parent.timestamp {
            current = parent.timestamp + 1;
        }

        Some(Block::new(parent.digest(), parent.height + 1, current))
    }
}

impl<E> commonware_consensus::VerifyingApplication<E> for Application
where
    E: Rng + Spawner + Metrics + Clock,
{
    async fn verify(
        &mut self,
        (runtime_context, _): (E, Self::Context),
        mut ancestry: AncestorStream<Self::SigningScheme, Self::Block>,
    ) -> bool {
        let Some(block) = ancestry.next().await else {
            return false;
        };
        let Some(parent) = ancestry.next().await else {
            return false;
        };

        // Verify the block
        if block.timestamp <= parent.timestamp {
            return false;
        }
        let current = runtime_context.current().epoch_millis();
        if block.timestamp > current + SYNCHRONY_BOUND {
            return false;
        }

        // The height and digest invariants are enforced in `Marshaled`:
        // - The block height must be one greater than the parent's height.
        // - The block's parent digest must match the parent's digest.

        true
    }
}

impl Reporter for Application {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        if let Update::Block(block, ack_rx) = activity {
            info!(height = block.height(), "finalized block");
            ack_rx.acknowledge();
        }
    }
}
