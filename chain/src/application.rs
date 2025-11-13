use alto_types::{Block, PublicKey, Scheme};
use commonware_consensus::{
    marshal::{
        ancestry::{AncestorStream, AncestryProvider},
        coding::types::CodingCommitment,
        Update,
    },
    simplex::types::Context,
    Application, Block as _, Reporter,
};
use commonware_cryptography::{Digestible, Hasher, Sha256};
use commonware_runtime::{Clock, Metrics, Spawner};
use commonware_utils::{SystemTimeExt, Acknowledgement};
use futures::StreamExt;
use rand::Rng;
use tracing::info;

/// Genesis message to use during initialization.
const GENESIS: &[u8] = b"commonware is neat";

#[derive(Clone)]
pub struct AltoApp {
    genesis: Block,
}

impl AltoApp {
    pub fn new() -> Self {
        let genesis = Block::new(Sha256::hash(GENESIS), 0, 0, Vec::new());
        Self { genesis }
    }
}

impl Default for AltoApp {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Application<E> for AltoApp
where
    E: Rng + Spawner + Metrics + Clock,
{
    type SigningScheme = Scheme;
    type Context = Context<CodingCommitment, PublicKey>;
    type Block = Block;

    async fn genesis(&mut self) -> Self::Block {
        self.genesis.clone()
    }

    async fn propose<A: AncestryProvider<Block = Self::Block>>(
        &mut self,
        (mut runtime_context, _context): (E, Self::Context),
        mut ancestry: AncestorStream<A, Self::Block>,
    ) -> Option<Self::Block> {
        let parent = ancestry.next().await?;

        // Create a new block
        let mut current = runtime_context.current().epoch_millis();
        if current <= parent.timestamp {
            current = parent.timestamp + 1;
        }

        // Generate some random data.
        let mut junk = vec![0u8; 8 * 1024 * 1024];
        runtime_context.fill_bytes(&mut junk);

        Some(Block::new(
            parent.digest(),
            parent.height + 1,
            current,
            junk,
        ))
    }
}

impl Reporter for AltoApp {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        if let Update::Block(block, ack_rx) = activity {
            info!(height = block.height(), "finalized block");
            ack_rx.acknowledge();
        }
    }
}
